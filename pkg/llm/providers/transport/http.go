// Package transport supplies shared bounded HTTP transport for model adapters.
package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

type Options struct {
	BaseURL        string
	APIKey         string
	AllowHTTP      bool
	HTTPClient     *http.Client
	ConnectTimeout time.Duration
	HeaderTimeout  time.Duration
	IdleTimeout    time.Duration
}
type Client struct {
	options Options
	format  string
	http    *http.Client
}

func New(format string, o Options) (*Client, error) {
	if format != "openai" && format != "anthropic" {
		return nil, llm.Invalid("unknown provider format")
	}
	if o.APIKey == "" {
		return nil, llm.Invalid("provider API key is required")
	}
	u, e := url.Parse(o.BaseURL)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, llm.Invalid("invalid provider base URL")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		loopback := u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
		if u.Scheme != "http" || !o.AllowHTTP || !loopback {
			return nil, llm.Invalid("provider requires HTTPS; HTTP is only allowed for explicit loopback development")
		}
	}
	o.BaseURL = strings.TrimRight(o.BaseURL, "/")
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 3 * time.Second
	}
	if o.HeaderTimeout <= 0 {
		o.HeaderTimeout = 10 * time.Second
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 30 * time.Second
	}
	var client http.Client
	if o.HTTPClient != nil {
		client = *o.HTTPClient
	} else {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.DialContext = (&net.Dialer{Timeout: o.ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
		tr.TLSHandshakeTimeout = o.ConnectTimeout
		tr.ResponseHeaderTimeout = o.HeaderTimeout
		tr.MaxIdleConnsPerHost = 16
		client.Transport = tr
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{options: o, format: format, http: &client}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }
func (c *Client) Generate(ctx context.Context, r llm.Request) (llm.Response, error) {
	if e := r.Validate(); e != nil {
		return llm.Response{}, e
	}
	body, cleanup, e := c.post(ctx, r, false)
	if e != nil {
		return llm.Response{}, e
	}
	defer cleanup()
	data, e := io.ReadAll(io.LimitReader(body, (2<<20)+1))
	if e != nil {
		return llm.Response{}, c.readError(ctx, e)
	}
	if len(data) > 2<<20 {
		return llm.Response{}, llm.ProtocolError("response body exceeds limit")
	}
	out, e := protocol.DecodeResponse(c.format, data)
	if e == nil {
		e = llm.ValidateResponse(r, out)
	}
	return out, e
}
func (c *Client) Stream(ctx context.Context, r llm.Request, sink llm.Sink) (llm.Response, error) {
	if e := r.Validate(); e != nil {
		return llm.Response{}, e
	}
	if sink == nil {
		return llm.Response{}, llm.Invalid("stream sink required")
	}
	body, cleanup, e := c.post(ctx, r, true)
	if e != nil {
		return llm.Response{}, e
	}
	defer cleanup()
	out, e := protocol.DecodeStream(c.format, io.LimitReader(body, 2<<20), sink)
	if e == nil {
		e = llm.ValidateResponse(r, out)
	}
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	return out, e
}
func (c *Client) post(parent context.Context, r llm.Request, stream bool) (io.ReadCloser, func(), error) {
	data, e := protocol.EncodeRequest(c.format, r, stream)
	if e != nil {
		return nil, nil, e
	}
	ctx, cancel := context.WithCancel(parent)
	path := "/chat/completions"
	if c.format == "anthropic" {
		path = "/messages"
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.options.BaseURL+path, bytes.NewReader(data))
	if e != nil {
		cancel()
		return nil, nil, llm.Invalid("invalid provider request")
	}
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	if c.format == "openai" {
		req.Header.Set("Authorization", "Bearer "+c.options.APIKey)
	} else {
		req.Header.Set("x-api-key", c.options.APIKey)
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	response, e := c.http.Do(req)
	if e != nil {
		cancel()
		if parent.Err() != nil {
			return nil, nil, parent.Err()
		}
		var op *net.OpError
		retry := errors.As(e, &op) && op.Op == "dial"
		return nil, nil, &llm.Error{Code: "upstream_unavailable", Message: "upstream connection failed", Status: 502, Retryable: retry, Cause: e}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		cancel()
		status := 502
		code := "upstream_error"
		retry := false
		switch response.StatusCode {
		case 400, 422:
			status = 400
			code = "invalid_request"
		case 429:
			status = 429
			code = "rate_limited"
			retry = true
		case 502, 503, 504:
			retry = true
		case 401, 403:
			code = "upstream_authentication_error"
		}
		return nil, nil, &llm.Error{Code: code, Message: "upstream request failed", Status: status, Retryable: retry, RetryAfter: response.Header.Get("Retry-After")}
	}
	if stream && !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		response.Body.Close()
		cancel()
		return nil, nil, llm.ProtocolError("upstream did not return SSE")
	}
	timer := time.AfterFunc(c.options.IdleTimeout, cancel)
	reader := &idleBody{ReadCloser: response.Body, timer: timer, timeout: c.options.IdleTimeout}
	return reader, func() { timer.Stop(); cancel(); response.Body.Close() }, nil
}
func (c *Client) readError(ctx context.Context, e error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return &llm.Error{Code: "upstream_read_error", Message: "upstream response read failed", Status: 502, Cause: e}
}

type idleBody struct {
	io.ReadCloser
	timer   *time.Timer
	timeout time.Duration
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if n > 0 {
		b.timer.Reset(b.timeout)
	}
	return n, e
}
