// Package service owns the northbound HTTP boundary, not the model orchestration.
package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kkang3290/route-mind/internal/biz"
	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

type Credential struct {
	config.Principal
	Key string
}
type Options struct {
	Server     config.Server
	Principals []Credential
	Logger     *slog.Logger
}
type Dependencies = biz.Application
type bucket struct {
	tokens float64
	at     time.Time
	active int
}
type Handler struct {
	options                                                   Options
	app                                                       Dependencies
	mux                                                       *http.ServeMux
	slots                                                     chan struct{}
	mu                                                        sync.Mutex
	buckets                                                   map[string]*bucket
	requests, failures, elapsed, firstTokens, firstTokenNanos atomic.Int64
}
type principalKey struct{}
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func New(o Options, a Dependencies) (http.Handler, error) {
	if a.Catalog == nil || a.Gateway == nil || a.Engine == nil {
		return nil, llm.Invalid("catalog, gateway and engine are required")
	}
	if o.Server.MaxConcurrent <= 0 {
		o.Server.MaxConcurrent = 32
	}
	if o.Server.PerPrincipalConcurrent <= 0 {
		o.Server.PerPrincipalConcurrent = 4
	}
	if o.Server.MaxBodyBytes <= 0 {
		o.Server.MaxBodyBytes = 1 << 20
	}
	if o.Server.RequestSeconds <= 0 {
		o.Server.RequestSeconds = 180
	}
	if o.Server.WriteSeconds <= 0 {
		o.Server.WriteSeconds = 15
	}
	if o.Server.RatePerMinute <= 0 {
		o.Server.RatePerMinute = 60
	}
	if o.Server.Burst <= 0 {
		o.Server.Burst = 10
	}
	if o.Server.AllowInsecureLocal {
		host, _, e := net.SplitHostPort(o.Server.Address)
		ip := net.ParseIP(host)
		if e != nil || ip == nil || !ip.IsLoopback() {
			return nil, llm.Invalid("unauthenticated mode requires literal loopback listening")
		}
	}
	if len(o.Principals) == 0 && !o.Server.AllowInsecureLocal {
		return nil, llm.Invalid("API authentication is required")
	}
	keys := map[string]bool{}
	ids := map[string]bool{}
	for _, p := range o.Principals {
		if p.ID == "" || p.Key == "" || keys[p.Key] || ids[p.ID] {
			return nil, llm.Invalid("invalid or ambiguous API credentials")
		}
		keys[p.Key] = true
		ids[p.ID] = true
	}
	a.Policy.Limits = agent.NormalizeLimits(a.Policy.Limits)
	h := &Handler{options: o, app: a, mux: http.NewServeMux(), slots: make(chan struct{}, o.Server.MaxConcurrent), buckets: map[string]*bucket{}}
	h.mux.HandleFunc("/v1/chat/completions", h.compat("openai"))
	h.mux.HandleFunc("/v1/messages", h.compat("anthropic"))
	h.mux.HandleFunc("/v1/models", h.models)
	h.mux.HandleFunc("/v1/agent/runs", h.createRun)
	h.mux.HandleFunc("/v1/agent/runs/", h.runResource)
	h.mux.HandleFunc("/metrics", h.metrics)
	h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, format(r), llm.Failure("not_found", "route not found", 404))
	})
	return h, nil
}
func format(r *http.Request) string {
	if r.URL.Path == "/v1/messages" {
		return "anthropic"
	}
	return "openai"
}
func route(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/v1/agent/runs/") {
		return "/v1/agent/runs/{id}"
	}
	switch r.URL.Path {
	case "/healthz", "/readyz", "/metrics", "/v1/models", "/v1/messages", "/v1/chat/completions", "/v1/agent/runs":
		return r.URL.Path
	}
	return "unmatched"
}
func (h *Handler) ServeHTTP(raw http.ResponseWriter, r *http.Request) {
	w := &responseWriter{ResponseWriter: raw}
	start := time.Now()
	w.Header().Set("X-Request-ID", agent.NewID())
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	w.Header().Set("Cache-Control", "no-store")
	if r.TLS != nil {
		w.Header().Set("Strict-Transport-Security", "max-age=31536000")
	}
	defer func() {
		if recover() != nil {
			if w.status == 0 {
				writeError(w, format(r), llm.Failure("internal_error", "internal error", 500))
			} else if strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
				_ = frame(w, "error", protocol.ErrorPayload(format(r), llm.Failure("internal_error", "stream interrupted", 500)), h.options.Server.WriteSeconds)
			}
		}
		h.requests.Add(1)
		h.elapsed.Add(time.Since(start).Nanoseconds())
		if w.status >= 400 {
			h.failures.Add(1)
		}
		if h.options.Logger != nil {
			h.options.Logger.Info("http_request", "request_id", w.Header().Get("X-Request-ID"), "method", r.Method, "route", route(r), "status", w.status, "duration_ms", time.Since(start).Milliseconds())
		}
	}()
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
		if !method(w, r, "GET") {
			return
		}
		if r.URL.Path == "/readyz" && h.app.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if e := h.app.Ready(ctx); e != nil {
				writeError(w, format(r), llm.Failure("not_ready", "service not ready; verify database and migrations", 503))
				return
			}
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
		return
	}
	p, e := h.authenticate(r)
	if e != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeError(w, format(r), e)
		return
	}
	release, e := h.admit(p.ID)
	if e != nil {
		w.Header().Set("Retry-After", "1")
		writeError(w, format(r), e)
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(h.options.Server.RequestSeconds)*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, principalKey{}, p)
	// Context cancellation alone does not unblock net/http Body.Read. Bound the
	// socket read too, without a whole-response timeout that would kill SSE.
	if deadline, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(w).SetReadDeadline(deadline)
	}
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}
func (h *Handler) authenticate(r *http.Request) (config.Principal, error) {
	auth := r.Header.Get("Authorization")
	key := ""
	if auth != "" {
		parts := strings.Fields(auth)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			key = parts[1]
		}
	}
	if r.Header.Get("x-api-key") != "" {
		if r.URL.Path != "/v1/messages" || auth != "" {
			return config.Principal{}, llm.Failure("unauthorized", "invalid API credential", 401)
		}
		key = r.Header.Get("x-api-key")
	}
	if key != "" && len(key) <= 8192 {
		digest := sha256.Sum256([]byte(key))
		for _, p := range h.options.Principals {
			want := sha256.Sum256([]byte(p.Key))
			if subtle.ConstantTimeCompare(want[:], digest[:]) == 1 {
				return p.Principal, nil
			}
		}
	}
	if key == "" && auth == "" && r.Header.Get("x-api-key") == "" && h.options.Server.AllowInsecureLocal {
		return config.Principal{ID: "local-development", AllowAgent: true, Models: h.app.Policy.Models, Providers: h.app.Policy.Providers}, nil
	}
	return config.Principal{}, llm.Failure("unauthorized", "missing or invalid API credential", 401)
}
func (h *Handler) admit(id string) (func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	b := h.buckets[id]
	if b == nil {
		b = &bucket{tokens: float64(h.options.Server.Burst), at: now}
		h.buckets[id] = b
	}
	b.tokens += now.Sub(b.at).Seconds() * float64(h.options.Server.RatePerMinute) / 60
	if b.tokens > float64(h.options.Server.Burst) {
		b.tokens = float64(h.options.Server.Burst)
	}
	b.at = now
	if b.tokens < 1 || b.active >= h.options.Server.PerPrincipalConcurrent {
		return nil, llm.Failure("rate_limited", "principal rate or concurrency limit reached", 429)
	}
	select {
	case h.slots <- struct{}{}:
	default:
		return nil, llm.Failure("overloaded", "active request capacity reached", 503)
	}
	b.tokens--
	b.active++
	return func() { <-h.slots; h.mu.Lock(); b.active--; h.mu.Unlock() }, nil
}
func method(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Method != want {
		w.Header().Set("Allow", want)
		writeError(w, format(r), llm.Failure("method_not_allowed", "method not allowed", 405))
		return false
	}
	return true
}
func readBody(w http.ResponseWriter, r *http.Request, max int) ([]byte, error) {
	media, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || media != "application/json" || r.Header.Get("Content-Encoding") != "" {
		return nil, llm.Failure("unsupported_media_type", "application/json without content encoding is required", 415)
	}
	b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(max)))
	if e != nil {
		var limit *http.MaxBytesError
		if errors.As(e, &limit) {
			return nil, llm.Failure("request_too_large", "request body exceeds limit", 413)
		}
		return nil, llm.Invalid("request body could not be read")
	}
	return b, nil
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	b, e := json.Marshal(v)
	if e != nil {
		status = 500
		b = []byte(`{"error":{"code":"internal_error","message":"response encoding failed"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func writeError(w http.ResponseWriter, f string, e error) {
	ue := llm.AsError(e)
	if ue.RetryAfter != "" {
		w.Header().Set("Retry-After", ue.RetryAfter)
	}
	writeJSON(w, ue.Status, protocol.ErrorPayload(f, e))
}
func (h *Handler) compat(f string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !method(w, r, "POST") {
			return
		}
		b, e := readBody(w, r, h.options.Server.MaxBodyBytes)
		if e != nil {
			writeError(w, f, e)
			return
		}
		req, o, e := protocol.DecodeRequest(f, b)
		if e != nil {
			writeError(w, f, e)
			return
		}
		p := r.Context().Value(principalKey{}).(config.Principal)
		var sink llm.Sink
		var encoder *protocol.Encoder
		lazy := &streamWriter{w: w, seconds: h.options.Server.WriteSeconds}
		if o.Stream {
			encoder = protocol.NewEncoder(f, lazy, "msg_"+strings.ReplaceAll(agent.NewID(), "-", ""), req.Model, o.IncludeUsage)
			first := true
			sink = func(v llm.Event) error {
				if first && v.Type == "text_delta" && v.Text != "" {
					first = false
					h.firstTokens.Add(1)
					h.firstTokenNanos.Add(time.Since(lazy.at).Nanoseconds())
				}
				return encoder.Event(v)
			}
			lazy.at = time.Now()
		}
		out, e := h.app.Execute(r.Context(), p, req, sink)
		if e != nil {
			if lazy.started {
				h.streamFailed(w, r, e)
				_ = encoder.Error(e)
			} else {
				writeError(w, f, e)
			}
			return
		}
		if o.Stream {
			if e = encoder.Finish(out); e != nil {
				h.streamFailed(w, r, e)
				_ = encoder.Error(e)
			}
			return
		}
		b, e = protocol.EncodeResponse(f, out)
		if e != nil {
			writeError(w, f, e)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write(b)
	}
}
func (h *Handler) streamFailed(w http.ResponseWriter, r *http.Request, e error) {
	h.failures.Add(1)
	if h.options.Logger != nil {
		h.options.Logger.Warn("stream_failed", "request_id", w.Header().Get("X-Request-ID"), "route", route(r), "code", llm.AsError(e).Code)
	}
}

func (h *Handler) models(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	p := r.Context().Value(principalKey{}).(config.Principal)
	data := []any{}
	for _, m := range h.app.Catalog.Models() {
		if slices.Contains(p.Models, m.ID) && slices.Contains(p.Providers, m.Provider) {
			data = append(data, map[string]any{"id": m.ID, "object": "model", "created": 0, "owned_by": "route-mind"})
		}
	}
	if p.AllowAgent {
		data = append(data, map[string]any{"id": "auto", "object": "model", "created": 0, "owned_by": "route-mind"})
	}
	writeJSON(w, 200, map[string]any{"object": "list", "data": data})
}
func (h *Handler) metrics(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	m := &h.app.Gateway.Metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = fmt.Fprintf(w, "route_mind_http_requests_total %d\nroute_mind_http_errors_total %d\nroute_mind_http_duration_seconds_sum %g\nroute_mind_first_token_seconds_sum %g\nroute_mind_first_token_seconds_count %d\nroute_mind_upstream_attempts_total %d\nroute_mind_upstream_retries_total %d\nroute_mind_upstream_errors_total %d\nroute_mind_upstream_inflight %d\n", h.requests.Load(), h.failures.Load(), float64(h.elapsed.Load())/1e9, float64(h.firstTokenNanos.Load())/1e9, h.firstTokens.Load(), m.Attempts.Load(), m.Retries.Load(), m.Errors.Load(), m.Inflight.Load())
	_, _ = fmt.Fprintf(w, "route_mind_reported_input_tokens_total %d\nroute_mind_reported_output_tokens_total %d\nroute_mind_unknown_usage_attempts_total %d\nroute_mind_model_calls_total{phase=\"decision\"} %d\nroute_mind_model_calls_total{phase=\"work\"} %d\nroute_mind_model_calls_total{phase=\"respond\"} %d\nroute_mind_model_calls_total{phase=\"direct\"} %d\n", m.InputTokens.Load(), m.OutputTokens.Load(), m.UnknownUsage.Load(), m.DecisionCalls.Load(), m.WorkCalls.Load(), m.RespondCalls.Load(), m.DirectCalls.Load())
}

type streamWriter struct {
	w       http.ResponseWriter
	started bool
	seconds int
	at      time.Time
}

func (s *streamWriter) Write(b []byte) (int, error) {
	controller := http.NewResponseController(s.w)
	if e := controller.SetWriteDeadline(time.Now().Add(time.Duration(s.seconds) * time.Second)); e != nil && !errors.Is(e, http.ErrNotSupported) {
		return 0, e
	}
	if !s.started {
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("X-Accel-Buffering", "no")
		s.w.WriteHeader(200)
		s.started = true
	}
	n, e := s.w.Write(b)
	if e == nil {
		e = controller.Flush()
	}
	return n, e
}
func frame(w http.ResponseWriter, event string, v any, seconds int) error {
	data, e := json.Marshal(v)
	if e != nil {
		return e
	}
	s := &streamWriter{w: w, seconds: seconds}
	_, e = fmt.Fprintf(s, "event: %s\ndata: %s\n\n", event, data)
	return e
}
