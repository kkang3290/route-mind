package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/server"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

func fixture(t *testing.T, override ...config.Server) (http.Handler, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		req, o, e := protocol.DecodeRequest("openai", b)
		if e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		out := llm.Response{ID: "upstream", Model: req.Model, Content: []llm.Block{{Type: "text", Text: "public answer"}}, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 3, OutputTokens: 2}}
		if o.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			en := protocol.NewEncoder("openai", w, "upstream", req.Model, true)
			_ = en.Event(llm.Event{Type: "text_delta", Text: "public "})
			w.(http.Flusher).Flush()
			_ = en.Event(llm.Event{Type: "text_delta", Text: "answer"})
			_ = en.Finish(out)
		} else {
			b, _ := protocol.EncodeResponse("openai", out)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(up.Close)
	p, e := openai.New(transport.Options{BaseURL: up.URL, APIKey: "test", AllowHTTP: true})
	if e != nil {
		t.Fatal(e)
	}
	c, _ := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 262144, MaxOutputTokens: 4096, Stream: true}, {ID: "secret", Provider: "p", Upstream: "private", MaxInputBytes: 262144, MaxOutputTokens: 4096, Stream: true}})
	g, _ := llm.NewGateway(c, map[string]llm.Provider{"p": p}, llm.GatewayOptions{})
	en, _ := agent.NewEngine(c, g, agent.RuleDecider{}, nil)
	settings := config.Server{Address: "127.0.0.1:8000", MaxBodyBytes: 4096, RatePerMinute: 100000, Burst: 100, MaxConcurrent: 8, PerPrincipalConcurrent: 4, RequestSeconds: 10, WriteSeconds: 2}
	if len(override) > 0 {
		settings = override[0]
	}
	h, e := server.New(server.Options{Server: settings, Principals: []server.Credential{{Principal: config.Principal{ID: "alice", Models: []string{"small"}, Providers: []string{"p"}, AllowAgent: true}, Key: "gateway-test-key"}}}, server.Dependencies{Catalog: c, Gateway: g, Engine: en, Policy: config.Agent{Models: []string{"small"}, Providers: []string{"p"}, Limits: agent.DefaultLimits()}})
	if e != nil {
		t.Fatal(e)
	}
	return h, calls
}
func post(h http.Handler, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestPrincipalRateLimitRejectsBeforeAnyAdditionalUpstream(t *testing.T) {
	h, calls := fixture(t, config.Server{Address: "127.0.0.1:0", RatePerMinute: 1, Burst: 1})
	body := `{"model":"small","messages":[{"role":"user","content":"hi"}]}`
	first := post(h, "/v1/chat/completions", body, "gateway-test-key")
	if first.Code != 200 {
		t.Fatal(first.Body)
	}
	second := post(h, "/v1/chat/completions", body, "gateway-test-key")
	if second.Code != 429 || second.Header().Get("Retry-After") == "" || calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d", second.Code, calls.Load())
	}
}

func TestRequestDeadlineInterruptsAnIncompleteBody(t *testing.T) {
	h, calls := fixture(t, config.Server{Address: "127.0.0.1:0", RequestSeconds: 2, RatePerMinute: 60, Burst: 10})
	s := httptest.NewServer(h)
	defer s.Close()
	conn, e := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, e = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer gateway-test-key\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{")
	if e != nil {
		t.Fatal(e)
	}
	resp, e := http.ReadResponse(bufio.NewReader(conn), nil)
	if e != nil {
		t.Fatalf("incomplete body ignored request deadline: %v", e)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 400 || calls.Load() != 0 {
		t.Fatalf("status=%d upstream=%d", resp.StatusCode, calls.Load())
	}
}

func TestRawKratosRoutesAuthenticateBeforeUpstream(t *testing.T) {
	h, c := fixture(t)
	for _, key := range []string{"", "wrong"} {
		w := post(h, "/v1/chat/completions", `{"model":"small","messages":[{"role":"user","content":"hi"}]}`, key)
		if w.Code != 401 || c.Load() != 0 || w.Header().Get("X-Request-ID") == "" {
			t.Fatalf("status=%d calls=%d", w.Code, c.Load())
		}
	}
}
func TestCrossProtocolDirectAndAutoStreaming(t *testing.T) {
	for _, format := range []string{"openai", "anthropic"} {
		for _, model := range []string{"small", "auto"} {
			for _, stream := range []bool{false, true} {
				t.Run(format+model+map[bool]string{false: "JSON", true: "SSE"}[stream], func(t *testing.T) {
					h, c := fixture(t)
					path := "/v1/chat/completions"
					body := map[string]any{"model": model, "messages": []any{map[string]string{"role": "user", "content": "hi"}}, "stream": stream, "max_tokens": 100}
					if format == "anthropic" {
						path = "/v1/messages"
					}
					b, _ := json.Marshal(body)
					r := httptest.NewRequest("POST", path, bytes.NewReader(b))
					r.Header.Set("Content-Type", "application/json")
					if format == "anthropic" {
						r.Header.Set("x-api-key", "gateway-test-key")
					} else {
						r.Header.Set("Authorization", "Bearer gateway-test-key")
					}
					w := httptest.NewRecorder()
					h.ServeHTTP(w, r)
					if w.Code != 200 || c.Load() != 1 {
						t.Fatalf("status=%d calls=%d body=%s", w.Code, c.Load(), w.Body.String())
					}
					var out llm.Response
					var e error
					if stream {
						out, e = protocol.DecodeStream(format, w.Body, func(llm.Event) error { return nil })
					} else {
						out, e = protocol.DecodeResponse(format, w.Body.Bytes())
					}
					if e != nil || out.Text() != "public answer" || out.Model != model {
						t.Fatalf("out=%+v err=%v", out, e)
					}
				})
			}
		}
	}
}
func TestModelPolicyBodyBoundsAndUnknownFieldsBeforeCalling(t *testing.T) {
	h, c := fixture(t)
	for _, tc := range []struct {
		body   string
		status int
	}{{`{"model":"secret","messages":[{"role":"user","content":"hi"}]}`, 403}, {`{"model":"small","messages":[{"role":"user","content":"hi"}],"thinking":true}`, 400}, {`{"model":"small","messages":[{"role":"user","content":"` + strings.Repeat("x", 5000) + `"}]}`, 413}} {
		w := post(h, "/v1/chat/completions", tc.body, "gateway-test-key")
		if w.Code != tc.status || c.Load() != 0 {
			t.Fatalf("status=%d want=%d calls=%d", w.Code, tc.status, c.Load())
		}
	}
}
func TestModelsDoNotExposePrivateAliases(t *testing.T) {
	h, _ := fixture(t)
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer gateway-test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), "secret") || !strings.Contains(w.Body.String(), "auto") {
		t.Fatal(w.Body.String())
	}
}

var _ = context.Background
