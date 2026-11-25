package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/reliability"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

type pipeWriter struct {
	conn    net.Conn
	header  http.Header
	started chan struct{}
	once    sync.Once
}

func (w *pipeWriter) Header() http.Header                { return w.header }
func (w *pipeWriter) WriteHeader(int)                    {}
func (w *pipeWriter) Flush()                             {}
func (w *pipeWriter) SetReadDeadline(d time.Time) error  { return w.conn.SetReadDeadline(d) }
func (w *pipeWriter) SetWriteDeadline(d time.Time) error { return w.conn.SetWriteDeadline(d) }
func (w *pipeWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.conn.Write(b)
}

type unwrappingWriter struct{ http.ResponseWriter }

func (w unwrappingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestWrappedStreamPreservesLeaseCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	cancel(lost)
	raw := httptest.NewRecorder()
	guard := &responseWriter{ResponseWriter: raw, requestContext: ctx}
	s := &streamWriter{w: unwrappingWriter{guard}, seconds: 30}
	if _, err := s.Write([]byte("data: [DONE]\n\n")); !errors.Is(err, lost) || raw.Body.Len() != 0 {
		t.Fatalf("wrapped stream bypassed cancellation: err=%v body=%s", err, raw.Body.String())
	}
}

func TestLeaseLossInterruptsBlockedStreamWithoutExtendingDeadline(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	raw := &pipeWriter{conn: server, header: make(http.Header), started: make(chan struct{})}
	w := &responseWriter{ResponseWriter: raw, requestContext: ctx}
	s := &streamWriter{w: w, seconds: 30}
	done := make(chan error, 1)
	stop := context.AfterFunc(ctx, w.interruptIO)
	defer stop()
	go func() { _, err := s.Write([]byte("data: partial\n\n")); done <- err }()
	<-raw.started
	cancel(lost)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked stream claimed successful write")
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss did not interrupt a blocked stream write")
	}
	if _, err := s.Write([]byte("data: [DONE]\n\n")); !errors.Is(err, lost) {
		t.Fatalf("later stream write extended the cancelled deadline: %v", err)
	}
}

func TestCompletedRequestCancellationDoesNotPoisonReusedConnection(t *testing.T) {
	cancels := make(chan context.CancelCauseFunc, 2)
	var first context.CancelCauseFunc
	h, calls := boundary(t, admissionFunc(func(ctx context.Context, _ string) (llm.Permit, error) {
		child, cancel := context.WithCancelCause(ctx)
		cancels <- cancel
		return entryPermit{child}, nil
	}))
	s := httptest.NewServer(h)
	defer s.Close()
	transport := &http.Transport{MaxConnsPerHost: 1}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for i := range 2 {
		reused := false
		r, _ := http.NewRequest("GET", s.URL+"/v1/models", nil)
		r.Header.Set("Authorization", "Bearer boundary-test")
		r = r.WithContext(httptrace.WithClientTrace(r.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}))
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || (i == 1 && !reused) {
			t.Fatalf("request=%d status=%d reused=%v err=%v", i, response.StatusCode, reused, err)
		}
		current := <-cancels
		if first == nil {
			first = current
		}
		first(&llm.Error{Control: true, Code: "lease_lost", Status: 503})
	}
	if calls.Load() != 0 {
		t.Fatal("model listing dispatched upstream")
	}
}

type admissionFunc func(context.Context, string) (llm.Permit, error)

func (f admissionFunc) Acquire(ctx context.Context, id string) (llm.Permit, error) { return f(ctx, id) }

type entryPermit struct{ ctx context.Context }

func (p entryPermit) Context() context.Context          { return p.ctx }
func (p entryPermit) Check() error                      { return llm.ContextError(p.ctx) }
func (p entryPermit) Complete(llm.AttemptOutcome) error { return nil }

type boundaryProvider struct{ calls *atomic.Int64 }

func (p boundaryProvider) Generate(context.Context, llm.Request) (llm.Response, error) {
	p.calls.Add(1)
	return llm.Response{Content: []llm.Block{{Type: "text", Text: "answer"}}, FinishReason: "stop"}, nil
}
func (p boundaryProvider) Stream(ctx context.Context, r llm.Request, s llm.Sink) (llm.Response, error) {
	v, e := p.Generate(ctx, r)
	if e == nil {
		e = s(llm.Event{Type: "text_delta", Text: "answer"})
	}
	return v, e
}
func boundary(t *testing.T, a reliability.Admission) (http.Handler, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	catalog, err := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 262144, MaxOutputTokens: 4096, Stream: true}})
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := llm.NewGateway(catalog, map[string]llm.Provider{"p": boundaryProvider{calls}}, llm.GatewayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := agent.NewEngine(catalog, gateway, agent.RuleDecider{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(Options{Admission: a, Server: config.Server{Address: "127.0.0.1:0", RequestSeconds: 30, RatePerMinute: 10000, Burst: 100}, Principals: []Credential{{Principal: config.Principal{ID: "alice", Models: []string{"small"}, Providers: []string{"p"}}, Key: "boundary-test"}}}, Dependencies{Catalog: catalog, Gateway: gateway, Engine: engine, Policy: config.Agent{Models: []string{"small"}, Providers: []string{"p"}}})
	if err != nil {
		t.Fatal(err)
	}
	return h, calls
}
func TestSharedAdmissionRejectsBeforeBodyAndUpstream(t *testing.T) {
	checked := 0
	denied := &llm.Error{Control: true, Code: "coordination_unavailable", Message: "unavailable", Status: 503}
	h, calls := boundary(t, admissionFunc(func(ctx context.Context, id string) (llm.Permit, error) {
		checked++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("admission outside request deadline")
		}
		if id != "alice" {
			t.Error("untrusted principal scope")
		}
		return nil, denied
	}))
	for _, auth := range []bool{false, true} {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{`))
		if auth {
			req.Header.Set("Authorization", "Bearer boundary-test")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		want := 401
		if auth {
			want = 503
		}
		if w.Code != want {
			t.Fatalf("status=%d want=%d", w.Code, want)
		}
	}
	if checked != 1 || calls.Load() != 0 {
		t.Fatalf("checked=%d calls=%d", checked, calls.Load())
	}
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer boundary-test")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || checked != 1 {
		t.Fatalf("metrics depend on shared backend: %d checked=%d", w.Code, checked)
	}
}
func TestLeaseLossInterruptsIncompleteBodyWith503(t *testing.T) {
	cancels := make(chan context.CancelCauseFunc, 1)
	h, calls := boundary(t, admissionFunc(func(ctx context.Context, _ string) (llm.Permit, error) {
		ctx, cancel := context.WithCancelCause(ctx)
		cancels <- cancel
		return entryPermit{ctx}, nil
	}))
	s := httptest.NewServer(h)
	defer s.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer boundary-test\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case cancel := <-cancels:
		cancel(&llm.Error{Control: true, Code: "lease_lost", Message: "lease lost", Status: 503})
	case <-time.After(time.Second):
		t.Fatal("entry permit not acquired")
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("lease loss did not interrupt read: %v", err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != 503 || !strings.Contains(string(body), "lease_lost") || calls.Load() != 0 {
		t.Fatalf("status=%d body=%s calls=%d", response.StatusCode, body, calls.Load())
	}
}
