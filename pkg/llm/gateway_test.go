package llm_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type provider struct {
	generate func(context.Context, llm.Request) (llm.Response, error)
	stream   func(context.Context, llm.Request, llm.Sink) (llm.Response, error)
}

func (p provider) Generate(c context.Context, r llm.Request) (llm.Response, error) {
	return p.generate(c, r)
}
func (p provider) Stream(c context.Context, r llm.Request, s llm.Sink) (llm.Response, error) {
	return p.stream(c, r, s)
}
func gateway(t *testing.T, p llm.Provider, options llm.GatewayOptions) *llm.Gateway {
	t.Helper()
	c, e := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 4096, MaxOutputTokens: 1024, Stream: true}})
	if e != nil {
		t.Fatal(e)
	}
	g, e := llm.NewGateway(c, map[string]llm.Provider{"p": p}, options)
	if e != nil {
		t.Fatal(e)
	}
	return g
}
func request() llm.Request {
	return llm.Request{Model: "small", Messages: []llm.Message{llm.TextMessage("user", "hi")}, MaxTokens: 32}
}
func answer() llm.Response {
	return llm.Response{Content: []llm.Block{{Type: "text", Text: "answer"}}, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 2, OutputTokens: 1}}
}

func TestInvalidUsageCannotDecreaseAccounting(t *testing.T) {
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		v := answer()
		v.Usage.InputTokens = -100
		return v, nil
	}}, llm.GatewayOptions{})
	out, e := g.Generate(context.Background(), request())
	if e == nil || out.Usage.Known || g.Metrics.InputTokens.Load() != 0 {
		t.Fatal("negative upstream usage was accepted into accounting")
	}
}

func TestMetricsCountReportedUsageAndUnknownAttempts(t *testing.T) {
	calls := 0
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		calls++
		if calls == 1 {
			return llm.Response{}, &llm.Error{Code: "transient", Message: "try later", Status: 503, Retryable: true}
		}
		return answer(), nil
	}}, llm.GatewayOptions{Retries: 1, BaseBackoff: time.Millisecond})
	r := request()
	r.Purpose = "work"
	if _, e := g.Generate(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	if g.Metrics.InputTokens.Load() != 2 || g.Metrics.OutputTokens.Load() != 1 || g.Metrics.UnknownUsage.Load() != 1 || g.Metrics.WorkCalls.Load() != 2 {
		t.Fatal("retry usage or routing phase missing from metrics")
	}
}

func TestGatewayRetriesBeforeOutputAndResolvesUpstreamName(t *testing.T) {
	calls := 0
	g := gateway(t, provider{generate: func(_ context.Context, r llm.Request) (llm.Response, error) {
		calls++
		if r.Model != "native" {
			t.Fatalf("upstream alias leaked: %s", r.Model)
		}
		if calls == 1 {
			return llm.Response{}, &llm.Error{Code: "rate_limited", Message: "limited", Status: 429, Retryable: true, RetryAfter: "0"}
		}
		return answer(), nil
	}}, llm.GatewayOptions{Retries: 1})
	r, e := g.Generate(context.Background(), request())
	if e != nil || r.Text() != "answer" || r.Model != "small" || calls != 2 {
		t.Fatalf("response=%+v calls=%d err=%v", r, calls, e)
	}
}
func TestGatewayNeverReplaysStreamAfterPublicOutput(t *testing.T) {
	calls := 0
	var text string
	g := gateway(t, provider{stream: func(_ context.Context, _ llm.Request, s llm.Sink) (llm.Response, error) {
		calls++
		if e := s(llm.Event{Type: "text_delta", Text: "partial"}); e != nil {
			return llm.Response{}, e
		}
		return llm.Response{}, &llm.Error{Message: "broken", Status: 503, Retryable: true}
	}}, llm.GatewayOptions{Retries: 1})
	_, e := g.Stream(context.Background(), request(), func(v llm.Event) error { text += v.Text; return nil })
	if e == nil || calls != 1 || text != "partial" {
		t.Fatalf("calls=%d text=%q error=%v", calls, text, e)
	}
}
func TestGatewayReleasesConcurrencyOnCancellation(t *testing.T) {
	entered := make(chan struct{}, 1)
	var n atomic.Int32
	g := gateway(t, provider{generate: func(c context.Context, _ llm.Request) (llm.Response, error) {
		if n.Add(1) == 1 {
			entered <- struct{}{}
			<-c.Done()
			return llm.Response{}, c.Err()
		}
		return answer(), nil
	}}, llm.GatewayOptions{MaxConcurrent: 1, ProviderConcurrent: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := g.Generate(ctx, request()); done <- e }()
	<-entered
	if _, e := g.Generate(context.Background(), request()); e == nil {
		t.Fatal("overload accepted")
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := g.Generate(context.Background(), request()); e != nil {
		t.Fatalf("permit leaked: %v", e)
	}
}
func TestGatewayCircuitAndAttemptObserver(t *testing.T) {
	var before, after int
	observer := llm.ObserverFuncs{BeforeFunc: func(context.Context, llm.Request) error { before++; return nil }, AfterFunc: func(context.Context, llm.Request, llm.Response, error) error { after++; return nil }}
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		return llm.Response{}, &llm.Error{Message: "down", Status: 503, Retryable: true}
	}}, llm.GatewayOptions{CircuitFailures: 1, CircuitCooldown: time.Hour})
	ctx := llm.WithObserver(context.Background(), observer)
	if _, e := g.Generate(ctx, request()); e == nil {
		t.Fatal("upstream error lost")
	}
	if _, e := g.Generate(ctx, request()); e == nil {
		t.Fatal("open circuit accepted")
	}
	if before != 1 || after != 1 {
		t.Fatalf("attempts before=%d after=%d", before, after)
	}
}
