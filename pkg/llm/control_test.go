package llm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

// A successful checkpoint can outlive its permit. Sending after cancellation
// would create an uncoordinated, potentially billable upstream operation.
type controllerFunc func(context.Context, string) (llm.Permit, error)

func (f controllerFunc) Acquire(ctx context.Context, id string) (llm.Permit, error) {
	return f(ctx, id)
}

type testPermit struct {
	ctx    context.Context
	check  func() error
	finish func(llm.AttemptOutcome) error
}

func (p testPermit) Context() context.Context { return p.ctx }
func (p testPermit) Check() error {
	if p.check != nil {
		return p.check()
	}
	return nil
}
func (p testPermit) Complete(o llm.AttemptOutcome) error { return p.finish(o) }

func TestGatewayLocalCapacityIsTerminalControl(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		close(entered)
		<-unblock
		return answer(), nil
	}}, llm.GatewayOptions{MaxConcurrent: 1, ProviderConcurrent: 1})
	done := make(chan struct{})
	go func() { defer close(done); _, _ = g.Generate(context.Background(), request()) }()
	defer func() { close(unblock); <-done }()
	<-entered
	_, err := g.Generate(context.Background(), request())
	if e := llm.AsError(err); e.Code != "overloaded" || !e.Control {
		t.Fatalf("capacity rejection permits Agent fallback: %+v", e)
	}
}

func TestGatewayCancelledCheckpointPreservesPermitCause(t *testing.T) {
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	var cancel context.CancelCauseFunc
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		t.Fatal("checkpoint failure dispatched")
		return answer(), nil
	}}, llm.GatewayOptions{Controller: controllerFunc(func(ctx context.Context, _ string) (llm.Permit, error) {
		child, stop := context.WithCancelCause(ctx)
		cancel = stop
		return testPermit{ctx: child, finish: func(llm.AttemptOutcome) error { return lost }}, nil
	})})
	ctx := llm.WithObserver(context.Background(), llm.ObserverFuncs{BeforeFunc: func(ctx context.Context, _ llm.Request) error { cancel(lost); return ctx.Err() }})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) {
		t.Fatalf("checkpoint erased control cause: %v", err)
	}
}

func TestGatewayBackoffCancellationKeepsControlCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	calls := 0
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		calls++
		time.AfterFunc(100*time.Millisecond, func() { cancel(lost) })
		return llm.Response{}, &llm.Error{Code: "upstream_error", Status: 502, Retryable: true}
	}}, llm.GatewayOptions{Retries: 2, BaseBackoff: 2 * time.Second})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestGatewaySharedDenialPreventsDispatchAndObserver(t *testing.T) {
	denied := &llm.Error{Control: true, Code: "coordination_unavailable", Message: "unavailable", Status: 503, Retryable: true}
	calls, before := 0, 0
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) { calls++; return answer(), nil }}, llm.GatewayOptions{Retries: 2, Controller: controllerFunc(func(_ context.Context, id string) (llm.Permit, error) {
		if id != "p" {
			t.Errorf("wrong provider: %s", id)
		}
		return nil, denied
	})})
	ctx := llm.WithObserver(context.Background(), llm.ObserverFuncs{BeforeFunc: func(context.Context, llm.Request) error { before++; return nil }})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, denied) || calls != 0 || before != 0 {
		t.Fatalf("err=%v calls=%d before=%d", err, calls, before)
	}
}

func TestGatewayPermitLossDuringCheckpointPairsObserver(t *testing.T) {
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	invalid := false
	calls, after, finished := 0, 0, 0
	c := controllerFunc(func(ctx context.Context, _ string) (llm.Permit, error) {
		return testPermit{ctx: ctx, check: func() error {
			if invalid {
				return lost
			}
			return nil
		}, finish: func(o llm.AttemptOutcome) error {
			finished++
			if o.Dispatched || o.Kind != llm.OutcomeNeutral {
				t.Errorf("invalid outcome: %+v", o)
			}
			return lost
		}}, nil
	})
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) { calls++; return answer(), nil }}, llm.GatewayOptions{Controller: c})
	ctx := llm.WithObserver(context.Background(), llm.ObserverFuncs{BeforeFunc: func(context.Context, llm.Request) error { invalid = true; return nil }, AfterFunc: func(context.Context, llm.Request, llm.Response, error) error { after++; return nil }})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) || calls != 0 || after != 1 || finished != 1 {
		t.Fatalf("err=%v calls=%d after=%d finished=%d", err, calls, after, finished)
	}
}

func TestGatewayCompleteFailureIsAccountedWithoutRetry(t *testing.T) {
	lost := &llm.Error{Control: true, Code: "coordination_unavailable", Message: "unavailable", Status: 503, Retryable: true}
	calls, after := 0, 0
	c := controllerFunc(func(ctx context.Context, _ string) (llm.Permit, error) {
		return testPermit{ctx: ctx, finish: func(o llm.AttemptOutcome) error {
			if !o.Dispatched || o.Kind != llm.OutcomeSuccess {
				t.Errorf("outcome=%+v", o)
			}
			return lost
		}}, nil
	})
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) { calls++; return answer(), nil }}, llm.GatewayOptions{Controller: c, Retries: 2})
	ctx := llm.WithObserver(context.Background(), llm.ObserverFuncs{AfterFunc: func(_ context.Context, _ llm.Request, v llm.Response, err error) error {
		after++
		if v.Usage.InputTokens != 2 || !errors.Is(err, lost) {
			t.Errorf("usage/cause lost: %+v %v", v, err)
		}
		return nil
	}})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) || calls != 1 || after != 1 {
		t.Fatalf("err=%v calls=%d after=%d", err, calls, after)
	}
}

func TestGatewayCheckpointCancellationPreventsDispatch(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	lost := llm.Failure("lease_lost", "coordination lease lost", 503)
	calls, after := 0, 0
	ctx = llm.WithObserver(ctx, llm.ObserverFuncs{
		BeforeFunc: func(context.Context, llm.Request) error { cancel(lost); return nil },
		AfterFunc: func(_ context.Context, _ llm.Request, _ llm.Response, err error) error {
			after++
			if !errors.Is(err, lost) {
				t.Errorf("checkpoint cancellation cause lost: %v", err)
			}
			return nil
		},
	})
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) { calls++; return answer(), nil }}, llm.GatewayOptions{})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) || calls != 0 || after != 1 || g.Metrics.Attempts.Load() != 0 {
		t.Fatalf("err=%v dispatched=%d after=%d attempts=%d", err, calls, after, g.Metrics.Attempts.Load())
	}
}

func TestGatewayPreservesCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	lost := llm.Failure("coordination_unavailable", "coordination unavailable", 503)
	g := gateway(t, provider{generate: func(c context.Context, _ llm.Request) (llm.Response, error) {
		cancel(lost)
		return llm.Response{}, c.Err()
	}}, llm.GatewayOptions{})
	_, err := g.Generate(ctx, request())
	if !errors.Is(err, lost) {
		t.Fatalf("want coordination cause, got %v", err)
	}
}

func TestGatewayDownstreamFailureDoesNotOpenCircuit(t *testing.T) {
	calls := 0
	writeErr := errors.New("downstream disconnected")
	g := gateway(t, provider{stream: func(_ context.Context, _ llm.Request, s llm.Sink) (llm.Response, error) {
		calls++
		if err := s(llm.Event{Type: "text_delta", Text: "answer"}); err != nil {
			return llm.Response{}, err
		}
		return answer(), nil
	}}, llm.GatewayOptions{CircuitFailures: 1, CircuitCooldown: time.Hour})
	if _, err := g.Stream(context.Background(), request(), func(llm.Event) error { return writeErr }); !errors.Is(err, writeErr) {
		t.Fatal(err)
	}
	if _, err := g.Stream(context.Background(), request(), func(llm.Event) error { return nil }); err != nil {
		t.Fatalf("downstream failure opened upstream circuit: %v", err)
	}
	if calls != 2 {
		t.Fatalf("dispatched %d calls, want 2", calls)
	}
}

func TestGatewayUpstreamAuthenticationDoesNotOpenCircuit(t *testing.T) {
	calls := 0
	g := gateway(t, provider{generate: func(context.Context, llm.Request) (llm.Response, error) {
		calls++
		if calls == 1 {
			return llm.Response{}, llm.Failure("upstream_authentication_error", "upstream request failed", 502)
		}
		return answer(), nil
	}}, llm.GatewayOptions{CircuitFailures: 1, CircuitCooldown: time.Hour})
	if _, err := g.Generate(context.Background(), request()); err == nil {
		t.Fatal("authentication error missing")
	}
	if _, err := g.Generate(context.Background(), request()); err != nil {
		t.Fatalf("authentication failure opened health circuit: %v", err)
	}
}
