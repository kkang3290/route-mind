package llm

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// AttemptObserver is called for every real upstream attempt, including retries.
// Returning an error stops the call; it must not perform additional model calls.
type AttemptObserver interface {
	Before(context.Context, Request) error
	After(context.Context, Request, Response, error) error
}
type ObserverFuncs struct {
	BeforeFunc func(context.Context, Request) error
	AfterFunc  func(context.Context, Request, Response, error) error
}

func (o ObserverFuncs) Before(c context.Context, r Request) error {
	if o.BeforeFunc != nil {
		return o.BeforeFunc(c, r)
	}
	return nil
}
func (o ObserverFuncs) After(c context.Context, r Request, v Response, e error) error {
	if o.AfterFunc != nil {
		return o.AfterFunc(c, r, v, e)
	}
	return nil
}

type observerKey struct{}

func WithObserver(c context.Context, o AttemptObserver) context.Context {
	return context.WithValue(c, observerKey{}, o)
}

type GatewayOptions struct {
	Controller         AttemptController
	MaxConcurrent      int
	ProviderConcurrent int
	Retries            int
	BaseBackoff        time.Duration
	CircuitFailures    int
	CircuitCooldown    time.Duration
}
type Metrics struct {
	Attempts      atomic.Int64
	Retries       atomic.Int64
	Errors        atomic.Int64
	Inflight      atomic.Int64
	InputTokens   atomic.Int64
	OutputTokens  atomic.Int64
	UnknownUsage  atomic.Int64
	DecisionCalls atomic.Int64
	WorkCalls     atomic.Int64
	RespondCalls  atomic.Int64
	DirectCalls   atomic.Int64
}
type circuit struct {
	failures int
	until    time.Time
}
type Gateway struct {
	Catalog       *Catalog
	providers     map[string]Provider
	options       GatewayOptions
	slots         chan struct{}
	providerSlots map[string]chan struct{}
	mu            sync.Mutex
	circuits      map[string]circuit
	Metrics       Metrics
}

func NewGateway(c *Catalog, p map[string]Provider, o GatewayOptions) (*Gateway, error) {
	if c == nil {
		return nil, Invalid("model catalog is required")
	}
	if o.Retries < 0 || o.Retries > 3 {
		return nil, Invalid("retry count outside supported range")
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = 32
	}
	if o.ProviderConcurrent <= 0 {
		o.ProviderConcurrent = 8
	}
	if o.BaseBackoff <= 0 {
		o.BaseBackoff = 10 * time.Millisecond
	}
	if o.CircuitCooldown <= 0 {
		o.CircuitCooldown = 15 * time.Second
	}
	g := &Gateway{Catalog: c, providers: map[string]Provider{}, options: o, slots: make(chan struct{}, o.MaxConcurrent), providerSlots: map[string]chan struct{}{}, circuits: map[string]circuit{}}
	for k, v := range p {
		if v == nil {
			return nil, Invalid("nil provider")
		}
		g.providers[k] = v
		g.providerSlots[k] = make(chan struct{}, o.ProviderConcurrent)
	}
	for _, m := range c.Models() {
		if g.providers[m.Provider] == nil {
			return nil, Invalid("model references an unknown provider")
		}
	}
	return g, nil
}
func (g *Gateway) Generate(c context.Context, r Request) (Response, error) {
	return g.invoke(c, r, nil)
}
func (g *Gateway) Stream(c context.Context, r Request, s Sink) (Response, error) {
	if s == nil {
		return Response{}, Invalid("stream sink is required")
	}
	return g.invoke(c, r, s)
}
func (g *Gateway) invoke(ctx context.Context, r Request, sink Sink) (Response, error) {
	m, e := g.Catalog.Resolve(r, sink != nil)
	if e != nil {
		return Response{}, e
	}
	if r.ToolChoice.Mode == "none" {
		r.Tools = nil
	}
	var last error
	for attempt := 0; attempt <= g.options.Retries; attempt++ {
		if ctx.Err() != nil {
			return Response{}, context.Cause(ctx)
		}
		if g.options.Controller == nil && g.open(m.Provider) {
			return Response{}, &Error{Control: true, Code: "circuit_open", Message: "upstream circuit is open", Status: 503}
		}
		release, e := g.acquire(m.Provider)
		if e != nil {
			return Response{}, e
		}
		defer release() // Idempotent; also protects custom Provider panics.
		var permit Permit
		callCtx := ctx
		if g.options.Controller != nil {
			permit, e = g.options.Controller.Acquire(ctx, m.Provider)
			if e != nil {
				release()
				return Response{}, e
			}
			callCtx = permit.Context()
		}
		outcome := AttemptOutcome{Kind: OutcomeNeutral}
		finish := sync.OnceValue(func() error {
			if permit != nil {
				return permit.Complete(outcome)
			}
			return nil
		})
		defer finish()
		check := func() error {
			if err := ContextError(callCtx); err != nil {
				return err
			}
			if permit != nil {
				return permit.Check()
			}
			return nil
		}
		observer, _ := ctx.Value(observerKey{}).(AttemptObserver)
		if observer != nil {
			if e = observer.Before(callCtx, r); e != nil {
				// A checkpoint may return ctx.Err rather than the permit's cause.
				// Keep storage/CAS failures authoritative, but never turn lease
				// loss into an ordinary cancellation eligible for Agent fallback.
				if code := AsError(e).Code; code != "storage_unavailable" && code != "conflict" {
					if cause := ContextError(callCtx); cause != nil {
						e = cause
					}
				}
				_ = finish()
				release()
				return Response{}, e
			}
		}
		if e = check(); e != nil {
			_ = finish()
			if observer != nil {
				if oe := observer.After(ctx, r, Response{}, e); oe != nil {
					e = oe
				}
			}
			release()
			return Response{}, e
		}
		g.Metrics.Attempts.Add(1)
		switch r.Purpose {
		case "decision":
			g.Metrics.DecisionCalls.Add(1)
		case "work":
			g.Metrics.WorkCalls.Add(1)
		case "respond":
			g.Metrics.RespondCalls.Add(1)
		default:
			g.Metrics.DirectCalls.Add(1)
		}
		g.Metrics.Inflight.Add(1)
		native := r
		native.Model = m.Upstream
		emitted, sinkFailed := false, false
		var v Response
		if sink == nil {
			v, e = g.providers[m.Provider].Generate(callCtx, native)
		} else {
			v, e = g.providers[m.Provider].Stream(callCtx, native, func(event Event) error {
				if err := check(); err != nil {
					return err
				}
				emitted = true
				err := sink(event)
				sinkFailed = sinkFailed || err != nil
				return err
			})
		}
		permitErr := check()
		if permitErr != nil {
			e = permitErr
		}
		g.Metrics.Inflight.Add(-1)
		release()
		v.Model = r.Model
		if !validUsage(v.Usage) {
			v.Usage = Usage{}
			e = ProtocolError("invalid usage counters")
		}
		if e == nil {
			e = ValidateResponse(r, v)
		}
		if v.Usage.Known {
			g.Metrics.InputTokens.Add(v.Usage.InputTokens)
			g.Metrics.OutputTokens.Add(v.Usage.OutputTokens)
		}
		if !v.Usage.Known || e != nil {
			g.Metrics.UnknownUsage.Add(1)
		}
		outcome = classifyOutcome(e, sinkFailed || permitErr != nil)
		outcome.Dispatched = true
		if g.options.Controller == nil && outcome.Kind != OutcomeNeutral {
			g.outcome(m.Provider, outcome.Kind)
		}
		if ce := finish(); ce != nil {
			e = ce
		}
		if observer != nil {
			if oe := observer.After(ctx, r, v, e); oe != nil {
				return v, oe
			}
		}
		if ce := ContextError(ctx); ce != nil {
			e = ce
		}
		if e == nil {
			return v, nil
		}
		g.Metrics.Errors.Add(1)
		last = e
		var ue *Error
		if emitted || attempt == g.options.Retries || !errors.As(e, &ue) || !ue.Retryable || ue.Control || ctx.Err() != nil {
			return v, e
		}
		delay := g.options.BaseBackoff * time.Duration(1<<attempt)
		// Bounded deterministic jitter avoids correlated retry bursts without shared RNG state.
		delay += time.Duration(time.Now().UnixNano() % int64(delay/2+1))
		if ue.RetryAfter != "" {
			if seconds, pe := strconv.Atoi(ue.RetryAfter); pe == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			} else if date, pe := http.ParseTime(ue.RetryAfter); pe == nil {
				delay = time.Until(date)
				if delay < 0 {
					delay = 0
				}
			}
		}
		if delay > 30*time.Second {
			return v, e
		}
		if d, ok := ctx.Deadline(); ok && time.Until(d) <= delay {
			return v, e
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return v, context.Cause(ctx)
		case <-timer.C:
		}
		g.Metrics.Retries.Add(1)
	}
	return Response{}, last
}
func (g *Gateway) acquire(id string) (func(), error) {
	select {
	case g.slots <- struct{}{}:
	default:
		return nil, &Error{Control: true, Code: "overloaded", Message: "upstream concurrency limit reached", Status: 503, RetryAfter: "1"}
	}
	select {
	case g.providerSlots[id] <- struct{}{}:
		return sync.OnceFunc(func() { <-g.providerSlots[id]; <-g.slots }), nil
	default:
		<-g.slots
		return nil, &Error{Control: true, Code: "overloaded", Message: "provider concurrency limit reached", Status: 503, RetryAfter: "1"}
	}
}
func (g *Gateway) open(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	v := g.circuits[id]
	if !v.until.IsZero() {
		if time.Now().Before(v.until) {
			return true
		}
		g.circuits[id] = circuit{}
	}
	return false
}
func (g *Gateway) outcome(id string, kind OutcomeKind) {
	if g.options.CircuitFailures <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if kind == OutcomeSuccess {
		g.circuits[id] = circuit{}
		return
	}
	v := g.circuits[id]
	v.failures++
	if v.failures >= g.options.CircuitFailures {
		v.until = time.Now().Add(g.options.CircuitCooldown)
	}
	g.circuits[id] = v
}

// Reject negative or implausibly large per-attempt usage before accounting.
func validUsage(u Usage) bool {
	return u.InputTokens >= 0 && u.OutputTokens >= 0 && u.InputTokens <= 1<<40 && u.OutputTokens <= 1<<40
}

func ValidateResponse(r Request, v Response) error {
	if !validUsage(v.Usage) {
		return ProtocolError("invalid usage counters")
	}
	if len(v.Content) > 512 {
		return ProtocolError("too many content blocks")
	}
	names := map[string]bool{}
	for _, t := range r.Tools {
		names[t.Name] = true
	}
	ids := map[string]bool{}
	count := 0
	for _, b := range v.Content {
		switch b.Type {
		case "text":
			if len(b.Text) > 1<<20 {
				return ProtocolError("oversized text")
			}
		case "tool_call":
			count++
			if r.ToolChoice.Mode == "none" || !names[b.Name] || b.ID == "" || ids[b.ID] || len(b.Arguments) > 32768 {
				return ProtocolError("invalid tool invocation")
			}
			if r.ToolChoice.Mode == "named" && b.Name != r.ToolChoice.Name {
				return ProtocolError("forced tool mismatch")
			}
			var obj map[string]any
			if jsonUnmarshalObject(b.Arguments, &obj) != nil {
				return ProtocolError("invalid tool arguments")
			}
			ids[b.ID] = true
		default:
			return ProtocolError("unsupported response content")
		}
	}
	if count > 16 {
		return ProtocolError("too many calls")
	}
	if (r.ToolChoice.Mode == "required" || r.ToolChoice.Mode == "named") && count == 0 {
		return ProtocolError("forced tool not called")
	}
	if v.FinishReason != "stop" && v.FinishReason != "length" && v.FinishReason != "tool_calls" {
		return ProtocolError("missing finish reason")
	}
	if (count > 0) != (v.FinishReason == "tool_calls") {
		return ProtocolError("tool finish reason mismatch")
	}
	return nil
}
