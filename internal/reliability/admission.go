package reliability

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type bucket struct {
	tokens float64
	at     time.Time
	active int
}
type localAdmission struct {
	mu                     sync.Mutex
	buckets                map[string]*bucket
	slots                  chan struct{}
	principal, rate, burst int
}
type localPermit struct {
	ctx     context.Context
	release func()
}

func (p localPermit) Context() context.Context          { return p.ctx }
func (p localPermit) Check() error                      { return llm.ContextError(p.ctx) }
func (p localPermit) Complete(llm.AttemptOutcome) error { p.release(); return nil }

// NewLocalAdmission preserves the single-process token bucket and active slots.
func NewLocalAdmission(max, principal, rate, burst int) (Admission, error) {
	if max < 1 || principal < 1 || principal > max || rate < 1 || burst < 1 {
		return nil, errors.New("invalid local admission limits")
	}
	return &localAdmission{buckets: map[string]*bucket{}, slots: make(chan struct{}, max), principal: principal, rate: rate, burst: burst}, nil
}
func (a *localAdmission) Acquire(ctx context.Context, id string) (llm.Permit, error) {
	if err := llm.ContextError(ctx); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	b := a.buckets[id]
	if b == nil {
		b = &bucket{tokens: float64(a.burst), at: now}
		a.buckets[id] = b
	}
	b.tokens += now.Sub(b.at).Seconds() * float64(a.rate) / 60
	if b.tokens > float64(a.burst) {
		b.tokens = float64(a.burst)
	}
	b.at = now
	if b.tokens < 1 || b.active >= a.principal {
		return nil, &llm.Error{Control: true, Code: "rate_limited", Message: "principal rate or concurrency limit reached", Status: 429, RetryAfter: "1"}
	}
	select {
	case a.slots <- struct{}{}:
	default:
		return nil, &llm.Error{Control: true, Code: "overloaded", Message: "active request capacity reached", Status: 503, RetryAfter: "1"}
	}
	b.tokens--
	b.active++
	return localPermit{ctx, sync.OnceFunc(func() { <-a.slots; a.mu.Lock(); b.active--; a.mu.Unlock() })}, nil
}
