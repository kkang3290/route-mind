package redis

import (
	"context"
	"sync"
	"time"

	"github.com/kkang3290/route-mind/pkg/llm"
)

type lease struct {
	owner              *Coordinator
	ctx                context.Context
	cancel             context.CancelCauseFunc
	kind, id           string
	keys               []string
	mu                 sync.Mutex
	until              time.Time
	failure            error
	finishing, started bool
	timer              *time.Timer
	stop, done         chan struct{}
	once               sync.Once
	result             error
}

func newLease(owner *Coordinator, parent context.Context, kind, id string, keys []string, started time.Time) *lease {
	ctx, cancel := context.WithCancelCause(parent)
	p := &lease{owner: owner, ctx: ctx, cancel: cancel, kind: kind, id: id, keys: keys, until: started.Add(time.Duration(owner.settings.LeaseTTLMS-owner.settings.OperationTimeoutMS) * time.Millisecond), stop: make(chan struct{}), done: make(chan struct{})}
	p.timer = time.AfterFunc(time.Until(p.until), p.expire)
	return p
}
func (p *lease) Context() context.Context { return p.ctx }
func (p *lease) Check() error             { p.mu.Lock(); defer p.mu.Unlock(); return p.checkLocked() }
func (p *lease) checkLocked() error {
	if p.failure != nil {
		return p.failure
	}
	if err := llm.ContextError(p.ctx); err != nil {
		return err
	}
	if !time.Now().Before(p.until) {
		p.failure = lostLease()
		p.cancel(p.failure)
		return p.failure
	}
	return nil
}
func (p *lease) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finishing {
		return
	}
	if delay := time.Until(p.until); delay > 0 {
		p.timer.Reset(delay)
		return
	}
	if p.failure == nil {
		p.failure = lostLease()
		p.owner.Metrics.LeaseLost.Add(1)
		p.cancel(p.failure)
	}
}
func (p *lease) fail(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finishing {
		return
	}
	if p.failure == nil {
		if cause := llm.ContextError(p.ctx); cause != nil {
			err = cause
		}
		p.failure = err
		p.cancel(err)
	}
}
func (p *lease) maintain() {
	defer p.owner.wg.Done()
	defer close(p.done)
	ticker := time.NewTicker(time.Duration(p.owner.settings.RenewIntervalMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-p.owner.ctx.Done():
			p.fail(unavailable())
			return
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if err := p.Check(); err != nil {
				return
			}
			started := time.Now()
			if err := p.owner.execute(p.ctx, p.keys, "renew", p.kind, p.id, llm.AttemptOutcome{Kind: llm.OutcomeNeutral}); err != nil {
				p.fail(err)
				return
			}
			p.mu.Lock()
			if p.finishing {
				p.mu.Unlock()
				return
			}
			// A successful late acknowledgement cannot revive our expired local window.
			if err := p.checkLocked(); err != nil {
				p.mu.Unlock()
				return
			}
			next := started.Add(time.Duration(p.owner.settings.LeaseTTLMS-p.owner.settings.OperationTimeoutMS) * time.Millisecond)
			if !time.Now().Before(next) {
				p.failure = lostLease()
				p.cancel(p.failure)
				p.mu.Unlock()
				return
			}
			p.until = next
			p.timer.Reset(time.Until(next))
			p.mu.Unlock()
		}
	}
}
func (p *lease) Complete(outcome llm.AttemptOutcome) error {
	p.once.Do(func() {
		p.mu.Lock()
		prior := p.checkLocked()
		p.finishing = true
		started := p.started
		p.timer.Stop()
		p.mu.Unlock()
		close(p.stop)
		if started {
			<-p.done
		}
		if prior != nil {
			outcome.Kind = llm.OutcomeNeutral
		}
		err := p.owner.execute(context.Background(), p.keys, "complete", p.kind, p.id, outcome)
		if prior != nil {
			err = prior
		} else if cause := llm.ContextError(p.ctx); cause != nil {
			err = cause
		} else if !time.Now().Before(p.until) {
			err = lostLease()
		}
		p.result = err
		// Gateway.After uses the caller context, not this disposed permit context.
		// Detaching the child also avoids accumulating children of long-lived callers.
		p.cancel(context.Canceled)
	})
	return p.result
}
