//go:build redis_integration

package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/reliability"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/pkg/llm"
	goredis "github.com/redis/go-redis/v9"
)

func fixture(t *testing.T, change func(*Policy)) (*Coordinator, *Coordinator, *goredis.Client, reliability.Settings) {
	t.Helper()
	raw, admin, settings := testutil.Redis(t)
	p := Policy{RequestLimit: 4, PrincipalLimit: 2, RatePerMinute: 1, Burst: 2, UpstreamLimit: 4, ProviderLimit: 2, CircuitFailures: 1, CircuitCooldown: 100 * time.Millisecond, Providers: []ProviderIdentity{{"p", "openai", "https://p.example/v1"}, {"q", "anthropic", "https://q.example/v1"}}}
	if change != nil {
		change(&p)
	}
	a, err := New(context.Background(), raw, settings, p)
	if err != nil {
		t.Fatalf("Redis coordinator startup: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	b, err := New(context.Background(), raw, settings, p)
	if err != nil {
		t.Fatalf("second coordinator startup: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return a, b, admin, settings
}

func complete(t *testing.T, p llm.Permit, kind llm.OutcomeKind) {
	t.Helper()
	if err := p.Complete(llm.AttemptOutcome{Kind: kind, Dispatched: kind != llm.OutcomeNeutral}); err != nil {
		t.Fatal(err)
	}
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || llm.AsError(err).Code != code || !llm.AsError(err).Control {
		t.Fatalf("want control %s, got %v", code, err)
	}
}

func TestRedisPoolWaitIsBounded(t *testing.T) {
	a, _, _, s := fixture(t, nil)
	s.MaxConnections = 1
	limited, err := New(context.Background(), os.Getenv("TEST_REDIS_URL"), s, a.policy)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		_ = limited.client.BLPop(ctx, 2*time.Second, a.prefix+"test-blocker").Err()
	}()
	end := time.Now().Add(time.Second)
	for limited.client.PoolStats().IdleConns != 0 {
		if time.Now().After(end) {
			t.Fatal("pool was not occupied")
		}
		time.Sleep(time.Millisecond)
	}
	started := time.Now()
	_, err = limited.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "coordination_unavailable")
	if time.Since(started) > time.Second || limited.client.PoolStats().TotalConns > 1 {
		t.Fatal("pool wait or hard connection bound exceeded")
	}
	<-blocked
}

func TestRedisConcurrentCloseCancelsLeases(t *testing.T) {
	a, _, _, _ := fixture(t, nil)
	p, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- a.Close() }()
	}
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close retained lease maintenance")
		}
	}
	requireCode(t, p.Check(), "coordination_unavailable")
	_, err = a.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "coordination_unavailable")
}

func TestRedisPrincipalAndNamespaceIsolation(t *testing.T) {
	a, b, _, _ := fixture(t, func(p *Policy) { p.PrincipalLimit = 1 })
	isolated, _, _, _ := fixture(t, nil)
	alice, err := a.Admission().Acquire(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	defer complete(t, alice, llm.OutcomeNeutral)
	_, err = b.Admission().Acquire(context.Background(), "alice")
	requireCode(t, err, "rate_limited")
	for _, tc := range []struct {
		owner     *Coordinator
		principal string
	}{{b, "bob"}, {isolated, "alice"}} {
		p, err := tc.owner.Admission().Acquire(context.Background(), tc.principal)
		if err != nil {
			t.Fatal(err)
		}
		complete(t, p, llm.OutcomeNeutral)
	}
}

func TestRedisExpiredHalfOpenProbeCanBeReplaced(t *testing.T) {
	a, b, _, _ := fixture(t, nil)
	p, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeFailure)
	time.Sleep(120 * time.Millisecond)
	id := uniqueID()
	keys := a.keys("upstream", "p", id)
	err = a.execute(context.Background(), keys, "acquire", "upstream", id, llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "circuit_open")
	// No lease maintainer exists for the simulated crashed probe.
	time.Sleep(1900 * time.Millisecond)
	p, err = b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeSuccess)
}

func TestRedisNeutralProbeDoesNotDeclareRecovery(t *testing.T) {
	a, b, _, _ := fixture(t, nil)
	p, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeFailure)
	time.Sleep(120 * time.Millisecond)
	p, err = b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeNeutral)
	p, err = a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Complete(llm.AttemptOutcome{Kind: llm.OutcomeNeutral, Dispatched: true, RetryAfter: "1"}); err != nil {
		t.Fatal(err)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "circuit_open")
	time.Sleep(1100 * time.Millisecond)
	p, err = b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeSuccess)
}

func TestRedisExpiredRenewAndReleaseCannotAffectNewPermit(t *testing.T) {
	a, b, admin, _ := fixture(t, func(p *Policy) { p.UpstreamLimit = 1; p.ProviderLimit = 1 })
	id := uniqueID()
	keys := a.keys("upstream", "p", id)
	// Deliberately omit a maintainer to model an owner that crashed.
	err := a.execute(context.Background(), keys, "acquire", "upstream", id, llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1900 * time.Millisecond)
	p, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	err = a.execute(context.Background(), keys, "renew", "upstream", id, llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
	requireCode(t, err, "lease_lost")
	err = a.execute(context.Background(), keys, "complete", "upstream", id, llm.AttemptOutcome{Kind: llm.OutcomeSuccess, Dispatched: true})
	requireCode(t, err, "lease_lost")
	if p.Check() != nil || admin.ZCard(context.Background(), b.prefix+"upstream:global").Val() != 1 {
		t.Fatal("late old operation removed new permit")
	}
	complete(t, p, llm.OutcomeSuccess)
}

func TestRedisRateIsSharedBeyondLeaseTTL(t *testing.T) {
	a, b, _, _ := fixture(t, nil)
	p, err := a.Admission().Acquire(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeNeutral)
	p, err = b.Admission().Acquire(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeNeutral)
	_, err = a.Admission().Acquire(context.Background(), "alice")
	requireCode(t, err, "rate_limited")
	time.Sleep(1900 * time.Millisecond)
	_, err = b.Admission().Acquire(context.Background(), "alice")
	requireCode(t, err, "rate_limited")
	if err = a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRedisRequestConcurrencyIsSharedAndDenialsDoNotCharge(t *testing.T) {
	a, b, _, _ := fixture(t, func(p *Policy) { p.RequestLimit = 1; p.PrincipalLimit = 1 })
	first, err := a.Admission().Acquire(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Admission().Acquire(context.Background(), "bob")
	requireCode(t, err, "overloaded")
	complete(t, first, llm.OutcomeNeutral)
	// Both burst tokens must remain after the global capacity rejection.
	for range 2 {
		p, err := b.Admission().Acquire(context.Background(), "bob")
		if err != nil {
			t.Fatal(err)
		}
		complete(t, p, llm.OutcomeNeutral)
	}
	_, err = a.Admission().Acquire(context.Background(), "bob")
	requireCode(t, err, "rate_limited")
}

func TestRedisUpstreamLimitsAreSharedAndReleaseIsIdempotent(t *testing.T) {
	a, b, _, _ := fixture(t, func(p *Policy) { p.UpstreamLimit = 2; p.ProviderLimit = 1 })
	p, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "overloaded")
	q, err := b.Upstream().Acquire(context.Background(), "q")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeSuccess)
	newer, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, p, llm.OutcomeSuccess)
	_, err = a.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "overloaded")
	complete(t, newer, llm.OutcomeSuccess)
	complete(t, q, llm.OutcomeSuccess)
}

func TestRedisBreakerSingleProbeRejectsLateOldSuccess(t *testing.T) {
	a, b, _, _ := fixture(t, nil)
	old, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	failed, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, failed, llm.OutcomeFailure)
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "circuit_open")
	time.Sleep(120 * time.Millisecond)
	probe, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, old, llm.OutcomeSuccess)
	_, err = a.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "circuit_open")
	complete(t, probe, llm.OutcomeSuccess)
	next, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, next, llm.OutcomeSuccess)
}

func TestRedisRenewalKeepsLongRequestAlive(t *testing.T) {
	a, b, _, _ := fixture(t, func(p *Policy) { p.UpstreamLimit = 1; p.ProviderLimit = 1 })
	permit, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2100 * time.Millisecond)
	if err = permit.Check(); err != nil {
		t.Fatalf("long call lost renewed lease: %v", err)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "overloaded")
	complete(t, permit, llm.OutcomeSuccess)
	next, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, next, llm.OutcomeSuccess)
}

func TestRedisPolicyMismatchDoesNotResetSharedState(t *testing.T) {
	a, _, _, settings := fixture(t, nil)
	settings.LeaseTTLMS = 2100
	p := Policy{RequestLimit: 4, PrincipalLimit: 2, RatePerMinute: 1, Burst: 2, UpstreamLimit: 4, ProviderLimit: 2, CircuitFailures: 1, CircuitCooldown: 100 * time.Millisecond, Providers: []ProviderIdentity{{"p", "openai", "https://p.example/v1"}, {"q", "anthropic", "https://q.example/v1"}}}
	c, err := New(context.Background(), os.Getenv("TEST_REDIS_URL"), settings, p)
	if c != nil {
		_ = c.Close()
	}
	if err == nil {
		t.Fatal("incompatible coordinator was admitted")
	}
	// An invalid contender must not poison an already healthy coordinator.
	if err = a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}
