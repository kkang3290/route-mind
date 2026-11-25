//go:build redis_integration

package redis

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/service"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	goredis "github.com/redis/go-redis/v9"
)

type wireHook struct {
	run func(context.Context, goredis.Cmder, goredis.ProcessHook) error
}

func (h wireHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h wireHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error { return h.run(ctx, cmd, next) }
}
func (h wireHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}
func scriptAction(cmd goredis.Cmder) string {
	args := cmd.Args()
	if (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(args) > 9 {
		if s, ok := args[9].(string); ok {
			return s
		}
	}
	return ""
}

type measuredProvider struct{ calls *atomic.Int64 }

func (p measuredProvider) Generate(context.Context, llm.Request) (llm.Response, error) {
	p.calls.Add(1)
	return llm.Response{Content: []llm.Block{{Type: "text", Text: "ok"}}, FinishReason: "stop"}, nil
}
func (p measuredProvider) Stream(ctx context.Context, r llm.Request, s llm.Sink) (llm.Response, error) {
	out, err := p.Generate(ctx, r)
	if err == nil {
		err = s(llm.Event{Type: "text_delta", Text: "ok"})
	}
	return out, err
}

type interruptedStream struct{ measuredProvider }

func (p interruptedStream) Stream(ctx context.Context, _ llm.Request, sink llm.Sink) (llm.Response, error) {
	p.calls.Add(1)
	if err := sink(llm.Event{Type: "text_delta", Text: "partial"}); err != nil {
		return llm.Response{}, err
	}
	<-ctx.Done()
	return llm.Response{}, context.Cause(ctx)
}
func TestRedisLeaseLossAfterStreamOutputNeverEmitsSuccess(t *testing.T) {
	a, _, _, _ := fixture(t, nil)
	a.client.AddHook(wireHook{run: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil && scriptAction(cmd) == "renew" {
			return io.ErrUnexpectedEOF
		}
		return err
	}})
	calls := &atomic.Int64{}
	catalog, _ := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 4096, MaxOutputTokens: 256, Stream: true}})
	gateway, err := llm.NewGateway(catalog, map[string]llm.Provider{"p": interruptedStream{measuredProvider{calls}}}, llm.GatewayOptions{Controller: a.Upstream(), Retries: 2})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := agent.NewEngine(catalog, gateway, agent.RuleDecider{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h, err := service.New(service.Options{Admission: a.Admission(), Principals: []service.Credential{{Principal: config.Principal{ID: "alice", Models: []string{"small"}, Providers: []string{"p"}}, Key: "stream-fixture-only"}}}, service.Dependencies{Catalog: catalog, Gateway: gateway, Engine: engine, Policy: config.Agent{Models: []string{"small"}, Providers: []string{"p"}}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	client := &http.Client{Timeout: 4 * time.Second}
	r, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"small","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	r.Header.Set("Authorization", "Bearer stream-fixture-only")
	r.Header.Set("Content-Type", "application/json")
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body) // EOF or reset is valid after output.
	if response.StatusCode != 200 || !strings.Contains(string(body), "partial") || strings.Contains(string(body), "[DONE]") || calls.Load() != 1 || gateway.Metrics.Retries.Load() != 0 {
		t.Fatalf("status=%d body=%s calls=%d retries=%d", response.StatusCode, body, calls.Load(), gateway.Metrics.Retries.Load())
	}
}

func TestRedisWriteThenScriptErrorNeverDispatches(t *testing.T) {
	a, b, admin, _ := fixture(t, func(p *Policy) { p.UpstreamLimit = 1; p.ProviderLimit = 1 })
	var injected atomic.Bool
	a.client.AddHook(wireHook{run: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		if scriptAction(cmd) == "acquire" && injected.CompareAndSwap(false, true) {
			// Real Redis executes the actual production allocation prefix, then a
			// wrong-type operation fails after both allocations/TTLs were written.
			// Lua isolates this prefix but must NOT be assumed to roll it back.
			source := strings.Replace(coordinationSource, "  touch(KEYS[2]); touch(KEYS[3])", "  touch(KEYS[2]); touch(KEYS[3]); redis.call('INCR', KEYS[2])", 1)
			args := append([]any{"eval", source}, cmd.Args()[2:]...)
			return next(ctx, goredis.NewCmd(ctx, args...))
		}
		return next(ctx, cmd)
	}})
	catalog, _ := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 4096, MaxOutputTokens: 256}})
	calls := &atomic.Int64{}
	g, err := llm.NewGateway(catalog, map[string]llm.Provider{"p": measuredProvider{calls}}, llm.GatewayOptions{Controller: a.Upstream(), Retries: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.Generate(context.Background(), llm.Request{Model: "small", MaxTokens: 32, Messages: []llm.Message{llm.TextMessage("user", "hi")}})
	requireCode(t, err, "coordination_unavailable")
	if calls.Load() != 0 || g.Metrics.Attempts.Load() != 0 {
		t.Fatal("script error dispatched upstream")
	}
	if n := admin.ZCard(context.Background(), a.prefix+"upstream:global").Val(); n != 1 {
		t.Fatalf("fault did not follow a real write: %d", n)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "overloaded")
	time.Sleep(1900 * time.Millisecond)
	permit, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, permit, llm.OutcomeSuccess)
}

func TestRedisCommitWithLostAcknowledgementNeverDispatches(t *testing.T) {
	a, b, admin, _ := fixture(t, func(p *Policy) { p.UpstreamLimit = 1; p.ProviderLimit = 1 })
	var injected atomic.Bool
	a.client.AddHook(wireHook{run: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil && scriptAction(cmd) == "acquire" && injected.CompareAndSwap(false, true) {
			return io.ErrUnexpectedEOF
		}
		return err
	}})
	catalog, _ := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 4096, MaxOutputTokens: 256}})
	calls := &atomic.Int64{}
	gateway, err := llm.NewGateway(catalog, map[string]llm.Provider{"p": measuredProvider{calls}}, llm.GatewayOptions{Controller: a.Upstream(), Retries: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gateway.Generate(context.Background(), llm.Request{Model: "small", MaxTokens: 32, Messages: []llm.Message{llm.TextMessage("user", "hi")}})
	requireCode(t, err, "coordination_unavailable")
	if calls.Load() != 0 || gateway.Metrics.Attempts.Load() != 0 {
		t.Fatal("uncertain permission was used to dispatch")
	}
	if n, err := admin.ZCard(context.Background(), a.prefix+"upstream:global").Result(); err != nil || n != 1 {
		t.Fatalf("fault did not occur after allocation: count=%d err=%v", n, err)
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "overloaded")
	time.Sleep(1900 * time.Millisecond)
	permit, err := b.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	complete(t, permit, llm.OutcomeSuccess)
}

func TestRedisWatchdogExpiresDuringStalledRenewAcknowledgement(t *testing.T) {
	a, _, _, _ := fixture(t, nil)
	blocked, entered := make(chan struct{}), make(chan struct{})
	var injected atomic.Bool
	a.client.AddHook(wireHook{run: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if err == nil && scriptAction(cmd) == "renew" && injected.CompareAndSwap(false, true) {
			close(entered)
			<-blocked
		}
		return err
	}})
	permit, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	// Always unblock the intentionally stalled external boundary before cleanup.
	unblock := func() {
		select {
		case <-blocked:
		default:
			close(blocked)
		}
	}
	defer unblock()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("renewal never reached boundary")
	}
	select {
	case <-permit.Context().Done():
		requireCode(t, context.Cause(permit.Context()), "lease_lost")
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog depended on stalled Redis operation")
	}
	unblock()
	requireCode(t, permit.Complete(llm.AttemptOutcome{Kind: llm.OutcomeSuccess, Dispatched: true}), "lease_lost")
}

func TestRedisPolicyDoesNotMergeEscapedProviderPaths(t *testing.T) {
	a, _, _, settings := fixture(t, nil)
	p := a.policy
	p.Providers = append([]ProviderIdentity(nil), p.Providers...)
	p.Providers[0].Endpoint = "https://p.example/v1%2F"
	other, err := New(context.Background(), os.Getenv("TEST_REDIS_URL"), settings, p)
	if other != nil {
		_ = other.Close()
	}
	requireCode(t, err, "coordination_policy_mismatch")
}

func TestRedisPolicyLossCancelsLeaseWithoutRecreatingPolicy(t *testing.T) {
	a, b, admin, _ := fixture(t, nil)
	permit, err := a.Upstream().Acquire(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	if err = admin.Del(context.Background(), a.prefix+"policy").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-permit.Context().Done():
		requireCode(t, context.Cause(permit.Context()), "coordination_policy_mismatch")
	case <-time.After(time.Second):
		t.Fatal("lease continued without policy")
	}
	_, err = b.Upstream().Acquire(context.Background(), "p")
	requireCode(t, err, "coordination_policy_mismatch")
	if exists, _ := admin.Exists(context.Background(), a.prefix+"policy").Result(); exists != 0 {
		t.Fatal("active coordinator recreated policy")
	}
	_ = permit.Complete(llm.AttemptOutcome{Kind: llm.OutcomeNeutral})
}

func TestRedisWrongKeyTypeDoesNotPartiallyAllocate(t *testing.T) {
	a, _, admin, _ := fixture(t, nil)
	keys := a.keys("request", "alice", "unused")
	// Scope bucket is a hash, not a string. Its corruption is detected before ZADD.
	if err := admin.Set(context.Background(), keys[3], "wrong-type", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	_, err := a.Admission().Acquire(context.Background(), "alice")
	requireCode(t, err, "coordination_unavailable")
	if n, _ := admin.ZCard(context.Background(), keys[1]).Result(); n != 0 {
		t.Fatal("type rejection left an active global allocation")
	}
}

func TestRedisCompletedPermissionKeysAreReclaimed(t *testing.T) {
	a, _, admin, _ := fixture(t, func(p *Policy) { p.Burst = 100; p.RatePerMinute = 60000 })
	for range 50 {
		permit, err := a.Admission().Acquire(context.Background(), "alice")
		if err != nil {
			t.Fatal(err)
		}
		complete(t, permit, llm.OutcomeNeutral)
	}
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		var cursor uint64
		var retained []string
		for {
			keys, next, err := admin.Scan(context.Background(), cursor, a.prefix+"*", 100).Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				if !strings.HasSuffix(key, ":policy") && !strings.HasSuffix(key, ":test-owner") {
					retained = append(retained, key)
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(retained) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("completed permission metadata or active sets retained without a bound")
}
