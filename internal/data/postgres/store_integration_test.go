//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kkang3290/route-mind/internal/data/postgres"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/migrations"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

func setup(t *testing.T) (*postgres.Store, *pgxpool.Pool, string) {
	t.Helper()
	dsn, pool, schema := testutil.Database(t)
	r, e := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Up(context.Background()); e != nil {
		t.Fatal(e)
	}
	r.Close()
	s, e := postgres.New(pool, schema, postgres.Options{})
	if e != nil {
		t.Fatal(e)
	}
	return s, pool, schema
}
func digest(s string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(s))) }
func newRun(t *testing.T) *agent.Run {
	t.Helper()
	r, e := agent.NewRun(llm.Request{Messages: []llm.Message{llm.TextMessage("user", "hello")}, MaxTokens: 128}, agent.RunOptions{Owner: "alice", AllowedModels: []string{"small"}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func waiting(t *testing.T, s *postgres.Store) *agent.Run {
	t.Helper()
	r, _, e := s.Create(context.Background(), newRun(t), "create-key", digest("create"))
	if e != nil {
		t.Fatal(e)
	}
	r.Counters.Steps = 1
	r.Status = "awaiting_tool_results"
	r.History = []llm.Message{{Role: "assistant", Content: []llm.Block{{Type: "tool_call", ID: "call_public", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}}
	r.ToolCalls = []agent.ToolCall{{ID: "call_public", ProviderID: "upstream-id", Name: "lookup", Arguments: json.RawMessage(`{}`), Executor: "client", Step: 1, Status: "waiting"}}
	r.Batch = []string{"call_public"}
	if e = s.Save(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestCreateOwnerIsolationIdempotencyAndAtomicRollback(t *testing.T) {
	s, pool, schema := setup(t)
	ctx := context.Background()
	r, duplicate, e := s.Create(ctx, newRun(t), "k", digest("body"))
	if e != nil || duplicate {
		t.Fatalf("duplicate=%v err=%v", duplicate, e)
	}
	other := newRun(t)
	copy, duplicate, e := s.Create(ctx, other, "k", digest("body"))
	if e != nil || !duplicate || copy.ID != r.ID {
		t.Fatalf("idempotency lost: %+v %v", copy, e)
	}
	if _, _, e = s.Create(ctx, newRun(t), "k", digest("different")); llm.AsError(e).Status != 409 {
		t.Fatalf("conflict not rejected: %v", e)
	}
	if _, e = s.Get(ctx, "bob", r.ID); llm.AsError(e).Status != 404 {
		t.Fatal("owner leak")
	}
	oldRevision := r.Revision
	r.ToolCalls = []agent.ToolCall{{ID: "bad", ProviderID: "p", Name: "x", Arguments: json.RawMessage(`[]`), Executor: "server", Step: 1, Status: "running"}}
	r.Status = "responding"
	if e = s.Save(ctx, r); e == nil {
		t.Fatal("invalid journal accepted")
	}
	stored, e := s.Get(ctx, "alice", r.ID)
	if e != nil || stored.Revision != oldRevision || stored.Status != "running" {
		t.Fatalf("partial transaction committed: %+v %v", stored, e)
	}
	var count int
	if e = pool.QueryRow(ctx, "SELECT count(*) FROM "+testutil.Qualified(schema, "agent_tool_calls")).Scan(&count); e != nil || count != 0 {
		t.Fatal("journal transaction not atomic")
	}
}
func TestConcurrentResumeAndRestartDoNotExecuteTwice(t *testing.T) {
	s, pool, schema := setup(t)
	r := waiting(t, s)
	restarted, e := postgres.New(pool, schema, postgres.Options{})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	type result struct {
		r   *agent.Run
		err error
	}
	answers := make(chan result, 2)
	for i := range 2 {
		wg.Go(func() {
			run, _, e := restarted.Resume(context.Background(), "alice", r.ID, r.Revision, fmt.Sprintf("resume-%d", i), digest("results"), []agent.ToolResult{{CallID: "call_public", Content: "found"}})
			answers <- result{run, e}
		})
	}
	wg.Wait()
	var accepted *agent.Run
	success := 0
	for range 2 {
		a := <-answers
		if a.err == nil {
			accepted = a.r
			success++
		} else if llm.AsError(a.err).Status != 409 {
			t.Fatal(a.err)
		}
	}
	if success != 1 || accepted.Counters.Steps != 1 || accepted.Status != "running" {
		t.Fatalf("accepted=%d state=%+v", success, accepted)
	}
	var stale agent.Run
	encoded, _ := json.Marshal(accepted)
	if e = json.Unmarshal(encoded, &stale); e != nil {
		t.Fatal(e)
	}
	accepted.Status = "completed"
	accepted.Output = &llm.Response{Content: []llm.Block{{Type: "text", Text: "done"}}, FinishReason: "stop"}
	if e = restarted.Save(context.Background(), accepted); e != nil {
		t.Fatal(e)
	}
	stale.Status = "failed"
	if e = restarted.Save(context.Background(), &stale); llm.AsError(e).Status != 409 {
		t.Fatal("stale owner overwrote terminal state")
	}
}
func TestUnknownCommitIsReconciledWithoutRepeatingTheOperation(t *testing.T) {
	s, pool, schema := setup(t)
	r, _, e := s.Create(context.Background(), newRun(t), "key", digest("body"))
	if e != nil {
		t.Fatal(e)
	}
	faulty, e := postgres.New(commitFaultPool{pool}, schema, postgres.Options{})
	if e != nil {
		t.Fatal(e)
	}
	r.Status = "completed"
	r.Output = &llm.Response{Content: []llm.Block{{Type: "text", Text: "saved"}}, FinishReason: "stop"}
	if e = faulty.Save(context.Background(), r); e != nil {
		t.Fatalf("committed checkpoint not reconciled: %v", e)
	}
	stored, e := s.Get(context.Background(), "alice", r.ID)
	if e != nil || stored.Status != "completed" || stored.Output.Text() != "saved" {
		t.Fatal("committed result missing")
	}
}

type commitFaultPool struct{ *pgxpool.Pool }

func (p commitFaultPool) BeginTx(c context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, e := p.Pool.BeginTx(c, o)
	if e != nil {
		return nil, e
	}
	return commitFaultTx{tx}, nil
}

type commitFaultTx struct{ pgx.Tx }

func (t commitFaultTx) Commit(c context.Context) error {
	if e := t.Tx.Commit(c); e != nil {
		return e
	}
	return fmt.Errorf("simulated lost commit acknowledgement")
}
func TestDeadlineCleanupFencesRunningToolsAndCascadesRetention(t *testing.T) {
	s, pool, schema := setup(t)
	r, _, e := s.Create(context.Background(), newRun(t), "key", digest("body"))
	if e != nil {
		t.Fatal(e)
	}
	r.ToolCalls = []agent.ToolCall{{ID: "c", ProviderID: "u", Name: "sum", Arguments: json.RawMessage(`{}`), Executor: "server", Step: 1, Status: "running"}}
	if e = s.Save(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(context.Background(), "UPDATE "+testutil.Qualified(schema, "agent_runs")+" SET deadline_at=now()-interval '1 second' WHERE public_id=$1", r.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Reap(context.Background(), 16); e != nil {
		t.Fatal(e)
	}
	stored, e := s.Get(context.Background(), "alice", r.ID)
	if e != nil || stored.Status != "failed" || stored.ToolCalls[0].Status != "outcome_unknown" {
		t.Fatalf("deadline result=%+v err=%v", stored, e)
	}
	if e = s.Save(context.Background(), r); llm.AsError(e).Status != 409 {
		t.Fatal("expired operation still owns run")
	}
	if _, e = pool.Exec(context.Background(), "UPDATE "+testutil.Qualified(schema, "agent_runs")+" SET expires_at=now()-interval '1 millisecond' WHERE public_id=$1", r.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Reap(context.Background(), 16); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get(context.Background(), "alice", r.ID); llm.AsError(e).Status != 404 {
		t.Fatal("expired terminal run retained")
	}
	var n int
	for _, table := range []string{"agent_tool_calls", "request_idempotency"} {
		if e = pool.QueryRow(context.Background(), "SELECT count(*) FROM "+testutil.Qualified(schema, table)).Scan(&n); e != nil || n != 0 {
			t.Fatalf("cascade failed: table=%s count=%d err=%v", table, n, e)
		}
	}
}
