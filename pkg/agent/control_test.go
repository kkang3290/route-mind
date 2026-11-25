package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

func TestEntryLeaseLossDuringToolPreservesUnknownOutcome(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lost", Status: 503}
	executed, models := 0, 0
	registry, err := agent.NewToolRegistry([]agent.ServerTool{{Spec: agent.ToolSpec{Executor: "server", Version: "1", Definition: llm.Tool{Name: "sum", Parameters: json.RawMessage(`{"type":"object"}`)}}, Execute: func(context.Context, json.RawMessage) (string, error) {
		executed++
		cancel(lost)
		return "possibly committed", nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := registry.Spec("sum")
	eng, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) {
		models++
		return llm.Response{Content: []llm.Block{{Type: "tool_call", ID: "first", Name: "sum", Arguments: json.RawMessage(`{}`)}, {Type: "tool_call", ID: "second", Name: "sum", Arguments: json.RawMessage(`{}`)}}, FinishReason: "tool_calls"}, nil
	}}, agent.RuleDecider{}, registry)
	run, err := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"strong"}, Tools: []agent.ToolSpec{spec}})
	if err != nil {
		t.Fatal(err)
	}
	err = eng.Advance(ctx, run, nil, nil)
	if !errors.Is(err, lost) || run.Status != "failed" || executed != 1 || models != 1 || len(run.ToolCalls) != 2 || run.ToolCalls[0].Status != "outcome_unknown" || run.Output != nil {
		t.Fatalf("err=%v status=%s tools=%d models=%d journal=%+v", err, run.Status, executed, models, run.ToolCalls)
	}
}

func TestDecisionControlErrorsCannotFallback(t *testing.T) {
	for _, code := range []string{"coordination_unavailable", "lease_lost", "coordination_policy_mismatch", "overloaded", "circuit_open"} {
		t.Run(code, func(t *testing.T) {
			denied := &llm.Error{Control: true, Code: code, Message: "control denied", Status: 503}
			calls := 0
			eng, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) { calls++; return response("must not execute"), nil }}, agent.DeciderFunc(func(context.Context, agent.View) (agent.Decision, error) { return agent.Decision{}, denied }), nil)
			run, err := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"small"}})
			if err != nil {
				t.Fatal(err)
			}
			err = eng.Advance(context.Background(), run, nil, nil)
			if !errors.Is(err, denied) || calls != 0 || run.Status != "failed" || run.Counters.Steps != 0 {
				t.Fatalf("err=%v calls=%d status=%s steps=%d", err, calls, run.Status, run.Counters.Steps)
			}
		})
	}
}

func TestLeaseCauseSurvivesCancelledCheckpoint(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lease lost", Status: 503}
	eng, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) { t.Fatal("unexpected dispatch"); return llm.Response{}, nil }}, agent.RuleDecider{}, nil)
	run, err := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	err = eng.Advance(ctx, run, nil, func(c context.Context, _ *agent.Run) error {
		if c.Err() == nil && context.Cause(ctx) == nil {
			cancel(lost)
			return c.Err()
		}
		return nil
	})
	if !errors.Is(err, lost) || run.Error == nil || run.Error.Code != "lease_lost" {
		t.Fatalf("err=%v detail=%+v", err, run.Error)
	}
}

func TestEntryLeaseCauseIsPersistedAsFailure(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	lost := &llm.Error{Control: true, Code: "lease_lost", Message: "lease lost", Status: 503}
	cancel(lost)
	eng, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) {
		t.Fatal("unexpected provider call")
		return llm.Response{}, nil
	}}, agent.RuleDecider{}, nil)
	run, err := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"small"}})
	if err != nil {
		t.Fatal(err)
	}
	err = eng.Advance(ctx, run, nil, nil)
	if !errors.Is(err, lost) || run.Status != "failed" || run.Error == nil || run.Error.Code != "lease_lost" {
		t.Fatalf("err=%v status=%s detail=%+v", err, run.Status, run.Error)
	}
}
