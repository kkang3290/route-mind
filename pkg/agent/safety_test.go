package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

func TestSmallModelCapacityIsNotBypassedByDecider(t *testing.T) {
	original := strings.Repeat("x", 70000)
	seen := ""
	chosen := ""
	en, _ := engine(t, fakeProvider{generate: func(r llm.Request) (llm.Response, error) {
		chosen = r.Model
		for _, m := range r.Messages {
			for _, b := range m.Content {
				if b.Text == original {
					seen = b.Text
				}
			}
		}
		return response("final"), nil
	}}, agent.DeciderFunc(func(context.Context, agent.View) (agent.Decision, error) {
		return agent.Decision{Action: "respond", Model: "small"}, nil
	}), nil)
	r := input()
	r.Messages = []llm.Message{llm.TextMessage("user", original)}
	run, e := agent.NewRun(r, agent.RunOptions{AllowedModels: []string{"small", "strong"}})
	if e != nil {
		t.Fatal(e)
	}
	if e = en.Advance(context.Background(), run, nil, nil); e != nil || chosen != "strong-native" || seen != original {
		t.Fatalf("selected=%s preserved=%d err=%v", chosen, len(seen), e)
	}
}
func TestLastStepReservesFinalAnswerAndHonorsToolNone(t *testing.T) {
	r := input()
	r.Tools = []llm.Tool{{Name: "client", Parameters: json.RawMessage(`{"type":"object"}`)}}
	r.ToolChoice = llm.ToolChoice{Mode: "none"}
	calls := 0
	en, _ := engine(t, fakeProvider{generate: func(req llm.Request) (llm.Response, error) {
		calls++
		if len(req.Tools) != 0 || req.MaxTokens != 512 {
			t.Fatal("final call lost caller constraints")
		}
		return response("final"), nil
	}}, agent.DeciderFunc(func(context.Context, agent.View) (agent.Decision, error) {
		return agent.Decision{Action: "work", Model: "strong"}, nil
	}), nil)
	limits := agent.DefaultLimits()
	limits.MaxSteps = 1
	run, e := agent.NewRun(r, agent.RunOptions{AllowedModels: []string{"strong"}, Limits: limits})
	if e != nil {
		t.Fatal(e)
	}
	if e = en.Advance(context.Background(), run, nil, nil); e != nil || calls != 1 || run.Status != "completed" {
		t.Fatalf("status=%s calls=%d err=%v", run.Status, calls, e)
	}
}
func TestDatabaseCheckpointFailureDoesNotStartOrReplayExternalCalls(t *testing.T) {
	for _, after := range []bool{false, true} {
		calls := 0
		en, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) { calls++; return response("final"), nil }}, agent.RuleDecider{}, nil)
		run, e := agent.NewRun(input(), agent.RunOptions{AllowedModels: []string{"small"}})
		if e != nil {
			t.Fatal(e)
		}
		e = en.Advance(context.Background(), run, nil, func(context.Context, *agent.Run) error {
			if !after || calls > 0 {
				return llm.Failure("storage_unavailable", "injected", 503)
			}
			return nil
		})
		want := 0
		if after {
			want = 1
		}
		if e == nil || calls != want || run.Output != nil {
			t.Fatalf("after=%v calls=%d err=%v", after, calls, e)
		}
	}
}

func TestTerminalSnapshotNeverRequestsMoreToolExecution(t *testing.T) {
	r := &agent.Run{Status: "failed", ToolCalls: []agent.ToolCall{{ID: "c", Executor: "client", Status: "waiting", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}
	if len(r.Snapshot().RequiredActions) != 0 {
		t.Fatal("failed run still instructs client to execute tools")
	}
}
func TestServerToolCancellationAndPanicDoNotClaimKnownOutcome(t *testing.T) {
	for _, mode := range []string{"cancel_checkpoint", "cancel_during_tool", "panic"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			executed := 0
			registry, _ := agent.NewToolRegistry([]agent.ServerTool{{Spec: agent.ToolSpec{Executor: "server", Version: "1", Definition: llm.Tool{Name: "sum", Parameters: json.RawMessage(`{"type":"object"}`)}}, Execute: func(context.Context, json.RawMessage) (string, error) {
				executed++
				if mode == "panic" {
					panic("after possible side effect")
				}
				cancel()
				return "late result", nil
			}}})
			spec, _ := registry.Spec("sum")
			en, _ := engine(t, fakeProvider{generate: func(llm.Request) (llm.Response, error) {
				return llm.Response{Content: []llm.Block{{Type: "tool_call", ID: "x", Name: "sum", Arguments: json.RawMessage(`{}`)}}, FinishReason: "tool_calls"}, nil
			}}, agent.RuleDecider{}, registry)
			run, e := agent.NewRun(input(), agent.RunOptions{AllowedModels: []string{"strong"}, Tools: []agent.ToolSpec{spec}})
			if e != nil {
				t.Fatal(e)
			}
			e = en.Advance(ctx, run, nil, func(_ context.Context, r *agent.Run) error {
				if mode == "cancel_checkpoint" && len(r.ToolCalls) > 0 && r.ToolCalls[0].Status == "running" {
					cancel()
				}
				return nil
			})
			if e == nil || len(run.ToolCalls) != 1 || run.ToolCalls[0].Status != "outcome_unknown" || run.Output != nil {
				t.Fatalf("error=%v run=%+v", e, run)
			}
			if mode == "cancel_checkpoint" && executed != 0 {
				t.Fatal("tool executed after cancellation during database checkpoint")
			}
		})
	}
}
