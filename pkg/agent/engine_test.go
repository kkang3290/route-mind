package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

type fakeProvider struct {
	generate func(llm.Request) (llm.Response, error)
}

func (p fakeProvider) Generate(_ context.Context, r llm.Request) (llm.Response, error) {
	return p.generate(r)
}
func (p fakeProvider) Stream(c context.Context, r llm.Request, s llm.Sink) (llm.Response, error) {
	v, e := p.Generate(c, r)
	if e != nil {
		return v, e
	}
	for _, b := range v.Content {
		if b.Type == "text" {
			if e = s(llm.Event{Type: "text_delta", Text: b.Text}); e != nil {
				return v, e
			}
		}
	}
	return v, nil
}
func response(text string) llm.Response {
	return llm.Response{Content: []llm.Block{{Type: "text", Text: text}}, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 3, OutputTokens: 2}}
}
func catalog(t *testing.T) *llm.Catalog {
	t.Helper()
	c, e := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "small-native", MaxInputBytes: 65536, MaxOutputTokens: 4096, Stream: true, CostRank: 1, QualityTier: 1}, {ID: "strong", Provider: "p", Upstream: "strong-native", MaxInputBytes: 262144, MaxOutputTokens: 4096, Stream: true, Tools: true, ForcedTools: true, CostRank: 10, QualityTier: 3}, {ID: "decision", Provider: "p", Upstream: "decision-native", MaxInputBytes: 262144, MaxOutputTokens: 4096, CostRank: 1}})
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func engine(t *testing.T, p fakeProvider, d agent.Decider, tools *agent.ToolRegistry) (*agent.Engine, *llm.Gateway) {
	t.Helper()
	c := catalog(t)
	g, e := llm.NewGateway(c, map[string]llm.Provider{"p": p}, llm.GatewayOptions{})
	if e != nil {
		t.Fatal(e)
	}
	eng, e := agent.NewEngine(c, g, d, tools)
	if e != nil {
		t.Fatal(e)
	}
	return eng, g
}
func input() llm.Request {
	return llm.Request{Model: "auto", Messages: []llm.Message{llm.TextMessage("user", "solve the task")}, MaxTokens: 512}
}
func TestRealEngineChangesModelAfterEachObservationWithoutLeakingWork(t *testing.T) {
	selected := []string{}
	decisions := 0
	d := agent.DeciderFunc(func(_ context.Context, v agent.View) (agent.Decision, error) {
		decisions++
		switch decisions {
		case 1:
			return agent.Decision{Action: "work", Model: "small", Instruction: "extract facts"}, nil
		case 2:
			if !strings.Contains(string(mustJSON(v.History)), "PRIVATE_EXTRACT") {
				t.Fatal("missing first observation")
			}
			return agent.Decision{Action: "work", Model: "strong", Instruction: "analyze facts"}, nil
		default:
			if !strings.Contains(string(mustJSON(v.History)), "PRIVATE_ANALYSIS") {
				t.Fatal("missing analysis")
			}
			return agent.Decision{Action: "respond", Model: "small"}, nil
		}
	})
	eng, _ := engine(t, fakeProvider{generate: func(r llm.Request) (llm.Response, error) {
		selected = append(selected, r.Model)
		if len(selected) == 1 {
			return response("PRIVATE_EXTRACT"), nil
		}
		if len(selected) == 2 {
			return response("PRIVATE_ANALYSIS"), nil
		}
		if len(r.Tools) != 0 {
			t.Fatal("final response still has tools")
		}
		return response("FINAL"), nil
	}}, d, nil)
	run, e := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"small", "strong"}})
	if e != nil {
		t.Fatal(e)
	}
	var public string
	e = eng.Advance(context.Background(), run, func(v llm.Event) error {
		if v.Type == "text_delta" {
			public += v.Text
		}
		return nil
	}, nil)
	if e != nil || run.Status != "completed" || public != "FINAL" || strings.Join(selected, ",") != "small-native,strong-native,small-native" {
		t.Fatalf("status=%s public=%q models=%v error=%v", run.Status, public, selected, e)
	}
	if run.Counters.Calls != 3 || run.TotalUsage.OutputTokens != 6 {
		t.Fatalf("unmetered calls: %+v", run.Counters)
	}
	snapshot := string(mustJSON(run.Snapshot()))
	if strings.Contains(snapshot, "PRIVATE_") {
		t.Fatal("snapshot leaked internal work")
	}
}
func TestMixedToolBatchPausesAndResumesWithoutReexecutingServerTool(t *testing.T) {
	executed := 0
	definition := llm.Tool{Name: "sum", Parameters: json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"],"additionalProperties":false}`)}
	tools, e := agent.NewToolRegistry([]agent.ServerTool{{Spec: agent.ToolSpec{Executor: "server", Definition: definition, Version: "v1"}, Execute: func(context.Context, json.RawMessage) (string, error) { executed++; return "3", nil }}})
	if e != nil {
		t.Fatal(e)
	}
	d := agent.DeciderFunc(func(_ context.Context, v agent.View) (agent.Decision, error) {
		if len(v.History) == 0 {
			return agent.Decision{Action: "work", Model: "strong"}, nil
		}
		return agent.Decision{Action: "respond", Model: "small"}, nil
	})
	eng, _ := engine(t, fakeProvider{generate: func(r llm.Request) (llm.Response, error) {
		if len(r.Tools) > 0 {
			return llm.Response{Content: []llm.Block{{Type: "text", Text: "PRIVATE_WORK"}, {Type: "tool_call", ID: "same", Name: "sum", Arguments: json.RawMessage(`{"a":3}`)}, {Type: "tool_call", ID: "other", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}}, FinishReason: "tool_calls", Usage: llm.Usage{Known: true}}, nil
		}
		return response("done"), nil
	}}, d, tools)
	spec, _ := tools.Spec("sum")
	run, e := agent.NewRun(input(), agent.RunOptions{Owner: "alice", AllowedModels: []string{"small", "strong"}, Tools: []agent.ToolSpec{spec, {Executor: "client", Definition: llm.Tool{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}}}})
	if e != nil {
		t.Fatal(e)
	}
	var journalBeforeExecution bool
	e = eng.Advance(context.Background(), run, nil, func(_ context.Context, r *agent.Run) error {
		for _, c := range r.ToolCalls {
			if c.Name == "sum" && c.Status == "running" && executed == 0 {
				journalBeforeExecution = true
			}
		}
		return nil
	})
	snapshot := run.Snapshot()
	if e != nil || run.Status != "awaiting_tool_results" || executed != 1 || len(snapshot.RequiredActions) != 1 || !journalBeforeExecution {
		t.Fatalf("run=%+v executed=%d error=%v", snapshot, executed, e)
	}
	if strings.Contains(string(mustJSON(snapshot)), "PRIVATE_WORK") {
		t.Fatal("waiting snapshot leaked work")
	}
	if e = agent.ApplyResults(run, []agent.ToolResult{{CallID: "unknown", Content: "bad"}}); e == nil {
		t.Fatal("unknown tool result accepted")
	}
	if e = agent.ApplyResults(run, []agent.ToolResult{{CallID: snapshot.RequiredActions[0].ID, Content: "found"}}); e != nil {
		t.Fatal(e)
	}
	if e = eng.Advance(context.Background(), run, nil, nil); e != nil || run.Status != "completed" || executed != 1 {
		t.Fatalf("status=%s executed=%d error=%v", run.Status, executed, e)
	}
}
func TestForcedClientToolCannotBeBypassedByRespondDecision(t *testing.T) {
	r := input()
	r.Tools = []llm.Tool{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}}
	r.ToolChoice = llm.ToolChoice{Mode: "named", Name: "lookup"}
	eng, _ := engine(t, fakeProvider{generate: func(r llm.Request) (llm.Response, error) {
		if r.ToolChoice.Mode != "named" {
			t.Fatal("forced tool constraint removed")
		}
		return llm.Response{Content: []llm.Block{{Type: "text", Text: "PRIVATE"}, {Type: "tool_call", ID: "c", Name: "lookup", Arguments: json.RawMessage(`{}`)}}, FinishReason: "tool_calls"}, nil
	}}, agent.DeciderFunc(func(context.Context, agent.View) (agent.Decision, error) {
		return agent.Decision{Action: "respond", Model: "small"}, nil
	}), nil)
	run, e := agent.NewRun(r, agent.RunOptions{Owner: "alice", Compatibility: true, AllowedModels: []string{"small", "strong"}})
	if e != nil {
		t.Fatal(e)
	}
	if e = eng.Advance(context.Background(), run, nil, nil); e != nil {
		t.Fatal(e)
	}
	if run.Output == nil || run.Output.FinishReason != "tool_calls" || len(run.Output.Content) != 1 || run.Output.Content[0].Type != "tool_call" {
		t.Fatalf("forced tool not delivered: %+v", run.Output)
	}
}
func TestLLMDeciderIsMeteredAndInvalidOutputFallsBackWithoutRepairLoop(t *testing.T) {
	c := catalog(t)
	decisionCalls := 0
	g, e := llm.NewGateway(c, map[string]llm.Provider{"p": fakeProvider{generate: func(r llm.Request) (llm.Response, error) {
		if r.Model == "decision-native" {
			decisionCalls++
			return response(`{"action":"respond","model_id":"auto","instruction":""}`), nil
		}
		return response("fallback"), nil
	}}}, llm.GatewayOptions{})
	if e != nil {
		t.Fatal(e)
	}
	d, e := agent.NewLLMDecider(g, "decision", 256)
	if e != nil {
		t.Fatal(e)
	}
	eng, e := agent.NewEngine(c, g, d, nil)
	if e != nil {
		t.Fatal(e)
	}
	run, e := agent.NewRun(input(), agent.RunOptions{Owner: "a", AllowedModels: []string{"small", "strong"}})
	if e != nil {
		t.Fatal(e)
	}
	if e = eng.Advance(context.Background(), run, nil, nil); e != nil || decisionCalls != 1 || run.Counters.Calls != 2 || run.Output.Text() != "fallback" {
		t.Fatalf("decision=%d counters=%+v error=%v", decisionCalls, run.Counters, e)
	}
	if _, e = agent.NewLLMDecider(g, "auto", 256); e == nil {
		t.Fatal("recursive decider allowed")
	}
}
func TestSchemaBlocksRemoteReferencesAndInvalidToolArguments(t *testing.T) {
	for _, schema := range []string{`{"$ref":"https://example.com/schema"}`, `{"type":"object","properties":{"a":{"$ref":"file:///etc/passwd"}}}`} {
		if _, e := agent.NewToolRegistry([]agent.ServerTool{{Spec: agent.ToolSpec{Executor: "server", Version: "v1", Definition: llm.Tool{Name: "unsafe", Parameters: json.RawMessage(schema)}}, Execute: func(context.Context, json.RawMessage) (string, error) { return "", nil }}}); e == nil {
			t.Fatal("remote reference accepted")
		}
	}
}
func mustJSON(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}
