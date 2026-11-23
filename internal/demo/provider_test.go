package demo_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/demo"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

func TestOfflineDemoExercisesThreeRealExecutionSteps(t *testing.T) {
	up := httptest.NewServer(demo.Handler())
	defer up.Close()
	p, e := openai.New(transport.Options{BaseURL: up.URL + "/v1", AllowHTTP: true, APIKey: "local-mock-only"})
	if e != nil {
		t.Fatal(e)
	}
	models := []llm.Model{}
	for _, name := range []string{"small", "strong", "decision"} {
		models = append(models, llm.Model{ID: name, Provider: "mock", Upstream: "mock-" + name, MaxInputBytes: 262144, MaxOutputTokens: 4096, Stream: true, Tools: true, ForcedTools: true})
	}
	c, _ := llm.NewCatalog(models)
	g, _ := llm.NewGateway(c, map[string]llm.Provider{"mock": p}, llm.GatewayOptions{})
	d, _ := agent.NewLLMDecider(g, "decision", 256)
	en, _ := agent.NewEngine(c, g, d, nil)
	run, e := agent.NewRun(llm.Request{Messages: []llm.Message{llm.TextMessage("user", "extract analyze summarize")}}, agent.RunOptions{AllowedModels: []string{"small", "strong"}, AllowedProviders: []string{"mock"}})
	if e != nil {
		t.Fatal(e)
	}
	var public strings.Builder
	e = en.Advance(context.Background(), run, func(v llm.Event) error {
		if v.Type == "text_delta" {
			public.WriteString(v.Text)
		}
		return nil
	}, nil)
	if e != nil || run.Counters.Steps != 3 || run.Counters.Calls != 6 || run.Status != "completed" || strings.Contains(public.String(), "PRIVATE") {
		t.Fatalf("run=%+v text=%s err=%v", run.Snapshot(), public.String(), e)
	}
	chosen := []string{}
	for _, s := range run.Steps {
		if s.Kind != "decision" {
			chosen = append(chosen, s.Model)
		}
	}
	if strings.Join(chosen, ",") != "small,strong,small" {
		t.Fatal(chosen)
	}
}
