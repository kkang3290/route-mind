package agent_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

// External programs need only public packages, no Kratos or PostgreSQL types.
// This executable example uses a LOCAL HTTP fixture, never a paid provider.
func ExampleEngine() {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"fixture","model":"native","choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
	}))
	defer upstream.Close()
	provider, e := openai.New(transport.Options{BaseURL: upstream.URL, APIKey: "fixture-only", AllowHTTP: true})
	if e != nil {
		panic(e)
	}
	defer provider.Close()
	catalog, e := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "local", Upstream: "native", MaxInputBytes: 65536, MaxOutputTokens: 4096}})
	if e != nil {
		panic(e)
	}
	gateway, e := llm.NewGateway(catalog, map[string]llm.Provider{"local": provider}, llm.GatewayOptions{})
	if e != nil {
		panic(e)
	}
	custom := agent.DeciderFunc(func(_ context.Context, v agent.View) (agent.Decision, error) {
		return agent.Decision{Action: "respond", Model: "small", Instruction: "Answer concisely."}, nil
	})
	engine, e := agent.NewEngine(catalog, gateway, custom, nil)
	if e != nil {
		panic(e)
	}
	run, e := agent.NewRun(llm.Request{Messages: []llm.Message{llm.TextMessage("user", "hello")}}, agent.RunOptions{AllowedModels: []string{"small"}, AllowedProviders: []string{"local"}})
	if e != nil {
		panic(e)
	}
	if e = engine.Advance(context.Background(), run, nil, nil); e != nil {
		panic(e)
	}
	fmt.Println(run.Status, run.Output.Text(), run.Counters.Calls)
	// Output: completed answer 1
}
