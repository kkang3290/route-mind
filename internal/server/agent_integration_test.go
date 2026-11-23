//go:build integration

package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/data/postgres"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/server"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/migrations"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
)

type nativeProvider struct{ calls atomic.Int64 }

func (p *nativeProvider) Generate(_ context.Context, r llm.Request) (llm.Response, error) {
	p.calls.Add(1)
	out := llm.Response{ID: "up", Model: r.Model, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 10, OutputTokens: 2}}
	if len(r.Tools) > 0 {
		out.Content = []llm.Block{{Type: "text", Text: "PRIVATE work"}}
		for i, t := range r.Tools {
			args := `{}`
			if t.Name == "sum" {
				args = `{"a":2,"b":3}`
			}
			out.Content = append(out.Content, llm.Block{Type: "tool_call", ID: []string{"up_a", "up_b"}[i], Name: t.Name, Arguments: json.RawMessage(args)})
		}
		out.FinishReason = "tool_calls"
		return out, nil
	}
	text := "final"
	for _, m := range r.Messages {
		for _, b := range m.Content {
			if b.Type == "tool_result" {
				text += "/" + b.Text
			}
		}
	}
	out.Content = []llm.Block{{Type: "text", Text: text}}
	return out, nil
}
func (p *nativeProvider) Stream(c context.Context, r llm.Request, s llm.Sink) (llm.Response, error) {
	out, e := p.Generate(c, r)
	if e == nil {
		e = s(llm.Event{Type: "text_delta", Text: out.Text()})
	}
	return out, e
}
func nativeFixture(t *testing.T) (func(agent.RunStore) http.Handler, *postgres.Store, *nativeProvider, *atomic.Int64) {
	t.Helper()
	dsn, pool, schema := testutil.Database(t)
	m, e := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Up(context.Background()); e != nil {
		t.Fatal(e)
	}
	m.Close()
	store, e := postgres.New(pool, schema, postgres.Options{})
	if e != nil {
		t.Fatal(e)
	}
	c, _ := llm.NewCatalog([]llm.Model{{ID: "small", Provider: "p", Upstream: "native", MaxInputBytes: 262144, MaxOutputTokens: 4096, Tools: true, ForcedTools: true, Stream: true}})
	p := &nativeProvider{}
	g, _ := llm.NewGateway(c, map[string]llm.Provider{"p": p}, llm.GatewayOptions{})
	executions := &atomic.Int64{}
	registry, _ := agent.NewToolRegistry([]agent.ServerTool{{Spec: agent.ToolSpec{Executor: "server", Version: "1", Definition: llm.Tool{Name: "sum", Parameters: json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}`)}}, Execute: func(context.Context, json.RawMessage) (string, error) { executions.Add(1); return "5", nil }}})
	factory := func(st agent.RunStore) http.Handler {
		en, _ := agent.NewEngine(c, g, agent.RuleDecider{}, registry)
		h, e := server.New(server.Options{Server: config.Server{Address: "127.0.0.1:0", RatePerMinute: 100000, Burst: 100}, Principals: []server.Credential{{Principal: config.Principal{ID: "alice", AllowAgent: true, Models: []string{"small"}, Providers: []string{"p"}, ServerTools: []string{"sum"}}, Key: "alice-key"}, {Principal: config.Principal{ID: "bob", AllowAgent: true, Models: []string{"small"}, Providers: []string{"p"}}, Key: "bob-key"}}}, server.Dependencies{Catalog: c, Gateway: g, Engine: en, Tools: registry, Store: st, Policy: config.Agent{Models: []string{"small"}, Providers: []string{"p"}, Limits: agent.DefaultLimits()}})
		if e != nil {
			t.Fatal(e)
		}
		return h
	}
	return factory, store, p, executions
}
func nativeRequest(h http.Handler, method, path, body, key, idem string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Idempotency-Key", idem)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

const nativeBody = `{"input":[{"role":"user","content":[{"type":"text","text":"add and ask client"}]}],"tools":[{"executor":"server","name":"sum"},{"executor":"client","name":"local","parameters":{"type":"object"}}],"tool_choice":{"mode":"required"}}`

func TestNativeMixedToolsSurviveServiceRecreationAndDeduplicate(t *testing.T) {
	factory, store, p, executions := nativeFixture(t)
	h := factory(store)
	w := nativeRequest(h, "POST", "/v1/agent/runs", nativeBody, "alice-key", "create-1")
	if w.Code != 201 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var run agent.Snapshot
	if e := json.Unmarshal(w.Body.Bytes(), &run); e != nil {
		t.Fatal(e)
	}
	if run.Status != "awaiting_tool_results" || len(run.RequiredActions) != 1 || executions.Load() != 1 || p.calls.Load() != 1 || strings.Contains(w.Body.String(), "PRIVATE") {
		t.Fatalf("snapshot=%s tools=%d calls=%d", w.Body, executions.Load(), p.calls.Load())
	}
	path := w.Header().Get("Location")
	if path != "/v1/agent/runs/"+run.ID {
		t.Fatal("missing resource Location")
	}
	w = nativeRequest(h, "POST", "/v1/agent/runs", nativeBody, "alice-key", "create-1")
	if w.Code != 200 || p.calls.Load() != 1 {
		t.Fatalf("replay=%d body=%s", w.Code, w.Body)
	}
	if got := nativeRequest(h, "GET", path, "", "bob-key", "").Code; got != 404 {
		t.Fatalf("owner isolation=%d", got)
	}
	h = factory(store)
	w = nativeRequest(h, "GET", path, "", "alice-key", "")
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	body, _ := json.Marshal(map[string]any{"revision": run.Revision, "results": []agent.ToolResult{{CallID: run.RequiredActions[0].ID, Content: "client answer"}}})
	w = nativeRequest(h, "POST", path+"/tool-results", string(body), "alice-key", "resume-1")
	if w.Code != 200 {
		t.Fatalf("resume status=%d body=%s", w.Code, w.Body)
	}
	var final agent.Snapshot
	_ = json.Unmarshal(w.Body.Bytes(), &final)
	if final.Status != "completed" || final.Output == nil || final.Output.Text() != "final/5/client answer" || final.Counters.Calls != 2 || executions.Load() != 1 || p.calls.Load() != 2 {
		t.Fatalf("final=%s tools=%d calls=%d", w.Body, executions.Load(), p.calls.Load())
	}
	w = nativeRequest(h, "POST", path+"/tool-results", string(body), "alice-key", "resume-1")
	if w.Code != 200 || p.calls.Load() != 2 || executions.Load() != 1 {
		t.Fatalf("resume replay=%d body=%s", w.Code, w.Body)
	}
}

func TestRunDeletionRequiresOwnerRevisionAndNoActiveOperation(t *testing.T) {
	factory, store, _, _ := nativeFixture(t)
	h := factory(store)
	w := nativeRequest(h, "POST", "/v1/agent/runs", nativeBody, "alice-key", "delete-create")
	if w.Code != 201 {
		t.Fatal(w.Body)
	}
	var run agent.Snapshot
	_ = json.Unmarshal(w.Body.Bytes(), &run)
	path := "/v1/agent/runs/" + run.ID
	remove := func(key, rev string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("DELETE", path, nil)
		r.Header.Set("Authorization", "Bearer "+key)
		r.Header.Set("If-Match", rev)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w = remove("bob-key", fmt.Sprintf("\"%d\"", run.Revision)); w.Code != 404 {
		t.Fatalf("foreign deletion=%d %s", w.Code, w.Body)
	}
	if w = remove("alice-key", "\"1\""); w.Code != 409 {
		t.Fatalf("stale deletion=%d %s", w.Code, w.Body)
	}
	if w = remove("alice-key", fmt.Sprintf("\"%d\"", run.Revision)); w.Code != 204 {
		t.Fatalf("owned deletion=%d %s", w.Code, w.Body)
	}
	if w = nativeRequest(h, "GET", path, "", "alice-key", ""); w.Code != 404 {
		t.Fatalf("deleted run visible: %s", w.Body)
	}
}

type finalFailure struct{ agent.RunStore }

func (s finalFailure) Save(c context.Context, r *agent.Run) error {
	if r.Status == "completed" {
		return llm.Failure("storage_unavailable", "injected checkpoint failure", 503)
	}
	return s.RunStore.Save(c, r)
}
func TestPostCommitStreamFailureIsCountedAsAnError(t *testing.T) {
	factory, store, _, _ := nativeFixture(t)
	h := factory(finalFailure{store})
	w := nativeRequest(h, "POST", "/v1/agent/runs", `{"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"stream":true}`, "alice-key", "metrics-fail")
	if !strings.Contains(w.Body.String(), "event: error") {
		t.Fatal("fault did not reach public stream")
	}
	metrics := nativeRequest(h, "GET", "/metrics", "", "alice-key", "")
	if !strings.Contains(metrics.Body.String(), "route_mind_http_errors_total 1\n") {
		t.Fatalf("HTTP 200 hid stream failure from telemetry: %s", metrics.Body)
	}
}

func TestNativeSSERequiresDurableFinalCheckpoint(t *testing.T) {
	factory, store, p, _ := nativeFixture(t)
	h := factory(finalFailure{store})
	w := nativeRequest(h, "POST", "/v1/agent/runs", `{"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"stream":true}`, "alice-key", "stream-fail")
	wire, _ := io.ReadAll(w.Body)
	if w.Code != 200 || !strings.Contains(string(wire), "final") || !strings.Contains(string(wire), "event: error") || strings.Contains(string(wire), "event: run.completed") || p.calls.Load() != 1 {
		t.Fatalf("status=%d wire=%s calls=%d", w.Code, wire, p.calls.Load())
	}
}
func TestNativeRejectsPrivilegeExpansionAndMissingIdempotency(t *testing.T) {
	factory, store, p, _ := nativeFixture(t)
	h := factory(store)
	for _, tc := range []struct {
		body, key string
		status    int
	}{{nativeBody, "", 400}, {strings.Replace(nativeBody, `"required"`, `"none"`, 1), "none", 201}, {`{"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"allowed_models":["auto"]}`, "invalid-model", 403}, {`{"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"limits":{"max_calls":9999}}`, "big-budget", 400}} {
		before := p.calls.Load()
		w := nativeRequest(h, "POST", "/v1/agent/runs", tc.body, "alice-key", tc.key)
		if w.Code != tc.status {
			t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body)
		}
		if tc.status >= 400 && p.calls.Load() != before {
			t.Fatal("invalid request reached upstream")
		}
	}
}
