package demo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/demo"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

type scenarioStats struct{ Calls, Active, Peak int }

func readStats(t *testing.T, base string) scenarioStats {
	t.Helper()
	r, _ := http.NewRequest("GET", base+"/_stats", nil)
	r.Header.Set("Authorization", "Bearer local-mock-only")
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var stats scenarioStats
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&stats) != nil {
		t.Fatalf("stats unavailable: %d", response.StatusCode)
	}
	return stats
}
func TestScenarioFaultsAreBoundedAndMeasured(t *testing.T) {
	h, err := demo.NewScenario(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	p, err := openai.New(transport.Options{BaseURL: server.URL + "/v1", AllowHTTP: true, APIKey: "local-mock-only"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	request := llm.Request{Model: "mock-small", MaxTokens: 32, Messages: []llm.Message{llm.TextMessage("user", "hi")}}
	if _, err = p.Generate(context.Background(), request); err == nil {
		t.Fatal("first request did not fail")
	}
	if _, err = p.Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stats := readStats(t, server.URL)
	if stats.Calls != 2 || stats.Active != 0 || stats.Peak != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	r := httptest.NewRequest("GET", "/_stats", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("unauthenticated mock statistics exposed")
	}
}
func TestScenarioDelayHonorsCancellation(t *testing.T) {
	h, err := demo.NewScenario(time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"mock-small","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`))
		r.Header.Set("Authorization", "Bearer local-mock-only")
		r.Header.Set("Content-Type", "application/json")
		response, _ := http.DefaultClient.Do(r)
		if response != nil {
			response.Body.Close()
		}
	}()
	end := time.Now().Add(time.Second)
	for readStats(t, server.URL).Active != 1 {
		if time.Now().After(end) {
			t.Fatal("delayed request never became active")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled request remained blocked")
	}
	for readStats(t, server.URL).Active != 0 {
		if time.Now().After(end) {
			t.Fatal("mock retained canceled upstream request")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestScenarioRejectsUnboundedControls(t *testing.T) {
	for _, pair := range []struct {
		delay    time.Duration
		failures int
	}{{-1, 0}, {61 * time.Second, 0}, {0, -1}, {0, 1001}} {
		if _, err := demo.NewScenario(pair.delay, pair.failures); err == nil {
			t.Fatal("invalid mock scenario accepted")
		}
	}
}
