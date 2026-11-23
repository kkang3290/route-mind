//go:build integration

package app_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/app"
	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/migrations"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/protocol"
)

func TestApplicationRequiresExplicitMigrationAndAssemblesLLMDecider(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		req, _, e := protocol.DecodeRequest("openai", b)
		if e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		text := "assembled answer"
		if req.Model == "decision-native" {
			text = `{"action":"respond","model_id":"small","instruction":"Answer the user."}`
		}
		out := llm.Response{ID: "test", Model: req.Model, Content: []llm.Block{{Type: "text", Text: text}}, FinishReason: "stop", Usage: llm.Usage{Known: true, InputTokens: 2, OutputTokens: 3}}
		b, _ = protocol.EncodeResponse("openai", out)
		_, _ = w.Write(b)
	}))
	defer up.Close()
	text := fmt.Sprintf(`server:
  address: 127.0.0.1:0
principals:
  - id: alice
    key_env: API_KEY
    allow_agent: true
providers:
  - id: mock
    format: openai
    base_url: %s
    key_env: MOCK_KEY
    allow_http: true
models:
  - {id: decision, provider: mock, upstream: decision-native, max_input_bytes: 262144, max_output_tokens: 4096}
  - {id: small, provider: mock, upstream: small-native, max_input_bytes: 262144, max_output_tokens: 4096, stream: true}
agent:
  decision_model: decision
  models: [small]
  providers: [mock]
database:
  schema: %s
  allow_insecure: true
`, up.URL, schema)
	path := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(path, []byte(text), 0600); e != nil {
		t.Fatal(e)
	}
	cfg, e := config.Load(path, func(name string) string {
		switch name {
		case "DATABASE_URL":
			return dsn
		case "API_KEY":
			return "test-api-key"
		case "MOCK_KEY":
			return "test-mock-key"
		}
		return ""
	})
	if e != nil {
		t.Fatal(e)
	}
	if runtime, e := app.Open(context.Background(), cfg, nil); e == nil {
		runtime.Close()
		t.Fatal("application silently initialized schema")
	}
	var exists bool
	if e = pool.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", schema+".schema_migrations").Scan(&exists); e != nil || exists {
		t.Fatalf("read-only startup created metadata: %v %v", exists, e)
	}
	m, e := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	if e = m.Up(context.Background()); e != nil {
		t.Fatal(e)
	}
	m.Close()
	runtime, e := app.Open(context.Background(), cfg, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer runtime.Close()
	r := httptest.NewRequest("POST", "/v1/agent/runs", strings.NewReader(`{"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer test-api-key")
	r.Header.Set("Idempotency-Key", "assemble")
	w := httptest.NewRecorder()
	runtime.HTTP.ServeHTTP(w, r)
	if w.Code != 201 || !strings.Contains(w.Body.String(), "assembled answer") || !strings.Contains(w.Body.String(), `"calls":2`) {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
}
