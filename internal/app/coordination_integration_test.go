//go:build integration && redis_integration

package app_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/app"
	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/demo"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/migrations"
)

func runtimePair(t *testing.T) (string, string, func()) {
	t.Helper()
	dsn, _, schema := testutil.Database(t)
	runner, err := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if err != nil {
		t.Fatal(err)
	}
	err = runner.Up(context.Background())
	runner.Close()
	if err != nil {
		t.Fatal(err)
	}
	raw, admin, settings := testutil.Redis(t)
	mock := httptest.NewServer(demo.Handler())
	t.Cleanup(mock.Close)
	runtime, err := config.Load("../../configs/config.yaml", func(key string) string {
		switch key {
		case "DATABASE_URL":
			return dsn
		case "GATEWAY_API_KEY":
			return "runtime-coordination-test"
		case "MOCK_API_KEY":
			return "local-mock-only"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime.Config.Server.Address = "127.0.0.1:0"
	runtime.Config.Server.RatePerMinute = 1
	runtime.Config.Server.Burst = 1
	runtime.Config.Coordination = settings
	runtime.RedisURL = raw
	runtime.Config.Database.Schema = schema
	for i := range runtime.Config.Providers {
		runtime.Config.Providers[i].BaseURL = mock.URL + "/v1"
	}
	launch := func() string {
		rt, err := app.Open(context.Background(), runtime, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(rt.Close)
		server := httptest.NewServer(rt.HTTP)
		t.Cleanup(server.Close)
		return server.URL
	}
	a, b := launch(), launch()
	return a, b, func() {
		if err := admin.Del(context.Background(), "route-mind:{"+settings.Namespace+"}:policy").Err(); err != nil {
			t.Fatal(err)
		}
	}
}
func runtimeCall(t *testing.T, base, path, body string) (int, string) {
	t.Helper()
	method := "GET"
	var reader io.Reader
	if body != "" {
		method = "POST"
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer runtime-coordination-test")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, string(data)
}
func TestRuntimeUsesSharedRedisAdmission(t *testing.T) {
	a, b, _ := runtimePair(t)
	body := `{"model":"small","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`
	if status, text := runtimeCall(t, a, "/v1/chat/completions", body); status != 200 {
		t.Fatalf("first status=%d body=%s", status, text)
	}
	if status, text := runtimeCall(t, b, "/v1/chat/completions", body); status != 429 {
		t.Fatalf("replica escaped shared rate: status=%d body=%s", status, text)
	}
}
func TestRuntimeReadinessTracksRedisPolicy(t *testing.T) {
	a, b, remove := runtimePair(t)
	if status, _ := runtimeCall(t, a, "/readyz", ""); status != 200 {
		t.Fatal(status)
	}
	remove()
	if status, _ := runtimeCall(t, b, "/readyz", ""); status != 503 {
		t.Fatalf("Redis policy loss ignored by readiness: %d", status)
	}
	if status, _ := runtimeCall(t, b, "/healthz", ""); status != 200 {
		t.Fatal(status)
	}
	if status, body := runtimeCall(t, b, "/metrics", ""); status != 200 || !strings.Contains(body, "route_mind_coordination_operations_total") {
		t.Fatalf("coordination metrics unavailable: status=%d", status)
	}
	if status, _ := runtimeCall(t, b, "/v1/models", ""); status != 503 {
		t.Fatalf("runtime silently fell back to local admission: %d", status)
	}
}
