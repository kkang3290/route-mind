package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/config"
)

const valid = `server:
  address: 127.0.0.1:8000
principals:
  - id: alice
    key_env: GATEWAY_KEY
    allow_agent: true
    models: [small]
    providers: [p]
providers:
  - id: p
    format: openai
    base_url: https://provider.example/v1
    key_env: PROVIDER_KEY
models:
  - id: small
    provider: p
    upstream: native
    max_input_bytes: 262144
    max_output_tokens: 4096
    stream: true
agent:
  decision_model: small
  models: [small]
  providers: [p]
database:
  url_env: DATABASE_URL
  schema: public
  allow_insecure: true
`

func environment(name string) string {
	switch name {
	case "GATEWAY_KEY":
		return "gateway-test-key"
	case "PROVIDER_KEY":
		return "provider-test-key"
	case "DATABASE_URL":
		return "postgres://user:local-only@127.0.0.1/route_mind_test?sslmode=disable"
	}
	return ""
}
func load(t *testing.T, text string, env func(string) string) (*config.Runtime, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if e := os.WriteFile(path, []byte(text), 0600); e != nil {
		t.Fatal(e)
	}
	return config.Load(path, env)
}
func TestSecureConfigurationAndSecretReferences(t *testing.T) {
	r, e := load(t, valid, environment)
	if e != nil {
		t.Fatal(e)
	}
	if r.Config.Server.RequestSeconds <= 1 || r.ProviderKeys["p"] != "provider-test-key" || r.Keys["alice"] != "gateway-test-key" {
		t.Fatal("runtime config not resolved")
	}
	if _, e = load(t, valid, func(name string) string {
		if name == "GATEWAY_KEY" {
			return ""
		}
		return environment(name)
	}); e == nil || strings.Contains(e.Error(), "provider-test-key") {
		t.Fatal("missing gateway credential not rejected safely")
	}
}
func TestServerToolAllowlistUsesRegisteredTools(t *testing.T) {
	for _, names := range []string{"sum", "lookup", "sum, lookup", "unknown"} {
		t.Run(names, func(t *testing.T) {
			text := strings.Replace(valid, "    providers: [p]", "    providers: [p]\n    server_tools: ["+names+"]", 1)
			_, e := load(t, text, environment)
			if (e != nil) != (names == "unknown") {
				t.Fatalf("server_tools=%s: %v", names, e)
			}
		})
	}
}

func TestConfigurationRejectsRecursiveRoutingAndUnknownFields(t *testing.T) {
	for _, text := range []string{strings.Replace(valid, "decision_model: small", "decision_model: auto", 1), strings.Replace(valid, "address: 127.0.0.1:8000", "address: 0.0.0.0:8000\n  allow_insecure_local: true", 1), valid + "typo_option: true\n", strings.Replace(valid, "id: small", "id: auto", 1)} {
		if _, e := load(t, text, environment); e == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
