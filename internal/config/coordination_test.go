package config_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/config"
)

const redisConfig = `coordination:
  mode: redis
  namespace: integration
  url_env: REDIS_URL
  allow_insecure_local: true
gateway:
  retries: 0
  circuit_failures: 0
`

func TestShippedConfigurationsLoadWithoutNetwork(t *testing.T) {
	for _, name := range []string{"config.yaml", "redis.example.yaml", "providers.example.yaml"} {
		t.Run(name, func(t *testing.T) {
			_, err := config.Load("../../configs/"+name, func(key string) string {
				switch key {
				case "DATABASE_URL":
					return "postgres://app:configuration-test-only@127.0.0.1/route_mind_test?sslmode=verify-full"
				case "REDIS_URL":
					return "redis://127.0.0.1:6379/0"
				case "APP_ENV":
					return "development"
				default:
					return "configuration-test-only"
				}
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRedisConfigurationResolvesSecretsWithoutSerializingThem(t *testing.T) {
	for _, raw := range []string{"redis://127.0.0.1:6379/0", "rediss://service:redis-secret-sentinel@redis.example:6379/0"} {
		t.Run(strings.Split(raw, ":")[0], func(t *testing.T) {
			runtime, err := load(t, valid+redisConfig, func(k string) string {
				if k == "REDIS_URL" {
					return raw
				}
				return environment(k)
			})
			if err != nil {
				t.Fatalf("valid redis configuration rejected: %v", err)
			}
			data, err := json.Marshal(runtime)
			if err != nil || strings.Contains(string(data), "redis-secret-sentinel") || strings.Contains(string(data), raw) {
				t.Fatalf("runtime leaked Redis URL: %v", err)
			}
		})
	}
}

func TestRedisProductionRejectsEmptyPassword(t *testing.T) {
	_, err := load(t, valid+redisConfig, func(k string) string {
		switch k {
		case "REDIS_URL":
			return "rediss://service:@redis.example:6379/0"
		case "APP_ENV":
			return "production"
		}
		return environment(k)
	})
	if err == nil || !strings.Contains(err.Error(), "production Redis requires authentication") {
		t.Fatalf("production Redis did not reject the empty password: %v", err)
	}
}

func TestCoordinationDefaultsAndExplicitZero(t *testing.T) {
	defaults, err := load(t, valid, environment)
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Config.Coordination.Mode != "local" || defaults.Config.Gateway.Retries != 1 || defaults.Config.Gateway.CircuitFailures != 5 {
		t.Fatalf("unexpected defaults: %+v", defaults.Config.Gateway)
	}
	explicit, err := load(t, valid+redisConfig, func(k string) string {
		if k == "REDIS_URL" {
			return "redis://127.0.0.1:6379/0"
		}
		return environment(k)
	})
	if err != nil {
		t.Fatal(err)
	}
	if explicit.Config.Gateway.Retries != 0 || explicit.Config.Gateway.CircuitFailures != 0 {
		t.Fatal("explicit zero was replaced with defaults")
	}
}

func TestRedisConfigurationRejectsUnsafeTransportAndOptions(t *testing.T) {
	for _, tc := range []struct{ name, url, change, replacement string }{
		{"remote plaintext", "redis://redis.example:6379/0", "", ""},
		{"hostname plaintext", "redis://localhost:6379/0", "", ""},
		{"disable TLS verification", "rediss://redis.example:6379/0?skip_verify=true", "", ""},
		{"override retries", "rediss://redis.example:6379/0?max_retries=3", "", ""},
		{"unknown scheme", "http://127.0.0.1:6379/0", "", ""},
		{"missing namespace", "redis://127.0.0.1:6379/0", "namespace: integration", "namespace: ''"},
		{"hash tag injection", "redis://127.0.0.1:6379/0", "namespace: integration", "namespace: '{other}'"},
		{"insecure not explicit", "redis://127.0.0.1:6379/0", "allow_insecure_local: true", "allow_insecure_local: false"},
		{"invalid renewal ratio", "redis://127.0.0.1:6379/0", "namespace: integration", "namespace: integration\n  lease_ttl_ms: 3000\n  renew_interval_ms: 2000"},
		{"negative retries", "redis://127.0.0.1:6379/0", "retries: 0", "retries: -1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := valid + redisConfig
			if tc.change != "" {
				text = strings.Replace(text, tc.change, tc.replacement, 1)
			}
			_, err := load(t, text, func(k string) string {
				if k == "REDIS_URL" {
					return tc.url
				}
				return environment(k)
			})
			if err == nil {
				t.Fatal("unsafe Redis configuration accepted")
			}
			if strings.Contains(err.Error(), tc.url) {
				t.Fatal("error exposed URL")
			}
		})
	}
}
