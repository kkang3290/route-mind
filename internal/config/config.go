// Package config loads an operator-owned file; secrets are resolved only through
// explicit environment-variable references, never supplied by API callers.
package config

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/kkang3290/route-mind/internal/tools"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"gopkg.in/yaml.v3"
)

type Server struct {
	Address                string `yaml:"address"`
	AllowInsecureLocal     bool   `yaml:"allow_insecure_local"`
	MaxBodyBytes           int    `yaml:"max_body_bytes"`
	RequestSeconds         int    `yaml:"request_seconds"`
	WriteSeconds           int    `yaml:"write_seconds"`
	MaxConcurrent          int    `yaml:"max_concurrent"`
	PerPrincipalConcurrent int    `yaml:"per_principal_concurrent"`
	RatePerMinute          int    `yaml:"rate_per_minute"`
	Burst                  int    `yaml:"burst"`
}
type Principal struct {
	ID          string   `yaml:"id"`
	KeyEnv      string   `yaml:"key_env"`
	AllowAgent  bool     `yaml:"allow_agent"`
	Models      []string `yaml:"models"`
	Providers   []string `yaml:"providers"`
	ServerTools []string `yaml:"server_tools"`
}
type Provider struct {
	ID        string `yaml:"id"`
	Format    string `yaml:"format"`
	BaseURL   string `yaml:"base_url"`
	KeyEnv    string `yaml:"key_env"`
	AllowHTTP bool   `yaml:"allow_http"`
}
type Agent struct {
	DecisionModel string       `yaml:"decision_model"`
	Models        []string     `yaml:"models"`
	Providers     []string     `yaml:"providers"`
	Limits        agent.Limits `yaml:"limits"`
}
type Database struct {
	URLEnv              string `yaml:"url_env"`
	Schema              string `yaml:"schema"`
	AllowInsecure       bool   `yaml:"allow_insecure"`
	MaxConnections      int32  `yaml:"max_connections"`
	MaxRuns             int    `yaml:"max_runs"`
	MaxRunsPerPrincipal int    `yaml:"max_runs_per_principal"`
}
type Config struct {
	Server     Server      `yaml:"server"`
	Principals []Principal `yaml:"principals"`
	Providers  []Provider  `yaml:"providers"`
	Models     []llm.Model `yaml:"models"`
	Agent      Agent       `yaml:"agent"`
	Database   Database    `yaml:"database"`
}
type Runtime struct {
	Config       Config
	Catalog      *llm.Catalog
	Keys         map[string]string `json:"-" yaml:"-"`
	ProviderKeys map[string]string `json:"-" yaml:"-"`
	DatabaseURL  string            `json:"-" yaml:"-"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)
var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func defaultInt(v *int, d int) {
	if *v == 0 {
		*v = d
	}
}
func secret(env func(string) string, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("credential environment reference is required")
	}
	v := env(name)
	if strings.TrimSpace(v) == "" || len(v) > 8192 || strings.ContainsAny(v, "\r\n") {
		return "", fmt.Errorf("credential environment variable %s is missing or invalid", name)
	}
	return v, nil
}
func Load(path string, env func(string) string) (*Runtime, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, fmt.Errorf("open configuration: %w", e)
	}
	defer f.Close()
	data, e := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if e != nil || len(data) > 1<<20 {
		return nil, fmt.Errorf("configuration unreadable or exceeds 1 MiB")
	}
	var c Config
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if e = decoder.Decode(&c); e != nil {
		return nil, fmt.Errorf("invalid configuration YAML: %w", e)
	}
	var extra any
	if e = decoder.Decode(&extra); e != io.EOF {
		return nil, fmt.Errorf("configuration must contain exactly one YAML document")
	}
	if env == nil {
		env = os.Getenv
	}
	s := &c.Server
	if s.Address == "" {
		s.Address = "127.0.0.1:8000"
	}
	host, _, e := net.SplitHostPort(s.Address)
	if e != nil {
		return nil, fmt.Errorf("invalid HTTP listen address")
	}
	ip := net.ParseIP(host)
	if s.AllowInsecureLocal && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("unauthenticated development mode requires a literal loopback listen address")
	}
	defaultInt(&s.MaxBodyBytes, 1<<20)
	defaultInt(&s.RequestSeconds, 180)
	defaultInt(&s.WriteSeconds, 15)
	defaultInt(&s.MaxConcurrent, 32)
	defaultInt(&s.PerPrincipalConcurrent, 4)
	defaultInt(&s.RatePerMinute, 60)
	defaultInt(&s.Burst, 10)
	if s.MaxBodyBytes < 1024 || s.MaxBodyBytes > 2<<20 || s.RequestSeconds < 2 || s.RequestSeconds > 3600 || s.WriteSeconds < 1 || s.WriteSeconds > 300 || s.MaxConcurrent < 1 || s.MaxConcurrent > 4096 || s.PerPrincipalConcurrent < 1 || s.PerPrincipalConcurrent > s.MaxConcurrent || s.RatePerMinute < 1 || s.Burst < 1 {
		return nil, fmt.Errorf("invalid server resource limits")
	}
	catalog, e := llm.NewCatalog(c.Models)
	if e != nil {
		return nil, e
	}
	r := &Runtime{Catalog: catalog, Keys: map[string]string{}, ProviderKeys: map[string]string{}}
	providerIDs := map[string]bool{}
	for _, p := range c.Providers {
		if !identifier.MatchString(p.ID) || providerIDs[p.ID] || (p.Format != "openai" && p.Format != "anthropic") {
			return nil, fmt.Errorf("invalid or duplicate provider")
		}
		providerIDs[p.ID] = true
		if p.BaseURL != "" {
			u, e := url.Parse(p.BaseURL)
			if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return nil, fmt.Errorf("invalid provider base URL")
			}
			if u.Scheme != "https" {
				ip := net.ParseIP(u.Hostname())
				if u.Scheme != "http" || !p.AllowHTTP || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
					return nil, fmt.Errorf("provider requires HTTPS; plaintext is explicit loopback development only")
				}
			}
		}
		key, e := secret(env, p.KeyEnv)
		if e != nil {
			return nil, e
		}
		r.ProviderKeys[p.ID] = key
	}
	for _, m := range c.Models {
		if !providerIDs[m.Provider] {
			return nil, fmt.Errorf("model references an unknown provider")
		}
	}
	c.Agent.Limits = agent.NormalizeLimits(c.Agent.Limits)
	if c.Agent.Limits.RetentionSeconds < c.Agent.Limits.DeadlineSeconds || c.Agent.Limits.DeadlineSeconds > 3600 || c.Agent.Limits.MaxSteps > 64 || c.Agent.Limits.MaxCalls > 256 || c.Agent.Limits.MaxHistoryBytes > 1<<20 {
		return nil, fmt.Errorf("invalid agent limits")
	}
	decision, ok := catalog.Get(c.Agent.DecisionModel)
	if !ok || c.Agent.DecisionModel == "auto" || decision.MaxOutputTokens < c.Agent.Limits.DecisionTokens {
		return nil, fmt.Errorf("a concrete, capable decision model is required")
	}
	if len(c.Agent.Models) == 0 || len(c.Agent.Providers) == 0 {
		return nil, fmt.Errorf("agent model and provider allowlists are required")
	}
	for _, p := range c.Agent.Providers {
		if !providerIDs[p] {
			return nil, fmt.Errorf("unknown agent provider")
		}
	}
	if !slices.Contains(c.Agent.Providers, decision.Provider) {
		return nil, fmt.Errorf("decision provider is outside the agent data-flow allowlist")
	}
	for _, id := range c.Agent.Models {
		m, ok := catalog.Get(id)
		if !ok || !slices.Contains(c.Agent.Providers, m.Provider) {
			return nil, fmt.Errorf("invalid agent execution candidate")
		}
	}
	registry, e := tools.Registry()
	if e != nil {
		return nil, e
	}
	ids := map[string]bool{}
	keys := map[string]bool{}
	if len(c.Principals) == 0 && !s.AllowInsecureLocal {
		return nil, fmt.Errorf("at least one authenticated principal is required")
	}
	if len(c.Principals) > 128 {
		return nil, fmt.Errorf("too many principals")
	}
	for i := range c.Principals {
		p := &c.Principals[i]
		if !identifier.MatchString(p.ID) || ids[p.ID] || p.ID == "local-development" {
			return nil, fmt.Errorf("invalid or duplicate principal")
		}
		ids[p.ID] = true
		key, e := secret(env, p.KeyEnv)
		if e != nil {
			return nil, e
		}
		if keys[key] {
			return nil, fmt.Errorf("API keys must map to exactly one principal")
		}
		keys[key] = true
		r.Keys[p.ID] = key
		if len(p.Models) == 0 {
			p.Models = append([]string(nil), c.Agent.Models...)
		}
		if len(p.Providers) == 0 {
			p.Providers = append([]string(nil), c.Agent.Providers...)
		}
		for _, id := range p.Models {
			m, ok := catalog.Get(id)
			if !ok || !slices.Contains(p.Providers, m.Provider) {
				return nil, fmt.Errorf("principal references an unavailable model")
			}
		}
		for _, id := range p.Providers {
			if !providerIDs[id] {
				return nil, fmt.Errorf("principal references an unknown provider")
			}
		}
		if p.AllowAgent && !slices.Contains(p.Providers, decision.Provider) {
			return nil, fmt.Errorf("principal cannot send agent context to the decision provider")
		}
		for _, name := range p.ServerTools {
			if _, ok := registry.Spec(name); !ok {
				return nil, fmt.Errorf("unknown built-in server tool")
			}
		}
	}
	d := &c.Database
	if d.URLEnv == "" {
		d.URLEnv = "DATABASE_URL"
	}
	if d.Schema == "" {
		d.Schema = "public"
	}
	if !schemaName.MatchString(d.Schema) {
		return nil, fmt.Errorf("invalid database schema")
	}
	if d.MaxConnections == 0 {
		d.MaxConnections = 16
	}
	defaultInt(&d.MaxRuns, 4096)
	defaultInt(&d.MaxRunsPerPrincipal, 128)
	if d.MaxConnections < 1 || d.MaxConnections > 256 || d.MaxRuns < 1 || d.MaxRunsPerPrincipal < 1 {
		return nil, fmt.Errorf("invalid database limits")
	}
	r.DatabaseURL, e = secret(env, d.URLEnv)
	if e != nil {
		return nil, e
	}
	u, e := url.Parse(r.DatabaseURL)
	if e != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" {
		return nil, fmt.Errorf("database URL must be a PostgreSQL URL")
	}
	if !d.AllowInsecure && u.Query().Get("sslmode") != "verify-full" {
		return nil, fmt.Errorf("database TLS requires sslmode=verify-full unless explicitly configured for insecure development")
	}
	if d.AllowInsecure && env("APP_ENV") == "production" {
		return nil, fmt.Errorf("insecure database transport is forbidden in production")
	}
	r.Config = c
	return r, nil
}
