// Package app assembles the production service. Schema changes are deliberately
// absent: deploy cmd/migrate with a separate DDL identity before starting it.
package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"

	kratoshttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kkang3290/route-mind/internal/config"
	"github.com/kkang3290/route-mind/internal/data/postgres"
	rediscoordination "github.com/kkang3290/route-mind/internal/data/redis"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/server"
	"github.com/kkang3290/route-mind/internal/tools"
	"github.com/kkang3290/route-mind/pkg/agent"
	"github.com/kkang3290/route-mind/pkg/llm"
	"github.com/kkang3290/route-mind/pkg/llm/providers/anthropic"
	"github.com/kkang3290/route-mind/pkg/llm/providers/openai"
	"github.com/kkang3290/route-mind/pkg/llm/providers/transport"
)

type Runtime struct {
	HTTP         *kratoshttp.Server
	pool         *pgxpool.Pool
	providers    []*transport.Client
	coordination *rediscoordination.Coordinator
	cancel       context.CancelFunc
	done         chan struct{}
	once         sync.Once
}

func Open(parent context.Context, r *config.Runtime, logger *slog.Logger) (runtime *Runtime, resultErr error) {
	if r == nil || r.Catalog == nil {
		return nil, errors.New("validated runtime configuration is required")
	}
	ctx, cancel := context.WithCancel(parent)
	runtime = &Runtime{cancel: cancel, done: make(chan struct{})}
	defer func() {
		if resultErr != nil {
			cancel()
			if runtime.coordination != nil {
				_ = runtime.coordination.Close()
			}
			if runtime.pool != nil {
				runtime.pool.Close()
			}
			for _, p := range runtime.providers {
				p.Close()
			}
		}
	}()
	cfg := r.Config
	db, e := pgxpool.ParseConfig(r.DatabaseURL)
	if e != nil {
		return runtime, errors.New("invalid database connection configuration")
	}
	db.MaxConns = cfg.Database.MaxConnections
	db.MinConns = 0
	db.ConnConfig.ConnectTimeout = 5 * time.Second
	db.MaxConnIdleTime = 5 * time.Minute
	db.MaxConnLifetime = 30 * time.Minute
	db.ConnConfig.RuntimeParams["search_path"] = cfg.Database.Schema
	db.ConnConfig.RuntimeParams["application_name"] = "route-mind"
	db.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	db.ConnConfig.RuntimeParams["lock_timeout"] = "1000"
	db.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	runtime.pool, e = pgxpool.NewWithConfig(ctx, db)
	if e != nil {
		return runtime, errors.New("database connection pool initialization failed")
	}
	startup, stop := context.WithTimeout(ctx, 10*time.Second)
	e = migration.CheckSchema(startup, runtime.pool, cfg.Database.Schema)
	stop()
	if e != nil {
		return runtime, errors.New("database schema unavailable or incompatible; inspect migration status and run the separate migration job")
	}
	store, e := postgres.New(runtime.pool, cfg.Database.Schema, postgres.Options{MaxRuns: cfg.Database.MaxRuns, MaxRunsPerPrincipal: cfg.Database.MaxRunsPerPrincipal})
	if e != nil {
		return runtime, e
	}
	providers := map[string]llm.Provider{}
	for _, p := range cfg.Providers {
		opts := transport.Options{BaseURL: p.BaseURL, APIKey: r.ProviderKeys[p.ID], AllowHTTP: p.AllowHTTP}
		var client *transport.Client
		if p.Format == "openai" {
			client, e = openai.New(opts)
		} else {
			client, e = anthropic.New(opts)
		}
		if e != nil {
			return runtime, e
		}
		providers[p.ID] = client
		runtime.providers = append(runtime.providers, client)
	}
	gatewayOptions := llm.GatewayOptions{MaxConcurrent: cfg.Gateway.MaxConcurrent, ProviderConcurrent: cfg.Gateway.ProviderConcurrent, Retries: cfg.Gateway.Retries, CircuitFailures: cfg.Gateway.CircuitFailures, CircuitCooldown: time.Duration(cfg.Gateway.CircuitCooldownSeconds) * time.Second}
	if cfg.Coordination.Mode == "redis" {
		identities := make([]rediscoordination.ProviderIdentity, 0, len(cfg.Providers))
		for _, p := range cfg.Providers {
			identities = append(identities, rediscoordination.ProviderIdentity{ID: p.ID, Format: p.Format, Endpoint: p.BaseURL})
		}
		policy := rediscoordination.Policy{RequestLimit: cfg.Server.MaxConcurrent, PrincipalLimit: cfg.Server.PerPrincipalConcurrent, RatePerMinute: cfg.Server.RatePerMinute, Burst: cfg.Server.Burst, UpstreamLimit: cfg.Gateway.MaxConcurrent, ProviderLimit: cfg.Gateway.ProviderConcurrent, CircuitFailures: cfg.Gateway.CircuitFailures, CircuitCooldown: gatewayOptions.CircuitCooldown, Providers: identities}
		runtime.coordination, e = rediscoordination.New(ctx, r.RedisURL, cfg.Coordination, policy)
		if e != nil {
			return runtime, e
		}
		gatewayOptions.Controller = runtime.coordination.Upstream()
	}
	gateway, e := llm.NewGateway(r.Catalog, providers, gatewayOptions)
	if e != nil {
		return runtime, e
	}
	registry, e := tools.Registry()
	if e != nil {
		return runtime, e
	}
	decider, e := agent.NewLLMDecider(gateway, cfg.Agent.DecisionModel, cfg.Agent.Limits.DecisionTokens)
	if e != nil {
		return runtime, e
	}
	engine, e := agent.NewEngine(r.Catalog, gateway, decider, registry)
	if e != nil {
		return runtime, e
	}
	credentials := []server.Credential{}
	for _, p := range cfg.Principals {
		credentials = append(credentials, server.Credential{Principal: p, Key: r.Keys[p.ID]})
	}
	httpOptions := server.Options{Server: cfg.Server, Principals: credentials, Logger: logger}
	if runtime.coordination != nil {
		httpOptions.Admission = runtime.coordination.Admission()
		httpOptions.CoordinationMetrics = runtime.coordination.WriteMetrics
	}
	runtime.HTTP, e = server.New(httpOptions, server.Dependencies{Catalog: r.Catalog, Gateway: gateway, Engine: engine, Tools: registry, Store: store, Policy: cfg.Agent, Ready: func(c context.Context) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := migration.CheckSchema(c, runtime.pool, cfg.Database.Schema); err != nil {
			return err
		}
		if runtime.coordination != nil {
			return runtime.coordination.Ready(c)
		}
		return nil
	}})
	if e != nil {
		return runtime, e
	}
	runtime.HTTP.Server.BaseContext = func(net.Listener) context.Context { return ctx }
	runtime.HTTP.Server.RegisterOnShutdown(cancel)
	go func() {
		defer close(runtime.done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e := store.Reap(ctx, 64); e != nil && ctx.Err() == nil && logger != nil {
					logger.Error("run_maintenance_failed", "code", llm.AsError(e).Code)
				}
			}
		}
	}()
	return runtime, nil
}
func (r *Runtime) Close() {
	r.once.Do(func() {
		r.cancel()
		<-r.done
		if r.coordination != nil {
			_ = r.coordination.Close()
		}
		for _, p := range r.providers {
			p.Close()
		}
		r.pool.Close()
	})
}
