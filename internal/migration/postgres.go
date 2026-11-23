package migration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strconv"
	"strings"
	"time"

	gm "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	pgdriver "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/kkang3290/route-mind/migrations"
)

var schemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type Options struct {
	Schema           string
	Source           fs.FS
	LockTimeout      time.Duration
	StatementTimeout time.Duration
}
type Runner struct {
	m *gm.Migrate
}

func Open(ctx context.Context, dsn string, o Options) (*Runner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.Schema == "" {
		o.Schema = "public"
	}
	if !schemaName.MatchString(o.Schema) {
		return nil, fmt.Errorf("invalid database schema name")
	}
	if o.Source == nil {
		o.Source = migrations.Files
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = 5 * time.Second
	}
	if o.StatementTimeout <= 0 {
		o.StatementTimeout = 30 * time.Second
	}
	config, e := pgx.ParseConfig(dsn)
	if e != nil {
		return nil, fmt.Errorf("invalid migration database configuration")
	}
	config.ConnectTimeout = 5 * time.Second
	config.RuntimeParams["search_path"] = o.Schema
	config.RuntimeParams["application_name"] = "route-mind-migrate"
	config.RuntimeParams["statement_timeout"] = strconv.FormatInt(o.StatementTimeout.Milliseconds(), 10)
	config.RuntimeParams["lock_timeout"] = strconv.FormatInt(o.LockTimeout.Milliseconds(), 10)
	db := stdlib.OpenDB(*config)
	db.SetMaxOpenConns(1)
	if e = db.PingContext(ctx); e != nil {
		db.Close()
		return nil, fmt.Errorf("migration database unavailable")
	}
	driver, e := pgdriver.WithInstance(db, &pgdriver.Config{SchemaName: o.Schema, MigrationsTable: "schema_migrations", StatementTimeout: o.StatementTimeout})
	if e != nil {
		db.Close()
		return nil, fmt.Errorf("initialize migration metadata: %w", e)
	}
	source, e := iofs.New(o.Source, ".")
	if e != nil {
		driver.Close()
		return nil, e
	}
	m, e := gm.NewWithInstance("iofs", source, "pgx5", rollbackDriver{driver})
	if e != nil {
		source.Close()
		driver.Close()
		return nil, e
	}
	m.LockTimeout = o.LockTimeout
	return &Runner{m: m}, nil
}

// Explicit BEGIN scripts leave a failed transaction open on their pinned session.
// Roll it back without clearing migrate's independently committed dirty marker.
type rollbackDriver struct{ database.Driver }

func (d rollbackDriver) Run(r io.Reader) error {
	e := d.Driver.Run(r)
	if e != nil {
		_ = d.Driver.Run(strings.NewReader("ROLLBACK;"))
	}
	return e
}
func (r *Runner) Close() { r.m.Close() }
func (r *Runner) execute(ctx context.Context, operation func() error) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- operation() }()
	select {
	case e := <-done:
		if errors.Is(e, gm.ErrNoChange) {
			return nil
		}
		return e
	case <-ctx.Done():
		select {
		case r.m.GracefulStop <- true:
		default:
		}
		<-done
		return ctx.Err()
	}
}
func (r *Runner) Up(ctx context.Context) error { return r.execute(ctx, r.m.Up) }
func (r *Runner) Down(ctx context.Context, steps int) error {
	if steps <= 0 {
		return fmt.Errorf("explicit positive down step count required")
	}
	return r.execute(ctx, func() error { return r.m.Steps(-steps) })
}

// Force only changes migration metadata. The CLI requires explicit operator acknowledgement.
func (r *Runner) Force(ctx context.Context, version int) error {
	return r.execute(ctx, func() error { return r.m.Force(version) })
}

type State struct {
	Version uint `json:"version"`
	Dirty   bool `json:"dirty"`
	Exists  bool `json:"metadata_exists"`
}

func Status(ctx context.Context, pool *pgxpool.Pool, schema string) (State, error) {
	var s State
	if !schemaName.MatchString(schema) {
		return s, fmt.Errorf("invalid schema")
	}
	query := "SELECT version,dirty FROM " + pgx.Identifier{schema, "schema_migrations"}.Sanitize() + " LIMIT 1"
	e := pool.QueryRow(ctx, query).Scan(&s.Version, &s.Dirty)
	if errors.Is(e, pgx.ErrNoRows) {
		s.Exists = true
		return s, nil
	}
	var pe *pgconn.PgError
	if errors.As(e, &pe) && pe.Code == "42P01" {
		return s, nil
	}
	if e != nil {
		return s, fmt.Errorf("cannot read migration metadata")
	}
	s.Exists = true
	return s, nil
}

// CheckSchema never constructs a migration driver, creates metadata, or executes DDL.
func CheckSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	s, e := Status(ctx, pool, schema)
	if e != nil {
		return e
	}
	if !s.Exists || s.Version < migrations.InitialVersion || s.Dirty {
		return fmt.Errorf("database schema is missing, outdated or dirty; run the migration job")
	}
	contracts := map[string]map[string]string{"agent_runs": {"public_id": "uuid", "revision": "bigint", "state": "jsonb", "operation_id": "uuid", "deadline_at": "timestamp with time zone"}, "agent_steps": {"run_id": "bigint", "attempt": "integer", "usage": "jsonb"}, "agent_tool_calls": {"call_id": "text", "arguments": "jsonb", "result": "jsonb"}, "request_idempotency": {"principal": "text", "key_digest": "text", "request_digest": "text"}}
	for table, columns := range contracts {
		for name, kind := range columns {
			var actual string
			if e = pool.QueryRow(ctx, "SELECT data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name=$3", schema, table, name).Scan(&actual); e != nil || actual != kind {
				return fmt.Errorf("database schema contract is incompatible; check migrations")
			}
		}
	}
	for _, name := range []string{"agent_runs_public_id_key", "agent_steps_pkey", "agent_tool_calls_pkey", "request_idempotency_pkey"} {
		var valid bool
		if e = pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=$1 AND c.conname=$2 AND c.convalidated)", schema, name).Scan(&valid); e != nil || !valid {
			return fmt.Errorf("database constraint contract is incompatible")
		}
	}
	return nil
}
