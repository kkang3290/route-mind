//go:build integration

// Package testutil owns disposable, explicitly selected PostgreSQL test schemas.
package testutil

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kkang3290/route-mind/pkg/agent"
)

func Qualified(schema, table string) string { return pgx.Identifier{schema, table}.Sanitize() }
func Database(t *testing.T) (string, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests; no production DATABASE_URL fallback")
	}
	config, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal("invalid TEST_DATABASE_URL")
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatal("integration database name must end with _test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root, e := pgxpool.NewWithConfig(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	if e = root.Ping(ctx); e != nil {
		root.Close()
		t.Fatal("test PostgreSQL unavailable")
	}
	token := strings.ReplaceAll(agent.NewID(), "-", "")
	schema := "rm_test_" + token
	marker := "route-mind-test:" + token
	if _, e = root.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); e != nil {
		root.Close()
		t.Fatal(e)
	}
	if _, e = root.Exec(ctx, "COMMENT ON SCHEMA "+pgx.Identifier{schema}.Sanitize()+" IS '"+marker+"'"); e != nil {
		root.Close()
		t.Fatal(e)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, config)
	if e != nil {
		root.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		var evidence string
		e := root.QueryRow(cleanup, "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", schema).Scan(&evidence)
		if e != nil || evidence != marker || !strings.HasPrefix(schema, "rm_test_") {
			t.Errorf("refusing test schema cleanup without ownership evidence")
			root.Close()
			return
		}
		if _, e = root.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
			t.Errorf("test schema cleanup: %v", e)
		}
		root.Close()
	})
	return dsn, pool, schema
}
