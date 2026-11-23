//go:build integration

package migration_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kkang3290/route-mind/internal/migration"
	"github.com/kkang3290/route-mind/internal/testutil"
	"github.com/kkang3290/route-mind/migrations"
	"github.com/kkang3290/route-mind/pkg/agent"
	"io/fs"
)

func TestFreshUpRepeatedUpAndDownUp(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	ctx := context.Background()
	runner, e := migration.Open(ctx, dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	defer runner.Close()
	if e = runner.Up(ctx); e != nil {
		t.Fatal(e)
	}
	if e = runner.Up(ctx); e != nil {
		t.Fatalf("repeated up: %v", e)
	}
	if e = migration.CheckSchema(ctx, pool, schema); e != nil {
		t.Fatal(e)
	}
	for _, table := range []string{"agent_runs", "agent_steps", "agent_tool_calls", "request_idempotency"} {
		var n int
		if e = pool.QueryRow(ctx, "SELECT count(*) FROM "+testutil.Qualified(schema, table)).Scan(&n); e != nil {
			t.Fatal(e)
		}
	}
	if e = runner.Down(ctx, 1); e != nil {
		t.Fatal(e)
	}
	if e = runner.Up(ctx); e != nil {
		t.Fatal(e)
	}
}
func TestFailedTransactionalMigrationLeavesDirtyAndNoPartialDDL(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	fs := fstest.MapFS{"1_broken.up.sql": {Data: []byte("BEGIN; CREATE TABLE should_rollback(id bigint); SELECT definitely_missing_function(); COMMIT;")}, "1_broken.down.sql": {Data: []byte("DROP TABLE should_rollback;")}}
	runner, e := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: fs})
	if e != nil {
		t.Fatal(e)
	}
	defer runner.Close()
	if e = runner.Up(context.Background()); e == nil {
		t.Fatal("broken SQL accepted")
	}
	state, e := migration.Status(context.Background(), pool, schema)
	if e != nil || !state.Dirty {
		t.Fatalf("state=%+v err=%v", state, e)
	}
	var exists bool
	if e = pool.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", schema+".should_rollback").Scan(&exists); e != nil || exists {
		t.Fatalf("partial DDL survived: exists=%v err=%v", exists, e)
	}
	if e = runner.Up(context.Background()); e == nil || !strings.Contains(strings.ToLower(e.Error()), "dirty") {
		t.Fatalf("dirty not blocked: %v", e)
	}
}
func TestAdditiveUpgradePreservesExistingRunsAndOlderApplication(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	ctx := context.Background()
	initial, e := migration.Open(ctx, dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	if e = initial.Up(ctx); e != nil {
		t.Fatal(e)
	}
	initial.Close()
	id := agent.NewID()
	_, e = pool.Exec(ctx, "INSERT INTO "+testutil.Qualified(schema, "agent_runs")+" (public_id,principal,status,revision,schema_version,state,deadline_at,expires_at,created_at) VALUES($1,'fixture','completed',1,1,'{\"preserved\":true}',now()+interval '1 minute',now()+interval '1 day',now())", id)
	if e != nil {
		t.Fatal(e)
	}
	source := fstest.MapFS{}
	entries, _ := fs.ReadDir(migrations.Files, ".")
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") {
			b, _ := fs.ReadFile(migrations.Files, entry.Name())
			source[entry.Name()] = &fstest.MapFile{Data: b}
		}
	}
	source["20261123000200_expand.up.sql"] = &fstest.MapFile{Data: []byte("BEGIN; ALTER TABLE agent_runs ADD COLUMN rollout_note text; COMMIT;")}
	source["20261123000200_expand.down.sql"] = &fstest.MapFile{Data: []byte("BEGIN; ALTER TABLE agent_runs DROP COLUMN rollout_note; COMMIT;")}
	next, e := migration.Open(ctx, dsn, migration.Options{Schema: schema, Source: source})
	if e != nil {
		t.Fatal(e)
	}
	defer next.Close()
	if e = next.Up(ctx); e != nil {
		t.Fatal(e)
	}
	if e = migration.CheckSchema(ctx, pool, schema); e != nil {
		t.Fatalf("older binary rejected additive future schema: %v", e)
	}
	if e = next.Down(ctx, 1); e != nil {
		t.Fatal(e)
	}
	var preserved bool
	if e = pool.QueryRow(ctx, "SELECT (state->>'preserved')::boolean FROM "+testutil.Qualified(schema, "agent_runs")+" WHERE public_id=$1", id).Scan(&preserved); e != nil || !preserved {
		t.Fatalf("upgrade/down lost existing data: %v", e)
	}
}
func TestManualDirtyRecoveryRequiresRepairNotJustMetadata(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	ctx := context.Background()
	source := fstest.MapFS{"1_failed.up.sql": {Data: []byte("BEGIN; CREATE TABLE intended(id bigint); SELECT unavailable_function(); COMMIT;")}, "1_failed.down.sql": {Data: []byte("DROP TABLE intended;")}, "2_next.up.sql": {Data: []byte("BEGIN; ALTER TABLE intended ADD COLUMN recovered boolean; COMMIT;")}, "2_next.down.sql": {Data: []byte("ALTER TABLE intended DROP COLUMN recovered;")}}
	runner, e := migration.Open(ctx, dsn, migration.Options{Schema: schema, Source: source})
	if e != nil {
		t.Fatal(e)
	}
	defer runner.Close()
	if runner.Up(ctx) == nil {
		t.Fatal("expected dirty migration")
	}
	if _, e = pool.Exec(ctx, "CREATE TABLE "+testutil.Qualified(schema, "intended")+"(id bigint)"); e != nil {
		t.Fatal(e)
	}
	// The operator inspected the rolled-back transaction and applied the intended
	// version-1 DDL manually. Force now aligns metadata; it does not repair SQL.
	if e = runner.Force(ctx, 1); e != nil {
		t.Fatal(e)
	}
	if e = runner.Up(ctx); e != nil {
		t.Fatal(e)
	}
	state, e := migration.Status(ctx, pool, schema)
	if e != nil || state.Dirty || state.Version != 2 {
		t.Fatalf("state=%+v error=%v", state, e)
	}
}
func TestRuntimeRoleCannotDDLButCanCheckSchema(t *testing.T) {
	dsn, pool, schema := testutil.Database(t)
	ctx := context.Background()
	runner, e := migration.Open(ctx, dsn, migration.Options{Schema: schema, Source: migrations.Files})
	if e != nil {
		t.Fatal(e)
	}
	if e = runner.Up(ctx); e != nil {
		t.Fatal(e)
	}
	runner.Close()
	role := "rm_test_role_" + strings.ReplaceAll(agent.NewID(), "-", "")
	quoted := pgx.Identifier{role}.Sanitize()
	if _, e = pool.Exec(ctx, "CREATE ROLE "+quoted+" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE"); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		var exists bool
		if e := pool.QueryRow(c, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1 AND NOT rolsuper AND NOT rolcanlogin)", role).Scan(&exists); e != nil || !exists {
			t.Error("test-role ownership shape changed; refusing cleanup")
			return
		}
		_, e := pool.Exec(c, "DROP OWNED BY "+quoted)
		if e == nil {
			_, e = pool.Exec(c, "DROP ROLE "+quoted)
		}
		if e != nil {
			t.Error(e)
		}
	})
	for _, sql := range []string{"GRANT USAGE ON SCHEMA " + pgx.Identifier{schema}.Sanitize() + " TO " + quoted, "GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA " + pgx.Identifier{schema}.Sanitize() + " TO " + quoted, "GRANT USAGE ON ALL SEQUENCES IN SCHEMA " + pgx.Identifier{schema}.Sanitize() + " TO " + quoted} {
		if _, e = pool.Exec(ctx, sql); e != nil {
			t.Fatal(e)
		}
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.AfterConnect = func(c context.Context, conn *pgx.Conn) error { _, e := conn.Exec(c, "SET ROLE "+quoted); return e }
	restricted, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer restricted.Close()
	if e = migration.CheckSchema(ctx, restricted, schema); e != nil {
		t.Fatalf("runtime schema check requires DDL privileges: %v", e)
	}
	if _, e = restricted.Exec(ctx, "CREATE TABLE "+testutil.Qualified(schema, "forbidden")+"(id bigint)"); e == nil {
		t.Fatal("runtime role could create tables")
	}
}

func TestMigrationRunnersSerialize(t *testing.T) {
	dsn, _, schema := testutil.Database(t)
	fs := fstest.MapFS{"1_lock.up.sql": {Data: []byte("BEGIN; SELECT pg_sleep(0.15); CREATE TABLE lock_once(id bigint); COMMIT;")}, "1_lock.down.sql": {Data: []byte("DROP TABLE lock_once;")}}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			r, e := migration.Open(context.Background(), dsn, migration.Options{Schema: schema, Source: fs, LockTimeout: time.Second})
			if e == nil {
				defer r.Close()
				e = r.Up(context.Background())
			}
			errs <- e
		})
	}
	wg.Wait()
	for range 2 {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
}
