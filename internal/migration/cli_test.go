package migration_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kkang3290/route-mind/internal/migration"
)

func TestCLIValidatesDestructiveCommandsBeforeConnecting(t *testing.T) {
	env := func(key string) string {
		if key == "APP_ENV" {
			return "production"
		}
		return ""
	}
	for _, args := range [][]string{{"down", "--steps=1"}, {"force", "--version=1"}, {"force", "--version=1", "--ack-schema-verified"}, {"drop"}} {
		var out bytes.Buffer
		e := migration.CLI(context.Background(), args, &out, env)
		if e == nil {
			t.Fatalf("unsafe command accepted: %v", args)
		}
		if strings.Contains(e.Error(), "database unavailable") {
			t.Fatalf("unsafe command attempted a connection: %v", e)
		}
	}
}
func TestCLIVerifyChecksActualGitHistory(t *testing.T) {
	for _, change := range []string{"unchanged", "changed", "deleted", "older", "same_version", "newer"} {
		t.Run(change, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
			t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
			git := func(args ...string) {
				t.Helper()
				cmd := exec.Command("git", args...)
				if out, e := cmd.CombinedOutput(); e != nil {
					t.Fatalf("git %v: %v\n%s", args, e, out)
				}
			}
			writePair := func(version, name, sql string) {
				t.Helper()
				for _, direction := range []string{"up", "down"} {
					path := filepath.Join("migrations", version+"_"+name+"."+direction+".sql")
					if e := os.WriteFile(path, []byte(sql), 0600); e != nil {
						t.Fatal(e)
					}
				}
			}
			if e := os.Mkdir("migrations", 0700); e != nil {
				t.Fatal(e)
			}
			git("init", "-q")
			writePair("20261123000100", "initial", "BEGIN; SELECT 1; COMMIT;")
			if e := migration.WriteManifest("migrations"); e != nil {
				t.Fatal(e)
			}
			git("add", "migrations")
			git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath="+os.DevNull, "commit", "-qm", "Migration test baseline\n\nCo-Authored-By: Claude Opus 5 <noreply@anthropic.com>")
			switch change {
			case "changed":
				writePair("20261123000100", "initial", "BEGIN; SELECT 2; COMMIT;")
			case "deleted":
				for _, direction := range []string{"up", "down"} {
					if e := os.Remove(filepath.Join("migrations", "20261123000100_initial."+direction+".sql")); e != nil {
						t.Fatal(e)
					}
				}
				writePair("20261124000100", "replacement", "BEGIN; SELECT 1; COMMIT;")
			case "older":
				writePair("20261122000100", "older", "BEGIN; SELECT 1; COMMIT;")
			case "same_version":
				writePair("20261123000100", "renamed", "BEGIN; SELECT 1; COMMIT;")
			case "newer":
				writePair("20261124000100", "addition", "BEGIN; SELECT 1; COMMIT;")
			}
			if e := migration.WriteManifest("migrations"); e != nil {
				t.Fatal(e)
			}
			var out bytes.Buffer
			e := migration.CLI(context.Background(), []string{"verify", "--dir=migrations", "--base-ref=HEAD"}, &out, func(string) string { return "" })
			wantError := change != "unchanged" && change != "newer"
			if (e != nil) != wantError {
				t.Fatalf("%s: error=%v output=%s", change, e, &out)
			}
		})
	}
}

func TestCLIVerifiesEmbeddedReleaseWithoutDatabase(t *testing.T) {
	var out bytes.Buffer
	if e := migration.CLI(context.Background(), []string{"verify"}, &out, func(string) string { return "" }); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "verified") {
		t.Fatal(out.String())
	}
}
