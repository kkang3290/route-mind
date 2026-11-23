package migration_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/kkang3290/route-mind/internal/migration"
)

func fixture(up string) fstest.MapFS {
	f := fstest.MapFS{"20261123000100_init.up.sql": {Data: []byte(up)}, "20261123000100_init.down.sql": {Data: []byte("BEGIN; DROP TABLE sample; COMMIT;")}}
	var manifest string
	for _, name := range []string{"20261123000100_init.down.sql", "20261123000100_init.up.sql"} {
		manifest += fmt.Sprintf("%x  %s\n", sha256.Sum256(f[name].Data), name)
	}
	f["checksums.sha256"] = &fstest.MapFile{Data: []byte(manifest)}
	return f
}
func TestVerifyManifestRejectsChangedMissingAndUnpairedMigrations(t *testing.T) {
	valid := fixture("BEGIN; CREATE TABLE sample(id bigint); COMMIT;")
	if _, e := migration.Verify(valid); e != nil {
		t.Fatal(e)
	}
	changed := fixture("BEGIN; CREATE TABLE sample(id bigint); COMMIT;")
	changed["20261123000100_init.up.sql"].Data = []byte("ALTER TABLE sample ADD hacked text;")
	if _, e := migration.Verify(changed); e == nil {
		t.Fatal("checksum mismatch accepted")
	}
	delete(valid, "20261123000100_init.down.sql")
	if _, e := migration.Verify(valid); e == nil {
		t.Fatal("missing down accepted")
	}
}
func TestNewMigrationRejectsVersionReuseEvenWithDifferentName(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if e := migration.Create(dir, "first", now); e != nil {
		t.Fatal(e)
	}
	if e := migration.Create(dir, "different", now); e == nil {
		t.Fatal("same migration version reused under another name")
	}
}
func TestCollisionDoesNotLeaveOrphanUpFile(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if e := os.WriteFile(dir+"/20261123120000_add_runs.down.sql", []byte("owned existing down"), 0600); e != nil {
		t.Fatal(e)
	}
	if migration.Create(dir, "add_runs", now) == nil {
		t.Fatal("collision accepted")
	}
	if _, e := os.Stat(dir + "/20261123120000_add_runs.up.sql"); !os.IsNotExist(e) {
		t.Fatal("failed creation left an orphan up migration")
	}
}
func TestChecksumNeverFollowsMigrationOrManifestSymlinks(t *testing.T) {
	for _, target := range []string{"20261123000100_init.up.sql", "checksums.sha256"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			outside := t.TempDir() + "/outside"
			if e := os.WriteFile(outside, []byte("do not overwrite"), 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.Symlink(outside, dir+"/"+target); e != nil {
				t.Fatal(e)
			}
			if e := migration.WriteManifest(dir); e == nil {
				t.Fatal("symlink accepted")
			}
			b, _ := os.ReadFile(outside)
			if string(b) != "do not overwrite" {
				t.Fatal("external file overwritten")
			}
		})
	}
}

func TestNewMigrationNeverOverwritesAndValidatesNames(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	if e := migration.Create(dir, "add_runs", now); e != nil {
		t.Fatal(e)
	}
	path := dir + "/20261123120000_add_runs.up.sql"
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(before), "BEGIN;") {
		t.Fatal("transaction template missing")
	}
	if e = migration.Create(dir, "add_runs", now); e == nil {
		t.Fatal("existing migration overwritten")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("file changed")
	}
	if e = migration.Create(dir, "../../escape", now); e == nil {
		t.Fatal("unsafe migration name accepted")
	}
}
