package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kkang3290/route-mind/migrations"
)

func CLI(ctx context.Context, args []string, out io.Writer, env func(string) string) error {
	if len(args) == 0 {
		return fmt.Errorf("command required: new, checksum, verify, status, up, down, force")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	directory := flags.String("dir", "", "SQL directory for source-management commands")
	name := flags.String("name", "", "new migration name")
	base := flags.String("base-ref", "", "immutable history comparison commit")
	steps := flags.Int("steps", 0, "explicit down steps")
	version := flags.Int("version", -1, "manually verified version")
	ack := flags.Bool("ack-schema-verified", false, "operator inspected actual schema")
	ackLoss := flags.Bool("ack-data-loss", false, "acknowledge development data loss")
	reason := flags.String("reason", "", "manual recovery reason")
	if e := flags.Parse(args[1:]); e != nil || flags.NArg() != 0 {
		return fmt.Errorf("invalid migration command flags")
	}
	switch command {
	case "new":
		if *directory == "" {
			*directory = "migrations"
		}
		if e := Create(*directory, *name, time.Now()); e != nil {
			return e
		}
		if e := WriteManifest(*directory); e != nil {
			return e
		}
		_, e := fmt.Fprintln(out, "created paired migration; implement SQL and regenerate checksums before verification")
		return e
	case "checksum":
		if *directory == "" {
			*directory = "migrations"
		}
		return WriteManifest(*directory)
	case "verify":
		var files []File
		var e error
		if *directory == "" {
			files, e = Verify(migrations.Files)
		} else {
			files, e = Verify(os.DirFS(*directory))
		}
		if e != nil {
			return e
		}
		if *base != "" {
			if *directory == "" {
				*directory = "migrations"
			}
			if e = verifyGitHistory(ctx, *base, *directory); e != nil {
				return e
			}
		}
		_, e = fmt.Fprintf(out, "verified %d immutable SQL files\n", len(files))
		return e
	case "down":
		if env("APP_ENV") == "production" || env("ALLOW_DESTRUCTIVE_MIGRATIONS") != "1" || !*ackLoss || *steps <= 0 {
			return fmt.Errorf("down is development/test only and requires positive steps, ALLOW_DESTRUCTIVE_MIGRATIONS=1 and --ack-data-loss")
		}
	case "force":
		if *version < 0 || !*ack || strings.TrimSpace(*reason) == "" || len(*reason) > 512 || strings.ContainsAny(*reason, "\r\n") {
			return fmt.Errorf("force requires --version, --ack-schema-verified and a single-line --reason; it does not repair SQL")
		}
	case "up", "status":
	default:
		return fmt.Errorf("unsupported migration command")
	}
	schema := env("DATABASE_SCHEMA")
	if schema == "" {
		schema = "public"
	}
	dsn := env("MIGRATION_DATABASE_URL")
	if command == "status" {
		dsn = env("DATABASE_URL")
		if dsn == "" {
			dsn = env("MIGRATION_DATABASE_URL")
		}
	}
	if dsn == "" {
		return fmt.Errorf("database URL environment variable is required")
	}
	if command == "status" {
		config, e := pgxpool.ParseConfig(dsn)
		if e != nil {
			return fmt.Errorf("invalid database configuration")
		}
		config.ConnConfig.ConnectTimeout = 5 * time.Second
		config.MaxConns = 1
		pool, e := pgxpool.NewWithConfig(ctx, config)
		if e != nil {
			return fmt.Errorf("database unavailable")
		}
		defer pool.Close()
		state, e := Status(ctx, pool, schema)
		if e != nil {
			return e
		}
		files, e := Verify(migrations.Files)
		if e != nil {
			return e
		}
		var pending []uint
		for _, f := range files {
			if f.Direction == "up" && f.Version > state.Version {
				pending = append(pending, f.Version)
			}
		}
		compatible := CheckSchema(ctx, pool, schema) == nil
		return json.NewEncoder(out).Encode(struct {
			State
			Pending    []uint `json:"pending_versions"`
			Compatible bool   `json:"compatible"`
		}{state, pending, compatible})
	}
	if _, e := Verify(migrations.Files); e != nil {
		return e
	}
	runner, e := Open(ctx, dsn, Options{Schema: schema})
	if e != nil {
		return e
	}
	defer runner.Close()
	switch command {
	case "up":
		e = runner.Up(ctx)
	case "down":
		e = runner.Down(ctx, *steps)
	case "force":
		_, _ = fmt.Fprintf(out, "manual metadata recovery to version %d: %s\n", *version, *reason)
		e = runner.Force(ctx, *version)
	}
	if e != nil {
		return e
	}
	_, e = fmt.Fprintln(out, "migration command completed")
	return e
}
func verifyGitHistory(ctx context.Context, base, directory string) error {
	resolve := exec.CommandContext(ctx, "git", "rev-parse", "--verify", "--end-of-options", base+"^{commit}")
	commit, e := resolve.Output()
	if e != nil {
		return fmt.Errorf("cannot resolve migration baseline commit")
	}
	sha := strings.TrimSpace(string(commit))
	if len(sha) != 40 && len(sha) != 64 {
		return fmt.Errorf("invalid baseline commit")
	}
	rootBytes, e := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if e != nil {
		return e
	}
	root := strings.TrimSpace(string(rootBytes))
	absolute, e := filepath.Abs(directory)
	if e != nil {
		return e
	}
	resolved, e := filepath.EvalSymlinks(absolute)
	if e != nil {
		return e
	}
	relative, e := filepath.Rel(root, resolved)
	if e != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("migration history directory must be below the repository root")
	}
	relative = filepath.ToSlash(relative)
	tree := exec.CommandContext(ctx, "git", "ls-tree", "-r", "--name-only", "-z", sha, "--", relative)
	tree.Dir = root
	listing, e := tree.Output()
	if e != nil {
		return e
	}
	var maximum uint64
	old := map[string]bool{}
	for _, path := range strings.Split(string(listing), "\x00") {
		name := filepath.Base(path)
		match := filename.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		old[name] = true
		number, _ := strconv.ParseUint(match[1], 10, 64)
		if number > maximum {
			maximum = number
		}
		show := exec.CommandContext(ctx, "git", "show", sha+":"+path)
		show.Dir = root
		before, e := show.Output()
		if e != nil {
			return e
		}
		after, e := os.ReadFile(filepath.Join(resolved, name))
		if e != nil || !bytes.Equal(before, after) {
			return fmt.Errorf("published migration cannot change or disappear: %s", name)
		}
	}
	entries, e := os.ReadDir(resolved)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		match := filename.FindStringSubmatch(entry.Name())
		if match == nil || old[entry.Name()] {
			continue
		}
		number, _ := strconv.ParseUint(match[1], 10, 64)
		if number <= maximum {
			return fmt.Errorf("new migration must sort after every baseline migration")
		}
	}
	return nil
}
