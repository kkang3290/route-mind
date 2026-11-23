package migration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var filename = regexp.MustCompile(`^([0-9]{14})_([a-z][a-z0-9_]{0,63})\.(up|down)\.sql$`)
var migrationName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type File struct {
	Version   uint
	Name      string
	Direction string
}

func Verify(files fs.FS) ([]File, error) {
	entries, e := fs.ReadDir(files, ".")
	if e != nil {
		return nil, e
	}
	manifest, e := fs.ReadFile(files, "checksums.sha256")
	if e != nil {
		return nil, fmt.Errorf("migration checksum manifest is required")
	}
	hashes := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(manifest)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || len(fields[0]) != 64 {
			return nil, fmt.Errorf("invalid checksum manifest")
		}
		if _, e := hex.DecodeString(fields[0]); e != nil {
			return nil, fmt.Errorf("invalid checksum")
		}
		if _, ok := hashes[fields[1]]; ok {
			return nil, fmt.Errorf("duplicate manifest entry")
		}
		hashes[fields[1]] = fields[0]
	}
	versions := map[uint]map[string]string{}
	var out []File
	for _, entry := range entries {
		if entry.Type()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("migration sources must not be symlinks")
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		name := entry.Name()
		match := filename.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("invalid migration filename: %s", name)
		}
		version, e := strconv.ParseUint(match[1], 10, 64)
		if e != nil || version == 0 {
			return nil, fmt.Errorf("invalid migration version")
		}
		data, e := fs.ReadFile(files, name)
		if e != nil {
			return nil, e
		}
		if len(bytes.TrimSpace(data)) == 0 || bytes.Contains(data, []byte("-- TODO")) {
			return nil, fmt.Errorf("migration is unfinished: %s", name)
		}
		want, ok := hashes[name]
		if !ok || want != fmt.Sprintf("%x", sha256.Sum256(data)) {
			return nil, fmt.Errorf("migration checksum mismatch: %s", name)
		}
		delete(hashes, name)
		v := uint(version)
		if versions[v] == nil {
			versions[v] = map[string]string{}
		}
		if _, ok := versions[v][match[3]]; ok {
			return nil, fmt.Errorf("duplicate migration version")
		}
		versions[v][match[3]] = match[2]
		out = append(out, File{Version: v, Name: name, Direction: match[3]})
	}
	if len(out) == 0 || len(hashes) > 0 {
		return nil, fmt.Errorf("empty migrations or stale manifest entries")
	}
	for _, v := range versions {
		if len(v) != 2 || v["up"] != v["down"] {
			return nil, fmt.Errorf("migration up/down must be paired with matching names")
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func Create(directory, name string, now time.Time) error {
	if !migrationName.MatchString(name) {
		return fmt.Errorf("migration name must be lower_snake_case without paths")
	}
	version := now.UTC().Format("20060102150405")
	resolved, e := filepath.EvalSymlinks(directory)
	if e != nil {
		return e
	}
	directory, e = filepath.Abs(resolved)
	if e != nil {
		return e
	}
	lockPath := filepath.Join(directory, ".new_"+version+".lock")
	lock, e := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return fmt.Errorf("migration version is being created or requires inspection: %w", e)
	}
	lockInfo, e := lock.Stat()
	if e != nil {
		lock.Close()
		return e
	}
	defer func() { lock.Close(); removeOwned(lockPath, lockInfo) }()
	entries, e := os.ReadDir(directory)
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), version+"_") {
			return fmt.Errorf("migration version already exists")
		}
	}
	prefix := version + "_" + name
	type ownedFile struct {
		path string
		info os.FileInfo
	}
	var created []ownedFile
	complete := false
	defer func() {
		if !complete {
			for _, file := range created {
				removeOwned(file.path, file.info)
			}
		}
	}()
	for _, direction := range []string{"up", "down"} {
		path := filepath.Join(directory, prefix+"."+direction+".sql")
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if e != nil {
			return fmt.Errorf("create migration without overwriting existing files: %w", e)
		}
		info, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			return statErr
		}
		created = append(created, ownedFile{path: path, info: info})
		_, writeErr := fmt.Fprintf(f, "BEGIN;\n-- TODO: implement %s migration for %s.\nCOMMIT;\n", direction, name)
		closeErr := f.Close()
		if writeErr != nil {
			return writeErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	complete = true
	return nil
}

// removeOwned only removes a file this operation created and still owns.
func removeOwned(path string, info os.FileInfo) {
	current, e := os.Lstat(path)
	if e == nil && current.Mode().IsRegular() && os.SameFile(info, current) {
		_ = os.Remove(path)
	}
}

// WriteManifest is for newly created migrations; VerifyHistory still forbids rewriting old SQL.
func WriteManifest(directory string) error {
	entries, e := os.ReadDir(directory)
	if e != nil {
		return e
	}
	var lines []string
	for _, entry := range entries {
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("migration sources and manifest must not be symlinks")
		}
		if !filename.MatchString(entry.Name()) {
			continue
		}
		data, e := os.ReadFile(filepath.Join(directory, entry.Name()))
		if e != nil {
			return e
		}
		lines = append(lines, fmt.Sprintf("%x  %s", sha256.Sum256(data), entry.Name()))
	}
	sort.Strings(lines)
	temp, e := os.CreateTemp(directory, ".checksums-*")
	if e != nil {
		return e
	}
	info, e := temp.Stat()
	if e != nil {
		temp.Close()
		return e
	}
	defer func() { temp.Close(); removeOwned(temp.Name(), info) }()
	if e = temp.Chmod(0644); e != nil {
		return e
	}
	if _, e = temp.WriteString(strings.Join(lines, "\n") + "\n"); e != nil {
		return e
	}
	if e = temp.Sync(); e != nil {
		return e
	}
	if e = temp.Close(); e != nil {
		return e
	}
	return os.Rename(temp.Name(), filepath.Join(directory, "checksums.sha256"))
}
