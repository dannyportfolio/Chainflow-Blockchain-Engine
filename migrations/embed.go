package migrations

import (
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//go:embed *.sql
var fs embed.FS

type Migration struct {
	Version int
	Name    string
	UpSQL   string
	DownSQL string
}

func LoadUp() ([]Migration, error) {
	entries, err := fs.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	byVersion := make(map[int]*Migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var isUp bool
		var base string
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			isUp = true
			base = strings.TrimSuffix(name, ".up.sql")
		case strings.HasSuffix(name, ".down.sql"):
			isUp = false
			base = strings.TrimSuffix(name, ".down.sql")
		default:
			continue
		}

		idx := strings.IndexByte(base, '_')
		if idx <= 0 {
			return nil, fmt.Errorf("invalid migration filename %q: missing version prefix", name)
		}
		ver, err := strconv.Atoi(base[:idx])
		if err != nil || ver <= 0 {
			return nil, fmt.Errorf("invalid migration version in %q: %w", name, err)
		}

		raw, err := fs.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", name, err)
		}

		m, exists := byVersion[ver]
		if !exists {
			m = &Migration{Version: ver, Name: base[idx+1:]}
			byVersion[ver] = m
		} else if m.Name != base[idx+1:] {
			return nil, fmt.Errorf("mismatched migration name for version %d: %q vs %q", ver, m.Name, base[idx+1:])
		}
		if isUp {
			m.UpSQL = string(raw)
		} else {
			m.DownSQL = string(raw)
		}
	}

	versions := make([]int, 0, len(byVersion))
	for v := range byVersion {
		versions = append(versions, v)
	}
	sort.Ints(versions)

	out := make([]Migration, 0, len(versions))
	for i, v := range versions {
		if v != i+1 {
			return nil, fmt.Errorf("non-contiguous migration version %d (expected %d)", v, i+1)
		}
		m := byVersion[v]
		if strings.TrimSpace(m.UpSQL) == "" || strings.TrimSpace(m.DownSQL) == "" {
			return nil, fmt.Errorf("migration %d (%s) missing paired up/down SQL", m.Version, m.Name)
		}
		out = append(out, *m)
	}
	return out, nil
}
