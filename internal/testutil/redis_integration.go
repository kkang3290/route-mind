//go:build redis_integration

package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/kkang3290/route-mind/internal/reliability"
	goredis "github.com/redis/go-redis/v9"
)

// Redis creates an owned namespace, never a database-wide cleanup authority.
// The caller must explicitly supply a disposable test service, not REDIS_URL.
func Redis(t *testing.T) (string, *goredis.Client, reliability.Settings) {
	t.Helper()
	raw := os.Getenv("TEST_REDIS_URL")
	if raw == "" {
		t.Fatal("TEST_REDIS_URL must select a dedicated Redis test service")
	}
	opts, err := goredis.ParseURL(raw)
	if err != nil {
		t.Fatal("invalid TEST_REDIS_URL")
	}
	opts.MaxRetries = -1
	opts.PoolSize = 4
	opts.MaxActiveConns = 4
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	opts.ContextTimeoutEnabled = true
	admin := goredis.NewClient(opts)
	t.Cleanup(func() { _ = admin.Close() })
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		t.Fatal(err)
	}
	owner := hex.EncodeToString(id)
	settings := reliability.DefaultSettings()
	settings.Mode = "redis"
	settings.Namespace = "test-" + owner
	settings.AllowInsecureLocal = true
	// Leave scheduling headroom for concurrent PostgreSQL tests under -race.
	// Fault injection still exercises expiration explicitly; production defaults
	// remain 30s / 10s / 500ms rather than these shortened fixture windows.
	settings.LeaseTTLMS = 1800
	settings.RenewIntervalMS = 500
	settings.OperationTimeoutMS = 200
	settings.DialTimeoutMS = 500
	settings.MaxConnections = 4
	prefix := "route-mind:{" + settings.Namespace + "}:"
	marker := prefix + "test-owner"
	if ok, err := admin.SetNX(context.Background(), marker, owner, time.Hour).Result(); err != nil || !ok {
		t.Fatal("cannot establish test namespace ownership")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if value, err := admin.Get(ctx, marker).Result(); err != nil || value != owner {
			t.Error("refusing cleanup without namespace ownership")
			return
		}
		var cursor uint64
		var keys []string
		for {
			batch, next, err := admin.Scan(ctx, cursor, prefix+"*", 100).Result()
			if err != nil {
				t.Error("test namespace enumeration failed")
				return
			}
			keys = append(keys, batch...)
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(keys) > 0 {
			if err := admin.Del(ctx, keys...).Err(); err != nil {
				t.Error("owned test namespace cleanup failed")
			}
		}
	})
	return raw, admin, settings
}
