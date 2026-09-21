package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"walrusd/identity"
	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
)

// Reads never participate in the lease protocol (spec §9): a read on a
// database with no flushed state must not create a lease record, and reads
// served through the litestream VFS must not issue a single lease command.
// This holds the read path to the Redis command level, not just to the
// absence of a visible state change.
func TestReadsIssueNoLeaseCommands(t *testing.T) {
	client := redisClient(t)
	ctx := context.Background()

	store, err := lease.NewRedisStore(ctx, lease.RedisOptions{Addr: client.Options().Addr})
	if err != nil {
		t.Fatalf("dial redis: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	rt, err := runtime.New(store, "read-lease-check", runtime.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	id := fmt.Sprintf("users/read_lease_%d", rand.Int64())
	dbID, err := identity.NewDatabaseID(id)
	if err != nil {
		t.Fatal(err)
	}
	d := runtime.DatabaseDescriptor{
		DatabaseID: id,
		Storage: litestream.Profile{
			Provider: "file",
			FileRoot: t.TempDir(),
		},
		Credentials: runtime.StaticCredentials{},
	}
	leaseKey := dbID.LeaseKey(d.Storage.RootPrefix)

	// A read of an empty database takes no lease: no record may appear.
	if err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
		var n int
		return conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master`).Scan(&n)
	}); err != nil {
		t.Fatalf("read before write: %v", err)
	}
	if n, err := client.Exists(ctx, leaseKey).Result(); err != nil {
		t.Fatalf("check lease key: %v", err)
	} else if n != 0 {
		t.Fatalf("read created lease key %s", leaseKey)
	}

	// Seed flushed state, then confirm the lease ended released.
	if _, err := rt.WithWrite(ctx, d, "seed", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO kv VALUES ('k', 'v')`)
		return err
	}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if rec := leaseRecord(t, ctx, client, leaseKey); rec.State != "released" {
		t.Fatalf("lease state %q, want released", rec.State)
	}

	// VFS-backed reads must not issue lease commands at all.
	before := leaseCommandCounts(t, ctx, client)
	for i := 0; i < 3; i++ {
		var v string
		if err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
			return conn.QueryRowContext(ctx, `SELECT v FROM kv WHERE k = 'k'`).Scan(&v)
		}); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if v != "v" {
			t.Fatalf("read %d: got %q, want v", i, v)
		}
	}
	after := leaseCommandCounts(t, ctx, client)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("reads issued lease commands: %v -> %v", before, after)
	}
}

type record struct {
	State string `json:"state"`
}

func leaseRecord(t *testing.T, ctx context.Context, client *redis.Client, key string) record {
	t.Helper()
	body, err := client.HGet(ctx, key, "data").Result()
	if err != nil {
		t.Fatalf("read lease record: %v", err)
	}
	var rec record
	if err := json.Unmarshal([]byte(body), &rec); err != nil {
		t.Fatalf("decode lease record: %v", err)
	}
	return rec
}

// leaseCommandCounts returns the Redis call counters the lease store drives:
// HGETALL for reads and EVAL for the CAS scripts.
func leaseCommandCounts(t *testing.T, ctx context.Context, client *redis.Client) map[string]int64 {
	t.Helper()
	stats, err := client.Info(ctx, "commandstats").Result()
	if err != nil {
		t.Fatalf("commandstats: %v", err)
	}
	counts := map[string]int64{}
	for _, line := range strings.Split(stats, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "cmdstat_") {
			continue
		}
		name := strings.TrimPrefix(strings.SplitN(line, ":", 2)[0], "cmdstat_")
		switch name {
		case "hgetall", "hset", "eval", "evalsha":
			var calls int64
			if _, err := fmt.Sscanf(line, "cmdstat_"+name+":calls=%d", &calls); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			counts[name] = calls
		}
	}
	return counts
}

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("WALRUSD_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("WALRUSD_TEST_REDIS_ADDR not set; skipping live Redis test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 3 * time.Second})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return client
}
