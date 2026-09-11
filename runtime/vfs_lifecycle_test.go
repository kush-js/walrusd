package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"walrusd/lease"
	"walrusd/litestream"
	"walrusd/runtime"
	"walrusd/walrusderr"
)

func TestVFSRegistrationsBoundedUnderChurn(t *testing.T) {
	const limit = 4
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	cfg.MaxReadInstances = limit
	cfg.ReadInstanceIdleTTL = time.Hour
	cfg.RetryPolicy = runtime.RetryPolicy{}
	rt, err := runtime.New(lease.NewMemoryStore(), "api-1", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	root := t.TempDir()
	descriptor := func(i int) runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: fmt.Sprintf("users/churn-%d", i),
			Storage: litestream.Profile{
				Provider: "file",
				FileRoot: root,
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
	}

	before := litestream.RegisteredVFSCount()
	for cycle := 0; cycle < 2; cycle++ {
		for i := 0; i < 300; i++ {
			if err := rt.WithRead(context.Background(), descriptor(i), func(*sql.Conn) error {
				return nil
			}); err != nil {
				t.Fatalf("cycle %d database %d: %v", cycle, i, err)
			}
		}
	}
	after := litestream.RegisteredVFSCount()
	bound := before + uint64(limit+1)
	t.Logf("live VFS registrations before churn=%d after churn=%d bound=%d", before, after, bound)
	if after > bound {
		t.Fatalf("live VFS registrations grew to %d during churn; want <= %d", after, bound)
	}
}

func TestReadAfterVFSEviction(t *testing.T) {
	const limit = 2
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	cfg.MaxReadInstances = limit
	cfg.ReadInstanceIdleTTL = time.Hour
	cfg.RetryPolicy = runtime.RetryPolicy{}
	rt, err := runtime.New(lease.NewMemoryStore(), "api-1", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	root := t.TempDir()
	descriptor := func(id string) runtime.DatabaseDescriptor {
		return runtime.DatabaseDescriptor{
			DatabaseID: id,
			Storage: litestream.Profile{
				Provider: "file",
				FileRoot: root,
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
	}
	original := descriptor("users/evicted-read")
	if _, err := rt.WithWrite(context.Background(), original, "seed", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`); err != nil {
			return err
		}
		_, err := conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('still here')`)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	readValue := func(d runtime.DatabaseDescriptor) (string, error) {
		var value string
		err := rt.WithRead(context.Background(), d, func(conn *sql.Conn) error {
			return conn.QueryRowContext(context.Background(), `SELECT v FROM t LIMIT 1`).Scan(&value)
		})
		return value, err
	}
	if value, err := readValue(original); err != nil {
		t.Fatalf("initial read: %v", err)
	} else if value != "still here" {
		t.Fatalf("initial value = %q, want %q", value, "still here")
	}

	for i := 0; i < limit+3; i++ {
		if err := rt.WithRead(context.Background(), descriptor(fmt.Sprintf("users/evictor-%d", i)), func(*sql.Conn) error {
			return nil
		}); err != nil {
			t.Fatalf("evicting read %d: %v", i, err)
		}
	}

	value, err := readValue(original)
	if err != nil {
		t.Fatalf("read after eviction: %v", err)
	}
	if value != "still here" {
		t.Fatalf("value after eviction = %q, want %q", value, "still here")
	}
}

func TestConcurrentVFSChurnAndReads(t *testing.T) {
	const (
		limit      = 3
		dbCount    = 12
		readers    = 4
		churners   = 3
		readIters  = 20
		churnIters = 40
	)
	cfg := runtime.DefaultConfig()
	cfg.Litestream.HydrationEnabled = false
	cfg.MaxReadInstances = limit
	cfg.ReadInstanceIdleTTL = time.Hour
	cfg.RetryPolicy = runtime.RetryPolicy{}
	rt, err := runtime.New(lease.NewMemoryStore(), "api-1", cfg)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	defer rt.Close()

	root := t.TempDir()
	descriptors := make([]runtime.DatabaseDescriptor, dbCount)
	for i := range descriptors {
		d := runtime.DatabaseDescriptor{
			DatabaseID: fmt.Sprintf("users/concurrent-%d", i),
			Storage: litestream.Profile{
				Provider: "file",
				FileRoot: root,
			},
			Credentials: runtime.StaticCredentials{AccessKeyID: "k", SecretAccessKey: "s"},
		}
		if _, err := rt.WithWrite(context.Background(), d, "seed", func(conn *sql.Conn) error {
			if _, err := conn.ExecContext(context.Background(), `CREATE TABLE t (v TEXT)`); err != nil {
				return err
			}
			_, err := conn.ExecContext(context.Background(), `INSERT INTO t VALUES ('ok')`)
			return err
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		descriptors[i] = d
	}

	before := litestream.RegisteredVFSCount()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errorsCh := make(chan error, readers*readIters+churners*churnIters)
	var wg sync.WaitGroup

	for reader := 0; reader < readers; reader++ {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for i := 0; i < readIters; i++ {
				d := descriptors[(reader+i)%dbCount]
				err := rt.WithRead(ctx, d, func(conn *sql.Conn) error {
					var value string
					return conn.QueryRowContext(ctx, `SELECT v FROM t LIMIT 1`).Scan(&value)
				})
				if err := concurrentReadError(err); err != nil {
					errorsCh <- err
				}
			}
		}(reader)
	}

	for churner := 0; churner < churners; churner++ {
		wg.Add(1)
		go func(churner int) {
			defer wg.Done()
			for i := 0; i < churnIters; i++ {
				if _, err := rt.ReadDSN(ctx, descriptors[(churner+i)%dbCount]); err != nil {
					errorsCh <- fmt.Errorf("churn read DSN: %w", err)
				}
			}
		}(churner)
	}

	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	after := litestream.RegisteredVFSCount()
	bound := before + uint64(limit+2)
	t.Logf("live VFS registrations during concurrent churn: before=%d after=%d bound=%d", before, after, bound)
	if after > bound {
		t.Fatalf("live VFS registrations grew to %d during concurrent churn; want <= %d", after, bound)
	}
}

func concurrentReadError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(strings.ToLower(err.Error()), "no such vfs") {
		return fmt.Errorf("read observed torn-down VFS: %w", err)
	}
	if walrusderr.ClassOf(err) == "" {
		return fmt.Errorf("read returned unclassified error: %w", err)
	}
	return nil
}
