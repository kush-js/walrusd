package cache

import (
	"errors"
	"testing"
	"time"
)

type fake struct {
	key    string
	closed bool
}

func (f *fake) Key() string  { return f.key }
func (f *fake) Close() error { f.closed = true; return nil }

func TestCacheDisabled(t *testing.T) {
	c := New(0, 0)
	c.Put(&fake{key: "a"})
	if _, ok := c.Get("a"); ok {
		t.Fatal("disabled cache must not store")
	}
}

func TestCachePutGet(t *testing.T) {
	c := New(2, time.Minute)
	c.Put(&fake{key: "a"})
	if got, ok := c.Get("a"); !ok || got.Key() != "a" {
		t.Fatalf("get = %v, %v", got, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("missing key must miss")
	}
}

func TestCacheLRUEviction(t *testing.T) {
	c := New(2, time.Minute)
	a, b, d := &fake{key: "a"}, &fake{key: "b"}, &fake{key: "d"}
	c.Put(a)
	c.Put(b)
	c.Get("a") // a most recent; b is LRU
	c.Put(d)   // evicts b
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if !b.closed {
		t.Fatal("evicted instance must be closed")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a must survive")
	}
	if _, ok := c.Get("d"); !ok {
		t.Fatal("d must be cached")
	}
}

func TestCacheIdleTTL(t *testing.T) {
	now := time.Now()
	c := New(5, time.Minute)
	c.now = func() time.Time { return now }
	c.Put(&fake{key: "a"})

	now = now.Add(2 * time.Minute)
	if _, ok := c.Get("a"); ok {
		t.Fatal("idle instance must expire")
	}
	if c.Len() != 0 {
		t.Fatal("expired instance must be evicted")
	}
}

func TestCacheReplaceClosesOld(t *testing.T) {
	c := New(5, time.Minute)
	old := &fake{key: "a"}
	c.Put(old)
	c.Put(&fake{key: "a"})
	if !old.closed {
		t.Fatal("replaced instance must be closed")
	}
}

func TestCacheEvictAndClose(t *testing.T) {
	c := New(5, time.Minute)
	a := &fake{key: "a"}
	c.Put(a)
	c.Evict("a")
	if a.closed != true {
		t.Fatal("evict must close")
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("evicted key must miss")
	}
}

var errClosed = errors.New("closed")

func TestCacheCloseAll(t *testing.T) {
	c := New(5, time.Minute)
	a := &fake{key: "a"}
	c.Put(a)
	_ = c.Close()
	if !a.closed {
		t.Fatal("close must close instances")
	}
}
