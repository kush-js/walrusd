// Package cache implements the bounded LRU read-instance cache (spec §10).
// Read-only VFS sessions for recently used databases are reused across
// requests; they are evicted on capacity or idle TTL. Correctness never
// depends on the cache: a read session only surfaces remote-committed
// state, and eviction simply forces a fresh open.
package cache

import (
	"container/list"
	"sync"
	"time"
)

// ReadInstance is a cached, resumable read session handle. The runtime owns
// the concrete type; the cache only needs identity + last-use bookkeeping.
type ReadInstance interface {
	// Key identifies the cached instance (database key).
	Key() string
	// Close releases the instance's resources.
	Close() error
}

// clock allows tests to override time.
type clock func() time.Time

// Cache is a bounded LRU of read instances with idle-TTL eviction. It is
// safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	max      int
	idleTTL  time.Duration
	now      clock
	entries  map[string]*list.Element
	order    *list.List // front = most recently used
	capacity int        // current size
}

type entry struct {
	key      string
	instance ReadInstance
	lastUsed time.Time
}

// New builds a read-instance cache. max <= 0 and idleTTL <= 0 both disable
// caching (every read opens fresh), matching a minimal deployment.
func New(max int, idleTTL time.Duration) *Cache {
	return &Cache{
		max:     max,
		idleTTL: idleTTL,
		now:     time.Now,
		entries: make(map[string]*list.Element),
		order:   list.New(),
	}
}

// Get returns the cached instance for key, refreshing its LRU position.
// The caller must call Release (not Close) when finished using it.
func (c *Cache) Get(key string) (ReadInstance, bool) {
	if c.max <= 0 || c.idleTTL <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if c.now().Sub(e.lastUsed) > c.idleTTL {
		c.evictLocked(key, el, e)
		return nil, false
	}
	e.lastUsed = c.now()
	c.order.MoveToFront(el)
	return e.instance, true
}

// Put caches inst for its key, evicting the LRU entry when over capacity.
// An existing entry for the same key is closed and replaced.
func (c *Cache) Put(inst ReadInstance) {
	if c.max <= 0 || c.idleTTL <= 0 || inst == nil {
		return
	}
	key := inst.Key()
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.evictLocked(key, el, el.Value.(*entry))
	}
	for len(c.entries) >= c.max {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		e := oldest.Value.(*entry)
		c.evictLocked(e.key, oldest, e)
	}
	el := c.order.PushFront(&entry{key: key, instance: inst, lastUsed: c.now()})
	c.entries[key] = el
}

// Evict removes and closes the instance for key if cached.
func (c *Cache) Evict(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.evictLocked(key, el, el.Value.(*entry))
	}
}

// Close evicts and closes every cached instance.
func (c *Cache) Close() error {
	c.mu.Lock()
	keys := make([]string, 0, len(c.entries))
	for k := range c.entries {
		keys = append(keys, k)
	}
	c.mu.Unlock()
	var firstErr error
	for _, k := range keys {
		c.Evict(k)
	}
	return firstErr
}

// Len reports the number of cached instances.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *Cache) evictLocked(key string, el *list.Element, e *entry) {
	delete(c.entries, key)
	c.order.Remove(el)
	_ = e.instance.Close()
}
