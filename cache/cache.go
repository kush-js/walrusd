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
	onAdd    func(ReadInstance)
	onRemove func(ReadInstance)
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

// SetObserver installs callbacks notified after instances are added to or
// removed from the cache. Callbacks run without the cache lock held.
func (c *Cache) SetObserver(onAdd, onRemove func(ReadInstance)) {
	c.mu.Lock()
	c.onAdd = onAdd
	c.onRemove = onRemove
	c.mu.Unlock()
}

// Get returns the cached instance for key, refreshing its LRU position.
// The caller must call Release (not Close) when finished using it.
func (c *Cache) Get(key string) (ReadInstance, bool) {
	if c.max <= 0 || c.idleTTL <= 0 {
		return nil, false
	}
	c.mu.Lock()
	el, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	e := el.Value.(*entry)
	if c.now().Sub(e.lastUsed) > c.idleTTL {
		c.removeLocked(key, el)
		c.mu.Unlock()
		_ = e.instance.Close()
		c.notifyRemove(e.instance)
		return nil, false
	}
	e.lastUsed = c.now()
	c.order.MoveToFront(el)
	c.mu.Unlock()
	return e.instance, true
}

// Put caches inst for its key, evicting the LRU entry when over capacity.
// An existing entry for the same key is closed and replaced.
func (c *Cache) Put(inst ReadInstance) {
	if inst == nil {
		return
	}
	if c.max <= 0 || c.idleTTL <= 0 {
		_ = inst.Close()
		return
	}
	key := inst.Key()

	for {
		c.mu.Lock()
		if el, ok := c.entries[key]; ok {
			e := el.Value.(*entry)
			c.removeLocked(key, el)
			newEl := c.order.PushFront(&entry{key: key, instance: inst, lastUsed: c.now()})
			c.entries[key] = newEl
			c.mu.Unlock()
			_ = e.instance.Close()
			c.notifyRemove(e.instance)
			c.notifyAdd(inst)
			return
		}
		if len(c.entries) < c.max {
			el := c.order.PushFront(&entry{key: key, instance: inst, lastUsed: c.now()})
			c.entries[key] = el
			c.mu.Unlock()
			c.notifyAdd(inst)
			return
		}
		oldest := c.order.Back()
		if oldest == nil {
			c.mu.Unlock()
			_ = inst.Close()
			return
		}
		e := oldest.Value.(*entry)
		c.removeLocked(e.key, oldest)
		c.mu.Unlock()
		_ = e.instance.Close()
		c.notifyRemove(e.instance)
	}
}

// Evict removes and closes the instance for key if cached.
func (c *Cache) Evict(key string) {
	c.mu.Lock()
	el, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		return
	}
	e := el.Value.(*entry)
	c.removeLocked(key, el)
	c.mu.Unlock()
	_ = e.instance.Close()
	c.notifyRemove(e.instance)
}

// Close evicts and closes every cached instance.
func (c *Cache) Close() error {
	c.mu.Lock()
	entries := make([]ReadInstance, 0, len(c.entries))
	for {
		el := c.order.Back()
		if el == nil {
			break
		}
		e := el.Value.(*entry)
		c.removeLocked(e.key, el)
		entries = append(entries, e.instance)
	}
	c.mu.Unlock()
	var firstErr error
	for _, inst := range entries {
		if err := inst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.notifyRemove(inst)
	}
	return firstErr
}

// Len reports the number of cached instances.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *Cache) removeLocked(key string, el *list.Element) {
	delete(c.entries, key)
	c.order.Remove(el)
}

func (c *Cache) notifyAdd(inst ReadInstance) {
	c.mu.Lock()
	fn := c.onAdd
	c.mu.Unlock()
	if fn != nil {
		fn(inst)
	}
}

func (c *Cache) notifyRemove(inst ReadInstance) {
	c.mu.Lock()
	fn := c.onRemove
	c.mu.Unlock()
	if fn != nil {
		fn(inst)
	}
}
