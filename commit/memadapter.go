package commit

import (
	"context"
	"fmt"
	"sync"
)

// memAdapter is an in-memory Adapter implementing CAS semantics for tests and
// local development. It reproduces conditional-write behavior faithfully.
type memAdapter struct {
	mu   sync.Mutex
	objs map[string]memObj
	vers int
}

type memObj struct {
	body    []byte
	version string
}

// NewMemAdapter builds an in-memory CAS adapter.
func NewMemAdapter() Adapter {
	return &memAdapter{objs: map[string]memObj{}}
}

func (a *memAdapter) Get(_ context.Context, key string) ([]byte, Version, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.objs[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), o.body...), o.version, nil
}

func (a *memAdapter) CreateIfAbsent(_ context.Context, key string, body []byte) (Version, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.objs[key]; ok {
		return "", ErrConflict
	}
	return a.putLocked(key, body), nil
}

func (a *memAdapter) ReplaceIfVersion(_ context.Context, key string, expected Version, body []byte) (Version, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o, ok := a.objs[key]
	if ok && expected == "" {
		// Emulate R2: conditional create against existing object conflicts.
		return "", ErrConflict
	}
	if !ok {
		if expected != "" {
			return "", ErrConflict
		}
		return a.putLocked(key, body), nil
	}
	if o.version != expected {
		return "", ErrConflict
	}
	return a.putLocked(key, body), nil
}

func (a *memAdapter) putLocked(key string, body []byte) Version {
	a.vers++
	v := fmt.Sprintf("\"v%d\"", a.vers)
	a.objs[key] = memObj{body: append([]byte(nil), body...), version: v}
	return v
}
