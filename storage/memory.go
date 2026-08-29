package storage

import (
	"context"
	"crypto/md5"
	"sort"
	"strconv"
	"sync"
)

// MemoryStore is an in-memory ConditionalStore. It is strongly consistent
// by construction and used by the conformance suite and unit tests.
type MemoryStore struct {
	mu      sync.Mutex
	objects map[string]*memObject
}

type memObject struct {
	body    []byte
	version Version
}

// NewMemoryStore builds an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]*memObject)}
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), obj.body...), obj.version, nil
}

func (m *MemoryStore) CreateIfAbsent(_ context.Context, key string, body []byte) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key]; ok {
		return "", ErrAlreadyExists
	}
	v := nextVersion(key, body)
	m.objects[key] = &memObject{body: append([]byte(nil), body...), version: v}
	return v, nil
}

func (m *MemoryStore) ReplaceIfVersion(_ context.Context, key, expectedVersion string, body []byte) (Version, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return "", ErrNotFound
	}
	if obj.version != expectedVersion {
		return "", ErrConflict
	}
	v := nextVersion(key, body)
	obj.body = append([]byte(nil), body...)
	obj.version = v
	return v, nil
}

func (m *MemoryStore) DeleteIfVersion(_ context.Context, key, expectedVersion string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	if !ok {
		return ErrNotFound
	}
	if obj.version != expectedVersion {
		return ErrConflict
	}
	delete(m.objects, key)
	return nil
}

// Keys returns sorted keys (test helper).
func (m *MemoryStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func nextVersion(key string, body []byte) Version {
	sum := md5.Sum(append([]byte(key), body...))
	return `"` + strconv.Quote(string(sum[:]))[1:len(strconv.Quote(string(sum[:])))-1] + `"`
}

var _ ConditionalStore = (*MemoryStore)(nil)
var _ ConditionalStore = (*S3Store)(nil)
