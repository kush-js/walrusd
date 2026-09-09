package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
)

// MemoryStore is an in-process Store. It is strongly consistent by
// construction and used for local dev, unit tests, and single-process
// deployments. It provides no exclusion across processes: use Redis.
type MemoryStore struct {
	mu      sync.Mutex
	objects map[string]*memEntry
}

type memEntry struct {
	body  []byte
	token string
}

// NewMemoryStore builds an empty in-memory lease store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]*memEntry)}
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), e.body...), e.token, nil
}

func (m *MemoryStore) CreateIfAbsent(_ context.Context, key string, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key]; ok {
		return "", ErrAlreadyExists
	}
	tok := newToken()
	m.objects[key] = &memEntry{body: append([]byte(nil), body...), token: tok}
	return tok, nil
}

func (m *MemoryStore) ReplaceIfToken(_ context.Context, key, expectedToken string, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.objects[key]
	if !ok {
		return "", ErrNotFound
	}
	if e.token != expectedToken {
		return "", ErrConflict
	}
	tok := newToken()
	e.body = append([]byte(nil), body...)
	e.token = tok
	return tok, nil
}

// Keys returns sorted keys (test helper).
func (m *MemoryStore) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for k := range m.objects {
		keys = append(keys, k)
	}
	return keys
}

// newToken mints an unpredictable fencing token. Tokens are never derived
// from the body: identical records written twice must still differ so a
// stale holder can never match a successor's token.
func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("lease: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

var _ Store = (*MemoryStore)(nil)
