//go:build verify_examples

package lease

import "context"

// RedisStore mirrors the public Redis-backed API for the documentation
// verifier, backed by an in-process memory store instead of a service.
type RedisStore struct {
	store *MemoryStore
}

type RedisOptions struct {
	Addr     string
	Password string
	DB       int
}

func NewRedisStore(context.Context, RedisOptions) (*RedisStore, error) {
	return &RedisStore{store: NewMemoryStore()}, nil
}

func (s *RedisStore) Close() error { return nil }

func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	return s.store.Get(ctx, key)
}

func (s *RedisStore) CreateIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	return s.store.CreateIfAbsent(ctx, key, body)
}

func (s *RedisStore) ReplaceIfToken(
	ctx context.Context,
	key, expectedToken string,
	body []byte,
) (string, error) {
	return s.store.ReplaceIfToken(ctx, key, expectedToken, body)
}

var _ Store = (*RedisStore)(nil)
