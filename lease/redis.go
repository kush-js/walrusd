package lease

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore is a Store backed by Redis or Valkey (RESP-compatible).
// One small instance is enough: one hash per database, no TTL. Lease keys
// persist with their epoch history (spec §7.3); logical expiry in the
// record — never key eviction — governs takeover.
//
// Deployment requirements:
//   - Enable Redis persistence (AOF). A restart that wipes keys lets a new
//     owner create while a stale holder still believes it owns the lease.
//   - No key TTL is set on purpose: a TTL firing before logical expiry
//     would hand the lease to a successor while the old owner flushes.
type RedisStore struct {
	client *redis.Client
}

// RedisOptions configures a Redis/Valkey lease store.
type RedisOptions struct {
	// Addr is host:port (e.g. "127.0.0.1:6379").
	Addr string
	// Password for AUTH; empty for none.
	Password string
	// DB index; 0 default.
	DB int
}

// NewRedisStore connects and pings. A failed ping is fuse-blown early:
// without the lease store no write may proceed.
func NewRedisStore(ctx context.Context, opts RedisOptions) (*RedisStore, error) {
	if opts.Addr == "" {
		return nil, errors.New("lease: redis addr is required")
	}
	client := redis.NewClient(&redis.Options{
		Addr:        opts.Addr,
		Password:    opts.Password,
		DB:          opts.DB,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 3 * time.Second,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &RedisStore{client: client}, nil
}

// Close releases the client.
func (s *RedisStore) Close() error { return s.client.Close() }

// Lease hashes hold {data, token}. All mutations are single-key Lua so the
// check-and-set is atomic under concurrency from any process.
var (
	luaCreate = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
	return 0
end
redis.call('HSET', KEYS[1], 'data', ARGV[1], 'token', ARGV[2])
return 1
`)
	luaReplace = redis.NewScript(`
local t = redis.call('HGET', KEYS[1], 'token')
if not t then
	return 0
end
if t ~= ARGV[1] then
	return 2
end
redis.call('HSET', KEYS[1], 'data', ARGV[2], 'token', ARGV[3])
return 1
`)
)

func (s *RedisStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	m, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, "", err
	}
	data, ok := m["data"]
	if !ok {
		return nil, "", ErrNotFound
	}
	return []byte(data), m["token"], nil
}

func (s *RedisStore) CreateIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	tok := newToken()
	n, err := luaCreate.Run(ctx, s.client, []string{key}, string(body), tok).Int()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", ErrAlreadyExists
	}
	return tok, nil
}

func (s *RedisStore) ReplaceIfToken(ctx context.Context, key, expectedToken string, body []byte) (string, error) {
	tok := newToken()
	n, err := luaReplace.Run(ctx, s.client, []string{key}, expectedToken, string(body), tok).Int()
	if err != nil {
		return "", err
	}
	switch n {
	case 1:
		return tok, nil
	case 0:
		return "", ErrNotFound
	default:
		return "", ErrConflict
	}
}

var _ Store = (*RedisStore)(nil)
