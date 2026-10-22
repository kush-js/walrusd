package litestream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	ls "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ErrManifestNotFound means the database has no durable manifest yet. It is
// not an error: legacy replicas fall back to listing their levels once, and
// the next write creates the manifest.
var ErrManifestNotFound = errors.New("litestream: manifest not found")

// ManifestStore persists one small JSON document per database. The manifest
// is the durable index of LTX files, written after every flush, so opening a
// database is one GET instead of one LIST per level. It is not a cache: every
// request reads it from object storage.
type ManifestStore interface {
	Get(ctx context.Context) ([]byte, error)
	Put(ctx context.Context, data []byte) error
}

type manifestDocument struct {
	Version int            `json:"version"`
	Files   []manifestFile `json:"files"`
}

type manifestFile struct {
	Level     int       `json:"level"`
	MinTXID   uint64    `json:"min_txid"`
	MaxTXID   uint64    `json:"max_txid"`
	Size      int64     `json:"size,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

const manifestVersion = 1

// EncodeManifest serializes a file list, sorted by level and TXID.
func EncodeManifest(files []*ltx.FileInfo) ([]byte, error) {
	doc := manifestDocument{Version: manifestVersion, Files: make([]manifestFile, 0, len(files))}
	for _, f := range files {
		if f == nil {
			continue
		}
		doc.Files = append(doc.Files, manifestFile{
			Level:     f.Level,
			MinTXID:   uint64(f.MinTXID),
			MaxTXID:   uint64(f.MaxTXID),
			Size:      f.Size,
			CreatedAt: f.CreatedAt,
		})
	}
	sort.Slice(doc.Files, func(i, j int) bool {
		if doc.Files[i].Level != doc.Files[j].Level {
			return doc.Files[i].Level < doc.Files[j].Level
		}
		if doc.Files[i].MinTXID != doc.Files[j].MinTXID {
			return doc.Files[i].MinTXID < doc.Files[j].MinTXID
		}
		return doc.Files[i].MaxTXID < doc.Files[j].MaxTXID
	})
	return json.Marshal(doc)
}

// DecodeManifest parses and validates a manifest document.
func DecodeManifest(data []byte) ([]*ltx.FileInfo, error) {
	var doc manifestDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("litestream: decode manifest: %w", err)
	}
	if doc.Version != manifestVersion {
		return nil, fmt.Errorf("litestream: unsupported manifest version %d", doc.Version)
	}
	files := make([]*ltx.FileInfo, 0, len(doc.Files))
	seen := make(map[[3]uint64]struct{}, len(doc.Files))
	for _, f := range doc.Files {
		if f.Level < 0 || f.Level > ls.SnapshotLevel {
			return nil, fmt.Errorf("litestream: manifest level %d out of range", f.Level)
		}
		if f.MinTXID == 0 || f.MaxTXID < f.MinTXID {
			return nil, fmt.Errorf("litestream: manifest file range %d-%d is invalid", f.MinTXID, f.MaxTXID)
		}
		key := [3]uint64{uint64(f.Level), f.MinTXID, f.MaxTXID}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		files = append(files, &ltx.FileInfo{
			Level:     f.Level,
			MinTXID:   ltx.TXID(f.MinTXID),
			MaxTXID:   ltx.TXID(f.MaxTXID),
			Size:      f.Size,
			CreatedAt: f.CreatedAt,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Level != files[j].Level {
			return files[i].Level < files[j].Level
		}
		return files[i].MinTXID < files[j].MinTXID
	})
	return files, nil
}

// manifestMaxTXID returns the highest TXID in a file list.
func manifestMaxTXID(files []*ltx.FileInfo) ltx.TXID {
	var max ltx.TXID
	for _, f := range files {
		if f.MaxTXID > max {
			max = f.MaxTXID
		}
	}
	return max
}

// manifestHasTXID reports whether the manifest covers txid.
func manifestHasTXID(files []*ltx.FileInfo, txid ltx.TXID) bool {
	for _, f := range files {
		if f.MinTXID <= txid && txid <= f.MaxTXID {
			return true
		}
	}
	return false
}

// applyManifestDelta removes deleted files and adds written files. It is used
// by compaction after re-reading the manifest under the write lease.
func applyManifestDelta(base, written, deleted []*ltx.FileInfo) []*ltx.FileInfo {
	out := make([]*ltx.FileInfo, 0, len(base)+len(written))
	for _, f := range base {
		if manifestContains(deleted, f) {
			continue
		}
		out = append(out, f)
	}
	for _, f := range written {
		if !manifestContains(out, f) {
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Level != out[j].Level {
			return out[i].Level < out[j].Level
		}
		return out[i].MinTXID < out[j].MinTXID
	})
	return out
}

func manifestContains(files []*ltx.FileInfo, want *ltx.FileInfo) bool {
	for _, f := range files {
		if f.Level == want.Level && f.MinTXID == want.MinTXID && f.MaxTXID == want.MaxTXID {
			return true
		}
	}
	return false
}

func cloneFileInfos(files []*ltx.FileInfo) []*ltx.FileInfo {
	out := make([]*ltx.FileInfo, 0, len(files))
	for _, f := range files {
		if f == nil {
			continue
		}
		c := *f
		out = append(out, &c)
	}
	return out
}

// manifestClient answers LTXFiles from a manifest snapshot and records every
// write/delete made through it. A session-scoped instance means the manifest
// is read once per request, never cached across requests.
type manifestClient struct {
	ls.ReplicaClient

	mu      sync.Mutex
	files   []*ltx.FileInfo
	written []*ltx.FileInfo
	deleted []*ltx.FileInfo
}

func newManifestClient(inner ls.ReplicaClient, files []*ltx.FileInfo) *manifestClient {
	return &manifestClient{ReplicaClient: inner, files: cloneFileInfos(files)}
}

func (c *manifestClient) LTXFiles(_ context.Context, level int, seek ltx.TXID, _ bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	files := cloneFileInfos(c.files)
	c.mu.Unlock()

	out := make([]*ltx.FileInfo, 0, len(files))
	for _, f := range files {
		if f.Level == level && f.MinTXID >= seek {
			out = append(out, f)
		}
	}
	return ltx.NewFileInfoSliceIterator(out), nil
}

func (c *manifestClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	info, err := c.ReplicaClient.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = applyManifestDelta(c.files, []*ltx.FileInfo{info}, nil)
	c.written = append(c.written, info)
	return info, nil
}

func (c *manifestClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
	if err := c.ReplicaClient.DeleteLTXFiles(ctx, files); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.files = applyManifestDelta(c.files, nil, files)
	c.deleted = append(c.deleted, cloneFileInfos(files)...)
	return nil
}

func (c *manifestClient) DeleteAll(ctx context.Context) error {
	if err := c.ReplicaClient.DeleteAll(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.files = nil
	c.mu.Unlock()
	return nil
}

func (c *manifestClient) Files() []*ltx.FileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneFileInfos(c.files)
}

func (c *manifestClient) Written() []*ltx.FileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneFileInfos(c.written)
}

func (c *manifestClient) Deleted() []*ltx.FileInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneFileInfos(c.deleted)
}

// fileManifestStore writes the manifest next to the replica directory, with
// a temp-file + rename so readers never observe a partial document.
type fileManifestStore struct {
	path string
}

func (s *fileManifestStore) Get(_ context.Context) ([]byte, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrManifestNotFound
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *fileManifestStore) Put(_ context.Context, data []byte) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// memoryManifestStore shares manifests between runtimes in one process, like
// MemoryReplicaClient does for LTX files.
type memoryManifestStore struct {
	key string
}

var (
	memoryManifestMu sync.Mutex
	memoryManifests  = make(map[string][]byte)
)

func (s *memoryManifestStore) Get(_ context.Context) ([]byte, error) {
	memoryManifestMu.Lock()
	defer memoryManifestMu.Unlock()
	data, ok := memoryManifests[s.key]
	if !ok {
		return nil, ErrManifestNotFound
	}
	return append([]byte(nil), data...), nil
}

func (s *memoryManifestStore) Put(_ context.Context, data []byte) error {
	memoryManifestMu.Lock()
	defer memoryManifestMu.Unlock()
	memoryManifests[s.key] = append([]byte(nil), data...)
	return nil
}

// s3ManifestStore uses the same bucket/credentials as the replica client but
// a separate tiny object for the manifest.
type s3ManifestStore struct {
	client *awss3.Client
	bucket string
	key    string
}

func newS3ManifestStore(p Profile, replicaPrefix string) (*s3ManifestStore, error) {
	region := normalizeS3Region(p.Region)
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if p.AccessKeyID != "" || p.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(p.AccessKeyID, p.SecretAccessKey, ""),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("litestream: manifest s3 config: %w", err)
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if p.Endpoint != "" {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String(ensureEndpointScheme(p.Endpoint))
		}
	})
	return &s3ManifestStore{
		client: client,
		bucket: p.Bucket,
		key:    path.Join(replicaPrefix, "manifest.json"),
	}, nil
}

func (s *s3ManifestStore) Get(ctx context.Context) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchKey", "NotFound":
				return nil, ErrManifestNotFound
			}
		}
		return nil, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(io.LimitReader(out.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *s3ManifestStore) Put(ctx context.Context, data []byte) error {
	_, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(s.key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

func ensureEndpointScheme(endpoint string) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	return "https://" + endpoint
}

// NewManifestStore builds the persistent manifest location for a database.
func (b *Bridge) NewManifestStore(replicaPrefix string, p Profile) (ManifestStore, error) {
	switch p.Provider {
	case "file":
		if p.FileRoot == "" {
			return nil, fmt.Errorf("litestream: file provider requires FileRoot")
		}
		return &fileManifestStore{path: filepath.Join(p.FileRoot, replicaPrefix, "manifest.json")}, nil
	case "memory":
		return &memoryManifestStore{key: replicaPrefix + "/manifest.json"}, nil
	case "s3":
		return newS3ManifestStore(p, replicaPrefix)
	default:
		return nil, fmt.Errorf("litestream: unsupported provider %q", p.Provider)
	}
}

// ReadManifest returns the durable file list. ok is false when no manifest
// exists yet (legacy or empty database).
func (d *Database) ReadManifest(ctx context.Context) (files []*ltx.FileInfo, ok bool, err error) {
	data, err := d.manifest.Get(ctx)
	if errors.Is(err, ErrManifestNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	files, err = DecodeManifest(data)
	if err != nil {
		return nil, false, err
	}
	return files, true, nil
}

// WriteManifest persists the file list. The caller must hold the write lease.
func (d *Database) WriteManifest(ctx context.Context, files []*ltx.FileInfo) error {
	data, err := EncodeManifest(files)
	if err != nil {
		return err
	}
	return d.manifest.Put(ctx, data)
}

// ApplyManifestDelta is the exported form used by the runtime to merge a
// compaction delta into a freshly re-read manifest under the write lease.
func ApplyManifestDelta(base, written, deleted []*ltx.FileInfo) []*ltx.FileInfo {
	return applyManifestDelta(base, written, deleted)
}

// ManifestCovers reports whether txid is inside any file range.
func ManifestCovers(files []*ltx.FileInfo, txid uint64) bool {
	return manifestHasTXID(files, ltx.TXID(txid))
}

// ManifestMaxTXID returns the highest TXID the manifest covers.
func ManifestMaxTXID(files []*ltx.FileInfo) uint64 {
	return uint64(manifestMaxTXID(files))
}

// TailTXID lists level 0 only and returns its highest TXID. Writers use it
// after acquiring the lease to detect an LTX file that a failed manifest
// update left unindexed, so a later write cannot fork on a stale plan.
func (d *Database) TailTXID(ctx context.Context) (ltx.TXID, error) {
	itr, err := d.client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return 0, err
	}
	var max ltx.TXID
	for itr.Next() {
		if item := itr.Item(); item.MaxTXID > max {
			max = item.MaxTXID
		}
	}
	iterErr := itr.Err()
	closeErr := itr.Close()
	if iterErr != nil {
		return 0, iterErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	return max, nil
}

// BuildManifest lists every level once. It is only used to migrate a legacy
// replica or repair a manifest after a failed update.
func (d *Database) BuildManifest(ctx context.Context) ([]*ltx.FileInfo, error) {
	var files []*ltx.FileInfo
	for level := 0; level <= ls.SnapshotLevel; level++ {
		itr, err := d.client.LTXFiles(ctx, level, 0, false)
		if err != nil {
			return nil, err
		}
		for itr.Next() {
			files = append(files, itr.Item())
		}
		iterErr := itr.Err()
		closeErr := itr.Close()
		if iterErr != nil {
			return nil, iterErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return files, nil
}
