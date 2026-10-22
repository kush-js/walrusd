package litestream

import (
	"context"
	"errors"
	"io"
	"testing"

	ls "github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

func TestManifestRoundTrip(t *testing.T) {
	files := []*ltx.FileInfo{
		{Level: 1, MinTXID: 1, MaxTXID: 4, Size: 100},
		{Level: 0, MinTXID: 5, MaxTXID: 6, Size: 20},
	}
	data, err := EncodeManifest(files)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeManifest(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("files = %d, want 2", len(got))
	}
	if got[0].Level != 0 || got[0].MinTXID != 5 || got[0].MaxTXID != 6 {
		t.Fatalf("first file = %+v, want level 0 5-6", got[0])
	}
	if got[1].Level != 1 || got[1].MinTXID != 1 || got[1].MaxTXID != 4 {
		t.Fatalf("second file = %+v, want level 1 1-4", got[1])
	}
}

func TestManifestRejectsInvalidRanges(t *testing.T) {
	data, err := EncodeManifest([]*ltx.FileInfo{{Level: 0, MinTXID: 5, MaxTXID: 4}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := DecodeManifest(data); err == nil {
		t.Fatal("decode accepted an inverted range")
	}
}

type manifestStubClient struct {
	ls.ReplicaClient
	listCalls int
	written   *ltx.FileInfo
	deleted   []*ltx.FileInfo
}

func (c *manifestStubClient) LTXFiles(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
	c.listCalls++
	return nil, errors.New("LTXFiles must not be called")
}

func (c *manifestStubClient) WriteLTXFile(_ context.Context, level int, minTXID, maxTXID ltx.TXID, _ io.Reader) (*ltx.FileInfo, error) {
	c.written = &ltx.FileInfo{Level: level, MinTXID: minTXID, MaxTXID: maxTXID, Size: 42}
	return c.written, nil
}

func (c *manifestStubClient) DeleteLTXFiles(_ context.Context, files []*ltx.FileInfo) error {
	c.deleted = append(c.deleted, files...)
	return nil
}

func TestManifestClientServesPlanWithoutListing(t *testing.T) {
	stub := &manifestStubClient{}
	files := []*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 2},
		{Level: 0, MinTXID: 3, MaxTXID: 4},
	}
	client := newManifestClient(stub, files)

	itr, err := client.LTXFiles(context.Background(), 0, 3, false)
	if err != nil {
		t.Fatalf("LTXFiles: %v", err)
	}
	var got []*ltx.FileInfo
	for itr.Next() {
		got = append(got, itr.Item())
	}
	if err := itr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if stub.listCalls != 0 {
		t.Fatalf("manifest read called the underlying LTXFiles %d times", stub.listCalls)
	}
	if len(got) != 1 || got[0].MinTXID != 3 {
		t.Fatalf("items = %+v, want only 3-4", got)
	}

	if _, err := client.WriteLTXFile(context.Background(), 0, 5, 5, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	if files := client.Files(); len(files) != 3 {
		t.Fatalf("files after write = %d, want 3", len(files))
	}
	if err := client.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{files[0]}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if files := client.Files(); len(files) != 2 {
		t.Fatalf("files after delete = %d, want 2", len(files))
	}
}
