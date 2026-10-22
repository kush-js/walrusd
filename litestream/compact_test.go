package litestream

import (
	"testing"

	"github.com/superfly/ltx"
)

func TestManifestNeedsCompaction(t *testing.T) {
	tests := []struct {
		name      string
		files     []*ltx.FileInfo
		threshold int
		want      bool
	}{
		{"empty", nil, 2, false},
		{"below threshold", []*ltx.FileInfo{{Level: 0}, {Level: 0}}, 2, false},
		{"level zero over", []*ltx.FileInfo{{Level: 0}, {Level: 0}, {Level: 0}}, 2, true},
		{"higher level over", []*ltx.FileInfo{{Level: 0}, {Level: 1}, {Level: 1}, {Level: 1}}, 2, true},
		{"top level ignored", []*ltx.FileInfo{{Level: 8}, {Level: 8}, {Level: 8}}, 2, false},
		{"snapshot ignored", []*ltx.FileInfo{{Level: 9}, {Level: 9}, {Level: 9}}, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ManifestNeedsCompaction(tt.files, tt.threshold); got != tt.want {
				t.Fatalf("ManifestNeedsCompaction(%v, %d) = %v, want %v", tt.files, tt.threshold, got, tt.want)
			}
		})
	}
}
