package identity

import "testing"

func TestNewDatabaseID(t *testing.T) {
	valid := []string{
		"user_1a4b",
		"users/u1",
		"acme/agents/a7",
	}
	for _, id := range valid {
		got, err := NewDatabaseID(id)
		if err != nil {
			t.Fatalf("NewDatabaseID(%q): %v", id, err)
		}
		if got.String() != id {
			t.Fatalf("NewDatabaseID(%q).String() = %q", id, got.String())
		}
	}

	invalid := []string{
		"",
		"/users/u1",
		"users/u1/",
		"users//u1",
		".",
		"..",
		"users/./u1",
		"users/../u1",
		"users/%2e%2e/u1",
		"users/u1?admin=true",
		"users/u1#fragment",
		`users\..\u1`,
		"users/user 1",
		"users/café",
	}
	for _, id := range invalid {
		if _, err := NewDatabaseID(id); err == nil {
			t.Errorf("NewDatabaseID(%q) succeeded, want error", id)
		}
	}
}

func TestParseDatabaseID(t *testing.T) {
	got, err := ParseDatabaseID("org_1/users/u1")
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "org_1/users/u1" {
		t.Fatalf("ParseDatabaseID() = %q", got.String())
	}
	if _, err := ParseDatabaseID("../escape"); err == nil {
		t.Fatal("ParseDatabaseID accepted path traversal")
	}
}

func TestObjectKeys(t *testing.T) {
	db, err := NewDatabaseID("users/u1")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		root    string
		storage string
		lease   string
		replica string
	}{
		{"", "users/u1/", "users/u1/lease.json", "users/u1/replica"},
		{"tenant", "tenant/users/u1/", "tenant/users/u1/lease.json", "tenant/users/u1/replica"},
		{"tenant/", "tenant/users/u1/", "tenant/users/u1/lease.json", "tenant/users/u1/replica"},
	}
	for _, tt := range tests {
		if got := db.StoragePrefix(tt.root); got != tt.storage {
			t.Errorf("StoragePrefix(%q) = %q, want %q", tt.root, got, tt.storage)
		}
		if got := db.LeaseKey(tt.root); got != tt.lease {
			t.Errorf("LeaseKey(%q) = %q, want %q", tt.root, got, tt.lease)
		}
		if got := db.ReplicaPrefix(tt.root); got != tt.replica {
			t.Errorf("ReplicaPrefix(%q) = %q, want %q", tt.root, got, tt.replica)
		}
	}
}
