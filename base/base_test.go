package base

import "testing"

func TestParseDatabaseID(t *testing.T) {
	id, err := NewDatabaseID("org_smoke", "user_1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseDatabaseID(string(id))
	if err != nil || got != id {
		t.Fatalf("roundtrip: got=%q err=%v", got, err)
	}
	// Container smoke IDs: org_smoke/user_1.
	if _, err := ParseDatabaseID("org/org_smoke/user/user_1"); err != nil {
		t.Fatalf("container ID rejected: %v", err)
	}
}
