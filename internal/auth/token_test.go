package auth

import "testing"

func TestSigner(t *testing.T) {
	s, err := NewSigner("0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	tok := s.Issue("dev-1")
	if !s.Verify("dev-1", tok) {
		t.Fatal("token should verify for its own device")
	}
	if s.Verify("dev-2", tok) {
		t.Fatal("token must not verify for a different device")
	}
	if s.Verify("dev-1", "") {
		t.Fatal("empty token must not verify")
	}

	other, _ := NewSigner("fedcba9876543210")
	if other.Verify("dev-1", tok) {
		t.Fatal("token must not verify under a rotated secret")
	}

	if _, err := NewSigner("short"); err == nil {
		t.Fatal("short secrets should be rejected")
	}
}
