package security_test

import (
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/security"
)

func TestAPIKeyAuthenticator(t *testing.T) {
	t.Parallel()

	allowedAddr, _ := domain.ParseAddress("0x1111111111111111111111111111111111111111")
	otherAddr, _ := domain.ParseAddress("0x2222222222222222222222222222222222222222")

	auth, err := security.NewAPIKeyAuthenticator([]security.APIKeyEntry{
		{
			TenantID:       "tenant-a",
			RawKey:         "cf_test_secret_key_alpha",
			AllowedSenders: []domain.Address{allowedAddr},
		},
	})
	if err != nil {
		t.Fatalf("NewAPIKeyAuthenticator failed: %v", err)
	}

	principal, err := auth.Authenticate("cf_test_secret_key_alpha")
	if err != nil {
		t.Fatalf("expected valid key authentication, got %v", err)
	}
	if principal.TenantID != "tenant-a" {
		t.Fatalf("expected tenant-a, got %s", principal.TenantID)
	}
	if !auth.CanUseSender(principal, allowedAddr) {
		t.Fatal("expected allowed sender to pass")
	}
	if auth.CanUseSender(principal, otherAddr) {
		t.Fatal("expected unauthorized sender to be rejected")
	}

	if _, err := auth.Authenticate("wrong-key"); err == nil {
		t.Fatal("expected error for invalid key")
	}
	if _, err := auth.Authenticate(""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestTenantRateLimiter(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	limiter := security.NewTenantRateLimiter(2, 2, func() time.Time { return now })

	if !limiter.Allow("tenant-a") || !limiter.Allow("tenant-a") {
		t.Fatal("expected initial burst of 2 to pass")
	}
	if limiter.Allow("tenant-a") {
		t.Fatal("expected 3rd immediate request for tenant-a to be rate limited")
	}
	if !limiter.Allow("tenant-b") {
		t.Fatal("expected tenant-b bucket to be independent")
	}

	now = now.Add(500 * time.Millisecond)
	if !limiter.Allow("tenant-a") {
		t.Fatal("expected 1 token refilled after 500ms at 2 req/s")
	}
}
