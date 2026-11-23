package domain_test

import (
	"testing"

	"github.com/chainflow/chainflow/internal/domain"
)

func TestParseAddress_ValidAndInvalid(t *testing.T) {
	t.Parallel()

	addr, err := domain.ParseAddress("0xAbCdEf0123456789AbCdEf0123456789AbCdEf01")
	if err != nil {
		t.Fatalf("expected valid address, got error: %v", err)
	}
	if addr.String() != "0xabcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("unexpected canonical lowercase hex: %s", addr.String())
	}
	if addr.IsZero() {
		t.Fatal("expected non-zero address")
	}

	invalid := []string{
		"",
		"abcdef0123456789abcdef0123456789abcdef01",
		"0x1234",
		"0xzzzzef0123456789abcdef0123456789abcdef01",
	}
	for _, raw := range invalid {
		if _, err := domain.ParseAddress(raw); err == nil {
			t.Fatalf("expected error for invalid address %q", raw)
		}
	}
}

func TestParseHash_ValidAndInvalid(t *testing.T) {
	t.Parallel()

	valid := "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	h, err := domain.ParseHash(valid)
	if err != nil {
		t.Fatalf("expected valid hash, got error: %v", err)
	}
	if h.String() != valid {
		t.Fatalf("expected %s, got %s", valid, h.String())
	}
	if h.IsZero() {
		t.Fatal("expected non-zero hash")
	}

	if _, err := domain.ParseHash("0x1234"); err == nil {
		t.Fatal("expected error for short hash")
	}
}

func TestStatusCategories_AreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	allIntents := []domain.IntentStatus{
		domain.IntentCreated,
		domain.IntentQueued,
		domain.IntentProcessing,
		domain.IntentPrepared,
		domain.IntentBroadcasted,
		domain.IntentConfirming,
		domain.IntentRetrying,
		domain.IntentFinalized,
		domain.IntentFailed,
		domain.IntentCancelled,
		domain.IntentExpired,
		domain.IntentReplaced,
	}

	for _, s := range allIntents {
		if !s.IsValid() {
			t.Fatalf("status %q must be valid", s)
		}
		if s.IsTerminal() && s.IsActive() {
			t.Fatalf("status %q cannot be both terminal and active", s)
		}
		if !s.IsTerminal() && !s.IsActive() {
			t.Fatalf("status %q must be either terminal or active", s)
		}
	}

	if domain.IntentStatus("BOGUS").IsValid() {
		t.Fatal("unknown intent status must not be valid")
	}
}
