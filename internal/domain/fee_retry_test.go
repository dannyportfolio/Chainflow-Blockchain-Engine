package domain_test

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
)

func TestCalculateRBFBump_EnforcesMinBumpAndCap(t *testing.T) {
	t.Parallel()

	capWei := big.NewInt(100_000_000_000)
	prevPrio := big.NewInt(2_000_000_000)
	prevMax := big.NewInt(30_000_000_000)

	newPrio, newMax, err := domain.CalculateRBFBump(prevPrio, prevMax, capWei, 1500)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newPrio.Cmp(big.NewInt(2_300_000_000)) != 0 {
		t.Fatalf("expected 2.3 gwei priority fee, got %s", newPrio.String())
	}
	if newMax.Cmp(big.NewInt(34_500_000_000)) != 0 {
		t.Fatalf("expected 34.5 gwei max fee, got %s", newMax.String())
	}

	if _, _, err := domain.CalculateRBFBump(prevPrio, prevMax, capWei, 500); !errors.Is(err, domain.ErrInvalidBump) {
		t.Fatalf("expected ErrInvalidBump for 5%% bump, got %v", err)
	}

	tightCap := big.NewInt(32_000_000_000)
	if _, _, err := domain.CalculateRBFBump(prevPrio, prevMax, tightCap, 1500); !errors.Is(err, domain.ErrFeeCapExceeded) {
		t.Fatalf("expected ErrFeeCapExceeded, got %v", err)
	}

	smallPrio, smallMax, err := domain.CalculateRBFBump(big.NewInt(1), big.NewInt(2), capWei, 1500)
	if err != nil {
		t.Fatalf("unexpected error on small fees: %v", err)
	}
	if smallPrio.Cmp(big.NewInt(2)) < 0 || smallMax.Cmp(big.NewInt(3)) < 0 {
		t.Fatalf("expected at least +1 wei bump, got prio=%s max=%s", smallPrio, smallMax)
	}
}

func TestRetryPolicy_NextDelayExponentialAndCapped(t *testing.T) {
	t.Parallel()

	policy := domain.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   2 * time.Second,
		MaxDelay:    30 * time.Second,
	}

	d1 := policy.NextDelay(1, 0.0)
	d2 := policy.NextDelay(2, 0.0)
	d5 := policy.NextDelay(5, 0.0)

	if d1 != 2*time.Second {
		t.Fatalf("attempt 1: expected 2s, got %s", d1)
	}
	if d2 != 4*time.Second {
		t.Fatalf("attempt 2: expected 4s, got %s", d2)
	}
	if d5 != 30*time.Second {
		t.Fatalf("attempt 5: expected capped 30s, got %s", d5)
	}

	dj := policy.NextDelay(2, 0.25)
	if dj < 4*time.Second || dj > 5*time.Second {
		t.Fatalf("expected jittered delay in [4s, 5s], got %s", dj)
	}
}
