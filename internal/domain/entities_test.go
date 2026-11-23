package domain_test

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
)

func TestTransactionIntent_Validate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	reqHash, _ := domain.ParseHash("0x1111111111111111111111111111111111111111111111111111111111111111")

	validIntent := func() domain.TransactionIntent {
		i := *newTestIntent(t, domain.IntentCreated)
		i.IdempotencyKey = "idem-123"
		i.RequestHash = reqHash
		return i
	}

	if err := validIntent().Validate(); err != nil {
		t.Fatalf("expected valid intent, got %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*domain.TransactionIntent)
	}{
		{name: "missing id", mutate: func(i *domain.TransactionIntent) { i.ID = "" }},
		{name: "missing request hash", mutate: func(i *domain.TransactionIntent) { i.RequestHash = domain.Hash{} }},
		{name: "zero from address", mutate: func(i *domain.TransactionIntent) { i.From = domain.Address{} }},
		{name: "negative value", mutate: func(i *domain.TransactionIntent) { i.ValueWei = big.NewInt(-1) }},
		{name: "gas limit below 21000", mutate: func(i *domain.TransactionIntent) { g := uint64(20999); i.GasLimitOverride = &g }},
		{name: "expires_at before created_at", mutate: func(i *domain.TransactionIntent) { i.CreatedAt = now; i.ExpiresAt = now }},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			intent := validIntent()
			tc.mutate(&intent)
			if err := intent.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput for %s, got %v", tc.name, err)
			}
		})
	}
}
