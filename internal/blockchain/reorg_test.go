package blockchain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/domain"
)

type stubHeaderClient struct {
	blockchain.Client
	header blockchain.Header
	err    error
}

func (s *stubHeaderClient) HeaderByNumber(_ context.Context, _ *uint64) (blockchain.Header, error) {
	if s.err != nil {
		return blockchain.Header{}, s.err
	}
	return s.header, nil
}

func TestReorgVerifier_VerifyBlockHash(t *testing.T) {
	t.Parallel()

	expected, _ := domain.ParseHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	forked, _ := domain.ParseHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	ctx := context.Background()

	t.Run("all providers match canonical hash", func(t *testing.T) {
		v, err := blockchain.NewReorgVerifier(
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: expected}},
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: expected}},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		res, err := v.VerifyBlockHash(ctx, 100, expected)
		if err != nil {
			t.Fatalf("verify failed: %v", err)
		}
		if !res.Matches || res.Reorged || res.Disagreement {
			t.Fatalf("unexpected result: %+v", res)
		}
	})

	t.Run("all responding providers report different hash -> reorg", func(t *testing.T) {
		v, err := blockchain.NewReorgVerifier(
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: forked}},
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: forked}},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		res, err := v.VerifyBlockHash(ctx, 100, expected)
		if err != nil {
			t.Fatalf("verify failed: %v", err)
		}
		if res.Matches || !res.Reorged || res.CanonicalHash != forked {
			t.Fatalf("expected reorg with forked hash, got %+v", res)
		}
	})

	t.Run("provider disagreement prevents false reorg", func(t *testing.T) {
		v, err := blockchain.NewReorgVerifier(
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: forked}},
			&stubHeaderClient{header: blockchain.Header{Number: 100, Hash: expected}},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		res, err := v.VerifyBlockHash(ctx, 100, expected)
		if err != nil {
			t.Fatalf("verify failed: %v", err)
		}
		if res.Reorged || !res.Disagreement {
			t.Fatalf("expected disagreement without reorg, got %+v", res)
		}
	})

	t.Run("all providers error returns error", func(t *testing.T) {
		v, err := blockchain.NewReorgVerifier(
			&stubHeaderClient{err: errors.New("rpc down")},
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := v.VerifyBlockHash(ctx, 100, expected); err == nil {
			t.Fatal("expected error when all providers fail")
		}
	})
}
