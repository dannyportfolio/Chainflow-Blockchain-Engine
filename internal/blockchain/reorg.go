package blockchain

import (
	"context"
	"errors"
	"fmt"

	"github.com/chainflow/chainflow/internal/domain"
)

// BlockVerification describes the canonical status of a mined block across RPC providers.
type BlockVerification struct {
	BlockNumber   uint64
	CanonicalHash domain.Hash
	Matches       bool
	Reorged       bool
	Disagreement  bool
}

// ReorgVerifier checks whether a previously observed block hash remains on the canonical chain.
// When multiple providers are configured, a reorg is declared only when all responding providers
// report a different hash at the same height, preventing false positives from a lagging node.
type ReorgVerifier struct {
	clients []Client
}

func NewReorgVerifier(clients ...Client) (*ReorgVerifier, error) {
	valid := make([]Client, 0, len(clients))
	for _, c := range clients {
		if c != nil {
			valid = append(valid, c)
		}
	}
	if len(valid) == 0 {
		return nil, fmt.Errorf("%w: at least one blockchain client is required", domain.ErrInvalidInput)
	}
	return &ReorgVerifier{clients: valid}, nil
}

func (v *ReorgVerifier) VerifyBlockHash(ctx context.Context, blockNumber uint64, expectedHash domain.Hash) (BlockVerification, error) {
	if expectedHash.IsZero() {
		return BlockVerification{}, fmt.Errorf("%w: expected block hash must not be zero", domain.ErrInvalidInput)
	}

	num := blockNumber
	var matchCount, mismatchCount int
	var firstMismatchHash domain.Hash
	var errs []error

	for _, c := range v.clients {
		hdr, err := c.HeaderByNumber(ctx, &num)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if hdr.Hash == expectedHash {
			matchCount++
		} else {
			if mismatchCount == 0 {
				firstMismatchHash = hdr.Hash
			}
			mismatchCount++
		}
	}

	if matchCount == 0 && mismatchCount == 0 {
		return BlockVerification{}, errors.Join(errs...)
	}

	if matchCount > 0 && mismatchCount > 0 {
		return BlockVerification{
			BlockNumber:   blockNumber,
			CanonicalHash: expectedHash,
			Matches:       true,
			Reorged:       false,
			Disagreement:  true,
		}, nil
	}

	if matchCount > 0 {
		return BlockVerification{
			BlockNumber:   blockNumber,
			CanonicalHash: expectedHash,
			Matches:       true,
			Reorged:       false,
		}, nil
	}

	return BlockVerification{
		BlockNumber:   blockNumber,
		CanonicalHash: firstMismatchHash,
		Matches:       false,
		Reorged:       true,
	}, nil
}
