package blockchain

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ethereum/go-ethereum/core/types"

	"github.com/chainflow/chainflow/internal/domain"
)

type ManagedProvider struct {
	Config ProviderConfig
	Client Client
	Guard  *ProviderGuard
}

type Router struct {
	providers []ManagedProvider
}

func NewRouter(providers []ManagedProvider) (*Router, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("%w: at least one provider is required", domain.ErrInvalidInput)
	}
	ordered := make([]ManagedProvider, 0, len(providers))
	for _, p := range providers {
		if p.Client == nil {
			return nil, fmt.Errorf("%w: provider %q client must not be nil", domain.ErrInvalidInput, p.Config.Name)
		}
		if p.Guard == nil {
			p.Guard = NewProviderGuard(p.Config, nil)
		}
		ordered = append(ordered, p)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].Config.Priority < ordered[j].Config.Priority
	})
	return &Router{providers: ordered}, nil
}

func (r *Router) ChainID(ctx context.Context) (int64, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (int64, error) {
		return c.ChainID(cCtx)
	})
}

func (r *Router) BlockNumber(ctx context.Context) (uint64, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (uint64, error) {
		return c.BlockNumber(cCtx)
	})
}

func (r *Router) HeaderByNumber(ctx context.Context, number *uint64) (Header, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (Header, error) {
		return c.HeaderByNumber(cCtx, number)
	})
}

func (r *Router) PendingNonceAt(ctx context.Context, addr domain.Address) (uint64, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (uint64, error) {
		return c.PendingNonceAt(cCtx, addr)
	})
}

func (r *Router) LatestNonceAt(ctx context.Context, addr domain.Address) (uint64, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (uint64, error) {
		return c.LatestNonceAt(cCtx, addr)
	})
}

func (r *Router) SuggestGasFees(ctx context.Context) (FeeSuggestion, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (FeeSuggestion, error) {
		return c.SuggestGasFees(cCtx)
	})
}

func (r *Router) EstimateGas(ctx context.Context, msg CallMsg) (uint64, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (uint64, error) {
		return c.EstimateGas(cCtx, msg)
	})
}

func (r *Router) SendRawTransaction(ctx context.Context, rawTx []byte) error {
	var tx types.Transaction
	if err := tx.UnmarshalBinary(rawTx); err != nil {
		return fmt.Errorf("%w: invalid raw signed tx: %v", domain.ErrInvalidInput, err)
	}
	var txHash domain.Hash
	copy(txHash[:], tx.Hash().Bytes())

	var lastErr error
	for _, p := range r.providers {
		err := p.Guard.Do(ctx, func(cCtx context.Context) error {
			return p.Client.SendRawTransaction(cCtx, rawTx)
		})
		if err == nil {
			return nil
		}
		var ce *ClassifiedError
		if errors.As(err, &ce) && ce.Kind == ErrKindAlreadyKnown {
			return nil
		}
		if errors.Is(err, ErrCircuitOpen) || isRetryableFailover(err) {
			if _, found, recErr := r.ReconcileTransaction(ctx, txHash); recErr == nil && found {
				return nil
			}
			lastErr = err
			continue
		}
		return err
	}
	if lastErr != nil {
		return fmt.Errorf("%w: %v", ErrNoHealthyProviders, lastErr)
	}
	return ErrNoHealthyProviders
}

func (r *Router) TransactionReceipt(ctx context.Context, txHash domain.Hash) (Receipt, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (Receipt, error) {
		return c.TransactionReceipt(cCtx, txHash)
	})
}

func (r *Router) TransactionByHash(ctx context.Context, txHash domain.Hash) (ChainTransaction, error) {
	return callWithFailover(ctx, r.providers, func(cCtx context.Context, c Client) (ChainTransaction, error) {
		return c.TransactionByHash(cCtx, txHash)
	})
}

func (r *Router) ReconcileTransaction(ctx context.Context, txHash domain.Hash) (ChainTransaction, bool, error) {
	var lastErr error
	var checked int
	for _, p := range r.providers {
		var tx ChainTransaction
		err := p.Guard.Do(ctx, func(cCtx context.Context) error {
			var callErr error
			tx, callErr = p.Client.TransactionByHash(cCtx, txHash)
			return callErr
		})
		if err == nil {
			return tx, true, nil
		}
		if errors.Is(err, ErrTransactionNotFound) {
			checked++
			continue
		}
		lastErr = err
	}
	if checked > 0 {
		return ChainTransaction{}, false, nil
	}
	if lastErr != nil {
		return ChainTransaction{}, false, fmt.Errorf("%w: %v", ErrNoHealthyProviders, lastErr)
	}
	return ChainTransaction{}, false, ErrNoHealthyProviders
}

func (r *Router) CrossCheckReceipt(ctx context.Context, txHash domain.Hash, minBlock uint64) (Receipt, string, error) {
	var lastErr error
	for _, p := range r.providers {
		var head uint64
		err := p.Guard.Do(ctx, func(cCtx context.Context) error {
			var callErr error
			head, callErr = p.Client.BlockNumber(cCtx)
			return callErr
		})
		if err != nil {
			lastErr = err
			continue
		}
		if minBlock > 0 && head < minBlock {
			continue
		}

		var receipt Receipt
		err = p.Guard.Do(ctx, func(cCtx context.Context) error {
			var callErr error
			receipt, callErr = p.Client.TransactionReceipt(cCtx, txHash)
			return callErr
		})
		if err == nil {
			return receipt, p.Config.Name, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return Receipt{}, "", lastErr
	}
	return Receipt{}, "", ErrReceiptNotFound
}

func (r *Router) CrossCheckBlockHash(ctx context.Context, blockNumber uint64, expectedHash domain.Hash) (bool, error) {
	var lastErr error
	var checked int
	for _, p := range r.providers {
		var hdr Header
		err := p.Guard.Do(ctx, func(cCtx context.Context) error {
			var callErr error
			num := blockNumber
			hdr, callErr = p.Client.HeaderByNumber(cCtx, &num)
			return callErr
		})
		if err != nil {
			lastErr = err
			continue
		}
		checked++
		if hdr.Hash == expectedHash {
			return true, nil
		}
	}
	if checked > 0 {
		return false, nil
	}
	if lastErr != nil {
		return false, fmt.Errorf("%w: %v", ErrNoHealthyProviders, lastErr)
	}
	return false, ErrNoHealthyProviders
}

func (r *Router) Close() {
	for _, p := range r.providers {
		p.Client.Close()
	}
}

func callWithFailover[T any](ctx context.Context, providers []ManagedProvider, fn func(context.Context, Client) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for _, p := range providers {
		var out T
		err := p.Guard.Do(ctx, func(cCtx context.Context) error {
			var callErr error
			out, callErr = fn(cCtx, p.Client)
			return callErr
		})
		if err == nil {
			return out, nil
		}
		if errors.Is(err, ErrCircuitOpen) || isRetryableFailover(err) {
			lastErr = err
			continue
		}
		return zero, err
	}
	if lastErr != nil {
		return zero, fmt.Errorf("%w: %v", ErrNoHealthyProviders, lastErr)
	}
	return zero, ErrNoHealthyProviders
}

func isRetryableFailover(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.Retryable()
	}
	return false
}
