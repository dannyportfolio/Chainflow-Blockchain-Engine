package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/chainflow/chainflow/internal/domain"
)

type GasEstimatorConfig struct {
	CacheTTL          time.Duration
	GasLimitBufferBps uint64
	MinPriorityFeeWei *big.Int
	MaxFeeCapWei      *big.Int
}

type GasEstimator struct {
	client Client
	cfg    GasEstimatorConfig
	now    func() time.Time
	sf     singleflight.Group

	mu        sync.RWMutex
	cached    FeeSuggestion
	cachedAt  time.Time
	hasCached bool
}

func NewGasEstimator(client Client, cfg GasEstimatorConfig, now func() time.Time) *GasEstimator {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.GasLimitBufferBps == 0 {
		cfg.GasLimitBufferBps = 1500
	}
	return &GasEstimator{
		client: client,
		cfg:    cfg,
		now:    now,
	}
}

func (g *GasEstimator) SuggestFees(ctx context.Context) (FeeSuggestion, error) {
	if g.cfg.CacheTTL > 0 {
		g.mu.RLock()
		if g.hasCached && g.now().Sub(g.cachedAt) < g.cfg.CacheTTL {
			out := cloneFeeSuggestion(g.cached)
			g.mu.RUnlock()
			return g.applyBounds(out)
		}
		g.mu.RUnlock()
	}

	res, err, _ := g.sf.Do("suggest_fees", func() (any, error) {
		if g.cfg.CacheTTL > 0 {
			g.mu.RLock()
			if g.hasCached && g.now().Sub(g.cachedAt) < g.cfg.CacheTTL {
				out := cloneFeeSuggestion(g.cached)
				g.mu.RUnlock()
				return out, nil
			}
			g.mu.RUnlock()
		}
		// Detach caller cancellation so one aborted request does not fail coalesced waiters.
		sfCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		fees, err := g.client.SuggestGasFees(sfCtx)
		if err != nil {
			return FeeSuggestion{}, err
		}
		if g.cfg.CacheTTL > 0 {
			g.mu.Lock()
			g.cached = cloneFeeSuggestion(fees)
			g.cachedAt = g.now()
			g.hasCached = true
			g.mu.Unlock()
		}
		return fees, nil
	})
	if err != nil {
		return FeeSuggestion{}, err
	}
	if err := ctx.Err(); err != nil {
		return FeeSuggestion{}, err
	}
	return g.applyBounds(res.(FeeSuggestion))
}

func (g *GasEstimator) EstimateGasWithBuffer(ctx context.Context, msg CallMsg) (uint64, error) {
	est, err := g.client.EstimateGas(ctx, msg)
	if err != nil {
		return 0, err
	}
	buffered := est + (est*g.cfg.GasLimitBufferBps)/10000
	if buffered < 21000 {
		buffered = 21000
	}
	return buffered, nil
}

func (g *GasEstimator) applyBounds(f FeeSuggestion) (FeeSuggestion, error) {
	out := cloneFeeSuggestion(f)
	if g.cfg.MinPriorityFeeWei != nil && out.MaxPriorityFeePerGas.Cmp(g.cfg.MinPriorityFeeWei) < 0 {
		out.MaxPriorityFeePerGas = new(big.Int).Set(g.cfg.MinPriorityFeeWei)
		if out.MaxFeePerGas.Cmp(out.MaxPriorityFeePerGas) < 0 {
			out.MaxFeePerGas = new(big.Int).Set(out.MaxPriorityFeePerGas)
		}
	}
	if g.cfg.MaxFeeCapWei != nil && out.MaxFeePerGas.Cmp(g.cfg.MaxFeeCapWei) > 0 {
		return FeeSuggestion{}, fmt.Errorf("%w: suggested max fee %s > cap %s", domain.ErrFeeCapExceeded, out.MaxFeePerGas.String(), g.cfg.MaxFeeCapWei.String())
	}
	return out, nil
}

func cloneFeeSuggestion(f FeeSuggestion) FeeSuggestion {
	var base, tip, maxFee *big.Int
	if f.BaseFeeWei != nil {
		base = new(big.Int).Set(f.BaseFeeWei)
	}
	if f.MaxPriorityFeePerGas != nil {
		tip = new(big.Int).Set(f.MaxPriorityFeePerGas)
	}
	if f.MaxFeePerGas != nil {
		maxFee = new(big.Int).Set(f.MaxFeePerGas)
	}
	return FeeSuggestion{
		BaseFeeWei:           base,
		MaxPriorityFeePerGas: tip,
		MaxFeePerGas:         maxFee,
	}
}
