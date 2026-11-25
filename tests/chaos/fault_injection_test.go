package chaos_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chainflow/chainflow/internal/blockchain"
)

type flakyRPCClient struct {
	blockchain.Client
	failAfter int64
	calls     atomic.Int64
	errToFail error
	blockNum  uint64
}

func (f *flakyRPCClient) BlockNumber(_ context.Context) (uint64, error) {
	n := f.calls.Add(1)
	if f.failAfter >= 0 && n > f.failAfter {
		return 0, f.errToFail
	}
	return f.blockNum, nil
}

func TestChaos_RPCFailoverUnderConcurrentFaults(t *testing.T) {
	t.Parallel()

	primary := &flakyRPCClient{
		failAfter: 5,
		errToFail: blockchain.ClassifyRPCError("primary", &blockchain.HTTPError{StatusCode: 429, Body: "rate limited"}),
		blockNum:  100,
	}
	secondary := &flakyRPCClient{
		failAfter: -1,
		blockNum:  100,
	}

	router, err := blockchain.NewRouter([]blockchain.ManagedProvider{
		{
			Config: blockchain.ProviderConfig{Name: "primary", Priority: 1, MaxInflight: 100, RateLimitRPS: 10000, RateLimitBurst: 10000, RequestTimeout: time.Second, FailureThreshold: 2, OpenCooldown: 5 * time.Second},
			Client: primary,
		},
		{
			Config: blockchain.ProviderConfig{Name: "secondary", Priority: 2, MaxInflight: 100, RateLimitRPS: 10000, RateLimitBurst: 10000, RequestTimeout: time.Second, FailureThreshold: 10},
			Client: secondary,
		},
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	ctx := context.Background()
	const workers = 25
	const perWorker = 20

	var errCount atomic.Int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				bn, err := router.BlockNumber(ctx)
				if err != nil || bn != 100 {
					errCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("expected 0 errors after failover, got %d", errCount.Load())
	}
	if secondary.calls.Load() == 0 {
		t.Fatal("expected secondary provider to handle traffic after primary tripped")
	}
	_ = errors.New("")
}
