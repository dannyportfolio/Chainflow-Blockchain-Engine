package blockchain

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderGuard_CircuitBreakerLifecycle(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0)
	var mu sync.Mutex
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	g := NewProviderGuard(ProviderConfig{
		Name:             "alchemy",
		MaxInflight:      4,
		RateLimitRPS:     100,
		RateLimitBurst:   100,
		FailureThreshold: 3,
		OpenCooldown:     30 * time.Second,
		RequestTimeout:   5 * time.Second,
	}, clock)

	transientErr := &ClassifiedError{Provider: "alchemy", Kind: ErrKindTransient, Err: errors.New("503")}
	for i := 0; i < 3; i++ {
		err := g.Do(context.Background(), func(context.Context) error { return transientErr })
		if !errors.Is(err, transientErr) {
			t.Fatalf("call %d: got %v, want transientErr", i, err)
		}
	}
	if g.State() != CircuitOpen {
		t.Fatalf("State = %s, want CircuitOpen", g.State())
	}

	called := false
	err := g.Do(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if called {
		t.Fatal("fn should not be called while circuit is open")
	}

	// Advance past cooldown -> HalfOpen probe fails -> reopens.
	advance(31 * time.Second)
	err = g.Do(context.Background(), func(context.Context) error { return transientErr })
	if !errors.Is(err, transientErr) {
		t.Fatalf("probe failure got %v, want transientErr", err)
	}
	if g.State() != CircuitOpen {
		t.Fatalf("State after failed probe = %s, want CircuitOpen", g.State())
	}

	// Advance past cooldown -> HalfOpen probe succeeds -> closes.
	advance(31 * time.Second)
	if err := g.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("probe success got %v", err)
	}
	if g.State() != CircuitClosed {
		t.Fatalf("State after successful probe = %s, want CircuitClosed", g.State())
	}
}

func TestProviderGuard_DeterministicErrorsDoNotTripCircuit(t *testing.T) {
	t.Parallel()

	g := NewProviderGuard(ProviderConfig{
		Name:             "alchemy",
		MaxInflight:      4,
		RateLimitRPS:     100,
		RateLimitBurst:   100,
		FailureThreshold: 2,
		OpenCooldown:     10 * time.Second,
	}, nil)

	deterministicErrs := []error{
		&ClassifiedError{Provider: "alchemy", Kind: ErrKindNonceTooLow, Err: errors.New("nonce too low")},
		&ClassifiedError{Provider: "alchemy", Kind: ErrKindUnderpriced, Err: errors.New("replacement transaction underpriced")},
		&ClassifiedError{Provider: "alchemy", Kind: ErrKindAlreadyKnown, Err: errors.New("already known")},
		&ClassifiedError{Provider: "alchemy", Kind: ErrKindInsufficientFunds, Err: errors.New("insufficient funds")},
	}
	for i := 0; i < 10; i++ {
		err := deterministicErrs[i%len(deterministicErrs)]
		_ = g.Do(context.Background(), func(context.Context) error { return err })
	}
	if g.State() != CircuitClosed {
		t.Fatalf("State = %s, want CircuitClosed", g.State())
	}
}

func TestProviderGuard_BoundedInflightAndRateLimit(t *testing.T) {
	t.Parallel()

	g := NewProviderGuard(ProviderConfig{
		Name:             "infura",
		MaxInflight:      2,
		RateLimitRPS:     1000,
		RateLimitBurst:   1000,
		FailureThreshold: 5,
		OpenCooldown:     10 * time.Second,
	}, nil)

	var active, maxSeen atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := g.Do(context.Background(), func(context.Context) error {
				cur := active.Add(1)
				for {
					prev := maxSeen.Load()
					if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				active.Add(-1)
				return nil
			})
			if err != nil {
				t.Errorf("Do: %v", err)
			}
		}()
	}
	wg.Wait()

	if maxSeen.Load() > 2 {
		t.Fatalf("max concurrent inflight = %d, want <= 2", maxSeen.Load())
	}

	// Rate limit exhaustion returns ErrKindRateLimit without tripping circuit.
	now := time.Unix(1700000000, 0)
	rl := NewProviderGuard(ProviderConfig{
		Name:           "tight",
		MaxInflight:    4,
		RateLimitRPS:   1,
		RateLimitBurst: 1,
	}, func() time.Time { return now })

	if err := rl.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	err := rl.Do(context.Background(), func(context.Context) error { return nil })
	var ce *ClassifiedError
	if !errors.As(err, &ce) || ce.Kind != ErrKindRateLimit {
		t.Fatalf("expected ErrKindRateLimit on burst exhaustion, got %v", err)
	}
}
