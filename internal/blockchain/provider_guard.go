package blockchain

import (
	"context"
	"errors"
	"sync"
	"time"
)

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitOpen     CircuitState = "OPEN"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

type ProviderConfig struct {
	Name             string
	URL              string
	Priority         int
	MaxInflight      int
	RateLimitRPS     float64
	RateLimitBurst   int
	FailureThreshold int
	OpenCooldown     time.Duration
	RequestTimeout   time.Duration
}

type ProviderGuard struct {
	cfg ProviderConfig
	now func() time.Time
	sem chan struct{}

	mu               sync.Mutex
	state            CircuitState
	consecutiveFails int
	openedAt         time.Time
	halfOpenInFlight bool

	tokens     float64
	lastRefill time.Time
}

func NewProviderGuard(cfg ProviderConfig, now func() time.Time) *ProviderGuard {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.MaxInflight <= 0 {
		cfg.MaxInflight = 20
	}
	if cfg.RateLimitRPS <= 0 {
		cfg.RateLimitRPS = 50
	}
	if cfg.RateLimitBurst <= 0 {
		cfg.RateLimitBurst = 50
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenCooldown <= 0 {
		cfg.OpenCooldown = 30 * time.Second
	}
	return &ProviderGuard{
		cfg:        cfg,
		now:        now,
		sem:        make(chan struct{}, cfg.MaxInflight),
		state:      CircuitClosed,
		tokens:     float64(cfg.RateLimitBurst),
		lastRefill: now(),
	}
}

func (g *ProviderGuard) Name() string {
	return g.cfg.Name
}

func (g *ProviderGuard) State() CircuitState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == CircuitOpen && g.now().Sub(g.openedAt) >= g.cfg.OpenCooldown {
		return CircuitHalfOpen
	}
	return g.state
}

func (g *ProviderGuard) Do(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	isProbe, err := g.beforeCall()
	if err != nil {
		return err
	}

	if !g.allowToken() {
		g.releaseProbe(isProbe)
		return &ClassifiedError{
			Provider: g.cfg.Name,
			Kind:     ErrKindRateLimit,
			Err:      errors.New("provider local rate limit exceeded"),
		}
	}

	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	case <-ctx.Done():
		g.releaseProbe(isProbe)
		return ctx.Err()
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if g.cfg.RequestTimeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, g.cfg.RequestTimeout)
		defer cancel()
	}

	callErr := fn(callCtx)
	g.afterCall(isProbe, callErr)
	return callErr
}

func (g *ProviderGuard) beforeCall() (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	switch g.state {
	case CircuitClosed:
		return false, nil
	case CircuitOpen:
		if g.now().Sub(g.openedAt) < g.cfg.OpenCooldown {
			return false, ErrCircuitOpen
		}
		g.state = CircuitHalfOpen
		g.halfOpenInFlight = true
		return true, nil
	case CircuitHalfOpen:
		if g.halfOpenInFlight {
			return false, ErrCircuitOpen
		}
		g.halfOpenInFlight = true
		return true, nil
	default:
		return false, nil
	}
}

func (g *ProviderGuard) releaseProbe(isProbe bool) {
	if !isProbe {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.halfOpenInFlight = false
}

func (g *ProviderGuard) afterCall(isProbe bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if isProbe {
		g.halfOpenInFlight = false
	}

	if err == nil {
		g.consecutiveFails = 0
		g.state = CircuitClosed
		return
	}

	if !shouldTripCircuit(err) {
		return
	}

	if g.state == CircuitHalfOpen {
		g.state = CircuitOpen
		g.openedAt = g.now()
		return
	}

	g.consecutiveFails++
	if g.consecutiveFails >= g.cfg.FailureThreshold {
		g.state = CircuitOpen
		g.openedAt = g.now()
	}
}

func shouldTripCircuit(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.TripsCircuit()
	}
	return ClassifyRPCError("", err).TripsCircuit()
}

func (g *ProviderGuard) allowToken() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	elapsed := now.Sub(g.lastRefill).Seconds()
	if elapsed > 0 {
		g.tokens += elapsed * g.cfg.RateLimitRPS
		maxTokens := float64(g.cfg.RateLimitBurst)
		if g.tokens > maxTokens {
			g.tokens = maxTokens
		}
		g.lastRefill = now
	}
	if g.tokens < 1.0 {
		return false
	}
	g.tokens -= 1.0
	return true
}
