package security

import (
	"sync"
	"time"
)

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

type TenantRateLimiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	now     func() time.Time
	buckets map[string]*tokenBucket
}

func NewTenantRateLimiter(ratePerSec float64, burst int, now func() time.Time) *TenantRateLimiter {
	if ratePerSec <= 0 {
		ratePerSec = 50
	}
	if burst <= 0 {
		burst = 100
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &TenantRateLimiter{
		rate:    ratePerSec,
		burst:   float64(burst),
		now:     now,
		buckets: make(map[string]*tokenBucket),
	}
}

func (l *TenantRateLimiter) Allow(tenantID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[tenantID]
	if !ok {
		b = &tokenBucket{tokens: l.burst, lastRefill: now}
		l.buckets[tenantID] = b
	}

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.lastRefill = now
	}

	if b.tokens < 1.0 {
		return false
	}
	b.tokens -= 1.0
	return true
}
