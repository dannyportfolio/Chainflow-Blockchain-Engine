package domain

import (
	"fmt"
	"math/big"
	"time"
)

const minRBFBumpBps uint64 = 1000

type TransactionIntent struct {
	ID                string
	TenantID          string
	IdempotencyKey    string
	RequestHash       Hash
	ChainID           int64
	From              Address
	To                *Address
	ValueWei          *big.Int
	Calldata          []byte
	GasLimitOverride  *uint64
	MaxFeeCapWei      *big.Int
	Priority          int16
	Status            IntentStatus
	ExecutionOutcome  *ExecutionOutcome
	ActiveExecutionID string
	AttemptCount      int
	MaxAttempts       int
	NextRetryAt       *time.Time
	ExpiresAt         time.Time
	Version           int64
	LeaseOwner        string
	LeaseExpiresAt    *time.Time
	LastErrorCode     string
	LastErrorMessage  string
	TraceID           string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	FinalizedAt       *time.Time
}

func (i TransactionIntent) Validate() error {
	if i.ID == "" || i.TenantID == "" || i.IdempotencyKey == "" {
		return fmt.Errorf("%w: id, tenant_id, and idempotency_key are required", ErrInvalidInput)
	}
	if i.RequestHash.IsZero() {
		return fmt.Errorf("%w: request_hash is required", ErrInvalidInput)
	}
	if i.ChainID <= 0 {
		return fmt.Errorf("%w: chain_id must be positive", ErrInvalidInput)
	}
	if i.From.IsZero() {
		return fmt.Errorf("%w: from address must not be zero", ErrInvalidInput)
	}
	if i.ValueWei == nil || i.ValueWei.Sign() < 0 {
		return fmt.Errorf("%w: value_wei must be non-negative", ErrInvalidInput)
	}
	if i.GasLimitOverride != nil && *i.GasLimitOverride < 21000 {
		return fmt.Errorf("%w: gas_limit_override must be >= 21000", ErrInvalidInput)
	}
	if i.MaxFeeCapWei == nil || i.MaxFeeCapWei.Sign() <= 0 {
		return fmt.Errorf("%w: max_fee_cap_wei must be positive", ErrInvalidInput)
	}
	if i.MaxAttempts <= 0 {
		return fmt.Errorf("%w: max_attempts must be positive", ErrInvalidInput)
	}
	if !i.Status.IsValid() {
		return fmt.Errorf("%w: invalid status %q", ErrInvalidInput, i.Status)
	}
	if !i.CreatedAt.IsZero() && !i.ExpiresAt.After(i.CreatedAt) {
		return fmt.Errorf("%w: expires_at must be after created_at", ErrInvalidInput)
	}
	return nil
}

type TransactionExecution struct {
	ID                string
	IntentID          string
	AttemptNumber     int
	ChainID           int64
	From              Address
	Nonce             uint64
	GasLimit          uint64
	MaxPriorityFeeWei *big.Int
	MaxFeePerGasWei   *big.Int
	TxHash            *Hash
	RawSignedTx       []byte
	Status            ExecutionStatus
	RPCProviderUsed   string
	BroadcastedAt     *time.Time
	ErrorCode         string
	ErrorMessage      string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Wallet struct {
	ID                string
	ChainID           int64
	Address           Address
	Label             string
	SignerKeyRef      string
	Status            WalletStatus
	MaxInflightNonces int
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type NonceReservation struct {
	ID            int64
	ChainID       int64
	WalletAddress Address
	Nonce         uint64
	IntentID      string
	Status        NonceReservationStatus
	ReservedAt    time.Time
	UpdatedAt     time.Time
}

type TransactionReceipt struct {
	ExecutionID       string
	TxHash            Hash
	BlockNumber       uint64
	BlockHash         Hash
	TransactionIndex  uint32
	GasUsed           uint64
	EffectiveGasPrice *big.Int
	OnchainStatus     uint8
	Confirmations     uint64
	Reorged           bool
	ObservedAt        time.Time
	UpdatedAt         time.Time
}

type StateTransitionRecord struct {
	IntentID    string
	ExecutionID string
	FromStatus  IntentStatus
	ToStatus    IntentStatus
	Actor       string
	Reason      string
	OccurredAt  time.Time
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (p RetryPolicy) NextDelay(attempt int, jitterFactor float64) time.Duration {
	if attempt <= 1 {
		attempt = 1
	}
	base := p.BaseDelay
	if base <= 0 {
		base = time.Second
	}
	maxDelay := p.MaxDelay
	if maxDelay < base {
		maxDelay = base
	}

	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			delay = maxDelay
			break
		}
		delay *= 2
	}

	if jitterFactor > 0 {
		if jitterFactor > 1 {
			jitterFactor = 1
		}
		jitter := time.Duration(float64(delay) * jitterFactor)
		if delay > maxDelay-jitter {
			return maxDelay
		}
		delay += jitter
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func CalculateRBFBump(prevPriorityFee, prevMaxFee, maxFeeCap *big.Int, bumpBps uint64) (*big.Int, *big.Int, error) {
	if prevPriorityFee == nil || prevMaxFee == nil || maxFeeCap == nil {
		return nil, nil, fmt.Errorf("%w: nil fee argument", ErrInvalidInput)
	}
	if prevPriorityFee.Sign() < 0 || prevMaxFee.Sign() <= 0 || maxFeeCap.Sign() <= 0 {
		return nil, nil, fmt.Errorf("%w: fees must be positive", ErrInvalidInput)
	}
	if bumpBps < minRBFBumpBps {
		return nil, nil, fmt.Errorf("%w: bump %d bps < minimum %d bps", ErrInvalidBump, bumpBps, minRBFBumpBps)
	}

	newPrio := bumpByBps(prevPriorityFee, bumpBps)
	newMax := bumpByBps(prevMaxFee, bumpBps)

	if newMax.Cmp(maxFeeCap) > 0 {
		return nil, nil, fmt.Errorf("%w: bumped max fee %s > cap %s", ErrFeeCapExceeded, newMax.String(), maxFeeCap.String())
	}
	if newPrio.Cmp(newMax) > 0 {
		newPrio = new(big.Int).Set(newMax)
	}
	return newPrio, newMax, nil
}

func bumpByBps(v *big.Int, bumpBps uint64) *big.Int {
	if v.Sign() == 0 {
		return big.NewInt(1)
	}
	bps := new(big.Int).SetUint64(10000 + bumpBps)
	scaled := new(big.Int).Mul(v, bps)
	bumped := scaled.Div(scaled, big.NewInt(10000))
	if bumped.Cmp(v) <= 0 {
		bumped = new(big.Int).Add(v, big.NewInt(1))
	}
	return bumped
}
