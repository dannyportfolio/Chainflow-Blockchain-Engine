package application

import (
	"context"
	"errors"
	"time"

	"github.com/chainflow/chainflow/internal/domain"
)

var (
	ErrNotFound            = errors.New("record not found")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request payload")
	ErrWalletInflightLimit = errors.New("wallet in-flight nonce limit reached")
	ErrWalletNotActive     = errors.New("wallet is not active")
)

type OutboxEvent struct {
	ID              int64
	EventID         string
	AggregateType   string
	AggregateID     string
	Subject         string
	EventVersion    int16
	Payload         []byte
	TraceParent     string
	CreatedAt       time.Time
	AvailableAt     time.Time
	LockedUntil     *time.Time
	PublishedAt     *time.Time
	PublishAttempts int
	LastError       string
}

type IdempotencyCreateResult struct {
	Intent  domain.TransactionIntent
	Created bool
}

type IntentRepository interface {
	CreateIntentWithOutbox(
		ctx context.Context,
		intent domain.TransactionIntent,
		audit domain.StateTransitionRecord,
		outbox OutboxEvent,
	) (IdempotencyCreateResult, error)

	GetIntentByID(ctx context.Context, tenantID, intentID string) (domain.TransactionIntent, error)

	TransitionIntentWithOutbox(
		ctx context.Context,
		expectedVersion int64,
		updated domain.TransactionIntent,
		audit domain.StateTransitionRecord,
		outbox *OutboxEvent,
	) error

	ListStaleLeasedIntents(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error)
	ListDueRetries(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error)
	ListExpirableIntents(ctx context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error)
	ListStateEvents(ctx context.Context, intentID string) ([]domain.StateTransitionRecord, error)
}

type ExecutionRepository interface {
	CreateExecution(ctx context.Context, exec domain.TransactionExecution) error
	UpdateExecutionSigned(ctx context.Context, execID string, txHash domain.Hash, rawSignedTx []byte, now time.Time) error
	UpdateExecutionBroadcasted(ctx context.Context, execID string, provider string, broadcastedAt time.Time) error
	UpdateExecutionStatus(ctx context.Context, execID string, status domain.ExecutionStatus, errCode, errMsg string, now time.Time) error
	ListActiveExecutions(ctx context.Context, chainID int64, limit int) ([]domain.TransactionExecution, error)
	ListExecutionsByIntent(ctx context.Context, intentID string) ([]domain.TransactionExecution, error)
	UpsertReceipt(ctx context.Context, receipt domain.TransactionReceipt) error
}

type NonceCoordinator interface {
	ReserveNextNonce(ctx context.Context, chainID int64, wallet domain.Address, intentID string, now time.Time) (domain.NonceReservation, error)
	ConfirmNonce(ctx context.Context, chainID int64, wallet domain.Address, nonce uint64, now time.Time) error
	ReleaseUnbroadcastNonce(ctx context.Context, chainID int64, wallet domain.Address, nonce uint64, now time.Time) error
	ReconcilePendingNonce(ctx context.Context, chainID int64, wallet domain.Address, chainPendingNonce uint64, now time.Time) error
}

type WalletRepository interface {
	UpsertWallet(ctx context.Context, wallet domain.Wallet, initialNonce uint64) error
	GetWallet(ctx context.Context, chainID int64, address domain.Address) (domain.Wallet, error)
}

type OutboxRepository interface {
	ClaimUnpublished(ctx context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, ids []int64, publishedAt time.Time) error
	MarkPublishFailed(ctx context.Context, id int64, retryAt time.Time, errMsg string) error
	DeletePublishedBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}
