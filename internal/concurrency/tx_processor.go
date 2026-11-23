package concurrency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/messaging"
	"github.com/chainflow/chainflow/internal/security/signer"
)

type ProcessAction int

const (
	ActionAck ProcessAction = iota
	ActionNakDelay
	ActionDLQ
)

type ProcessResult struct {
	Action   ProcessAction
	NakDelay time.Duration
	Reason   string
}

type TxProcessorConfig struct {
	WorkerID string
	LeaseTTL time.Duration
	Now      func() time.Time
	NewID    func() (string, error)
	Logger   *slog.Logger
}

type TxProcessor struct {
	intents    application.IntentRepository
	executions application.ExecutionRepository
	wallets    application.WalletRepository
	nonces     application.NonceCoordinator
	gas        *blockchain.GasEstimator
	router     *blockchain.Router
	signer     signer.Signer
	cfg        TxProcessorConfig
	retry      domain.RetryPolicy
}

func NewTxProcessor(
	intents application.IntentRepository,
	executions application.ExecutionRepository,
	wallets application.WalletRepository,
	nonces application.NonceCoordinator,
	gas *blockchain.GasEstimator,
	router *blockchain.Router,
	sig signer.Signer,
	cfg TxProcessorConfig,
) *TxProcessor {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "tx-worker-1"
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.NewID == nil {
		cfg.NewID = func() (string, error) { return application.NewUUIDv7(cfg.Now()) }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &TxProcessor{
		intents:    intents,
		executions: executions,
		wallets:    wallets,
		nonces:     nonces,
		gas:        gas,
		router:     router,
		signer:     sig,
		cfg:        cfg,
		retry: domain.RetryPolicy{
			MaxAttempts: 5,
			BaseDelay:   2 * time.Second,
			MaxDelay:    30 * time.Second,
		},
	}
}

func (p *TxProcessor) ProcessMessage(ctx context.Context, subject string, data []byte) ProcessResult {
	var env struct {
		IntentID string `json:"intent_id"`
	}
	if err := json.Unmarshal(data, &env); err != nil || env.IntentID == "" {
		return ProcessResult{Action: ActionDLQ, Reason: "invalid_message_payload"}
	}

	intent, err := p.intents.GetIntentByIDForSystem(ctx, env.IntentID)
	if err != nil {
		if errors.Is(err, application.ErrNotFound) {
			return ProcessResult{Action: ActionDLQ, Reason: "intent_not_found"}
		}
		return ProcessResult{Action: ActionNakDelay, NakDelay: 2 * time.Second, Reason: err.Error()}
	}

	if intent.Status != domain.IntentQueued && intent.Status != domain.IntentRetrying {
		return ProcessResult{Action: ActionAck, Reason: "idempotent_skip_status_" + string(intent.Status)}
	}

	now := p.cfg.Now().UTC()
	expectedVersion := intent.Version
	auditProc, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:         domain.IntentProcessing,
		Actor:      "worker:" + p.cfg.WorkerID,
		Reason:     "lease_acquired",
		LeaseOwner: p.cfg.WorkerID,
		LeaseTTL:   p.cfg.LeaseTTL,
		Now:        now,
	})
	if err != nil {
		if errors.Is(err, domain.ErrIntentExpired) {
			return p.expireInline(ctx, expectedVersion, intent, now)
		}
		return ProcessResult{Action: ActionAck, Reason: err.Error()}
	}

	if err := p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, auditProc, nil); err != nil {
		if errors.Is(err, domain.ErrVersionConflict) || errors.Is(err, domain.ErrLeaseConflict) {
			return ProcessResult{Action: ActionAck, Reason: "lease_lost_to_peer"}
		}
		return ProcessResult{Action: ActionNakDelay, NakDelay: time.Second, Reason: err.Error()}
	}

	wallet, err := p.wallets.GetWallet(ctx, intent.ChainID, intent.From)
	if err != nil {
		return p.handleProcessingError(ctx, intent, "", 0, false, err)
	}
	if wallet.Status != domain.WalletActive {
		return p.handleProcessingError(ctx, intent, "", 0, false, application.ErrWalletNotActive)
	}

	fees, err := p.gas.SuggestFees(ctx)
	if err != nil {
		return p.handleProcessingError(ctx, intent, "", 0, false, err)
	}
	if intent.MaxFeeCapWei != nil && fees.MaxFeePerGas.Cmp(intent.MaxFeeCapWei) > 0 {
		return p.handleProcessingError(ctx, intent, "", 0, false,
			fmt.Errorf("%w: %s > intent cap %s", domain.ErrFeeCapExceeded, fees.MaxFeePerGas.String(), intent.MaxFeeCapWei.String()))
	}

	var gasLimit uint64
	if intent.GasLimitOverride != nil {
		gasLimit = *intent.GasLimitOverride
	} else {
		gasLimit, err = p.gas.EstimateGasWithBuffer(ctx, blockchain.CallMsg{
			From:     intent.From,
			To:       intent.To,
			ValueWei: intent.ValueWei,
			Data:     intent.Calldata,
		})
		if err != nil {
			return p.handleProcessingError(ctx, intent, "", 0, false, err)
		}
	}

	nonceRes, err := p.nonces.ReserveNextNonce(ctx, intent.ChainID, intent.From, intent.ID, p.cfg.Now().UTC())
	if err != nil {
		return p.handleProcessingError(ctx, intent, "", 0, false, err)
	}

	execID, err := p.cfg.NewID()
	if err != nil {
		_ = p.nonces.ReleaseUnbroadcastNonce(ctx, intent.ChainID, intent.From, nonceRes.Nonce, p.cfg.Now().UTC())
		return p.handleProcessingError(ctx, intent, "", 0, false, err)
	}

	execNow := p.cfg.Now().UTC()
	exec := domain.TransactionExecution{
		ID:                execID,
		IntentID:          intent.ID,
		AttemptNumber:     intent.AttemptCount,
		ChainID:           intent.ChainID,
		From:              intent.From,
		Nonce:             nonceRes.Nonce,
		GasLimit:          gasLimit,
		MaxPriorityFeeWei: new(big.Int).Set(fees.MaxPriorityFeePerGas),
		MaxFeePerGasWei:   new(big.Int).Set(fees.MaxFeePerGas),
		Status:            domain.ExecAllocatingNonce,
		CreatedAt:         execNow,
		UpdatedAt:         execNow,
	}
	if err := p.executions.CreateExecution(ctx, exec); err != nil {
		_ = p.nonces.ReleaseUnbroadcastNonce(ctx, intent.ChainID, intent.From, nonceRes.Nonce, p.cfg.Now().UTC())
		return p.handleProcessingError(ctx, intent, "", 0, false, err)
	}

	var toEth *common.Address
	if intent.To != nil {
		addr := common.BytesToAddress(intent.To.Bytes())
		toEth = &addr
	}
	signedTx, err := p.signer.SignEIP1559(ctx, signer.SignRequest{
		KeyRef:               wallet.SignerKeyRef,
		ChainID:              big.NewInt(intent.ChainID),
		From:                 common.BytesToAddress(intent.From.Bytes()),
		To:                   toEth,
		Nonce:                nonceRes.Nonce,
		ValueWei:             intent.ValueWei,
		Data:                 intent.Calldata,
		GasLimit:             gasLimit,
		MaxPriorityFeePerGas: fees.MaxPriorityFeePerGas,
		MaxFeePerGas:         fees.MaxFeePerGas,
	})
	if err != nil {
		return p.handleProcessingError(ctx, intent, execID, nonceRes.Nonce, true, err)
	}

	txHash, rawSigned, err := signer.EncodeSignedTx(signedTx)
	if err != nil {
		return p.handleProcessingError(ctx, intent, execID, nonceRes.Nonce, true, err)
	}

	signNow := p.cfg.Now().UTC()
	if err := p.executions.UpdateExecutionSigned(ctx, execID, txHash, rawSigned, signNow); err != nil {
		return p.handleProcessingError(ctx, intent, execID, nonceRes.Nonce, true, err)
	}

	expectedVersion = intent.Version
	auditPrep, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:          domain.IntentPrepared,
		Actor:       "worker:" + p.cfg.WorkerID,
		Reason:      "tx_signed",
		ExecutionID: execID,
		TxHash:      &txHash,
		LeaseOwner:  p.cfg.WorkerID,
		LeaseTTL:    p.cfg.LeaseTTL,
		Now:         signNow,
	})
	if err != nil {
		return p.handleProcessingError(ctx, intent, execID, nonceRes.Nonce, true, err)
	}
	if err := p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, auditPrep, nil); err != nil {
		return ProcessResult{Action: ActionNakDelay, NakDelay: time.Second, Reason: err.Error()}
	}

	if err := p.router.SendRawTransaction(ctx, rawSigned); err != nil {
		return p.handleProcessingError(ctx, intent, execID, nonceRes.Nonce, true, err)
	}

	bcastNow := p.cfg.Now().UTC()
	_ = p.executions.UpdateExecutionBroadcasted(ctx, execID, "router", bcastNow)

	expectedVersion = intent.Version
	auditBcast, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:          domain.IntentBroadcasted,
		Actor:       "worker:" + p.cfg.WorkerID,
		Reason:      "broadcast_accepted",
		ExecutionID: execID,
		TxHash:      &txHash,
		LeaseOwner:  p.cfg.WorkerID,
		Now:         bcastNow,
	})
	if err != nil {
		return ProcessResult{Action: ActionNakDelay, NakDelay: time.Second, Reason: err.Error()}
	}
	if err := p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, auditBcast, nil); err != nil {
		return ProcessResult{Action: ActionNakDelay, NakDelay: time.Second, Reason: err.Error()}
	}

	return ProcessResult{Action: ActionAck}
}

func (p *TxProcessor) expireInline(ctx context.Context, expectedVersion int64, intent domain.TransactionIntent, now time.Time) ProcessResult {
	audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:           domain.IntentExpired,
		Actor:        "worker:" + p.cfg.WorkerID,
		Reason:       "intent_expired_before_lease",
		ErrorCode:    "INTENT_EXPIRED",
		ErrorMessage: "intent expired before processing",
		Now:          now,
	})
	if err != nil {
		return ProcessResult{Action: ActionAck}
	}
	_ = p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil)
	return ProcessResult{Action: ActionAck, Reason: "expired"}
}

func (p *TxProcessor) handleProcessingError(
	ctx context.Context,
	intent domain.TransactionIntent,
	execID string,
	nonce uint64,
	hasNonce bool,
	cause error,
) ProcessResult {
	now := p.cfg.Now().UTC()
	retryable := isRetryableProcessingError(cause)
	expectedVersion := intent.Version

	if retryable && intent.AttemptCount < intent.MaxAttempts {
		nextRetry := now.Add(p.retry.NextDelay(intent.AttemptCount, 0.1))
		audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
			To:           domain.IntentRetrying,
			Actor:        "worker:" + p.cfg.WorkerID,
			Reason:       "transient_processing_error",
			LeaseOwner:   p.cfg.WorkerID,
			NextRetryAt:  &nextRetry,
			ErrorCode:    "TRANSIENT_ERROR",
			ErrorMessage: cause.Error(),
			Now:          now,
		})
		if err == nil {
			eventID, idErr := p.cfg.NewID()
			if idErr == nil {
				payload, mErr := json.Marshal(messaging.RetryEnvelope{
					EventID:             eventID,
					EventType:           "tx.retry.v1",
					OccurredAt:          now.Format(time.RFC3339Nano),
					IntentID:            intent.ID,
					AttemptNumber:       intent.AttemptCount + 1,
					RetryReason:         "TRANSIENT_ERROR",
					PreviousExecutionID: execID,
					ChainID:             intent.ChainID,
				})
				if mErr == nil {
					outbox := &application.OutboxEvent{
						EventID:       eventID,
						AggregateType: "transaction_intent",
						AggregateID:   intent.ID,
						Subject:       messaging.SubjectRetry,
						EventVersion:  1,
						Payload:       payload,
						TraceParent:   intent.TraceID,
						CreatedAt:     now,
						AvailableAt:   nextRetry,
					}
					if txErr := p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, outbox); txErr == nil {
						return ProcessResult{Action: ActionAck, Reason: "scheduled_retry"}
					}
				}
			}
		}
		return ProcessResult{Action: ActionNakDelay, NakDelay: 2 * time.Second, Reason: cause.Error()}
	}

	if hasNonce {
		_ = p.nonces.ReleaseUnbroadcastNonce(ctx, intent.ChainID, intent.From, nonce, now)
	}
	if execID != "" {
		_ = p.executions.UpdateExecutionStatus(ctx, execID, domain.ExecFailed, "PERMANENT_ERROR", cause.Error(), now)
	}

	audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
		To:           domain.IntentFailed,
		Actor:        "worker:" + p.cfg.WorkerID,
		Reason:       "permanent_or_exhausted_error",
		LeaseOwner:   p.cfg.WorkerID,
		ErrorCode:    "EXECUTION_FAILED",
		ErrorMessage: cause.Error(),
		Now:          now,
	})
	if err == nil {
		_ = p.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil)
	}
	return ProcessResult{Action: ActionDLQ, Reason: cause.Error()}
}

func isRetryableProcessingError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, application.ErrWalletInflightLimit) ||
		errors.Is(err, blockchain.ErrNoHealthyProviders) ||
		errors.Is(err, blockchain.ErrCircuitOpen) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ce *blockchain.ClassifiedError
	if errors.As(err, &ce) {
		return ce.Retryable()
	}
	if errors.Is(err, domain.ErrInvalidInput) ||
		errors.Is(err, domain.ErrFeeCapExceeded) ||
		errors.Is(err, signer.ErrMainnetSigningForbidden) ||
		errors.Is(err, signer.ErrKeyNotFound) ||
		errors.Is(err, signer.ErrFromAddressMismatch) ||
		errors.Is(err, application.ErrWalletNotActive) {
		return false
	}
	return true
}
