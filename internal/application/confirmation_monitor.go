package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/domain"
)

const RetryOutboxSubject = "chainflow.tx.retry"

type ChainObserver interface {
	BlockNumber(ctx context.Context) (uint64, error)
	LatestNonceAt(ctx context.Context, addr domain.Address) (uint64, error)
	TransactionReceipt(ctx context.Context, txHash domain.Hash) (blockchain.Receipt, error)
	TransactionByHash(ctx context.Context, txHash domain.Hash) (blockchain.ChainTransaction, error)
}

type BlockHashVerifier interface {
	VerifyBlockHash(ctx context.Context, blockNumber uint64, expectedHash domain.Hash) (blockchain.BlockVerification, error)
}

type ConfirmationMonitorConfig struct {
	ChainID               int64
	RequiredConfirmations uint64
	PollInterval          time.Duration
	BatchSize             int
	StalePendingTTL       time.Duration
	Now                   func() time.Time
	NewID                 func() (string, error)
	Logger                *slog.Logger
}

type ConfirmationMonitor struct {
	intents    IntentRepository
	executions ExecutionRepository
	nonces     NonceCoordinator
	chain      ChainObserver
	verifier   BlockHashVerifier
	cfg        ConfirmationMonitorConfig
}

func NewConfirmationMonitor(
	intents IntentRepository,
	executions ExecutionRepository,
	nonces NonceCoordinator,
	chain ChainObserver,
	verifier BlockHashVerifier,
	cfg ConfirmationMonitorConfig,
) *ConfirmationMonitor {
	if cfg.RequiredConfirmations == 0 {
		cfg.RequiredConfirmations = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.StalePendingTTL <= 0 {
		cfg.StalePendingTTL = 2 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.NewID == nil {
		cfg.NewID = func() (string, error) { return NewUUIDv7(cfg.Now()) }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &ConfirmationMonitor{
		intents:    intents,
		executions: executions,
		nonces:     nonces,
		chain:      chain,
		verifier:   verifier,
		cfg:        cfg,
	}
}

func (m *ConfirmationMonitor) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()

	if err := m.RunOnce(ctx); err != nil {
		m.cfg.Logger.Warn("confirmation monitor initial pass failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.RunOnce(ctx); err != nil {
				m.cfg.Logger.Warn("confirmation monitor pass failed", "error", err)
			}
		}
	}
}

func (m *ConfirmationMonitor) RunOnce(ctx context.Context) error {
	head, err := m.chain.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("fetch block number: %w", err)
	}

	execs, err := m.executions.ListActiveExecutions(ctx, m.cfg.ChainID, m.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("list active executions: %w", err)
	}

	var errs []error
	for _, exec := range execs {
		if err := m.processExecution(ctx, head, exec); err != nil {
			m.cfg.Logger.Warn("process execution failed",
				"execution_id", exec.ID,
				"intent_id", exec.IntentID,
				"status", exec.Status,
				"error", err,
			)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *ConfirmationMonitor) processExecution(ctx context.Context, head uint64, exec domain.TransactionExecution) error {
	if exec.TxHash == nil || exec.TxHash.IsZero() {
		return nil
	}
	intent, err := m.intents.GetIntentByIDForSystem(ctx, exec.IntentID)
	if err != nil {
		return err
	}
	if intent.Status.IsTerminal() {
		return nil
	}

	switch exec.Status {
	case domain.ExecBroadcasted:
		return m.checkBroadcasted(ctx, head, intent, exec)
	case domain.ExecConfirming:
		return m.checkConfirming(ctx, head, intent, exec)
	default:
		return nil
	}
}

func (m *ConfirmationMonitor) checkBroadcasted(ctx context.Context, head uint64, intent domain.TransactionIntent, exec domain.TransactionExecution) error {
	now := m.cfg.Now().UTC()
	receipt, err := m.chain.TransactionReceipt(ctx, *exec.TxHash)
	if err == nil {
		confs := uint64(1)
		if head >= receipt.BlockNumber {
			confs = head - receipt.BlockNumber + 1
		}
		effPrice := receipt.EffectiveGasPrice
		if effPrice == nil {
			effPrice = big.NewInt(0)
		}
		domReceipt := domain.TransactionReceipt{
			ExecutionID:       exec.ID,
			TxHash:            receipt.TxHash,
			BlockNumber:       receipt.BlockNumber,
			BlockHash:         receipt.BlockHash,
			TransactionIndex:  receipt.TransactionIndex,
			GasUsed:           receipt.GasUsed,
			EffectiveGasPrice: effPrice,
			OnchainStatus:     receipt.Status,
			Confirmations:     confs,
			ObservedAt:        now,
			UpdatedAt:         now,
		}
		if err := m.executions.UpsertReceipt(ctx, domReceipt); err != nil {
			return err
		}
		if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, domain.ExecConfirming, "", "", now); err != nil {
			return err
		}
		if intent.Status == domain.IntentBroadcasted {
			expectedVersion := intent.Version
			audit, err := domain.TransitionIntent(&intent, domain.IntentTransitionRequest{
				To:          domain.IntentConfirming,
				Actor:       "confirmation-monitor",
				Reason:      "receipt_observed",
				ExecutionID: exec.ID,
				TxHash:      exec.TxHash,
				Now:         now,
			})
			if err != nil {
				return err
			}
			if err := m.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, nil); err != nil {
				return err
			}
		}

		if confs >= m.cfg.RequiredConfirmations {
			return m.finalizeExecution(ctx, intent, exec, domReceipt, now)
		}
		return nil
	}
	if !errors.Is(err, blockchain.ErrReceiptNotFound) {
		return err
	}

	chainNonce, nonceErr := m.chain.LatestNonceAt(ctx, exec.From)
	if nonceErr == nil && chainNonce > exec.Nonce {
		_, txErr := m.chain.TransactionByHash(ctx, *exec.TxHash)
		if errors.Is(txErr, blockchain.ErrTransactionNotFound) {
			if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, domain.ExecReplaced, "NONCE_CONSUMED_EXTERNAL", "nonce consumed by another transaction", now); err != nil {
				return err
			}
			_ = m.nonces.ConfirmNonce(ctx, exec.ChainID, exec.From, exec.Nonce, now)
			return m.transitionIntent(ctx, intent, domain.IntentTransitionRequest{
				To:           domain.IntentReplaced,
				Actor:        "confirmation-monitor",
				Reason:       "external_nonce_replacement",
				ExecutionID:  exec.ID,
				TxHash:       exec.TxHash,
				ErrorCode:    "NONCE_CONSUMED_EXTERNAL",
				ErrorMessage: "nonce consumed by another transaction",
				Now:          now,
			}, nil)
		}
	}

	_, txErr := m.chain.TransactionByHash(ctx, *exec.TxHash)
	dropped := errors.Is(txErr, blockchain.ErrTransactionNotFound)
	stale := exec.BroadcastedAt != nil && now.Sub(*exec.BroadcastedAt) > m.cfg.StalePendingTTL
	if !dropped && !stale {
		return nil
	}

	execTarget := domain.ExecReplaced
	reason := "STALE_PENDING"
	if dropped {
		execTarget = domain.ExecDropped
		reason = "MEMPOOL_DROPPED"
	}
	if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, execTarget, reason, "triggering RBF retry", now); err != nil {
		return err
	}
	outbox, err := m.newRetryOutboxEvent(intent.ID, now)
	if err != nil {
		return err
	}
	return m.transitionIntent(ctx, intent, domain.IntentTransitionRequest{
		To:           domain.IntentRetrying,
		Actor:        "confirmation-monitor",
		Reason:       reason,
		ExecutionID:  exec.ID,
		TxHash:       exec.TxHash,
		ErrorCode:    reason,
		ErrorMessage: "re-queuing for RBF fee bump",
		Now:          now,
	}, &outbox)
}

func (m *ConfirmationMonitor) checkConfirming(ctx context.Context, head uint64, intent domain.TransactionIntent, exec domain.TransactionExecution) error {
	now := m.cfg.Now().UTC()
	storedReceipt, err := m.executions.GetReceiptByExecutionID(ctx, exec.ID)
	if err != nil {
		return err
	}

	if m.verifier != nil {
		ver, err := m.verifier.VerifyBlockHash(ctx, storedReceipt.BlockNumber, storedReceipt.BlockHash)
		if err != nil {
			return err
		}
		if ver.Reorged {
			return m.handleReorg(ctx, head, intent, exec, storedReceipt, now)
		}
		if ver.Disagreement {
			return nil
		}
	}

	confs := uint64(0)
	if head >= storedReceipt.BlockNumber {
		confs = head - storedReceipt.BlockNumber + 1
	}
	storedReceipt.Confirmations = confs
	storedReceipt.UpdatedAt = now
	if err := m.executions.UpsertReceipt(ctx, storedReceipt); err != nil {
		return err
	}

	if confs >= m.cfg.RequiredConfirmations {
		return m.finalizeExecution(ctx, intent, exec, storedReceipt, now)
	}
	return nil
}

func (m *ConfirmationMonitor) handleReorg(ctx context.Context, head uint64, intent domain.TransactionIntent, exec domain.TransactionExecution, oldReceipt domain.TransactionReceipt, now time.Time) error {
	newReceipt, err := m.chain.TransactionReceipt(ctx, *exec.TxHash)
	if err == nil {
		confs := uint64(1)
		if head >= newReceipt.BlockNumber {
			confs = head - newReceipt.BlockNumber + 1
		}
		effPrice := newReceipt.EffectiveGasPrice
		if effPrice == nil {
			effPrice = big.NewInt(0)
		}
		updated := domain.TransactionReceipt{
			ExecutionID:       exec.ID,
			TxHash:            newReceipt.TxHash,
			BlockNumber:       newReceipt.BlockNumber,
			BlockHash:         newReceipt.BlockHash,
			TransactionIndex:  newReceipt.TransactionIndex,
			GasUsed:           newReceipt.GasUsed,
			EffectiveGasPrice: effPrice,
			OnchainStatus:     newReceipt.Status,
			Confirmations:     confs,
			Reorged:           false,
			ObservedAt:        oldReceipt.ObservedAt,
			UpdatedAt:         now,
		}
		if err := m.executions.UpsertReceipt(ctx, updated); err != nil {
			return err
		}
		if confs >= m.cfg.RequiredConfirmations {
			return m.finalizeExecution(ctx, intent, exec, updated, now)
		}
		return nil
	}

	oldReceipt.Reorged = true
	oldReceipt.Confirmations = 0
	oldReceipt.UpdatedAt = now
	_ = m.executions.UpsertReceipt(ctx, oldReceipt)

	chainTx, txErr := m.chain.TransactionByHash(ctx, *exec.TxHash)
	if txErr == nil && chainTx.Pending {
		if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, domain.ExecBroadcasted, "REORG_TO_MEMPOOL", "tx returned to mempool after reorg", now); err != nil {
			return err
		}
		return m.transitionIntent(ctx, intent, domain.IntentTransitionRequest{
			To:          domain.IntentBroadcasted,
			Actor:       "confirmation-monitor",
			Reason:      "reorg_back_to_mempool",
			ExecutionID: exec.ID,
			TxHash:      exec.TxHash,
			Now:         now,
		}, nil)
	}

	if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, domain.ExecReorged, "REORGED", "transaction evicted during chain reorg", now); err != nil {
		return err
	}
	outbox, err := m.newRetryOutboxEvent(intent.ID, now)
	if err != nil {
		return err
	}
	return m.transitionIntent(ctx, intent, domain.IntentTransitionRequest{
		To:           domain.IntentRetrying,
		Actor:        "confirmation-monitor",
		Reason:       "REORGED",
		ExecutionID:  exec.ID,
		TxHash:       exec.TxHash,
		ErrorCode:    "REORGED",
		ErrorMessage: "transaction evicted during chain reorg",
		Now:          now,
	}, &outbox)
}

func (m *ConfirmationMonitor) finalizeExecution(ctx context.Context, intent domain.TransactionIntent, exec domain.TransactionExecution, receipt domain.TransactionReceipt, now time.Time) error {
	if err := m.executions.UpdateExecutionStatus(ctx, exec.ID, domain.ExecConfirmed, "", "", now); err != nil {
		return err
	}
	_ = m.nonces.ConfirmNonce(ctx, exec.ChainID, exec.From, exec.Nonce, now)

	outcome := domain.OutcomeSucceeded
	if receipt.OnchainStatus == 0 {
		outcome = domain.OutcomeReverted
	}
	return m.transitionIntent(ctx, intent, domain.IntentTransitionRequest{
		To:          domain.IntentFinalized,
		Actor:       "confirmation-monitor",
		Reason:      "confirmations_met",
		ExecutionID: exec.ID,
		TxHash:      exec.TxHash,
		Outcome:     &outcome,
		Now:         now,
	}, nil)
}

func (m *ConfirmationMonitor) transitionIntent(ctx context.Context, intent domain.TransactionIntent, req domain.IntentTransitionRequest, outbox *OutboxEvent) error {
	expectedVersion := intent.Version
	audit, err := domain.TransitionIntent(&intent, req)
	if err != nil {
		return err
	}
	return m.intents.TransitionIntentWithOutbox(ctx, expectedVersion, intent, audit, outbox)
}

func (m *ConfirmationMonitor) newRetryOutboxEvent(intentID string, now time.Time) (OutboxEvent, error) {
	eventID, err := m.cfg.NewID()
	if err != nil {
		return OutboxEvent{}, err
	}
	payload, err := json.Marshal(struct {
		IntentID string `json:"intent_id"`
	}{IntentID: intentID})
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		EventID:       eventID,
		AggregateType: "transaction_intent",
		AggregateID:   intentID,
		Subject:       RetryOutboxSubject,
		EventVersion:  1,
		Payload:       payload,
		CreatedAt:     now,
		AvailableAt:   now,
	}, nil
}
