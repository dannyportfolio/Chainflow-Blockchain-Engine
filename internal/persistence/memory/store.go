package memory

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/domain"
)

type walletKey struct {
	chainID int64
	addr    domain.Address
}

type idempotencyKey struct {
	tenantID string
	key      string
}

type Store struct {
	mu           sync.Mutex
	wallets      map[walletKey]domain.Wallet
	walletNonces map[walletKey]uint64
	intents      map[string]domain.TransactionIntent
	idempotency  map[idempotencyKey]string
	executions   map[string]domain.TransactionExecution
	receipts     map[string]domain.TransactionReceipt
	nonces       []domain.NonceReservation
	outbox       []application.OutboxEvent
	stateEvents  map[string][]domain.StateTransitionRecord
	nextNonceID  int64
	nextOutboxID int64
}

func NewStore() *Store {
	return &Store{
		wallets:      make(map[walletKey]domain.Wallet),
		walletNonces: make(map[walletKey]uint64),
		intents:      make(map[string]domain.TransactionIntent),
		idempotency:  make(map[idempotencyKey]string),
		executions:   make(map[string]domain.TransactionExecution),
		receipts:     make(map[string]domain.TransactionReceipt),
		stateEvents:  make(map[string][]domain.StateTransitionRecord),
	}
}

func (s *Store) UpsertWallet(_ context.Context, w domain.Wallet, initialNonce uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := walletKey{chainID: w.ChainID, addr: w.Address}
	s.wallets[k] = w
	if _, exists := s.walletNonces[k]; !exists {
		s.walletNonces[k] = initialNonce
	}
	return nil
}

func (s *Store) GetWallet(_ context.Context, chainID int64, address domain.Address) (domain.Wallet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.wallets[walletKey{chainID: chainID, addr: address}]
	if !ok {
		return domain.Wallet{}, application.ErrNotFound
	}
	return w, nil
}

func (s *Store) CreateIntentWithOutbox(
	_ context.Context,
	intent domain.TransactionIntent,
	audit domain.StateTransitionRecord,
	outbox application.OutboxEvent,
) (application.IdempotencyCreateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.wallets[walletKey{chainID: intent.ChainID, addr: intent.From}]; !ok {
		return application.IdempotencyCreateResult{}, fmt.Errorf("%w: sender wallet not registered", application.ErrNotFound)
	}

	ik := idempotencyKey{tenantID: intent.TenantID, key: intent.IdempotencyKey}
	if existingID, exists := s.idempotency[ik]; exists {
		existing := s.intents[existingID]
		if existing.RequestHash != intent.RequestHash {
			return application.IdempotencyCreateResult{}, application.ErrIdempotencyConflict
		}
		return application.IdempotencyCreateResult{Intent: cloneIntent(existing), Created: false}, nil
	}

	s.intents[intent.ID] = cloneIntent(intent)
	s.idempotency[ik] = intent.ID
	s.stateEvents[intent.ID] = append(s.stateEvents[intent.ID], audit)

	s.nextOutboxID++
	outbox.ID = s.nextOutboxID
	s.outbox = append(s.outbox, cloneOutbox(outbox))

	return application.IdempotencyCreateResult{Intent: cloneIntent(intent), Created: true}, nil
}

func (s *Store) GetIntentByID(_ context.Context, tenantID, intentID string) (domain.TransactionIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	intent, ok := s.intents[intentID]
	if !ok || intent.TenantID != tenantID {
		return domain.TransactionIntent{}, application.ErrNotFound
	}
	return cloneIntent(intent), nil
}

func (s *Store) GetIntentByIDForSystem(_ context.Context, intentID string) (domain.TransactionIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	intent, ok := s.intents[intentID]
	if !ok {
		return domain.TransactionIntent{}, application.ErrNotFound
	}
	return cloneIntent(intent), nil
}

func (s *Store) TransitionIntentWithOutbox(
	_ context.Context,
	expectedVersion int64,
	updated domain.TransactionIntent,
	audit domain.StateTransitionRecord,
	outbox *application.OutboxEvent,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.intents[updated.ID]
	if !ok {
		return application.ErrNotFound
	}
	if current.Version != expectedVersion {
		return domain.ErrVersionConflict
	}

	s.intents[updated.ID] = cloneIntent(updated)
	s.stateEvents[updated.ID] = append(s.stateEvents[updated.ID], audit)
	if outbox != nil {
		s.nextOutboxID++
		ev := cloneOutbox(*outbox)
		ev.ID = s.nextOutboxID
		s.outbox = append(s.outbox, ev)
	}
	return nil
}

func (s *Store) ListStaleLeasedIntents(_ context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []domain.TransactionIntent
	for _, intent := range s.intents {
		if (intent.Status == domain.IntentProcessing || intent.Status == domain.IntentPrepared) &&
			intent.LeaseExpiresAt != nil && !intent.LeaseExpiresAt.After(now) {
			out = append(out, cloneIntent(intent))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) ListDueRetries(_ context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []domain.TransactionIntent
	for _, intent := range s.intents {
		if intent.Status == domain.IntentRetrying && (intent.NextRetryAt == nil || !intent.NextRetryAt.After(now)) {
			out = append(out, cloneIntent(intent))
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) ListExpirableIntents(_ context.Context, now time.Time, limit int) ([]domain.TransactionIntent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []domain.TransactionIntent
	for _, intent := range s.intents {
		switch intent.Status {
		case domain.IntentCreated, domain.IntentQueued, domain.IntentProcessing, domain.IntentPrepared, domain.IntentRetrying:
			if !intent.ExpiresAt.After(now) {
				out = append(out, cloneIntent(intent))
				if limit > 0 && len(out) >= limit {
					return out, nil
				}
			}
		}
	}
	return out, nil
}

func (s *Store) ListStateEvents(_ context.Context, intentID string) ([]domain.StateTransitionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.stateEvents[intentID]
	out := make([]domain.StateTransitionRecord, len(events))
	copy(out, events)
	return out, nil
}

func (s *Store) CreateExecution(_ context.Context, exec domain.TransactionExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.intents[exec.IntentID]; !ok {
		return application.ErrNotFound
	}
	s.executions[exec.ID] = exec
	return nil
}

func (s *Store) UpdateExecutionSigned(_ context.Context, execID string, txHash domain.Hash, rawSignedTx []byte, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exec, ok := s.executions[execID]
	if !ok {
		return application.ErrNotFound
	}
	h := txHash
	exec.TxHash = &h
	exec.RawSignedTx = append([]byte(nil), rawSignedTx...)
	exec.Status = domain.ExecSigned
	exec.UpdatedAt = now
	s.executions[execID] = exec
	return nil
}

func (s *Store) UpdateExecutionBroadcasted(_ context.Context, execID string, provider string, broadcastedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exec, ok := s.executions[execID]
	if !ok {
		return application.ErrNotFound
	}
	exec.Status = domain.ExecBroadcasted
	exec.RPCProviderUsed = provider
	exec.BroadcastedAt = &broadcastedAt
	exec.UpdatedAt = broadcastedAt
	s.executions[execID] = exec
	return nil
}

func (s *Store) UpdateExecutionStatus(_ context.Context, execID string, status domain.ExecutionStatus, errCode, errMsg string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	exec, ok := s.executions[execID]
	if !ok {
		return application.ErrNotFound
	}
	exec.Status = status
	exec.ErrorCode = errCode
	exec.ErrorMessage = errMsg
	exec.UpdatedAt = now
	s.executions[execID] = exec
	return nil
}

func (s *Store) ListActiveExecutions(_ context.Context, chainID int64, limit int) ([]domain.TransactionExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []domain.TransactionExecution
	for _, exec := range s.executions {
		if exec.ChainID == chainID && (exec.Status == domain.ExecBroadcasted || exec.Status == domain.ExecConfirming) {
			out = append(out, exec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) ListExecutionsByIntent(_ context.Context, intentID string) ([]domain.TransactionExecution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []domain.TransactionExecution
	for _, exec := range s.executions {
		if exec.IntentID == intentID {
			out = append(out, exec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AttemptNumber < out[j].AttemptNumber
	})
	return out, nil
}

func (s *Store) UpsertReceipt(_ context.Context, receipt domain.TransactionReceipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.executions[receipt.ExecutionID]; !ok {
		return application.ErrNotFound
	}
	s.receipts[receipt.ExecutionID] = receipt
	return nil
}

func (s *Store) ReserveNextNonce(_ context.Context, chainID int64, walletAddr domain.Address, intentID string, now time.Time) (domain.NonceReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := walletKey{chainID: chainID, addr: walletAddr}
	w, ok := s.wallets[k]
	if !ok {
		return domain.NonceReservation{}, application.ErrNotFound
	}
	if w.Status != domain.WalletActive {
		return domain.NonceReservation{}, application.ErrWalletNotActive
	}

	activeCount := 0
	for _, r := range s.nonces {
		if r.ChainID == chainID && r.WalletAddress == walletAddr && r.Status == domain.NonceReserved {
			activeCount++
		}
	}
	if activeCount >= w.MaxInflightNonces {
		return domain.NonceReservation{}, application.ErrWalletInflightLimit
	}

	nonce := s.walletNonces[k]
	s.walletNonces[k] = nonce + 1
	s.nextNonceID++
	res := domain.NonceReservation{
		ID:            s.nextNonceID,
		ChainID:       chainID,
		WalletAddress: walletAddr,
		Nonce:         nonce,
		IntentID:      intentID,
		Status:        domain.NonceReserved,
		ReservedAt:    now,
		UpdatedAt:     now,
	}
	s.nonces = append(s.nonces, res)
	return res, nil
}

func (s *Store) ConfirmNonce(_ context.Context, chainID int64, walletAddr domain.Address, nonce uint64, now time.Time) error {
	return s.updateNonceStatus(chainID, walletAddr, nonce, domain.NonceConfirmed, now)
}

func (s *Store) ReleaseUnbroadcastNonce(_ context.Context, chainID int64, walletAddr domain.Address, nonce uint64, now time.Time) error {
	return s.updateNonceStatus(chainID, walletAddr, nonce, domain.NonceReleased, now)
}

func (s *Store) updateNonceStatus(chainID int64, walletAddr domain.Address, nonce uint64, target domain.NonceReservationStatus, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.nonces {
		r := &s.nonces[i]
		if r.ChainID == chainID && r.WalletAddress == walletAddr && r.Nonce == nonce && r.Status == domain.NonceReserved {
			r.Status = target
			r.UpdatedAt = now
			return nil
		}
	}
	return application.ErrNotFound
}

func (s *Store) ReconcilePendingNonce(_ context.Context, chainID int64, walletAddr domain.Address, chainPendingNonce uint64, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := walletKey{chainID: chainID, addr: walletAddr}
	if _, ok := s.wallets[k]; !ok {
		return application.ErrNotFound
	}
	if chainPendingNonce > s.walletNonces[k] {
		s.walletNonces[k] = chainPendingNonce
	}
	for i := range s.nonces {
		r := &s.nonces[i]
		if r.ChainID == chainID && r.WalletAddress == walletAddr && r.Status == domain.NonceReserved && r.Nonce < chainPendingNonce {
			r.Status = domain.NonceConsumedExternal
			r.UpdatedAt = now
		}
	}
	return nil
}

func (s *Store) ClaimUnpublished(_ context.Context, now time.Time, leaseDuration time.Duration, limit int) ([]application.OutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimed []application.OutboxEvent
	until := now.Add(leaseDuration)
	for i := range s.outbox {
		ev := &s.outbox[i]
		if ev.PublishedAt != nil || ev.AvailableAt.After(now) {
			continue
		}
		if ev.LockedUntil != nil && ev.LockedUntil.After(now) {
			continue
		}
		ev.LockedUntil = &until
		ev.PublishAttempts++
		claimed = append(claimed, cloneOutbox(*ev))
		if limit > 0 && len(claimed) >= limit {
			break
		}
	}
	return claimed, nil
}

func (s *Store) MarkPublished(_ context.Context, ids []int64, publishedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	idSet := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	for i := range s.outbox {
		if _, ok := idSet[s.outbox[i].ID]; ok {
			t := publishedAt
			s.outbox[i].PublishedAt = &t
			s.outbox[i].LockedUntil = nil
		}
	}
	return nil
}

func (s *Store) MarkPublishFailed(_ context.Context, id int64, retryAt time.Time, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range s.outbox {
		if s.outbox[i].ID == id {
			s.outbox[i].AvailableAt = retryAt
			s.outbox[i].LockedUntil = nil
			s.outbox[i].LastError = errMsg
			return nil
		}
	}
	return application.ErrNotFound
}

func (s *Store) DeletePublishedBefore(_ context.Context, before time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64
	kept := make([]application.OutboxEvent, 0, len(s.outbox))
	for _, ev := range s.outbox {
		if ev.PublishedAt != nil && ev.PublishedAt.Before(before) && (limit <= 0 || int(deleted) < limit) {
			deleted++
			continue
		}
		kept = append(kept, ev)
	}
	s.outbox = kept
	return deleted, nil
}

func cloneIntent(in domain.TransactionIntent) domain.TransactionIntent {
	out := in
	if in.ValueWei != nil {
		out.ValueWei = new(big.Int).Set(in.ValueWei)
	}
	if in.MaxFeeCapWei != nil {
		out.MaxFeeCapWei = new(big.Int).Set(in.MaxFeeCapWei)
	}
	if in.Calldata != nil {
		out.Calldata = append([]byte(nil), in.Calldata...)
	}
	return out
}

func cloneOutbox(in application.OutboxEvent) application.OutboxEvent {
	out := in
	if in.Payload != nil {
		out.Payload = append([]byte(nil), in.Payload...)
	}
	return out
}
