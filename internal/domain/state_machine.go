package domain

import (
	"fmt"
	"time"
)

var validIntentTransitions = map[IntentStatus]map[IntentStatus]bool{
	IntentCreated: {
		IntentQueued:    true,
		IntentCancelled: true,
		IntentExpired:   true,
	},
	IntentQueued: {
		IntentProcessing: true,
		IntentCancelled:  true,
		IntentExpired:    true,
	},
	IntentProcessing: {
		IntentPrepared: true,
		IntentRetrying: true,
		IntentExpired:  true,
		IntentFailed:   true,
	},
	IntentPrepared: {
		IntentBroadcasted: true,
		IntentRetrying:    true,
		IntentExpired:     true,
		IntentFailed:      true,
	},
	IntentBroadcasted: {
		IntentConfirming: true,
		IntentRetrying:   true,
		IntentReplaced:   true,
		IntentCancelled:  true,
	},
	IntentConfirming: {
		IntentFinalized:   true,
		IntentRetrying:    true,
		IntentBroadcasted: true,
	},
	IntentRetrying: {
		IntentProcessing: true,
		IntentCancelled:  true,
		IntentExpired:    true,
		IntentFailed:     true,
	},
}

var validExecutionTransitions = map[ExecutionStatus]map[ExecutionStatus]bool{
	ExecAllocatingNonce: {
		ExecSigned: true,
		ExecFailed: true,
	},
	ExecSigned: {
		ExecBroadcasted: true,
		ExecDropped:     true,
		ExecReplaced:    true,
		ExecFailed:      true,
	},
	ExecBroadcasted: {
		ExecConfirming: true,
		ExecDropped:    true,
		ExecReplaced:   true,
		ExecReorged:    true,
	},
	ExecConfirming: {
		ExecConfirmed:   true,
		ExecReorged:     true,
		ExecBroadcasted: true,
	},
}

type IntentTransitionRequest struct {
	To           IntentStatus
	Actor        string
	Reason       string
	ExecutionID  string
	TxHash       *Hash
	Outcome      *ExecutionOutcome
	LeaseOwner   string
	LeaseTTL     time.Duration
	NextRetryAt  *time.Time
	ErrorCode    string
	ErrorMessage string
	Now          time.Time
}

func CanTransitionIntent(from, to IntentStatus) bool {
	targets, ok := validIntentTransitions[from]
	if !ok {
		return false
	}
	return targets[to]
}

func TransitionIntent(intent *TransactionIntent, req IntentTransitionRequest) (StateTransitionRecord, error) {
	if intent == nil {
		return StateTransitionRecord{}, fmt.Errorf("%w: nil intent", ErrInvalidInput)
	}
	if !intent.Status.IsValid() || !req.To.IsValid() {
		return StateTransitionRecord{}, fmt.Errorf("%w: unknown status (%q -> %q)", ErrInvalidInput, intent.Status, req.To)
	}
	if intent.Status.IsTerminal() {
		return StateTransitionRecord{}, fmt.Errorf("%w: cannot leave %s", ErrTerminalState, intent.Status)
	}
	if !CanTransitionIntent(intent.Status, req.To) {
		return StateTransitionRecord{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, intent.Status, req.To)
	}
	if req.Actor == "" {
		return StateTransitionRecord{}, fmt.Errorf("%w: actor is required", ErrInvalidInput)
	}

	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	if intent.LeaseOwner != "" && intent.LeaseExpiresAt != nil && now.Before(*intent.LeaseExpiresAt) {
		if req.LeaseOwner != intent.LeaseOwner {
			return StateTransitionRecord{}, fmt.Errorf("%w: active lease held by %s", ErrLeaseConflict, intent.LeaseOwner)
		}
	}

	switch req.To {
	case IntentProcessing:
		if !intent.ExpiresAt.IsZero() && !now.Before(intent.ExpiresAt) {
			return StateTransitionRecord{}, fmt.Errorf("%w: expired at %s", ErrIntentExpired, intent.ExpiresAt.Format(time.RFC3339))
		}
		if req.LeaseOwner == "" || req.LeaseTTL <= 0 {
			return StateTransitionRecord{}, fmt.Errorf("%w: lease_owner and positive lease_ttl required for PROCESSING", ErrInvalidInput)
		}
	case IntentPrepared:
		if req.ExecutionID == "" || req.TxHash == nil || req.TxHash.IsZero() {
			return StateTransitionRecord{}, fmt.Errorf("%w: execution_id and non-zero tx_hash required for PREPARED", ErrInvalidInput)
		}
		if req.LeaseOwner == "" || req.LeaseTTL <= 0 {
			return StateTransitionRecord{}, fmt.Errorf("%w: lease_owner and positive lease_ttl required for PREPARED", ErrInvalidInput)
		}
	case IntentRetrying:
		if intent.MaxAttempts > 0 && intent.AttemptCount >= intent.MaxAttempts {
			return StateTransitionRecord{}, fmt.Errorf("%w: %d/%d attempts", ErrMaxAttemptsReached, intent.AttemptCount, intent.MaxAttempts)
		}
	case IntentFinalized:
		if req.Outcome == nil || !req.Outcome.IsValid() {
			return StateTransitionRecord{}, fmt.Errorf("%w: valid execution outcome required for FINALIZED", ErrInvalidInput)
		}
	}

	from := intent.Status
	intent.Status = req.To
	intent.Version++
	intent.UpdatedAt = now

	if req.ExecutionID != "" {
		intent.ActiveExecutionID = req.ExecutionID
	}
	if req.ErrorCode != "" || req.ErrorMessage != "" {
		intent.LastErrorCode = req.ErrorCode
		intent.LastErrorMessage = req.ErrorMessage
	}

	switch req.To {
	case IntentProcessing:
		intent.AttemptCount++
		exp := now.Add(req.LeaseTTL)
		intent.LeaseOwner = req.LeaseOwner
		intent.LeaseExpiresAt = &exp
		intent.NextRetryAt = nil
	case IntentPrepared:
		exp := now.Add(req.LeaseTTL)
		intent.LeaseOwner = req.LeaseOwner
		intent.LeaseExpiresAt = &exp
	case IntentRetrying:
		intent.LeaseOwner = ""
		intent.LeaseExpiresAt = nil
		intent.NextRetryAt = req.NextRetryAt
	case IntentFinalized:
		outcome := *req.Outcome
		intent.ExecutionOutcome = &outcome
		intent.FinalizedAt = &now
		intent.LeaseOwner = ""
		intent.LeaseExpiresAt = nil
		intent.NextRetryAt = nil
	default:
		intent.LeaseOwner = ""
		intent.LeaseExpiresAt = nil
	}

	return StateTransitionRecord{
		IntentID:    intent.ID,
		ExecutionID: intent.ActiveExecutionID,
		FromStatus:  from,
		ToStatus:    req.To,
		Actor:       req.Actor,
		Reason:      req.Reason,
		OccurredAt:  now,
	}, nil
}

func CanTransitionExecution(from, to ExecutionStatus) bool {
	targets, ok := validExecutionTransitions[from]
	if !ok {
		return false
	}
	return targets[to]
}

func TransitionExecution(exec *TransactionExecution, to ExecutionStatus, now time.Time) error {
	if exec == nil {
		return fmt.Errorf("%w: nil execution", ErrInvalidInput)
	}
	if !exec.Status.IsValid() || !to.IsValid() {
		return fmt.Errorf("%w: unknown execution status (%q -> %q)", ErrInvalidInput, exec.Status, to)
	}
	if exec.Status.IsTerminal() {
		return fmt.Errorf("%w: execution cannot leave %s", ErrTerminalState, exec.Status)
	}
	if !CanTransitionExecution(exec.Status, to) {
		return fmt.Errorf("%w: execution %s -> %s", ErrInvalidTransition, exec.Status, to)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	exec.Status = to
	exec.UpdatedAt = now
	return nil
}
