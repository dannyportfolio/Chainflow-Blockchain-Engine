package domain

import (
	"encoding/hex"
	"fmt"
	"strings"
)

type Address [20]byte

func ParseAddress(raw string) (Address, error) {
	var addr Address
	if err := parseFixedHex(raw, len(addr), addr[:], "address"); err != nil {
		return Address{}, err
	}
	return addr, nil
}

func (a Address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

func (a Address) Bytes() []byte {
	out := make([]byte, len(a))
	copy(out, a[:])
	return out
}

func (a Address) IsZero() bool {
	return a == (Address{})
}

type Hash [32]byte

func ParseHash(raw string) (Hash, error) {
	var h Hash
	if err := parseFixedHex(raw, len(h), h[:], "hash"); err != nil {
		return Hash{}, err
	}
	return h, nil
}

func parseFixedHex(raw string, expectedBytes int, dst []byte, label string) error {
	if !strings.HasPrefix(raw, "0x") && !strings.HasPrefix(raw, "0X") {
		return fmt.Errorf("%w: %s must have 0x prefix", ErrInvalidInput, label)
	}
	hexPart := raw[2:]
	if len(hexPart) != expectedBytes*2 {
		return fmt.Errorf("%w: %s must be %d bytes", ErrInvalidInput, label, expectedBytes)
	}
	decoded, err := hex.DecodeString(hexPart)
	if err != nil {
		return fmt.Errorf("%w: invalid %s hex: %v", ErrInvalidInput, label, err)
	}
	copy(dst, decoded)
	return nil
}

func (h Hash) String() string {
	return "0x" + hex.EncodeToString(h[:])
}

func (h Hash) Bytes() []byte {
	out := make([]byte, len(h))
	copy(out, h[:])
	return out
}

func (h Hash) IsZero() bool {
	return h == (Hash{})
}

type IntentStatus string

const (
	IntentCreated     IntentStatus = "CREATED"
	IntentQueued      IntentStatus = "QUEUED"
	IntentProcessing  IntentStatus = "PROCESSING"
	IntentPrepared    IntentStatus = "PREPARED"
	IntentBroadcasted IntentStatus = "BROADCASTED"
	IntentConfirming  IntentStatus = "CONFIRMING"
	IntentRetrying    IntentStatus = "RETRYING"

	IntentFinalized IntentStatus = "FINALIZED"
	IntentFailed    IntentStatus = "FAILED"
	IntentCancelled IntentStatus = "CANCELLED"
	IntentExpired   IntentStatus = "EXPIRED"
	IntentReplaced  IntentStatus = "REPLACED"
)

func (s IntentStatus) IsValid() bool {
	return s.IsActive() || s.IsTerminal()
}

func (s IntentStatus) IsActive() bool {
	switch s {
	case IntentCreated, IntentQueued, IntentProcessing, IntentPrepared,
		IntentBroadcasted, IntentConfirming, IntentRetrying:
		return true
	default:
		return false
	}
}

func (s IntentStatus) IsTerminal() bool {
	switch s {
	case IntentFinalized, IntentFailed, IntentCancelled, IntentExpired, IntentReplaced:
		return true
	default:
		return false
	}
}

type ExecutionStatus string

const (
	ExecAllocatingNonce ExecutionStatus = "ALLOCATING_NONCE"
	ExecSigned          ExecutionStatus = "SIGNED"
	ExecBroadcasted     ExecutionStatus = "BROADCASTED"
	ExecConfirming      ExecutionStatus = "CONFIRMING"

	ExecConfirmed ExecutionStatus = "CONFIRMED"
	ExecDropped   ExecutionStatus = "DROPPED"
	ExecReplaced  ExecutionStatus = "REPLACED"
	ExecReorged   ExecutionStatus = "REORGED"
	ExecFailed    ExecutionStatus = "FAILED"
)

func (s ExecutionStatus) IsValid() bool {
	switch s {
	case ExecAllocatingNonce, ExecSigned, ExecBroadcasted, ExecConfirming,
		ExecConfirmed, ExecDropped, ExecReplaced, ExecReorged, ExecFailed:
		return true
	default:
		return false
	}
}

func (s ExecutionStatus) IsTerminal() bool {
	switch s {
	case ExecConfirmed, ExecDropped, ExecReplaced, ExecReorged, ExecFailed:
		return true
	default:
		return false
	}
}

type ExecutionOutcome string

const (
	OutcomeSucceeded ExecutionOutcome = "SUCCEEDED"
	OutcomeReverted  ExecutionOutcome = "REVERTED"
)

func (o ExecutionOutcome) IsValid() bool {
	return o == OutcomeSucceeded || o == OutcomeReverted
}

type RetryReason string

const (
	RetryLeaseExpired        RetryReason = "LEASE_EXPIRED"
	RetryRPCTimeoutReconcile RetryReason = "RPC_TIMEOUT_RECONCILE"
	RetryMempoolDropped      RetryReason = "MEMPOOL_DROPPED"
	RetryStaleRBF            RetryReason = "STALE_RBF"
	RetryReorged             RetryReason = "REORGED"
	RetryTransientRPC        RetryReason = "TRANSIENT_RPC_ERROR"
)

type WalletStatus string

const (
	WalletActive  WalletStatus = "ACTIVE"
	WalletPaused  WalletStatus = "PAUSED"
	WalletRetired WalletStatus = "RETIRED"
)

type NonceReservationStatus string

const (
	NonceReserved         NonceReservationStatus = "RESERVED"
	NonceConfirmed        NonceReservationStatus = "CONFIRMED"
	NonceReleased         NonceReservationStatus = "RELEASED"
	NonceConsumedExternal NonceReservationStatus = "CONSUMED_EXTERNAL"
)
