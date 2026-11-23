package domain

import "errors"

var (
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrTerminalState      = errors.New("state is terminal")
	ErrInvalidInput       = errors.New("invalid domain input")
	ErrFeeCapExceeded     = errors.New("fee exceeds configured max fee cap")
	ErrIntentExpired      = errors.New("transaction intent has expired")
	ErrMaxAttemptsReached = errors.New("maximum retry attempts reached")
	ErrLeaseConflict      = errors.New("active lease owned by another worker")
	ErrNonceMismatch      = errors.New("nonce mismatch")
	ErrVersionConflict    = errors.New("optimistic version conflict")
	ErrInvalidBump        = errors.New("replacement fee bump below minimum threshold")
)
