package blockchain

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestClassifyRPCError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		err         error
		wantKind    RPCErrorKind
		wantRetry   bool
		wantTripsCB bool
	}{
		{name: "nonce too low", err: errors.New("nonce too low: next nonce 5, tx nonce 4"), wantKind: ErrKindNonceTooLow, wantRetry: false, wantTripsCB: false},
		{name: "replacement underpriced", err: errors.New("replacement transaction underpriced"), wantKind: ErrKindUnderpriced, wantRetry: false, wantTripsCB: false},
		{name: "tx underpriced", err: errors.New("transaction underpriced"), wantKind: ErrKindUnderpriced, wantRetry: false, wantTripsCB: false},
		{name: "already known", err: errors.New("already known"), wantKind: ErrKindAlreadyKnown, wantRetry: false, wantTripsCB: false},
		{name: "known transaction", err: errors.New("known transaction: 0xabc"), wantKind: ErrKindAlreadyKnown, wantRetry: false, wantTripsCB: false},
		{name: "insufficient funds", err: errors.New("insufficient funds for gas * price + value"), wantKind: ErrKindInsufficientFunds, wantRetry: false, wantTripsCB: false},
		{name: "http 429", err: &HTTPError{StatusCode: http.StatusTooManyRequests, Body: "rate limit"}, wantKind: ErrKindRateLimit, wantRetry: true, wantTripsCB: true},
		{name: "http 503", err: &HTTPError{StatusCode: http.StatusServiceUnavailable, Body: "unavailable"}, wantKind: ErrKindTransient, wantRetry: true, wantTripsCB: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, wantKind: ErrKindTimeout, wantRetry: true, wantTripsCB: true},
		{name: "nil returns nil", err: nil, wantKind: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyRPCError("alchemy", tc.err)
			if tc.err == nil {
				if got != nil {
					t.Fatalf("expected nil, got %+v", got)
				}
				return
			}
			if got.Kind != tc.wantKind {
				t.Fatalf("kind = %q, want %q", got.Kind, tc.wantKind)
			}
			if got.Retryable() != tc.wantRetry {
				t.Fatalf("Retryable() = %v, want %v", got.Retryable(), tc.wantRetry)
			}
			if got.TripsCircuit() != tc.wantTripsCB {
				t.Fatalf("TripsCircuit() = %v, want %v", got.TripsCircuit(), tc.wantTripsCB)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("expected errors.Is to unwrap original error")
			}
		})
	}
}
