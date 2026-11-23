package blockchain

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/rpc"
)

type RPCErrorKind string

const (
	ErrKindTransient         RPCErrorKind = "TRANSIENT"
	ErrKindRateLimit         RPCErrorKind = "RATE_LIMIT"
	ErrKindTimeout           RPCErrorKind = "TIMEOUT"
	ErrKindNonceTooLow       RPCErrorKind = "NONCE_TOO_LOW"
	ErrKindUnderpriced       RPCErrorKind = "UNDERPRICED"
	ErrKindAlreadyKnown      RPCErrorKind = "ALREADY_KNOWN"
	ErrKindInsufficientFunds RPCErrorKind = "INSUFFICIENT_FUNDS"
	ErrKindPermanent         RPCErrorKind = "PERMANENT"
)

var (
	ErrReceiptNotFound     = errors.New("transaction receipt not found")
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrChainIDMismatch     = errors.New("rpc chain_id mismatch")
	ErrCircuitOpen         = errors.New("provider circuit breaker is open")
	ErrNoHealthyProviders  = errors.New("no healthy rpc providers available")
)

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

type ClassifiedError struct {
	Provider string
	Kind     RPCErrorKind
	Err      error
}

func (e *ClassifiedError) Error() string {
	if e.Provider == "" {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("provider %s (%s): %v", e.Provider, e.Kind, e.Err)
}

func (e *ClassifiedError) Unwrap() error {
	return e.Err
}

func (e *ClassifiedError) Retryable() bool {
	switch e.Kind {
	case ErrKindTransient, ErrKindRateLimit, ErrKindTimeout:
		return true
	default:
		return false
	}
}

func (e *ClassifiedError) TripsCircuit() bool {
	return e.Retryable()
}

func ClassifyRPCError(provider string, err error) *ClassifiedError {
	if err == nil {
		return nil
	}
	var existing *ClassifiedError
	if errors.As(err, &existing) {
		return existing
	}

	kind := classifyKind(err)
	return &ClassifiedError{
		Provider: provider,
		Kind:     kind,
		Err:      err,
	}
}

func classifyKind(err error) RPCErrorKind {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrKindTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrKindTimeout
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return classifyHTTPStatus(httpErr.StatusCode)
	}
	var rpcHTTPErr rpc.HTTPError
	if errors.As(err, &rpcHTTPErr) {
		return classifyHTTPStatus(rpcHTTPErr.StatusCode)
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "nonce too low"):
		return ErrKindNonceTooLow
	case strings.Contains(msg, "replacement transaction underpriced"),
		strings.Contains(msg, "transaction underpriced"):
		return ErrKindUnderpriced
	case strings.Contains(msg, "already known"),
		strings.Contains(msg, "known transaction"):
		return ErrKindAlreadyKnown
	case strings.Contains(msg, "insufficient funds"):
		return ErrKindInsufficientFunds
	case strings.Contains(msg, "429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "rate limit"):
		return ErrKindRateLimit
	case strings.Contains(msg, "timeout"),
		strings.Contains(msg, "timed out"),
		strings.Contains(msg, "deadline exceeded"):
		return ErrKindTimeout
	case strings.Contains(msg, "502"),
		strings.Contains(msg, "503"),
		strings.Contains(msg, "504"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "eof"):
		return ErrKindTransient
	default:
		return ErrKindPermanent
	}
}

func classifyHTTPStatus(statusCode int) RPCErrorKind {
	switch {
	case statusCode == http.StatusTooManyRequests:
		return ErrKindRateLimit
	case statusCode == http.StatusRequestTimeout || statusCode == http.StatusGatewayTimeout:
		return ErrKindTimeout
	case statusCode >= 500 && statusCode <= 599:
		return ErrKindTransient
	default:
		return ErrKindPermanent
	}
}
