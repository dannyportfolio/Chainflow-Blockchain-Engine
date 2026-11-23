package security

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/chainflow/chainflow/internal/domain"
)

var ErrUnauthorized = errors.New("unauthorized")

type APIKeyEntry struct {
	TenantID       string
	RawKey         string
	KeySHA256Hex   string
	AllowedSenders []domain.Address
}

type TenantPrincipal struct {
	TenantID       string
	AllowedSenders map[domain.Address]struct{}
}

type APIKeyAuthenticator struct {
	keys map[[32]byte]TenantPrincipal
}

func NewAPIKeyAuthenticator(entries []APIKeyEntry) (*APIKeyAuthenticator, error) {
	keys := make(map[[32]byte]TenantPrincipal, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.TenantID) == "" {
			return nil, errors.New("api key tenant_id must not be empty")
		}
		var digest [32]byte
		switch {
		case entry.KeySHA256Hex != "":
			raw, err := hex.DecodeString(strings.TrimPrefix(entry.KeySHA256Hex, "0x"))
			if err != nil || len(raw) != 32 {
				return nil, fmt.Errorf("invalid sha256 key hash for tenant %s", entry.TenantID)
			}
			copy(digest[:], raw)
		case entry.RawKey != "":
			digest = sha256.Sum256([]byte(entry.RawKey))
		default:
			return nil, fmt.Errorf("empty key material for tenant %s", entry.TenantID)
		}

		allowed := make(map[domain.Address]struct{}, len(entry.AllowedSenders))
		for _, addr := range entry.AllowedSenders {
			allowed[addr] = struct{}{}
		}
		keys[digest] = TenantPrincipal{
			TenantID:       entry.TenantID,
			AllowedSenders: allowed,
		}
	}
	return &APIKeyAuthenticator{keys: keys}, nil
}

func (a *APIKeyAuthenticator) Authenticate(token string) (TenantPrincipal, error) {
	if token == "" {
		return TenantPrincipal{}, ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(token))
	p, ok := a.keys[digest]
	if !ok {
		return TenantPrincipal{}, ErrUnauthorized
	}
	return p, nil
}

func (a *APIKeyAuthenticator) CanUseSender(principal TenantPrincipal, sender domain.Address) bool {
	if len(principal.AllowedSenders) == 0 {
		return true
	}
	_, ok := principal.AllowedSenders[sender]
	return ok
}

func ParseAPIKeyEntries(raw string) ([]APIKeyEntry, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]APIKeyEntry, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		fields := strings.Split(item, ":")
		if len(fields) < 2 || len(fields) > 3 {
			return nil, fmt.Errorf("invalid API key entry format; expected tenant:sha256[:sender]")
		}
		entry := APIKeyEntry{
			TenantID:     strings.TrimSpace(fields[0]),
			KeySHA256Hex: strings.TrimSpace(fields[1]),
		}
		if len(fields) == 3 && strings.TrimSpace(fields[2]) != "" {
			addr, err := domain.ParseAddress(strings.TrimSpace(fields[2]))
			if err != nil {
				return nil, fmt.Errorf("invalid allowed sender address: %w", err)
			}
			entry.AllowedSenders = []domain.Address{addr}
		}
		out = append(out, entry)
	}
	return out, nil
}
