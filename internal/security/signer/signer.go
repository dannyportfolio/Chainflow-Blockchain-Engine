package signer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
)

var (
	ErrKeyNotFound             = errors.New("signer key reference not found")
	ErrMainnetSigningForbidden = errors.New("dev signer refuses to sign for mainnet chain_id 1")
	ErrFromAddressMismatch     = errors.New("request from address does not match signer key address")
)

type SignRequest struct {
	KeyRef               string
	ChainID              *big.Int
	From                 common.Address
	To                   *common.Address
	Nonce                uint64
	ValueWei             *big.Int
	Data                 []byte
	GasLimit             uint64
	MaxPriorityFeePerGas *big.Int
	MaxFeePerGas         *big.Int
}

type Signer interface {
	SignEIP1559(ctx context.Context, req SignRequest) (*types.Transaction, error)
	Address(ctx context.Context, keyRef string) (common.Address, error)
}

type DevSignerConfig struct {
	Keys         map[string]config.SecretString
	MaxFeeCapWei *big.Int
	AllowMainnet bool
}

type devKeyEntry struct {
	key  *ecdsa.PrivateKey
	addr common.Address
}

type DevSigner struct {
	keys         map[string]devKeyEntry
	maxFeeCapWei *big.Int
	allowMainnet bool
}

func NewDevSigner(cfg DevSignerConfig) (*DevSigner, error) {
	if cfg.MaxFeeCapWei == nil || cfg.MaxFeeCapWei.Sign() <= 0 {
		return nil, fmt.Errorf("%w: max_fee_cap_wei must be positive", domain.ErrInvalidInput)
	}
	parsed := make(map[string]devKeyEntry, len(cfg.Keys))
	for ref, secret := range cfg.Keys {
		if strings.TrimSpace(ref) == "" {
			return nil, fmt.Errorf("%w: signer key_ref must not be empty", domain.ErrInvalidInput)
		}
		rawHex := strings.TrimSpace(secret.UnsafeValue())
		rawHex = strings.TrimPrefix(strings.TrimPrefix(rawHex, "0x"), "0X")
		key, err := crypto.HexToECDSA(rawHex)
		if err != nil {
			// Never include rawHex or err details that might echo key material.
			return nil, fmt.Errorf("%w: invalid ECDSA private key for key_ref %q", domain.ErrInvalidInput, ref)
		}
		addr := crypto.PubkeyToAddress(key.PublicKey)
		parsed[ref] = devKeyEntry{key: key, addr: addr}
	}
	return &DevSigner{
		keys:         parsed,
		maxFeeCapWei: new(big.Int).Set(cfg.MaxFeeCapWei),
		allowMainnet: cfg.AllowMainnet,
	}, nil
}

func (s *DevSigner) String() string {
	return fmt.Sprintf("DevSigner{keys:%d,allowMainnet:%t,[REDACTED]}", len(s.keys), s.allowMainnet)
}

func (s *DevSigner) GoString() string {
	return s.String()
}

func (s *DevSigner) LogValue() slog.Value {
	return slog.StringValue("[REDACTED]")
}

func (s *DevSigner) Address(ctx context.Context, keyRef string) (common.Address, error) {
	if err := ctx.Err(); err != nil {
		return common.Address{}, err
	}
	entry, ok := s.keys[keyRef]
	if !ok {
		return common.Address{}, fmt.Errorf("%w: %s", ErrKeyNotFound, keyRef)
	}
	return entry.addr, nil
}

func (s *DevSigner) SignEIP1559(ctx context.Context, req SignRequest) (*types.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.ChainID == nil || req.ChainID.Sign() <= 0 {
		return nil, fmt.Errorf("%w: chain_id must be positive", domain.ErrInvalidInput)
	}
	if req.ChainID.Cmp(big.NewInt(1)) == 0 && !s.allowMainnet {
		return nil, ErrMainnetSigningForbidden
	}
	if req.GasLimit < 21000 {
		return nil, fmt.Errorf("%w: gas_limit must be >= 21000", domain.ErrInvalidInput)
	}
	if req.MaxPriorityFeePerGas == nil || req.MaxFeePerGas == nil {
		return nil, fmt.Errorf("%w: fee fields are required", domain.ErrInvalidInput)
	}
	if req.MaxPriorityFeePerGas.Sign() < 0 || req.MaxFeePerGas.Sign() <= 0 {
		return nil, fmt.Errorf("%w: fee fields must be non-negative and max_fee_per_gas positive", domain.ErrInvalidInput)
	}
	if req.MaxPriorityFeePerGas.Cmp(req.MaxFeePerGas) > 0 {
		return nil, fmt.Errorf("%w: max_priority_fee_per_gas exceeds max_fee_per_gas", domain.ErrInvalidInput)
	}
	if req.MaxFeePerGas.Cmp(s.maxFeeCapWei) > 0 {
		return nil, fmt.Errorf("%w: max_fee_per_gas %s exceeds cap %s", domain.ErrFeeCapExceeded, req.MaxFeePerGas.String(), s.maxFeeCapWei.String())
	}

	entry, ok := s.keys[req.KeyRef]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, req.KeyRef)
	}
	if req.From != entry.addr {
		return nil, fmt.Errorf("%w: got %s", ErrFromAddressMismatch, req.From.Hex())
	}

	value := big.NewInt(0)
	if req.ValueWei != nil {
		if req.ValueWei.Sign() < 0 {
			return nil, fmt.Errorf("%w: value_wei must be non-negative", domain.ErrInvalidInput)
		}
		value = new(big.Int).Set(req.ValueWei)
	}

	var dataCopy []byte
	if len(req.Data) > 0 {
		dataCopy = make([]byte, len(req.Data))
		copy(dataCopy, req.Data)
	}

	var toCopy *common.Address
	if req.To != nil {
		addr := *req.To
		toCopy = &addr
	}

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   new(big.Int).Set(req.ChainID),
		Nonce:     req.Nonce,
		GasTipCap: new(big.Int).Set(req.MaxPriorityFeePerGas),
		GasFeeCap: new(big.Int).Set(req.MaxFeePerGas),
		Gas:       req.GasLimit,
		To:        toCopy,
		Value:     value,
		Data:      dataCopy,
	})

	signed, err := types.SignTx(tx, types.NewLondonSigner(req.ChainID), entry.key)
	if err != nil {
		return nil, fmt.Errorf("sign eip-1559 tx: %w", err)
	}
	return signed, nil
}

func EncodeSignedTx(tx *types.Transaction) (domain.Hash, []byte, error) {
	if tx == nil {
		return domain.Hash{}, nil, fmt.Errorf("%w: nil transaction", domain.ErrInvalidInput)
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		return domain.Hash{}, nil, fmt.Errorf("marshal signed tx: %w", err)
	}
	var h domain.Hash
	copy(h[:], tx.Hash().Bytes())
	return h, raw, nil
}
