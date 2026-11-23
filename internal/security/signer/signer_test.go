package signer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/chainflow/chainflow/internal/config"
	"github.com/chainflow/chainflow/internal/domain"
)

const (
	testPrivKeyHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	testSepoliaID  = int64(11155111)
)

func mustTestAddress(t *testing.T) common.Address {
	t.Helper()
	key, err := crypto.HexToECDSA(testPrivKeyHex)
	if err != nil {
		t.Fatalf("HexToECDSA: %v", err)
	}
	return crypto.PubkeyToAddress(key.PublicKey)
}

func TestDevSigner_SignEIP1559_Success(t *testing.T) {
	t.Parallel()

	from := mustTestAddress(t)
	to := common.HexToAddress("0x000000000000000000000000000000000000dead")

	s, err := NewDevSigner(DevSignerConfig{
		Keys: map[string]config.SecretString{
			"hot-wallet-1": config.SecretString("0x" + testPrivKeyHex),
		},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}

	gotAddr, err := s.Address(context.Background(), "hot-wallet-1")
	if err != nil {
		t.Fatalf("Address: %v", err)
	}
	if gotAddr != from {
		t.Fatalf("Address = %s, want %s", gotAddr.Hex(), from.Hex())
	}

	tx, err := s.SignEIP1559(context.Background(), SignRequest{
		KeyRef:               "hot-wallet-1",
		ChainID:              big.NewInt(testSepoliaID),
		From:                 from,
		To:                   &to,
		Nonce:                7,
		ValueWei:             big.NewInt(1_000_000_000_000_000),
		Data:                 []byte{0xde, 0xad, 0xbe, 0xef},
		GasLimit:             25_000,
		MaxPriorityFeePerGas: big.NewInt(1_500_000_000),
		MaxFeePerGas:         big.NewInt(30_000_000_000),
	})
	if err != nil {
		t.Fatalf("SignEIP1559: %v", err)
	}

	sender, err := types.Sender(types.NewLondonSigner(big.NewInt(testSepoliaID)), tx)
	if err != nil {
		t.Fatalf("types.Sender: %v", err)
	}
	if sender != from {
		t.Fatalf("recovered sender = %s, want %s", sender.Hex(), from.Hex())
	}

	hash, raw, err := EncodeSignedTx(tx)
	if err != nil {
		t.Fatalf("EncodeSignedTx: %v", err)
	}
	if hash.String() != tx.Hash().Hex() {
		t.Fatalf("encoded hash %s != tx.Hash() %s", hash.String(), tx.Hash().Hex())
	}

	var decoded types.Transaction
	if err := decoded.UnmarshalBinary(raw); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if decoded.Hash() != tx.Hash() {
		t.Fatalf("decoded hash %s != tx.Hash() %s", decoded.Hash().Hex(), tx.Hash().Hex())
	}
}

func TestDevSigner_MainnetGuard(t *testing.T) {
	t.Parallel()

	from := mustTestAddress(t)
	to := common.HexToAddress("0x000000000000000000000000000000000000dead")
	req := SignRequest{
		KeyRef:               "hot-wallet-1",
		ChainID:              big.NewInt(1),
		From:                 from,
		To:                   &to,
		Nonce:                0,
		ValueWei:             big.NewInt(0),
		GasLimit:             21_000,
		MaxPriorityFeePerGas: big.NewInt(1_000_000_000),
		MaxFeePerGas:         big.NewInt(10_000_000_000),
	}

	guarded, err := NewDevSigner(DevSignerConfig{
		Keys:         map[string]config.SecretString{"hot-wallet-1": config.SecretString(testPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
		AllowMainnet: false,
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}
	if _, err := guarded.SignEIP1559(context.Background(), req); !errors.Is(err, ErrMainnetSigningForbidden) {
		t.Fatalf("expected ErrMainnetSigningForbidden, got %v", err)
	}

	allowed, err := NewDevSigner(DevSignerConfig{
		Keys:         map[string]config.SecretString{"hot-wallet-1": config.SecretString(testPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
		AllowMainnet: true,
	})
	if err != nil {
		t.Fatalf("NewDevSigner(AllowMainnet): %v", err)
	}
	if _, err := allowed.SignEIP1559(context.Background(), req); err != nil {
		t.Fatalf("expected mainnet signing with AllowMainnet=true to succeed, got %v", err)
	}
}

func TestDevSigner_Guardrails(t *testing.T) {
	t.Parallel()

	from := mustTestAddress(t)
	other := common.HexToAddress("0x000000000000000000000000000000000000beef")
	s, err := NewDevSigner(DevSignerConfig{
		Keys:         map[string]config.SecretString{"hot-wallet-1": config.SecretString(testPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(50_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}

	if _, err := s.Address(context.Background(), "missing"); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name    string
		ctx     context.Context
		req     SignRequest
		wantErr error
	}{
		{
			name: "cancelled context",
			ctx:  canceledCtx,
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(testSepoliaID), From: from,
				GasLimit: 21_000, MaxPriorityFeePerGas: big.NewInt(1), MaxFeePerGas: big.NewInt(2),
			},
			wantErr: context.Canceled,
		},
		{
			name: "from address mismatch",
			ctx:  context.Background(),
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(testSepoliaID), From: other,
				GasLimit: 21_000, MaxPriorityFeePerGas: big.NewInt(1), MaxFeePerGas: big.NewInt(2),
			},
			wantErr: ErrFromAddressMismatch,
		},
		{
			name: "fee cap exceeded",
			ctx:  context.Background(),
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(testSepoliaID), From: from,
				GasLimit: 21_000, MaxPriorityFeePerGas: big.NewInt(1), MaxFeePerGas: big.NewInt(60_000_000_000),
			},
			wantErr: domain.ErrFeeCapExceeded,
		},
		{
			name: "priority fee exceeds max fee",
			ctx:  context.Background(),
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(testSepoliaID), From: from,
				GasLimit: 21_000, MaxPriorityFeePerGas: big.NewInt(10), MaxFeePerGas: big.NewInt(5),
			},
			wantErr: domain.ErrInvalidInput,
		},
		{
			name: "gas limit below 21000",
			ctx:  context.Background(),
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(testSepoliaID), From: from,
				GasLimit: 20_999, MaxPriorityFeePerGas: big.NewInt(1), MaxFeePerGas: big.NewInt(2),
			},
			wantErr: domain.ErrInvalidInput,
		},
		{
			name: "non-positive chain id",
			ctx:  context.Background(),
			req: SignRequest{
				KeyRef: "hot-wallet-1", ChainID: big.NewInt(0), From: from,
				GasLimit: 21_000, MaxPriorityFeePerGas: big.NewInt(1), MaxFeePerGas: big.NewInt(2),
			},
			wantErr: domain.ErrInvalidInput,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.SignEIP1559(tc.ctx, tc.req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err %v, want errors.Is(_, %v)", err, tc.wantErr)
			}
		})
	}
}

func TestDevSigner_ZeroKeyLeakage(t *testing.T) {
	t.Parallel()

	s, err := NewDevSigner(DevSignerConfig{
		Keys:         map[string]config.SecretString{"hot-wallet-1": config.SecretString(testPrivKeyHex)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if err != nil {
		t.Fatalf("NewDevSigner: %v", err)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	logger.Info("signer initialized", slog.Any("signer", s))

	outputs := []string{
		fmt.Sprint(s),
		fmt.Sprintf("%#v", s),
		buf.String(),
	}
	for _, out := range outputs {
		if strings.Contains(strings.ToLower(out), testPrivKeyHex) {
			t.Fatalf("private key leaked into output: %s", out)
		}
	}

	badSecret := "deadbeefdeadbeefdeadbeefdeadbeef"
	_, badErr := NewDevSigner(DevSignerConfig{
		Keys:         map[string]config.SecretString{"bad": config.SecretString(badSecret)},
		MaxFeeCapWei: big.NewInt(100_000_000_000),
	})
	if badErr == nil {
		t.Fatal("expected error for invalid private key")
	}
	if strings.Contains(strings.ToLower(badErr.Error()), badSecret) {
		t.Fatalf("invalid key error leaked secret material: %v", badErr)
	}
}
