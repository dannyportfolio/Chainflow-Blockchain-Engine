package blockchain

import (
	"context"
	"math/big"

	"github.com/chainflow/chainflow/internal/domain"
)

type FeeSuggestion struct {
	BaseFeeWei           *big.Int
	MaxPriorityFeePerGas *big.Int
	MaxFeePerGas         *big.Int
}

type CallMsg struct {
	From     domain.Address
	To       *domain.Address
	ValueWei *big.Int
	Data     []byte
}

type Header struct {
	Number  uint64
	Hash    domain.Hash
	BaseFee *big.Int
}

type Receipt struct {
	TxHash            domain.Hash
	BlockNumber       uint64
	BlockHash         domain.Hash
	TransactionIndex  uint32
	GasUsed           uint64
	EffectiveGasPrice *big.Int
	Status            uint8
}

type ChainTransaction struct {
	Hash        domain.Hash
	Nonce       uint64
	BlockNumber *uint64
	BlockHash   *domain.Hash
	Pending     bool
}

type Client interface {
	ChainID(ctx context.Context) (int64, error)
	BlockNumber(ctx context.Context) (uint64, error)
	HeaderByNumber(ctx context.Context, number *uint64) (Header, error)
	PendingNonceAt(ctx context.Context, addr domain.Address) (uint64, error)
	LatestNonceAt(ctx context.Context, addr domain.Address) (uint64, error)
	SuggestGasFees(ctx context.Context) (FeeSuggestion, error)
	EstimateGas(ctx context.Context, msg CallMsg) (uint64, error)
	SendRawTransaction(ctx context.Context, rawTx []byte) error
	TransactionReceipt(ctx context.Context, txHash domain.Hash) (Receipt, error)
	TransactionByHash(ctx context.Context, txHash domain.Hash) (ChainTransaction, error)
	Close()
}
