package blockchain

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/chainflow/chainflow/internal/domain"
)

type EthBackend interface {
	ChainID(ctx context.Context) (*big.Int, error)
	BlockNumber(ctx context.Context) (uint64, error)
	HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error)
	PendingNonceAt(ctx context.Context, account common.Address) (uint64, error)
	NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error)
	SuggestGasTipCap(ctx context.Context) (*big.Int, error)
	FeeHistory(ctx context.Context, blockCount uint64, lastBlock *big.Int, rewardPercentiles []float64) (*ethereum.FeeHistory, error)
	EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error)
}

type GethClient struct {
	name            string
	eth             EthBackend
	expectedChainID int64
	closeFn         func()
}

func DialGethClient(ctx context.Context, name, rawURL string, expectedChainID int64) (*GethClient, error) {
	eth, err := ethclient.DialContext(ctx, rawURL)
	if err != nil {
		return nil, ClassifyRPCError(name, err)
	}
	client := NewGethClient(name, eth, expectedChainID)
	client.closeFn = eth.Close
	if expectedChainID > 0 {
		if err := client.VerifyChainID(ctx); err != nil {
			client.Close()
			return nil, err
		}
	}
	return client, nil
}

func NewGethClient(name string, eth EthBackend, expectedChainID int64) *GethClient {
	return &GethClient{
		name:            name,
		eth:             eth,
		expectedChainID: expectedChainID,
	}
}

func (g *GethClient) VerifyChainID(ctx context.Context) error {
	if g.expectedChainID <= 0 {
		return nil
	}
	got, err := g.ChainID(ctx)
	if err != nil {
		return err
	}
	if got != g.expectedChainID {
		return fmt.Errorf("%w: provider %s returned %d, want %d", ErrChainIDMismatch, g.name, got, g.expectedChainID)
	}
	return nil
}

func (g *GethClient) ChainID(ctx context.Context) (int64, error) {
	id, err := g.eth.ChainID(ctx)
	if err != nil {
		return 0, ClassifyRPCError(g.name, err)
	}
	if id == nil {
		return 0, ClassifyRPCError(g.name, errors.New("rpc returned nil chain_id"))
	}
	return id.Int64(), nil
}

func (g *GethClient) BlockNumber(ctx context.Context) (uint64, error) {
	n, err := g.eth.BlockNumber(ctx)
	if err != nil {
		return 0, ClassifyRPCError(g.name, err)
	}
	return n, nil
}

func (g *GethClient) HeaderByNumber(ctx context.Context, number *uint64) (Header, error) {
	var num *big.Int
	if number != nil {
		num = new(big.Int).SetUint64(*number)
	}
	h, err := g.eth.HeaderByNumber(ctx, num)
	if err != nil {
		return Header{}, ClassifyRPCError(g.name, err)
	}
	if h == nil || h.Number == nil {
		return Header{}, ClassifyRPCError(g.name, errors.New("rpc returned nil header"))
	}
	var domHash domain.Hash
	copy(domHash[:], h.Hash().Bytes())
	var baseFee *big.Int
	if h.BaseFee != nil {
		baseFee = new(big.Int).Set(h.BaseFee)
	}
	return Header{
		Number:  h.Number.Uint64(),
		Hash:    domHash,
		BaseFee: baseFee,
	}, nil
}

func (g *GethClient) PendingNonceAt(ctx context.Context, addr domain.Address) (uint64, error) {
	n, err := g.eth.PendingNonceAt(ctx, common.BytesToAddress(addr[:]))
	if err != nil {
		return 0, ClassifyRPCError(g.name, err)
	}
	return n, nil
}

func (g *GethClient) LatestNonceAt(ctx context.Context, addr domain.Address) (uint64, error) {
	n, err := g.eth.NonceAt(ctx, common.BytesToAddress(addr[:]), nil)
	if err != nil {
		return 0, ClassifyRPCError(g.name, err)
	}
	return n, nil
}

func (g *GethClient) SuggestGasFees(ctx context.Context) (FeeSuggestion, error) {
	var baseFee *big.Int
	var tip *big.Int

	if fh, err := g.eth.FeeHistory(ctx, 5, nil, []float64{50}); err == nil && fh != nil && len(fh.BaseFee) > 0 {
		baseFee = new(big.Int).Set(fh.BaseFee[len(fh.BaseFee)-1])
		tip = medianRewardTip(fh.Reward)
	}

	if baseFee == nil {
		hdr, err := g.eth.HeaderByNumber(ctx, nil)
		if err != nil {
			return FeeSuggestion{}, ClassifyRPCError(g.name, err)
		}
		if hdr.BaseFee != nil {
			baseFee = new(big.Int).Set(hdr.BaseFee)
		} else {
			baseFee = big.NewInt(0)
		}
	}

	if tip == nil {
		suggested, err := g.eth.SuggestGasTipCap(ctx)
		if err != nil {
			return FeeSuggestion{}, ClassifyRPCError(g.name, err)
		}
		tip = suggested
	}
	if tip.Sign() <= 0 {
		tip = big.NewInt(1_000_000_000)
	}

	maxFee := new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tip)
	if maxFee.Sign() <= 0 {
		maxFee = new(big.Int).Set(tip)
	}
	return FeeSuggestion{
		BaseFeeWei:           baseFee,
		MaxPriorityFeePerGas: tip,
		MaxFeePerGas:         maxFee,
	}, nil
}

func medianRewardTip(rewards [][]*big.Int) *big.Int {
	var sum *big.Int
	var count int64
	for _, block := range rewards {
		if len(block) > 0 && block[0] != nil && block[0].Sign() > 0 {
			if sum == nil {
				sum = new(big.Int)
			}
			sum.Add(sum, block[0])
			count++
		}
	}
	if count == 0 || sum == nil {
		return nil
	}
	return sum.Div(sum, big.NewInt(count))
}

func (g *GethClient) EstimateGas(ctx context.Context, msg CallMsg) (uint64, error) {
	var toPtr *common.Address
	if msg.To != nil {
		to := common.BytesToAddress(msg.To[:])
		toPtr = &to
	}
	gas, err := g.eth.EstimateGas(ctx, ethereum.CallMsg{
		From:  common.BytesToAddress(msg.From[:]),
		To:    toPtr,
		Value: msg.ValueWei,
		Data:  msg.Data,
	})
	if err != nil {
		return 0, ClassifyRPCError(g.name, err)
	}
	return gas, nil
}

func (g *GethClient) SendRawTransaction(ctx context.Context, rawTx []byte) error {
	var tx types.Transaction
	if err := tx.UnmarshalBinary(rawTx); err != nil {
		return fmt.Errorf("%w: invalid raw signed tx: %v", domain.ErrInvalidInput, err)
	}
	if err := g.eth.SendTransaction(ctx, &tx); err != nil {
		return ClassifyRPCError(g.name, err)
	}
	return nil
}

func (g *GethClient) TransactionReceipt(ctx context.Context, txHash domain.Hash) (Receipt, error) {
	r, err := g.eth.TransactionReceipt(ctx, common.BytesToHash(txHash[:]))
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return Receipt{}, ErrReceiptNotFound
		}
		return Receipt{}, ClassifyRPCError(g.name, err)
	}
	if r == nil || r.BlockNumber == nil {
		return Receipt{}, ClassifyRPCError(g.name, errors.New("rpc returned incomplete receipt"))
	}

	var gotHash, blockHash domain.Hash
	copy(gotHash[:], r.TxHash.Bytes())
	copy(blockHash[:], r.BlockHash.Bytes())
	if err := validateReceiptHash(txHash, gotHash); err != nil {
		return Receipt{}, err
	}

	var effPrice *big.Int
	if r.EffectiveGasPrice != nil {
		effPrice = new(big.Int).Set(r.EffectiveGasPrice)
	}
	return Receipt{
		TxHash:            gotHash,
		BlockNumber:       r.BlockNumber.Uint64(),
		BlockHash:         blockHash,
		TransactionIndex:  uint32(r.TransactionIndex),
		GasUsed:           r.GasUsed,
		EffectiveGasPrice: effPrice,
		Status:            uint8(r.Status),
	}, nil
}

func validateReceiptHash(requested, got domain.Hash) error {
	if requested != got {
		return fmt.Errorf("rpc receipt tx_hash mismatch: requested %s, got %s", requested.String(), got.String())
	}
	return nil
}

func (g *GethClient) TransactionByHash(ctx context.Context, txHash domain.Hash) (ChainTransaction, error) {
	tx, isPending, err := g.eth.TransactionByHash(ctx, common.BytesToHash(txHash[:]))
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return ChainTransaction{}, ErrTransactionNotFound
		}
		return ChainTransaction{}, ClassifyRPCError(g.name, err)
	}
	if tx == nil {
		return ChainTransaction{}, ErrTransactionNotFound
	}
	var gotHash domain.Hash
	copy(gotHash[:], tx.Hash().Bytes())
	if gotHash != txHash {
		return ChainTransaction{}, fmt.Errorf("rpc tx_hash mismatch: requested %s, got %s", txHash.String(), gotHash.String())
	}
	return ChainTransaction{
		Hash:    gotHash,
		Nonce:   tx.Nonce(),
		Pending: isPending,
	}, nil
}

func (g *GethClient) Close() {
	if g != nil && g.closeFn != nil {
		g.closeFn()
	}
}
