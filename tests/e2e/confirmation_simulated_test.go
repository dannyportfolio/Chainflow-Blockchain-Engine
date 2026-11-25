package e2e_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient/simulated"

	"github.com/chainflow/chainflow/internal/application"
	"github.com/chainflow/chainflow/internal/blockchain"
	"github.com/chainflow/chainflow/internal/domain"
	"github.com/chainflow/chainflow/internal/persistence/memory"
	"github.com/chainflow/chainflow/internal/security/signer"
)

func TestConfirmationMonitor_SimulatedEVM_FinalizesAfterRequiredConfirmations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	privKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	fromEth := crypto.PubkeyToAddress(privKey.PublicKey)
	toEth := common.HexToAddress("0x2222222222222222222222222222222222222222")

	sim := simulated.NewBackend(types.GenesisAlloc{
		fromEth: {Balance: new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))},
	})
	defer sim.Close()

	simClient := sim.Client()
	chainID, err := simClient.ChainID(ctx)
	if err != nil {
		t.Fatalf("chain id: %v", err)
	}

	rawTx := types.MustSignNewTx(privKey, types.LatestSignerForChainID(chainID), &types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     0,
		To:        &toEth,
		Value:     big.NewInt(1_000_000_000_000_000),
		Gas:       21000,
		GasTipCap: big.NewInt(1_000_000_000),
		GasFeeCap: big.NewInt(20_000_000_000),
	})
	if err := simClient.SendTransaction(ctx, rawTx); err != nil {
		t.Fatalf("send tx: %v", err)
	}

	txHash, err := domain.ParseHash(rawTx.Hash().Hex())
	if err != nil {
		t.Fatalf("parse hash: %v", err)
	}
	fromAddr, _ := domain.ParseAddress(fromEth.Hex())
	toAddr, _ := domain.ParseAddress(toEth.Hex())
	reqHash, _ := domain.ParseHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	now := time.Now().UTC()

	store := memory.NewStore()
	_ = store.UpsertWallet(ctx, domain.Wallet{
		ID: "01934a1a-0000-7000-8000-000000000001", ChainID: chainID.Int64(), Address: fromAddr,
		Label: "sim", SignerKeyRef: "dev:sim", Status: domain.WalletActive, MaxInflightNonces: 1,
		CreatedAt: now, UpdatedAt: now,
	}, 0)

	intent := domain.TransactionIntent{
		ID: "01934a1a-0000-7000-8000-000000000010", TenantID: "t1", IdempotencyKey: "k1",
		RequestHash: reqHash, ChainID: chainID.Int64(), From: fromAddr, To: &toAddr,
		ValueWei: big.NewInt(1_000_000_000_000_000), MaxFeeCapWei: big.NewInt(100_000_000_000),
		MaxAttempts: 5, AttemptCount: 1, Status: domain.IntentBroadcasted, Version: 1,
		ExpiresAt: now.Add(10 * time.Minute), CreatedAt: now, UpdatedAt: now,
	}
	if _, err := store.CreateIntentWithOutbox(ctx, intent, domain.StateTransitionRecord{
		IntentID: intent.ID, ToStatus: domain.IntentBroadcasted, Actor: "test", Reason: "seed", OccurredAt: now,
	}, application.OutboxEvent{
		EventID: "01934a1a-0000-7000-8000-000000000011", AggregateType: "transaction_intent",
		AggregateID: intent.ID, Subject: "chainflow.tx.dispatch", EventVersion: 1,
		Payload: []byte(`{}`), CreatedAt: now, AvailableAt: now,
	}); err != nil {
		t.Fatalf("create intent: %v", err)
	}
	res, err := store.ReserveNextNonce(ctx, chainID.Int64(), fromAddr, intent.ID, now)
	if err != nil {
		t.Fatalf("reserve nonce: %v", err)
	}

	_, rawBytes, err := signer.EncodeSignedTx(rawTx)
	if err != nil {
		t.Fatalf("encode tx: %v", err)
	}
	exec := domain.TransactionExecution{
		ID: "01934a1a-0000-7000-8000-000000000020", IntentID: intent.ID, AttemptNumber: 1,
		ChainID: chainID.Int64(), From: fromAddr, Nonce: res.Nonce, GasLimit: 21000,
		MaxPriorityFeeWei: big.NewInt(1_000_000_000), MaxFeePerGasWei: big.NewInt(20_000_000_000),
		TxHash: &txHash, RawSignedTx: rawBytes, Status: domain.ExecBroadcasted,
		BroadcastedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateExecution(ctx, exec); err != nil {
		t.Fatalf("create execution: %v", err)
	}

	bcClient := blockchain.NewGethClient("sim", simClient, chainID.Int64())
	router, err := blockchain.NewRouter([]blockchain.ManagedProvider{{
		Config: blockchain.ProviderConfig{Name: "sim", Priority: 1, RequestTimeout: 5 * time.Second},
		Client: bcClient,
	}})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	verifier, err := blockchain.NewReorgVerifier(bcClient)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	mon := application.NewConfirmationMonitor(store, store, store, router, verifier, application.ConfirmationMonitorConfig{
		ChainID: chainID.Int64(), RequiredConfirmations: 2, Now: func() time.Time { return now },
	})

	// Mine block 1 (tx included, 1 confirmation < 2 required)
	sim.Commit()
	if err := mon.RunOnce(ctx); err != nil {
		t.Fatalf("run 1 failed: %v", err)
	}
	got, _ := store.GetIntentByIDForSystem(ctx, intent.ID)
	if got.Status != domain.IntentConfirming {
		t.Fatalf("expected CONFIRMING after 1 block, got %s", got.Status)
	}

	// Mine block 2 (2 confirmations >= 2 required -> FINALIZED)
	sim.Commit()
	if err := mon.RunOnce(ctx); err != nil {
		t.Fatalf("run 2 failed: %v", err)
	}
	got, _ = store.GetIntentByIDForSystem(ctx, intent.ID)
	if got.Status != domain.IntentFinalized || got.ExecutionOutcome == nil || *got.ExecutionOutcome != domain.OutcomeSucceeded {
		t.Fatalf("expected FINALIZED(SUCCEEDED), got %s (%v)", got.Status, got.ExecutionOutcome)
	}
}
