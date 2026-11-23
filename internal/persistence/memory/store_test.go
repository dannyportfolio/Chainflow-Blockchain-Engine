package memory_test

import (
	"testing"

	"github.com/chainflow/chainflow/internal/persistence/memory"
	"github.com/chainflow/chainflow/internal/persistence/storetest"
)

func TestMemoryStore_SatisfiesContract(t *testing.T) {
	t.Parallel()

	storetest.RunStoreContractTests(t, func(t *testing.T) storetest.StoreBundle {
		t.Helper()
		s := memory.NewStore()
		return storetest.StoreBundle{
			Intents:    s,
			Executions: s,
			Nonces:     s,
			Wallets:    s,
			Outbox:     s,
		}
	})
}
