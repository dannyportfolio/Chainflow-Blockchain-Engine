package migrations_test

import (
	"strings"
	"testing"

	"github.com/chainflow/chainflow/migrations"
)

func TestLoadMigrations_AreValidAndPaired(t *testing.T) {
	t.Parallel()

	ups, err := migrations.LoadUp()
	if err != nil {
		t.Fatalf("LoadUp failed: %v", err)
	}
	if len(ups) == 0 {
		t.Fatal("expected at least one up migration")
	}

	requiredTables := []string{
		"wallets",
		"wallet_nonces",
		"transaction_intents",
		"transaction_executions",
		"nonce_reservations",
		"transaction_receipts",
		"outbox_events",
		"transaction_state_events",
	}

	combined := strings.Builder{}
	for i, m := range ups {
		if m.Version != i+1 {
			t.Fatalf("expected contiguous version %d, got %d (%s)", i+1, m.Version, m.Name)
		}
		if strings.TrimSpace(m.UpSQL) == "" || strings.TrimSpace(m.DownSQL) == "" {
			t.Fatalf("migration %s missing up or down SQL", m.Name)
		}
		combined.WriteString(m.UpSQL)
	}

	sqlAll := combined.String()
	for _, tbl := range requiredTables {
		if !strings.Contains(sqlAll, "CREATE TABLE "+tbl) {
			t.Fatalf("missing CREATE TABLE %s in up migrations", tbl)
		}
	}
}
