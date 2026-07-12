package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// newPoolBudgetSettings builds a minimal *settings.Settings carrying only the
// three values the pool-budget guard reads: the WindowBarrierCollapse flag,
// BatcherMaxConcurrent, and PostgresMaintenancePoolConns.
func newPoolBudgetSettings(batcherMaxConcurrent, maintPoolConns int, flagOn bool) *settings.Settings {
	s := &settings.Settings{}
	s.BlockValidation.WindowBarrierCollapse = flagOn
	s.UtxoStore.BatcherMaxConcurrent = batcherMaxConcurrent
	s.UtxoStore.PostgresMaintenancePoolConns = maintPoolConns
	return s
}

// poolBudgetStore is a minimal fake UTXO store that reports a chosen
// PoolMaxConns and panics on any other use, so the pool-budget guard is
// exercised in isolation without standing up a full ProcessBlockWindow.
type poolBudgetStore struct {
	utxo.Store // nil-embedded: any un-stubbed call panics, proving the guard touches nothing else
	poolMax    int
}

func (s *poolBudgetStore) PoolMaxConns() int { return s.poolMax }

// TestWindowOverlap_FailsClosed_OnSmallPool: flag ON, pool-bound store with
// pool_max_conns=80 and BatcherMaxConcurrent=64 => required 2*64+headroom > 80,
// so the guard must fail closed.
func TestWindowOverlap_FailsClosed_OnSmallPool(t *testing.T) {
	store := &poolBudgetStore{poolMax: 80}
	err := checkWindowPoolBudget(store, 64 /* batcherMaxConcurrent */, 16 /* maintPoolConns */)
	require.Error(t, err, "pool of 80 < 2*64+headroom must fail closed")
}

// TestWindowOverlap_FailsClosed_OnMaintPoolzero: pool is large enough but the
// maintenance pool is disabled (0), so the pruner would be starved under
// overlap => the guard must fail closed.
func TestWindowOverlap_FailsClosed_OnMaintPoolZero(t *testing.T) {
	store := &poolBudgetStore{poolMax: 200}
	err := checkWindowPoolBudget(store, 64 /* batcherMaxConcurrent */, 0 /* maintPoolConns */)
	require.Error(t, err, "PostgresMaintenancePoolConns=0 under overlap must fail closed")
}

// TestWindowOverlap_PassesBudget: pool 200, maint 16, maxConcurrent 64 =>
// 2*64+headroom (132) <= 200 AND maint > 0 => no error.
func TestWindowOverlap_PassesBudget(t *testing.T) {
	store := &poolBudgetStore{poolMax: 200}
	err := checkWindowPoolBudget(store, 64 /* batcherMaxConcurrent */, 16 /* maintPoolConns */)
	require.NoError(t, err, "pool 200 with maint 16 must pass the budget")
}

// TestWindowOverlap_NonPoolStore_Skips: a store that reports PoolMaxConns()==0
// (sentinel = not pool-bound) is never subject to the budget, regardless of the
// maintenance pool setting.
func TestWindowOverlap_NonPoolStore_Skips(t *testing.T) {
	store := &poolBudgetStore{poolMax: 0}
	err := checkWindowPoolBudget(store, 64 /* batcherMaxConcurrent */, 0 /* maintPoolConns */)
	require.NoError(t, err, "non-pool store (PoolMaxConns==0) must skip the budget check")
}

// TestWindowOverlap_FlagOff_NeverErrors: when WindowBarrierCollapse is OFF the
// entry guard is a no-op — it must never error even with an impossibly small
// pool and a disabled maintenance pool.
func TestWindowOverlap_FlagOff_NeverErrors(t *testing.T) {
	u := &BlockValidation{
		utxoStore: &poolBudgetStore{poolMax: 1},
	}
	// Flag defaults false in u.settings below.
	u.settings = newPoolBudgetSettings(1 /* batcherMaxConcurrent, absurdly tight */, 0 /* maintPoolConns */, false /* flag off */)
	require.NoError(t, u.guardWindowPoolBudget(), "flag OFF must never invoke the guard")
}

// TestWindowOverlap_FlagOn_UsesSettings: when the flag is ON the entry guard
// reads BatcherMaxConcurrent + PostgresMaintenancePoolConns from settings and
// the store's PoolMaxConns, and fails closed on a tight pool.
func TestWindowOverlap_FlagOn_UsesSettings(t *testing.T) {
	u := &BlockValidation{
		utxoStore: &poolBudgetStore{poolMax: 80},
	}
	u.settings = newPoolBudgetSettings(64, 16, true /* flag on */)
	require.Error(t, u.guardWindowPoolBudget(), "flag ON with pool 80 and maxConcurrent 64 must fail closed")

	// Same store, generous pool via settings passes.
	u2 := &BlockValidation{utxoStore: &poolBudgetStore{poolMax: 200}}
	u2.settings = newPoolBudgetSettings(64, 16, true)
	require.NoError(t, u2.guardWindowPoolBudget(), "flag ON with pool 200 and maint 16 must pass")
}
