package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
)

func TestSpendAndCreateMulti(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	t.Run("matches a loop of SpendAndCreate", func(t *testing.T) { tests.SpendAndCreateMultiMatchesLoop(t, store) })
	t.Run("a spend fails partway", func(t *testing.T) { tests.SpendAndCreateMultiSpendFailsPartway(t, store) })
	t.Run("a parent in the list exists", func(t *testing.T) { tests.SpendAndCreateMultiParentExists(t, store) })
	t.Run("a refusal writes nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusalWritesNothing(t, store) })
	t.Run("repeat at every cut point", func(t *testing.T) { tests.SpendAndCreateMultiRepeatAtCutPoints(t, store) })
}
