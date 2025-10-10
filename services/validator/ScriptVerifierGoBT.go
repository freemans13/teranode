/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the Go-BT script verification functionality, providing
script validation using the Bitcoin SV go-bt library implementation. This is
typically used as the default script interpreter.
*/
package validator

import (
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript/interpreter"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// init registers the Go-BT script verifier with the verification factory
// This is called automatically when the package is imported and sets up
// the Go-BT implementation as an available script verifier
func init() {
	TxScriptInterpreterFactory[TxInterpreterGoBT] = newScriptVerifierGoBt
}

// newScriptVerifierGoBt creates a new Go-BT script verifier instance.
func newScriptVerifierGoBt(l ulogger.Logger, po *settings.PolicySettings, pa *chaincfg.Params) TxScriptInterpreter {
	l.Infof("Use Script Verifier with GoBT")

	return &scriptVerifierGoBt{
		logger: l,
		policy: po,
		params: pa,
	}
}

// scriptVerifierGoBt implements the TxScriptInterpreter interface using Go-BT
type scriptVerifierGoBt struct {
	logger ulogger.Logger
	policy *settings.PolicySettings
	params *chaincfg.Params
}

// VerifyScript implements script verification using the Go-BT library
// This method verifies all inputs of a transaction against their corresponding
// locking scripts from the previous outputs using the Go-BT script interpreter.
//
// The verification process includes:
// 1. Panic recovery for script execution errors
// 2. Input script verification with appropriate flags based on block height
// 3. Special handling for historical quirks and known issues
//
// Parameters:
//   - tx: The transaction containing scripts to verify
//   - blockHeight: Current block height for validation context
//
// Returns:
//   - error: Any script verification errors encountered
//
// Special Cases:
//   - Handles negative shift amount errors for historical compatibility
//   - Provides special handling for blocks before height 800,000
func (v *scriptVerifierGoBt) VerifyScript(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				if strings.Contains(rErr.Error(), "negative shift amount") {
					err = errors.NewTxInvalidError("negative shift amount for tx %s: %v", tx.TxIDChainHash().String(), rErr)
					return
				}
			}

			err = errors.NewTxInvalidError("script execution failed: %v", r)
		}
	}()

	// TODO add the utxo heights to the tx verifier
	_ = utxoHeights

	// Verify each input's script
	for i, in := range tx.Inputs {
		prevOutput := &bt.Output{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: in.PreviousTxScript,
		}

		// Configure verification options based on block height
		opts := make([]interpreter.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter.WithTx(tx, i, prevOutput))

		// Add UAHF fork ID if after fork height
		if blockHeight > v.params.UahfForkHeight {
			opts = append(opts, interpreter.WithForkID())
		}

		// Add Genesis activation options if after genesis height
		if blockHeight >= v.params.GenesisActivationHeight {
			opts = append(opts, interpreter.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		// Execute script verification
		if err = interpreter.NewEngine().Execute(opts...); err != nil {
			return errors.NewTxInvalidError("script execution error", err)
		}
	}

	return nil
}

// Interpreter returns the Go-BT interpreter type identifier.
func (v *scriptVerifierGoBt) Interpreter() TxInterpreter {
	return TxInterpreterGoBT
}
