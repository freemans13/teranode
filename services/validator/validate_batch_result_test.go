package validator

import (
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestValidationResult_ZeroValueIsSuccess(t *testing.T) {
	r := ValidationResult{}
	require.NoError(t, r.Err)
	require.Equal(t, PhaseNone, r.Phase)
	require.Nil(t, r.Meta)
}

func TestValidationResult_WithError(t *testing.T) {
	h := chainhash.Hash{0x01}
	sentinel := errors.New("boom")
	r := ValidationResult{
		TxHash: h,
		Err:    sentinel,
		Phase:  PhaseCreate,
	}
	require.Equal(t, h, r.TxHash)
	require.ErrorIs(t, r.Err, sentinel)
	require.Equal(t, PhaseCreate, r.Phase)
}

func TestValidatePhase_ConstantOrdering(t *testing.T) {
	require.Equal(t, ValidatePhase(0), PhaseNone)
	require.Equal(t, ValidatePhase(1), PhaseGetParents)
	require.Equal(t, ValidatePhase(2), PhaseCPU)
	require.Equal(t, ValidatePhase(3), PhaseSpend)
	require.Equal(t, ValidatePhase(4), PhaseCreate)
	require.Equal(t, ValidatePhase(5), PhaseBlockAssembly)
	require.Equal(t, ValidatePhase(6), PhaseSetLocked)
}

func TestErrParentNotFound_Is(t *testing.T) {
	// Test that ErrParentNotFound is findable via errors.Is
	require.ErrorIs(t, ErrParentNotFound, ErrParentNotFound)

	// Test that a wrapped error preserves the target error via Unwrap
	wrapped := wrappedParentErr{parent: ErrParentNotFound}
	require.ErrorIs(t, wrapped, ErrParentNotFound)
}

// wrappedParentErr implements error and Unwrap for testing.
type wrappedParentErr struct {
	parent error
}

func (w wrappedParentErr) Error() string {
	return "parent error: " + w.parent.Error()
}

func (w wrappedParentErr) Unwrap() error {
	return w.parent
}
