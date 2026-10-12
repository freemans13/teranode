package logger

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// The optional capabilities a UTXO store can offer beside utxo.Store. Callers find each one by
// type assertion on the store they hold, and with logging=true the store they hold is this
// wrapper, so a capability the wrapper does not carry is a capability the caller never sees:
//
//   - pruner.PrunerServiceProvider: the pruner service gets no pruner and the store never prunes.
//   - pruner.Stamper: the stamp worker never stamps, and the rewind tool's preflight, which reads
//     Floors and StampDepth, permits a target that exposes a stamped height.
//
// The wrapper cannot carry them unconditionally. A wrapped sql or aerospike store is not a
// Stamper, and a wrapper that claimed to be one would hand the stamp worker a store with no
// stamp. So New returns the wrapper type whose method set matches the inner store's: each
// capability is offered exactly when the wrapped store offers it.

// stamperStore is the wrapper for an inner store that is a pruner.Stamper.
type stamperStore struct {
	*Store
	stamper pruner.Stamper
}

func (s *stamperStore) StampDepth() uint32 {
	return s.stamper.StampDepth()
}

func (s *stamperStore) WindowBlocks() uint32 {
	return s.stamper.WindowBlocks()
}

func (s *stamperStore) Floors(ctx context.Context) (pruner.StampFloors, error) {
	floors, err := s.stamper.Floors(ctx)
	s.logger.Debugf("[UTXOStore][logger][Floors] stampFence %d err %v : %s", floors.StampFence, err, caller())

	return floors, err
}

func (s *stamperStore) OpenDrain(ctx context.Context) (pruner.StampDrain, pruner.StampFloors, error) {
	drain, floors, err := s.stamper.OpenDrain(ctx)
	s.logger.Debugf("[UTXOStore][logger][OpenDrain] stampFence %d err %v : %s", floors.StampFence, err, caller())

	return drain, floors, err
}

// providerStore is the wrapper for an inner store that is a pruner.PrunerServiceProvider.
type providerStore struct {
	*Store
	provider pruner.PrunerServiceProvider
}

func (s *providerStore) GetPrunerService() (pruner.Service, error) {
	return s.provider.GetPrunerService()
}

// stamperProviderStore is the wrapper for an inner store that is both, which the utxoset store is.
type stamperProviderStore struct {
	*stamperStore
	provider pruner.PrunerServiceProvider
}

func (s *stamperProviderStore) GetPrunerService() (pruner.Service, error) {
	return s.provider.GetPrunerService()
}

// withCapabilities returns s as the wrapper type that offers the inner store's capabilities.
func withCapabilities(s *Store, inner utxo.Store) utxo.Store {
	stamper, isStamper := inner.(pruner.Stamper)
	provider, isProvider := inner.(pruner.PrunerServiceProvider)

	switch {
	case isStamper && isProvider:
		return &stamperProviderStore{stamperStore: &stamperStore{Store: s, stamper: stamper}, provider: provider}
	case isStamper:
		return &stamperStore{Store: s, stamper: stamper}
	case isProvider:
		return &providerStore{Store: s, provider: provider}
	default:
		return s
	}
}
