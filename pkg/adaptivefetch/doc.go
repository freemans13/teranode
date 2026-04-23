// Package adaptivefetch provides a small in-process state machine that
// decides whether block validation or subtree validation should fetch the
// subtreeData file from peers or skip it and recover any missing
// transactions individually.
//
// The state machine operates in two modes: pessimistic (always fetch
// subtreeData) and optimistic (skip subtreeData; recover missing txs
// individually). Mode transitions are driven entirely by counts of
// transactions hit in the local UTXO store vs transactions that had to
// be recovered from peers. No FSM state and no wall-clock time is
// consulted. See docs/superpowers/specs/2026-04-23-adaptive-subtreedata-fetch-design.md
// for the full design.
package adaptivefetch
