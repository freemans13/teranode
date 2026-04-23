package adaptivefetch

// Observation is a single measurement fed to State.Record.
//
// TotalTxs, LocalHits and MissingFetches are counts; Mode is the mode
// the observed work was done in (so the state machine can tell whether
// a tiny MissingFetches value is meaningful).
type Observation struct {
	TotalTxs       int
	LocalHits      int
	MissingFetches int
	Mode           Mode
}
