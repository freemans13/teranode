package adaptivefetch

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// State is the adaptive fetch state machine. Safe for concurrent use.
type State struct {
	mu sync.Mutex
	// mode is the current live mode (pessimistic or optimistic). Auto is a
	// bootstrap-only value and never appears here once New returns.
	mode Mode
	// allowPessToOpt records whether the operator opted into automatic
	// Pess→Opt transitions. Only BootstrapMode=ModeAuto sets this true;
	// pinned ModePessimistic stays pessimistic forever ("always fetch") and
	// pinned ModeOptimistic, having started in optimistic, also never trips
	// Pess→Opt. The Opt→Pess safety trip is always enabled regardless of
	// bootstrap so a degraded optimistic deployment can still self-recover.
	// Rationale: pinned pessimistic is the documented "always fetch
	// subtreeData" safe default; only auto-opted operators get drift.
	allowPessToOpt bool
	window         []Observation // ring buffer, cap = cfg.WindowSize
	windowHead     int           // next write position
	cfg            Config
	serviceName    string
	metrics        *metrics
}

// New constructs a State with the given Config, a label used for metrics, and
// a prometheus.Registerer to register the four collectors against.
// Pass prometheus.DefaultRegisterer in production and prometheus.NewRegistry()
// in tests to avoid inter-test collector conflicts.
func New(cfg Config, serviceName string, reg prometheus.Registerer) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	initial := cfg.BootstrapMode
	allowPessToOpt := false
	if initial == ModeAuto {
		initial = ModePessimistic
		allowPessToOpt = true
	}
	m := newMetrics(serviceName, reg)
	s := &State{
		mode:           initial,
		allowPessToOpt: allowPessToOpt,
		window:         make([]Observation, 0, cfg.WindowSize),
		cfg:            cfg,
		serviceName:    serviceName,
		metrics:        m,
	}
	s.emitMode()
	return s, nil
}

// emitMode updates the mode gauge to reflect s.mode. Callers must hold s.mu
// (or be in a single-threaded context such as New). The prometheus GaugeVec
// is itself concurrent-safe.
func (s *State) emitMode() {
	val := 0.0
	if s.mode == ModeOptimistic {
		val = 1.0
	}
	s.metrics.modeGauge.WithLabelValues(s.serviceName).Set(val)
}

// Mode returns the current mode.
// A nil receiver returns ModePessimistic.
func (s *State) Mode() Mode {
	if s == nil {
		return ModePessimistic
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// ShouldSkipSubtreeData reports whether the caller should skip the
// subtreeData download for the block/subtree it is about to process.
// A nil receiver returns false (pessimistic — always fetch subtreeData).
func (s *State) ShouldSkipSubtreeData() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode == ModeOptimistic
}

// Record adds an observation to the rolling window, emits per-observation
// metrics, and may transition modes.
// A nil receiver is a no-op.
func (s *State) Record(obs Observation) {
	s.recordWithMode(obs, false, ModePessimistic)
}

// RecordIfMode is like Record but discards the observation when the
// current mode no longer matches observedAt. It exists to close a
// race in the validation hot paths: callers sample ShouldSkipSubtreeData
// (or Mode) at the start of a unit of work, perform mode-specific work,
// and then call back to record the result. With concurrent workers the
// runtime mode can transition between sample and Record, which would
// otherwise apply a pessimistic-mode observation to the optimistic
// window (or vice versa) and skew transition decisions.
//
// observedAt is the mode the caller saw when it chose its code path.
// If the current mode differs at Record time the observation is dropped
// silently — losing one observation is far cheaper than corrupting the
// rolling window. A nil receiver is a no-op.
func (s *State) RecordIfMode(observedAt Mode, obs Observation) {
	s.recordWithMode(obs, true, observedAt)
}

// recordWithMode is the shared body of Record and RecordIfMode. When
// requireMode is true and the live mode differs from observedAt, the
// observation is dropped before any window mutation or metric update.
func (s *State) recordWithMode(obs Observation, requireMode bool, observedAt Mode) {
	if s == nil {
		return
	}
	// Defensive: ignore observations with nonsense counts. These should never
	// occur in production but a silently-corrupted observation would skew the
	// rolling average and either block a Pess→Opt transition or spuriously
	// trigger one.
	if obs.TotalTxs <= 0 {
		return
	}
	if obs.LocalHits < 0 || obs.LocalHits > obs.TotalTxs {
		return
	}
	if obs.MissingFetches < 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Mode-snapshot guard: if the caller sampled the mode at decision time
	// and the mode has since transitioned, the observation belongs to the
	// previous mode's window and must not contaminate the current one.
	if requireMode && s.mode != observedAt {
		return
	}

	if len(s.window) < s.cfg.WindowSize {
		s.window = append(s.window, obs)
	} else {
		s.window[s.windowHead] = obs
		s.windowHead = (s.windowHead + 1) % s.cfg.WindowSize
	}

	// Per-observation metrics.
	s.metrics.hitRate.WithLabelValues(s.serviceName).
		Observe(float64(obs.LocalHits) / float64(obs.TotalTxs))
	if obs.MissingFetches > 0 {
		s.metrics.missesTotal.WithLabelValues(s.serviceName).
			Add(float64(obs.MissingFetches))
	}

	prev := s.mode
	s.maybeTransition()
	if prev != s.mode {
		// Reset the rolling window on every mode transition. Each mode's
		// thresholds must be evaluated against observations collected while
		// in that mode — leaving stale observations from the previous mode
		// in the ring causes bouncing (e.g. an Opt→Pess trip would leave
		// the window full of perfect-hit-rate optimistic samples, and the
		// very next pessimistic Record would instantly satisfy the
		// Pess→Opt threshold and flip back). See pkg/adaptivefetch/state_test.go
		// TestTransition_ClearsWindow_NoImmediateBackflip for the
		// regression case.
		s.window = s.window[:0]
		s.windowHead = 0
		s.metrics.transitions.WithLabelValues(s.serviceName, prev.String(), s.mode.String()).Inc()
		s.emitMode()
	}
}

func (s *State) maybeTransition() {
	switch s.mode {
	case ModePessimistic:
		// Pess→Opt is only allowed when the operator chose BootstrapMode=auto.
		// Pinned ModePessimistic means "always fetch subtreeData" and must
		// never drift to optimistic. The Opt→Pess safety trip below remains
		// always-on so a degraded optimistic deployment can still recover.
		if !s.allowPessToOpt {
			return
		}
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgHitRateLocked() >= s.cfg.PessToOptHitRateThreshold {
			s.mode = ModeOptimistic
		}

	case ModeOptimistic:
		// Threshold semantics are inclusive (>=): a configured threshold
		// value is the *trip point*, not the first value above it. So
		// MissingFetches == OptToPessMissThreshold trips, and an average
		// equal to OptToPessAvgMissThreshold trips. This matches the
		// natural reading of "miss-count threshold of N misses".
		last := s.window[s.lastIndexLocked()]
		if last.MissingFetches >= s.cfg.OptToPessMissThreshold {
			s.mode = ModePessimistic
			return
		}
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgMissesLocked() >= s.cfg.OptToPessAvgMissThreshold {
			s.mode = ModePessimistic
		}
	}
}

func (s *State) avgMissesLocked() float64 {
	var sum int
	for _, o := range s.window {
		sum += o.MissingFetches
	}
	return float64(sum) / float64(len(s.window))
}

func (s *State) lastIndexLocked() int {
	if len(s.window) < s.cfg.WindowSize {
		return len(s.window) - 1
	}
	return (s.windowHead - 1 + s.cfg.WindowSize) % s.cfg.WindowSize
}

func (s *State) avgHitRateLocked() float64 {
	var sum float64
	for _, o := range s.window {
		sum += float64(o.LocalHits) / float64(o.TotalTxs)
	}
	return sum / float64(len(s.window))
}
