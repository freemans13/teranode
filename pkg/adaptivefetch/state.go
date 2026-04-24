package adaptivefetch

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// State is the adaptive fetch state machine. Safe for concurrent use.
type State struct {
	mu          sync.Mutex
	mode        Mode
	window      []Observation // ring buffer, cap = cfg.WindowSize
	windowHead  int           // next write position
	cfg         Config
	serviceName string
	metrics     *metrics
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
	if initial == ModeAuto {
		initial = ModePessimistic
	}
	m := newMetrics(serviceName, reg)
	s := &State{
		mode:        initial,
		window:      make([]Observation, 0, cfg.WindowSize),
		cfg:         cfg,
		serviceName: serviceName,
		metrics:     m,
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
		s.metrics.transitions.WithLabelValues(s.serviceName, prev.String(), s.mode.String()).Inc()
		s.emitMode()
	}
}

func (s *State) maybeTransition() {
	switch s.mode {
	case ModePessimistic:
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgHitRateLocked() >= s.cfg.PessToOptHitRateThreshold {
			s.mode = ModeOptimistic
		}

	case ModeOptimistic:
		last := s.window[s.lastIndexLocked()]
		if last.MissingFetches > s.cfg.OptToPessMissThreshold {
			s.mode = ModePessimistic
			return
		}
		if len(s.window) < s.cfg.WindowSize {
			return
		}
		if s.avgMissesLocked() > s.cfg.OptToPessAvgMissThreshold {
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
