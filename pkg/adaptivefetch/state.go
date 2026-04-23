package adaptivefetch

import "sync"

// State is the adaptive fetch state machine. Safe for concurrent use.
type State struct {
	mu          sync.Mutex
	mode        Mode
	window      []Observation // ring buffer, cap = cfg.WindowSize
	windowHead  int           // next write position
	cfg         Config
	serviceName string
}

// New constructs a State with the given Config and a label used for metrics.
func New(cfg Config, serviceName string) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	initial := cfg.BootstrapMode
	if initial == ModeAuto {
		initial = ModePessimistic
	}
	return &State{
		mode:        initial,
		window:      make([]Observation, 0, cfg.WindowSize),
		cfg:         cfg,
		serviceName: serviceName,
	}, nil
}

// Mode returns the current mode.
func (s *State) Mode() Mode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

// ShouldSkipSubtreeData reports whether the caller should skip the
// subtreeData download for the block/subtree it is about to process.
func (s *State) ShouldSkipSubtreeData() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode == ModeOptimistic
}

// Record adds an observation to the rolling window and may transition modes.
func (s *State) Record(obs Observation) {
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

	s.maybeTransition()
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
