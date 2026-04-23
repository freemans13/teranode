package settings

// AdaptiveFetchSettings controls the adaptive subtreeData fetch state
// machine used by blockvalidation and subtreevalidation. See
// docs/superpowers/specs/2026-04-23-adaptive-subtreedata-fetch-design.md.
type AdaptiveFetchSettings struct {
	BootstrapMode             string  `key:"adaptive_fetch_bootstrap_mode" desc:"Initial mode for the adaptive subtreeData fetch gate" default:"auto" category:"AdaptiveFetch" usage:"pessimistic | optimistic | auto" type:"string" longdesc:"### Purpose\nControls whether the node starts fetching subtreeData files on startup or skips them.\n\n### Values\n- **auto** (default) - Start pessimistic. Safe for every deployment.\n- **pessimistic** - Always fetch subtreeData until the rolling-window hit rate crosses PessToOptHitRateThreshold.\n- **optimistic** - Skip subtreeData from the first block. Appropriate only when the node is served by a tx distributor that reliably delivers every transaction (e.g. dev-scale-1/2 at 1M TPS). The node still self-corrects back to pessimistic if the distributor fails.\n\n### Recommendations\n- **auto** for mainnet / testnet / teratestnet\n- **optimistic** for dev-scale 1M-TPS clusters where even one subtreeData download is too expensive"`
	WindowSize                int     `key:"adaptive_fetch_window_size" desc:"Rolling-window size used for mode transitions" default:"10" category:"AdaptiveFetch" usage:"Number of recent observations to average" type:"int"`
	PessToOptHitRateThreshold float64 `key:"adaptive_fetch_pess_to_opt_hit_rate_threshold" desc:"Min avg local-hit rate (0..1) to switch pessimistic → optimistic" default:"0.99" category:"AdaptiveFetch" usage:"0..1" type:"float64"`
	OptToPessMissThreshold    int     `key:"adaptive_fetch_opt_to_pess_miss_threshold" desc:"Absolute missing-tx count in a single block that immediately trips optimistic → pessimistic" default:"100" category:"AdaptiveFetch" usage:"Immediate safety trip" type:"int"`
	OptToPessAvgMissThreshold float64 `key:"adaptive_fetch_opt_to_pess_avg_miss_threshold" desc:"Avg missing-tx count over the window that trips optimistic → pessimistic" default:"10" category:"AdaptiveFetch" usage:"Rolling-average safety trip" type:"float64"`
}
