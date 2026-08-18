package legacy

// feelerBudget returns how many peer slots to reserve for feeler probes.
//
// The reservation is what makes a probe paid for rather than borrowed. svnode
// expresses the same idea as arithmetic: its inbound ceiling is
// nMaxConnections - (nMaxOutbound + nMaxFeeler) (net.cpp:1261), so the feeler's
// permit comes out of the inbound share and never out of the outbound target.
// Teranode has one joint ceiling over inbound and automatic outbound rather
// than two separate ones, so the faithful translation is to lower that joint
// ceiling and leave the automatic outbound target completely alone.
//
// Three cases return zero, and each disables the probe loop and the reservation
// together, so the node can never end up paying for a feature that is off:
//
//   - A configured budget of zero or less. This is the single rollback lever.
//   - Connect-only mode. There the node's entire connectivity is the configured
//     list, MaxPeers has already been set to the length of that list, and the
//     address source the probe draws from is not installed at all — so a probe
//     could never run, and reserving a slot would strand a configured peer for
//     nothing.
//   - A budget that would leave no room for an ordinary peer. Reserving the
//     node's whole capacity for probing is never what an operator meant.
func feelerBudget(configured int, connectOnly bool, maxPeers int) int {
	if configured <= 0 || connectOnly {
		return 0
	}

	if maxPeers-configured < 1 {
		return 0
	}

	return configured
}

// peerAdmissionCeiling is how many inbound and automatic outbound peers the
// node will admit: MaxPeers less the slots held back for feeler probes.
//
// Named (addnode) peers are not bounded by this, and are not meant to be: they
// have their own budget and are additive, which is what CountExcludingPermanent
// exists to express.
func peerAdmissionCeiling(maxPeers, feelerSlots int) int {
	ceiling := maxPeers - feelerSlots
	if ceiling < 0 {
		return 0
	}

	return ceiling
}

// feelerAllowed reports whether the automatic outbound tier is at its target,
// which is the only condition under which svnode probes (net.cpp:1865).
//
// The reason for the gate is supply, not politeness. Below target the node is
// short of real peers and the replenishment loop is trying to close that gap; a
// probe launched then competes for exactly the dials the node is missing, and
// on a busy address book it can lose the race for a good address to itself.
//
// The target is read off the connection manager rather than recomputed from
// configuration on purpose. connmgr.New substitutes its own default when the
// caller leaves the target unset, so a recomputed target would be zero, every
// count would clear it, and the node would probe from a cold start with no
// outbound peers at all.
func (s *server) feelerAllowed() bool {
	if s.connManager == nil {
		return false
	}

	return s.connManager.AutomaticOutboundCount() >= int(s.connManager.TargetOutbound())
}
