package blockchain_api

// Is compares two FSM state types for equality using string representation.
func (s FSMStateType) Is(compareState FSMStateType) bool {
	return s.String() == compareState.String()
}
