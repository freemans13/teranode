package httpimpl

import (
	"net/http"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

// FSMHandler handles FSM state related API endpoints
type FSMHandler struct {
	blockchainClient blockchain.ClientI
	logger           ulogger.Logger
}

// NewFSMHandler creates a new FSM handler
func NewFSMHandler(blockchainClient blockchain.ClientI, logger ulogger.Logger) *FSMHandler {
	return &FSMHandler{
		blockchainClient: blockchainClient,
		logger:           logger,
	}
}

// getCurrentState retrieves the current FSM state and handles error logging
func (h *FSMHandler) getCurrentState(c echo.Context) (*blockchain.FSMStateType, error) {
	ctx := c.Request().Context()

	state, err := h.blockchainClient.GetFSMCurrentState(ctx)
	if err != nil {
		h.logger.Errorf("Failed to get blockchain FSM state: %v", err)

		return nil, echo.NewHTTPError(http.StatusInternalServerError, "Failed to get FSM state: "+err.Error())
	}

	h.logger.Debugf("Current blockchain FSM state: %s (%d)", state.String(), state.Number())

	return state, nil
}

// respondWithState formats and returns the current state as a JSON response
func (h *FSMHandler) respondWithState(c echo.Context, state *blockchain.FSMStateType) error {
	return c.JSON(http.StatusOK, map[string]interface{}{
		"state":       state.String(),
		"state_value": int(state.Number()),
	})
}

// GetFSMState retrieves the current FSM state from the blockchain service
func (h *FSMHandler) GetFSMState(c echo.Context) error {
	h.logger.Debugf("Getting current blockchain FSM state")

	state, err := h.getCurrentState(c)
	if err != nil {
		return err
	}

	return h.respondWithState(c, state)
}

// GetFSMEvents returns the available events for the blockchain FSM
func (h *FSMHandler) GetFSMEvents(c echo.Context) error {
	h.logger.Debugf("Getting available blockchain FSM events")

	// Since there's no direct API method to get available events,
	// we'll return all possible FSM event types from the enum
	events := make([]string, 0, len(blockchain_api.FSMEventType_name))
	for _, eventName := range blockchain_api.FSMEventType_name {
		events = append(events, eventName)
	}

	h.logger.Debugf("Available blockchain FSM events: %v", events)

	// Return the events as JSON
	return c.JSON(http.StatusOK, map[string]interface{}{
		"events": events,
	})
}

// GetFSMStates returns all possible FSM states
func (h *FSMHandler) GetFSMStates(c echo.Context) error {
	h.logger.Debugf("Getting all possible blockchain FSM states")

	// Create a map of all possible states
	states := make([]map[string]interface{}, 0)
	for name, value := range blockchain_api.FSMStateType_value {
		states = append(states, map[string]interface{}{
			"name":  name,
			"value": int(value),
		})
	}

	h.logger.Debugf("All possible blockchain FSM states: %v", states)

	return c.JSON(http.StatusOK, states)
}

// validateAndGetEventType validates the event request and returns the corresponding event type
func (h *FSMHandler) validateAndGetEventType(c echo.Context) (blockchain_api.FSMEventType, error) {
	// Parse the event from the request body
	var request struct {
		Event string `json:"event"`
	}

	if err := c.Bind(&request); err != nil {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid request body: "+err.Error())
	}

	if request.Event == "" {
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Event is required")
	}

	// Convert the event string to an FSMEventType
	eventType, ok := blockchain_api.FSMEventType_value[request.Event]
	if !ok {
		h.logger.Errorf("Invalid FSM event type: %s", request.Event)
		return 0, echo.NewHTTPError(http.StatusBadRequest, "Invalid event type: "+request.Event)
	}

	return blockchain_api.FSMEventType(eventType), nil
}

// SendFSMEvent sends a custom event to the blockchain FSM
func (h *FSMHandler) SendFSMEvent(c echo.Context) error {
	eventType, err := h.validateAndGetEventType(c)
	if err != nil {
		return err
	}

	ctx := c.Request().Context()

	h.logger.Infof("Sending custom FSM event: %s (%d)", eventType.String(), eventType)

	// Send the event to the blockchain service
	if err := h.blockchainClient.SendFSMEvent(ctx, eventType); err != nil {
		h.logger.Errorf("Failed to send custom FSM event %s: %v", eventType.String(), err)
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to send FSM event: "+err.Error())
	}

	state, err := h.getCurrentState(c)
	if err != nil {
		return err
	}

	return h.respondWithState(c, state)
}
