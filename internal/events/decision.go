package events

type Decision struct {
	EventID string `json:"event_id"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
}
