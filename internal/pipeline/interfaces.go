package pipeline

import (
	"context"

	"example.org/auguard/internal/events"
)

type EventSource interface {
	Next(context.Context) (events.Event, error)
}

type EventHandler interface {
	Handle(context.Context, events.Event) (events.Decision, error)
}
