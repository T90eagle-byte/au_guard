package pipeline

import (
	"context"
	"errors"

	"example.org/auguard/internal/events"
)

var ErrRejected = errors.New("event rejected")

type EventSource interface {
	Next(context.Context) (events.Event, error)
}

type EventHandler interface {
	Handle(context.Context, events.Event) (events.Decision, error)
}
