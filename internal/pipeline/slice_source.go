package pipeline

import (
	"context"
	"io"

	"example.org/auguard/internal/events"
)

type SliceSource struct {
	items []events.Event
	index int
}

func NewSliceSource(items []events.Event) *SliceSource {
	owned := append([]events.Event(nil), items...)
	return &SliceSource{items: owned}
}

func (s *SliceSource) Next(ctx context.Context) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}

	if s.index >= len(s.items) {
		return events.Event{}, io.EOF
	}

	event := s.items[s.index]
	s.index++
	return event, nil
}

var _ EventSource = (*SliceSource)(nil)
