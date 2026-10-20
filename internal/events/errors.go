package events

import "errors"

var ErrInvalidEvent = errors.New("invalid event")

type ValidationError struct {
	Field string
}

func (e *ValidationError) Error() string {
	return "invalid event field: " + e.Field
}

func (e *ValidationError) Unwrap() error {
	return ErrInvalidEvent
}
