package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"example.org/auguard/internal/events"
)

var (
	ErrInvalidJSONL  = errors.New("invalid JSONL record")
	ErrLineTooLong   = errors.New("JSONL line too long")
	ErrTrailingJSON  = errors.New("trailing JSON data")
	ErrTooManyEvents = errors.New("too many events")
)

type JSONLSource struct {
	reader       *bufio.Reader
	maxLineBytes int
	maxEvents    int
	count        int
	lineNo       int
}

func NewJSONLSource(
	in io.Reader,
	maxLineBytes int,
	maxEvents int,
) *JSONLSource {
	return &JSONLSource{
		reader:       bufio.NewReader(in),
		maxLineBytes: maxLineBytes,
		maxEvents:    maxEvents,
	}
}

func (s *JSONLSource) readLine() ([]byte, error) {
	line := make([]byte, 0, 4096)

	for {
		b, err := s.reader.ReadByte()

		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(line) == 0 {
					return nil, io.EOF
				}
				if len(line) > s.maxLineBytes {
					return nil, ErrLineTooLong
				}
				return line, nil
			}
			return nil, err
		}

		if b == '\n' {
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			if len(line) > s.maxLineBytes {
				return nil, ErrLineTooLong
			}
			return line, nil
		}

		line = append(line, b)

		if len(line) > s.maxLineBytes+1 ||
			(len(line) == s.maxLineBytes+1 && b != '\r') {
			return nil, ErrLineTooLong
		}
	}
}

func (s *JSONLSource) Next(ctx context.Context) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}

	line, err := s.readLine()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return events.Event{}, io.EOF
		}
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo+1, err,
		)
	}

	s.lineNo++

	if s.count >= s.maxEvents {
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo, ErrTooManyEvents,
		)
	}

	if len(line) == 0 {
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo, ErrInvalidJSONL,
		)
	}

	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()

	var event events.Event
	if err := dec.Decode(&event); err != nil {
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo, ErrInvalidJSONL,
		)
	}

	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo, ErrTrailingJSON,
		)
	}

	if err := event.Validate(); err != nil {
		return events.Event{}, fmt.Errorf(
			"line %d: %w", s.lineNo, err,
		)
	}

	s.count++
	return event, nil
}

var _ EventSource = (*JSONLSource)(nil)
