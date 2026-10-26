package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"example.org/auguard/internal/events"
)

type job struct {
	index int
	event events.Event
}

type result struct {
	index    int
	decision events.Decision
	err      error
}

type Stats struct {
	Read      int
	Succeeded int
	Rejected  int
	Failed    int
	MaxActive int
}

type producerSummary struct {
	Read     int
	Rejected int
}

func Process(
	ctx context.Context,
	src EventSource,
	handler EventHandler,
	workers, queue int,
	emit func(events.Decision) error,
) (Stats, error) {
	var stats Stats

	if src == nil || handler == nil || emit == nil {
		return stats, errors.New("invalid pipeline dependency")
	}
	if workers < 1 || workers > 16 || queue < 1 || queue > 64 {
		return stats, errors.New("invalid pipeline configuration")
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan job, queue)
	results := make(chan result, workers)
	permits := make(chan struct{}, workers+queue)
	summaryCh := make(chan producerSummary, 1)
	firstErr := make(chan error, 1)

	var failOnce sync.Once
	fail := func(err error) {
		if err == nil {
			return
		}
		failOnce.Do(func() {
			firstErr <- err
			cancel()
		})
	}

	var active atomic.Int64
	var maxActive atomic.Int64

	var producerWG sync.WaitGroup
	producerWG.Add(1)

	go func() {
		defer producerWG.Done()
		defer close(jobs)

		summary := producerSummary{}
		defer func() { summaryCh <- summary }()

		index := 0

		for {
			select {
			case permits <- struct{}{}:
			case <-ctx.Done():
				return
			}

			event, err := src.Next(ctx)
			if err != nil {
				<-permits

				switch {
				case errors.Is(err, io.EOF):
					return

				case errors.Is(err, ErrRejected):
					summary.Rejected++
					fail(err)
					return

				case errors.Is(err, context.Canceled),
					errors.Is(err, context.DeadlineExceeded):
					fail(err)
					return

				default:
					fail(fmt.Errorf("read event: %w", err))
					return
				}
			}

			summary.Read++

			select {
			case jobs <- job{index: index, event: event}:
				index++

			case <-ctx.Done():
				<-permits
				return
			}
		}
	}()

	var workersWG sync.WaitGroup

	for i := 0; i < workers; i++ {
		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for {
				var task job

				select {
				case <-ctx.Done():
					return

				case next, ok := <-jobs:
					if !ok {
						return
					}
					task = next
				}

				current := active.Add(1)
				for {
					previous := maxActive.Load()
					if current <= previous ||
						maxActive.CompareAndSwap(previous, current) {
						break
					}
				}

				decision, err := handler.Handle(ctx, task.event)
				active.Add(-1)

				if err != nil {
					fail(fmt.Errorf("handle event: %w", err))
					return
				}

				select {
				case results <- result{
					index:    task.index,
					decision: decision,
				}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var closerWG sync.WaitGroup
	closerWG.Add(1)

	go func() {
		defer closerWG.Done()
		workersWG.Wait()
		close(results)
	}()

	pending := make(map[int]events.Decision)
	nextIndex := 0
	var runErr error

	for {
		select {
		case err := <-firstErr:
			if runErr == nil {
				runErr = err
			}
			cancel()

		case item, ok := <-results:
			if !ok {
				goto finished
			}

			if runErr != nil || ctx.Err() != nil {
				<-permits
				continue
			}

			pending[item.index] = item.decision

			for {
				decision, exists := pending[nextIndex]
				if !exists {
					break
				}

				if err := emit(decision); err != nil {
					fail(fmt.Errorf("emit decision: %w", err))
					break
				}

				stats.Succeeded++
				delete(pending, nextIndex)
				nextIndex++
				<-permits
			}
		}
	}

finished:
	producerWG.Wait()
	closerWG.Wait()

	summary := <-summaryCh
	stats.Read = summary.Read
	stats.Rejected = summary.Rejected
	stats.MaxActive = int(maxActive.Load())

	select {
	case err := <-firstErr:
		if runErr == nil {
			runErr = err
		}
	default:
	}

	if runErr == nil {
		runErr = ctx.Err()
	}

	if runErr != nil &&
		!errors.Is(runErr, ErrRejected) &&
		!errors.Is(runErr, context.Canceled) &&
		!errors.Is(runErr, context.DeadlineExceeded) {
		stats.Failed = 1
	}

	return stats, runErr
}
