package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

func makePoolEvents(n int) []events.Event {
	items := make([]events.Event, n)

	for i := range items {
		items[i] = events.Event{
			ID:       fmt.Sprintf("demo_%02d", i),
			Kind:     "auth",
			Severity: 2,
			Message:  "fixture",
		}
	}

	return items
}

func TestProcessTenEvents(t *testing.T) {
	source := pipeline.NewSliceSource(makePoolEvents(10))

	var decisions []events.Decision

	emit := func(d events.Decision) error {
		decisions = append(decisions, d)
		return nil
	}

	stats, err := pipeline.Process(
		context.Background(),
		source,
		pipeline.RuleHandler{},
		4,
		2,
		emit,
	)

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if stats.Read != 10 ||
		stats.Succeeded != 10 ||
		stats.Rejected != 0 ||
		stats.Failed != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	if stats.MaxActive < 1 || stats.MaxActive > 4 {
		t.Fatalf("MaxActive = %d, want 1..4", stats.MaxActive)
	}

	if len(decisions) != 10 {
		t.Fatalf("got %d decisions, want 10", len(decisions))
	}

	for i, decision := range decisions {
		want := fmt.Sprintf("demo_%02d", i)

		if decision.EventID != want {
			t.Fatalf(
				"decision %d: got %q, want %q",
				i, decision.EventID, want,
			)
		}
	}

	t.Logf("stats: %+v", stats)
}

type blockedFirstHandler struct {
	firstStarted chan struct{}
	releaseFirst chan struct{}
	otherStarted chan struct{}
	once         sync.Once
}

func (h *blockedFirstHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	if e.ID == "demo_00" {
		close(h.firstStarted)

		select {
		case <-h.releaseFirst:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}
	} else {
		// Поздние события ждут начала обработки demo_00.
		select {
		case <-h.firstStarted:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}

		h.once.Do(func() {
			close(h.otherStarted)
		})
	}

	return pipeline.RuleHandler{}.Handle(ctx, e)
}

func TestProcessPreservesOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	handler := &blockedFirstHandler{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		otherStarted: make(chan struct{}),
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(handler.releaseFirst)
		})
	}
	defer release()

	type outcome struct {
		stats     pipeline.Stats
		decisions []events.Decision
		err       error
	}

	done := make(chan outcome, 1)

	go func() {
		var decisions []events.Decision

		stats, err := pipeline.Process(
			ctx,
			pipeline.NewSliceSource(makePoolEvents(6)),
			handler,
			3,
			2,
			func(d events.Decision) error {
				decisions = append(decisions, d)
				return nil
			},
		)

		done <- outcome{
			stats:     stats,
			decisions: decisions,
			err:       err,
		}
	}()

	waitFor := func(ch <-chan struct{}, name string) {
		t.Helper()

		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s", name)
		}
	}

	waitFor(handler.firstStarted, "first handler")
	waitFor(handler.otherStarted, "another handler")

	// Теперь разрешаем завершить первое событие.
	release()

	var got outcome

	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("Process did not finish")
	}

	if got.err != nil {
		t.Fatalf("Process failed: %v", got.err)
	}

	if len(got.decisions) != 6 {
		t.Fatalf("got %d decisions, want 6", len(got.decisions))
	}

	for i, decision := range got.decisions {
		want := fmt.Sprintf("demo_%02d", i)

		if decision.EventID != want {
			t.Fatalf(
				"position %d: got %q, want %q",
				i, decision.EventID, want,
			)
		}
	}

	if got.stats.MaxActive < 2 || got.stats.MaxActive > 3 {
		t.Fatalf(
			"MaxActive = %d, want 2..3",
			got.stats.MaxActive,
		)
	}

	t.Logf("ordered 6 decisions: %+v", got.stats)
}

type measuredSource struct {
	items []events.Event
	read  atomic.Int64
}

func (s *measuredSource) Next(
	ctx context.Context,
) (events.Event, error) {
	if err := ctx.Err(); err != nil {
		return events.Event{}, err
	}

	index := int(s.read.Load())

	if index >= len(s.items) {
		return events.Event{}, io.EOF
	}

	s.read.Add(1)
	return s.items[index], nil
}

type holdFirstHandler struct {
	firstStarted chan struct{}
	releaseFirst chan struct{}
	laterDone    chan struct{}
}

func (h *holdFirstHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	if e.ID == "demo_00" {
		close(h.firstStarted)

		select {
		case <-h.releaseFirst:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}
	}

	decision, err := pipeline.RuleHandler{}.Handle(ctx, e)

	if e.ID != "demo_00" && err == nil {
		select {
		case h.laterDone <- struct{}{}:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}
	}

	return decision, err
}

func TestProcessBackpressureLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	source := &measuredSource{
		items: makePoolEvents(8),
	}

	handler := &holdFirstHandler{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		laterDone:    make(chan struct{}, 8),
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(handler.releaseFirst)
		})
	}
	defer release()

	var emitted atomic.Int64

	type outcome struct {
		stats pipeline.Stats
		ids   []string
		err   error
	}

	done := make(chan outcome, 1)

	go func() {
		var ids []string

		stats, err := pipeline.Process(
			ctx,
			source,
			handler,
			3,
			2,
			func(d events.Decision) error {
				ids = append(ids, d.EventID)
				emitted.Add(1)
				return nil
			},
		)

		done <- outcome{
			stats: stats,
			ids:   ids,
			err:   err,
		}
	}()

	select {
	case <-handler.firstStarted:
	case <-ctx.Done():
		t.Fatal("first handler did not start")
	}

	// Дожидаемся завершения четырёх следующих обработок.
	for i := 0; i < 4; i++ {
		select {
		case <-handler.laterDone:
		case <-ctx.Done():
			t.Fatal("later handlers did not finish")
		}
	}

	readBefore := source.read.Load()
	emittedBefore := emitted.Load()

	t.Logf(
		"before release: read=%d emitted=%d limit=5",
		readBefore,
		emittedBefore,
	)

	if readBefore != 5 {
		t.Fatalf(
			"read=%d before release, want exactly 5",
			readBefore,
		)
	}

	if emittedBefore != 0 {
		t.Fatalf(
			"emitted=%d before first result, want 0",
			emittedBefore,
		)
	}

	// Разрешаем завершение первого события.
	release()

	var got outcome

	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("Process did not finish after release")
	}

	if got.err != nil {
		t.Fatalf("Process failed: %v", got.err)
	}

	if got.stats.Read != 8 ||
		got.stats.Succeeded != 8 ||
		got.stats.Rejected != 0 ||
		got.stats.Failed != 0 {
		t.Fatalf("unexpected stats: %+v", got.stats)
	}

	if got.stats.MaxActive > 3 {
		t.Fatalf(
			"MaxActive=%d exceeds workers=3",
			got.stats.MaxActive,
		)
	}

	if len(got.ids) != 8 {
		t.Fatalf("got %d decisions, want 8", len(got.ids))
	}

	for i, id := range got.ids {
		want := fmt.Sprintf("demo_%02d", i)

		if id != want {
			t.Fatalf(
				"position %d: got %q, want %q",
				i, id, want,
			)
		}
	}

	t.Logf(
		"after release: read=%d stats=%+v",
		source.read.Load(),
		got.stats,
	)
}

type cancelWaitingHandler struct {
	started chan struct{}
	exited  atomic.Int64
}

func (h *cancelWaitingHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	select {
	case h.started <- struct{}{}:
	case <-ctx.Done():
		return events.Decision{}, ctx.Err()
	}

	<-ctx.Done()
	h.exited.Add(1)

	return events.Decision{}, ctx.Err()
}

func TestProcessCancellationStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	handler := &cancelWaitingHandler{
		started: make(chan struct{}, 2),
	}

	type outcome struct {
		stats pipeline.Stats
		err   error
	}

	done := make(chan outcome, 1)

	go func() {
		stats, err := pipeline.Process(
			ctx,
			pipeline.NewSliceSource(makePoolEvents(20)),
			handler,
			2,
			1,
			func(events.Decision) error {
				return nil
			},
		)

		done <- outcome{stats: stats, err: err}
	}()

	// Ожидаем запуска обоих работников.
	for i := 0; i < 2; i++ {
		select {
		case <-handler.started:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}

	// Отменяем команду при двух занятых workers.
	cancel()

	var got outcome

	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not stop after cancellation")
	}

	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", got.err)
	}

	if got.stats.Failed != 0 {
		t.Fatalf("cancellation must not count as Failed: %+v", got.stats)
	}

	if got.stats.Succeeded != 0 {
		t.Fatalf("unexpected successful decisions: %+v", got.stats)
	}

	if handler.exited.Load() != 2 {
		t.Fatalf(
			"only %d of 2 workers stopped",
			handler.exited.Load(),
		)
	}

	t.Logf(
		"canceled: exited=%d stats=%+v",
		handler.exited.Load(),
		got.stats,
	)
}

type sinkFailureHandler struct {
	secondStarted chan struct{}
	secondExited  chan struct{}
}

func (h *sinkFailureHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	switch e.ID {
	case "demo_00":
		// Ждём, пока второй worker начнёт работу.
		select {
		case <-h.secondStarted:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}

	case "demo_01":
		close(h.secondStarted)
		defer close(h.secondExited)

		// Второй worker ждёт отмену после ошибки sink.
		<-ctx.Done()
		return events.Decision{}, ctx.Err()
	}

	return pipeline.RuleHandler{}.Handle(ctx, e)
}

func TestProcessSinkFailureStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	sinkErr := errors.New("test sink failure")

	handler := &sinkFailureHandler{
		secondStarted: make(chan struct{}),
		secondExited:  make(chan struct{}),
	}

	var emitCalls int

	stats, err := pipeline.Process(
		ctx,
		pipeline.NewSliceSource(makePoolEvents(10)),
		handler,
		2,
		1,
		func(d events.Decision) error {
			emitCalls++

			if d.EventID != "demo_00" {
				t.Errorf(
					"unexpected first decision: %s",
					d.EventID,
				)
			}

			return sinkErr
		},
	)

	if !errors.Is(err, sinkErr) {
		t.Fatalf("want original sink error, got %v", err)
	}

	if emitCalls != 1 {
		t.Fatalf("emit called %d times, want 1", emitCalls)
	}

	if stats.Succeeded != 0 || stats.Failed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	select {
	case <-handler.secondExited:
		// Второй обработчик завершился до возврата Process.
	default:
		t.Fatal("second worker was not stopped before return")
	}

	t.Logf(
		"sink failure: emitCalls=%d stats=%+v err=%v",
		emitCalls,
		stats,
		err,
	)
}

func TestProcessSourceStatistics(t *testing.T) {
	valid0 := `{"id":"demo_0","kind":"auth","severity":2,"message":"fixture"}` + "\n"
	valid1 := `{"id":"demo_1","kind":"file","severity":7,"message":"fixture"}` + "\n"
	rejected := `{"id":"demo_bad","kind":"auth","severity":99,"message":"fixture"}` + "\n"
	malformed := "{\"id\":\n"

	tests := []struct {
		name         string
		input        string
		wantRead     int
		wantSuccess  int
		wantRejected int
		wantFailed   int
		wantErr      error
	}{
		{
			name:        "valid_events",
			input:       valid0 + valid1,
			wantRead:    2,
			wantSuccess: 2,
		},
		{
			name:         "validation_rejected",
			input:        rejected,
			wantRejected: 1,
			wantErr:      pipeline.ErrRejected,
		},
		{
			name:         "valid_then_rejected",
			input:        valid0 + rejected,
			wantRead:     1,
			wantRejected: 1,
			wantErr:      pipeline.ErrRejected,
		},
		{
			name:       "malformed_json",
			input:      malformed,
			wantFailed: 1,
			wantErr:    pipeline.ErrInvalidJSONL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(), 3*time.Second,
			)
			defer cancel()

			source := pipeline.NewJSONLSource(
				strings.NewReader(tt.input),
				65536,
				1000,
			)

			var ids []string

			stats, err := pipeline.Process(
				ctx,
				source,
				pipeline.RuleHandler{},
				2,
				2,
				func(d events.Decision) error {
					ids = append(ids, d.EventID)
					return nil
				},
			)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf(
					"error=%v, want %v",
					err, tt.wantErr,
				)
			}

			if stats.Read != tt.wantRead ||
				stats.Rejected != tt.wantRejected ||
				stats.Failed != tt.wantFailed {
				t.Fatalf("unexpected statistics: %+v", stats)
			}

			if stats.Succeeded != len(ids) {
				t.Fatalf(
					"Succeeded=%d, emitted=%d",
					stats.Succeeded, len(ids),
				)
			}

			if tt.name == "valid_events" {
				if stats.Succeeded != tt.wantSuccess {
					t.Fatalf("unexpected success count: %+v", stats)
				}
				if len(ids) != 2 ||
					ids[0] != "demo_0" ||
					ids[1] != "demo_1" {
					t.Fatalf("unexpected decision order: %v", ids)
				}
			}

			if tt.name == "valid_then_rejected" {
				if len(ids) > 1 {
					t.Fatalf("too many decisions: %v", ids)
				}
				if len(ids) == 1 && ids[0] != "demo_0" {
					t.Fatalf("unexpected decision: %v", ids)
				}
			}

			t.Logf(
				"stats=%+v emitted=%d err=%v",
				stats, len(ids), err,
			)
		})
	}
}

func TestProcessInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		queue   int
	}{
		{"workers_zero", 0, 2},
		{"workers_17", 17, 2},
		{"queue_zero", 3, 0},
		{"queue_65", 3, 65},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &measuredSource{
				items: makePoolEvents(2),
			}

			var emitted int

			stats, err := pipeline.Process(
				context.Background(),
				source,
				pipeline.RuleHandler{},
				tt.workers,
				tt.queue,
				func(events.Decision) error {
					emitted++
					return nil
				},
			)

			if err == nil {
				t.Fatal("expected configuration error")
			}
			if source.read.Load() != 0 {
				t.Fatal("source was read with invalid configuration")
			}
			if emitted != 0 {
				t.Fatal("unexpected emit")
			}
			if stats != (pipeline.Stats{}) {
				t.Fatalf("unexpected statistics: %+v", stats)
			}
		})
	}
}

func TestProcessAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	source := &measuredSource{
		items: makePoolEvents(2),
	}

	var emitted int

	stats, err := pipeline.Process(
		ctx,
		source,
		pipeline.RuleHandler{},
		3,
		2,
		func(events.Decision) error {
			emitted++
			return nil
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}

	if source.read.Load() != 0 || emitted != 0 {
		t.Fatalf(
			"unexpected work: read=%d emitted=%d",
			source.read.Load(), emitted,
		)
	}

	if stats != (pipeline.Stats{}) {
		t.Fatalf("unexpected statistics: %+v", stats)
	}
}

type failingPoolHandler struct {
	err error
}

func (h failingPoolHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	return events.Decision{}, h.err
}

func TestProcessHandlerFailure(t *testing.T) {
	handlerErr := errors.New("test handler failure")
	var emitted int

	stats, err := pipeline.Process(
		context.Background(),
		pipeline.NewSliceSource(makePoolEvents(1)),
		failingPoolHandler{err: handlerErr},
		2,
		1,
		func(events.Decision) error {
			emitted++
			return nil
		},
	)

	if !errors.Is(err, handlerErr) {
		t.Fatalf("original handler error lost: %v", err)
	}

	if stats.Read != 1 ||
		stats.Succeeded != 0 ||
		stats.Rejected != 0 ||
		stats.Failed != 1 {
		t.Fatalf("unexpected statistics: %+v", stats)
	}

	if emitted != 0 {
		t.Fatalf("unexpected emit calls: %d", emitted)
	}

	t.Logf("handler failure: stats=%+v err=%v", stats, err)
}

type notifiedPoolSource struct {
	source *measuredSource
	reads  chan struct{}
}

func (s *notifiedPoolSource) Next(
	ctx context.Context,
) (events.Event, error) {
	ev, err := s.source.Next(ctx)
	if err == nil {
		s.reads <- struct{}{}
	}
	return ev, err
}

func TestProcessSingleWorkerBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()

	source := &notifiedPoolSource{
		source: &measuredSource{
			items: makePoolEvents(6),
		},
		reads: make(chan struct{}, 6),
	}

	handler := &holdFirstHandler{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		laterDone:    make(chan struct{}, 6),
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(handler.releaseFirst)
		})
	}
	defer release()

	type outcome struct {
		stats pipeline.Stats
		ids   []string
		err   error
	}

	done := make(chan outcome, 1)

	go func() {
		var ids []string

		stats, err := pipeline.Process(
			ctx,
			source,
			handler,
			1,
			1,
			func(d events.Decision) error {
				ids = append(ids, d.EventID)
				return nil
			},
		)

		done <- outcome{
			stats: stats,
			ids:   ids,
			err:   err,
		}
	}()

	select {
	case <-handler.firstStarted:
	case <-ctx.Done():
		t.Fatal("first worker did not start")
	}

	// Дожидаемся двух успешных чтений.
	for i := 0; i < 2; i++ {
		select {
		case <-source.reads:
		case <-ctx.Done():
			t.Fatal("source did not read two events")
		}
	}

	readBefore := source.source.read.Load()

	t.Logf(
		"before release: workers=1 queue=1 read=%d limit=2",
		readBefore,
	)

	if readBefore != 2 {
		t.Fatalf("read=%d, want 2", readBefore)
	}

	// Третье чтение до освобождения permit недопустимо.
	select {
	case <-source.reads:
		t.Fatal("source read beyond in-flight limit")
	default:
	}

	release()

	var got outcome
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("Process did not finish")
	}

	if got.err != nil {
		t.Fatalf("Process failed: %v", got.err)
	}

	if got.stats.Read != 6 ||
		got.stats.Succeeded != 6 ||
		got.stats.Failed != 0 ||
		got.stats.Rejected != 0 ||
		got.stats.MaxActive != 1 {
		t.Fatalf("unexpected stats: %+v", got.stats)
	}

	for i, id := range got.ids {
		want := fmt.Sprintf("demo_%02d", i)
		if id != want {
			t.Fatalf(
				"position %d: got %q, want %q",
				i, id, want,
			)
		}
	}

	t.Logf("after release: stats=%+v", got.stats)
}

type cancelOnEventHandler struct {
	cancel       context.CancelFunc
	firstStarted chan struct{}
	cancelCalled chan struct{}
	started      atomic.Int64
	exited       atomic.Int64
}

func (h *cancelOnEventHandler) Handle(
	ctx context.Context,
	e events.Event,
) (events.Decision, error) {
	h.started.Add(1)
	defer h.exited.Add(1)

	switch e.ID {
	case "demo_00":
		close(h.firstStarted)

	case "demo_01":
		select {
		case <-h.firstStarted:
		case <-ctx.Done():
			return events.Decision{}, ctx.Err()
		}

		// Обработчик demo_01 инициирует отмену.
		h.cancel()
		close(h.cancelCalled)
	}

	<-ctx.Done()
	return events.Decision{}, ctx.Err()
}

func TestProcessHandlerInitiatedCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()

	handler := &cancelOnEventHandler{
		cancel:       cancel,
		firstStarted: make(chan struct{}),
		cancelCalled: make(chan struct{}),
	}

	type outcome struct {
		stats pipeline.Stats
		err   error
	}

	done := make(chan outcome, 1)

	go func() {
		stats, err := pipeline.Process(
			ctx,
			pipeline.NewSliceSource(makePoolEvents(10)),
			handler,
			2,
			1,
			func(events.Decision) error {
				return nil
			},
		)

		done <- outcome{stats: stats, err: err}
	}()

	select {
	case <-handler.cancelCalled:
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatal("handler did not initiate cancellation")
		}
	}

	var got outcome
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not finish after cancellation")
	}

	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", got.err)
	}

	started := handler.started.Load()
	exited := handler.exited.Load()

	if started < 2 || started != exited {
		t.Fatalf(
			"handler calls not completed: started=%d exited=%d",
			started, exited,
		)
	}

	if got.stats.Failed != 0 ||
		got.stats.Succeeded != 0 {
		t.Fatalf("unexpected stats: %+v", got.stats)
	}

	t.Logf(
		"handler cancellation: started=%d exited=%d stats=%+v",
		started, exited, got.stats,
	)
}
