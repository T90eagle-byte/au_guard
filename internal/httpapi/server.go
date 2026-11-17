package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"example.org/auguard/internal/events"
)

const maxBody = 1 << 20

type Ingestor interface {
	Ingest(context.Context, events.Event) (events.Decision, error)
}
type Readiness interface{ Ping(context.Context) error }

type Server struct {
	Ingestor  Ingestor
	Readiness Readiness
	Logger    *slog.Logger
	// Нулевое значение отключает ограничение; CLI задаёт явные значения.
	RateLimit  int
	RateWindow time.Duration
}

var (
	errTooLarge = errors.New("превышен размер тела")
	errBadJSON  = errors.New("некорректный JSON")
	errOneValue = errors.New("требуется один объект")
)

func decodeEvent(raw []byte, max int) (events.Event, error) {
	var ev events.Event
	if len(raw) > max {
		return ev, errTooLarge
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ev, errBadJSON
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&ev); err != nil {
		return events.Event{}, errBadJSON
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return events.Event{}, errOneValue
	}
	if err := ev.Validate(); err != nil {
		return events.Event{}, err
	}
	return ev, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error": code})
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	register := func(path, method string, h http.HandlerFunc) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != method {
				w.Header().Set("Allow", method)
				problem(w, 405, "method_not_allowed")
				return
			}
			h(w, r)
		})
	}
	register("/healthz", "GET", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	register("/readyz", "GET", s.ready)
	register("/v1/events", "POST", s.ingest)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { problem(w, 404, "not_found") })
	var handler http.Handler = mux
	if s.RateLimit > 0 && s.RateWindow > 0 {
		handler = (&limiter{limit: s.RateLimit, window: s.RateWindow}).wrap(handler)
	}
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return withRequestID(handler, logger)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if s.Readiness == nil {
		writeJSON(w, 503, map[string]string{"status": "not_ready"})
		return
	}
	result := make(chan error, 1)
	go func() { result <- s.Readiness.Ping(ctx) }()
	select {
	case err := <-result:
		if err == nil && ctx.Err() == nil {
			writeJSON(w, 200, map[string]string{"status": "ready"})
			return
		}
	case <-ctx.Done():
	}
	writeJSON(w, 503, map[string]string{"status": "not_ready"})
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		problem(w, 415, "unsupported_media_type")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			problem(w, 413, "body_too_large")
		} else {
			problem(w, 400, "bad_json")
		}
		return
	}
	ev, err := decodeEvent(raw, maxBody)
	if err != nil {
		switch {
		case errors.Is(err, errTooLarge):
			problem(w, 413, "body_too_large")
		case errors.Is(err, events.ErrInvalidEvent):
			problem(w, 422, "invalid_event")
		case errors.Is(err, errOneValue):
			problem(w, 400, "one_value_required")
		default:
			problem(w, 400, "bad_json")
		}
		return
	}
	if s.Ingestor == nil {
		problem(w, 500, "processing_failed")
		return
	}
	decision, err := s.Ingestor.Ingest(r.Context(), ev)
	if err != nil || r.Context().Err() != nil {
		problem(w, 500, "processing_failed")
		return
	}
	writeJSON(w, 200, decision)
}

type requestIDKey struct{}
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}

func withRequestID(next http.Handler, logger *slog.Logger) http.Handler {
	return withRequestIDSource(next, logger, rand.Read)
}

func withRequestIDSource(next http.Handler, logger *slog.Logger, random func([]byte) (int, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		var idBytes [16]byte
		if n, err := random(idBytes[:]); err != nil || n != len(idBytes) {
			problem(w, 503, "request_id_unavailable")
			logger.Error("Не удалось создать идентификатор запроса", "request_id", "", "route", "unknown", "status", 503, "duration_ms", time.Since(start).Milliseconds())
			return
		}
		id := hex.EncodeToString(idBytes[:])
		w.Header().Set("X-Request-ID", id)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id))
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = 200
		}
		route := "unknown"
		switch r.URL.Path {
		case "/healthz", "/readyz", "/v1/events":
			route = r.URL.Path
		}
		logger.Info("HTTP-запрос", "request_id", id, "route", route, "status", sw.status, "duration_ms", time.Since(start).Milliseconds())
	})
}

// Ограничение общее для процесса и не хранит адреса клиентов.
type limiter struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	start  time.Time
	count  int
}

func (l *limiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		now := time.Now()
		if l.start.IsZero() || now.Sub(l.start) >= l.window {
			l.start = now
			l.count = 0
		}
		allowed := l.count < l.limit
		if allowed {
			l.count++
		}
		l.mu.Unlock()
		if !allowed {
			problem(w, 429, "rate_limited")
			return
		}
		next.ServeHTTP(w, r)
	})
}
