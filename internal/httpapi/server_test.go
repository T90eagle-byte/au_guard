package httpapi

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/pipeline"
)

const valid = `{"id":"evt-001","kind":"auth","severity":2,"message":"login denied"}`

type testIngestor struct {
	calls atomic.Int32
	fn    func(context.Context, events.Event) (events.Decision, error)
}

func (i *testIngestor) Ingest(ctx context.Context, e events.Event) (events.Decision, error) {
	i.calls.Add(1)
	if i.fn != nil {
		return i.fn(ctx, e)
	}
	return (pipeline.RuleHandler{}).Handle(ctx, e)
}

type readyFunc func(context.Context) error

func (f readyFunc) Ping(ctx context.Context) error { return f(ctx) }
func request(h http.Handler, method, path, body, contentType string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func setup() (*Server, *testIngestor, *bytes.Buffer) {
	i := &testIngestor{}
	logs := &bytes.Buffer{}
	s := &Server{Ingestor: i, Readiness: readyFunc(func(context.Context) error { return nil }), Logger: slog.New(slog.NewJSONHandler(logs, nil))}
	return s, i, logs
}

func TestHTTPScenarios(t *testing.T) {
	s, i, _ := setup()
	h := s.Handler()
	tests := []struct {
		name, method, path, body, ctype string
		status                          int
		code                            string
	}{
		{"health", "GET", "/healthz", "", "", 200, `"status":"ok"`},
		{"ready", "GET", "/readyz", "", "", 200, `"status":"ready"`},
		{"observe", "POST", "/v1/events", valid, "application/json; charset=utf-8", 200, `"action":"observe"`},
		{"review", "POST", "/v1/events", strings.Replace(valid, `"severity":2`, `"severity":7`, 1), "application/json", 200, `"action":"review"`},
		{"unknown_field", "POST", "/v1/events", strings.Replace(valid, `}`, `,"role":"administrator"}`, 1), "application/json", 400, `"error":"bad_json"`},
		{"two_objects", "POST", "/v1/events", valid + valid, "application/json", 400, `"error":"one_value_required"`},
		{"invalid_severity", "POST", "/v1/events", strings.Replace(valid, `"severity":2`, `"severity":11`, 1), "application/json", 422, `"error":"invalid_event"`},
		{"oversize", "POST", "/v1/events", strings.Repeat("x", maxBody+1), "application/json", 413, `"error":"body_too_large"`},
		{"method", "GET", "/v1/events", "", "", 405, `"error":"method_not_allowed"`},
		{"unknown_route", "GET", "/missing", "", "", 404, `"error":"not_found"`},
		{"missing_type", "POST", "/v1/events", valid, "", 415, `"error":"unsupported_media_type"`},
		{"wrong_type", "POST", "/v1/events", valid, "text/plain", 415, `"error":"unsupported_media_type"`},
		{"bad_json", "POST", "/v1/events", `{"id":`, "application/json", 400, `"error":"bad_json"`},
		{"empty", "POST", "/v1/events", "", "application/json", 400, `"error":"bad_json"`},
		{"trailing", "POST", "/v1/events", valid + " junk", "application/json", 400, `"error":"one_value_required"`},
		{"null", "POST", "/v1/events", "null", "application/json", 400, `"error":"bad_json"`},
		{"array", "POST", "/v1/events", "[]", "application/json", 400, `"error":"bad_json"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := i.calls.Load()
			w := request(h, tt.method, tt.path, tt.body, tt.ctype)
			if w.Code != tt.status || !strings.Contains(w.Body.String(), tt.code) {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if !json.Valid(w.Body.Bytes()) || w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
				t.Fatal("неверный формат ответа")
			}
			if w.Header().Get("X-Request-ID") == "" {
				t.Fatal("нет request ID")
			}
			if tt.status >= 400 && i.calls.Load() != before {
				t.Fatal("обработчик вызван для ошибочного запроса")
			}
			t.Logf("HTTP %d %s", w.Code, strings.TrimSpace(w.Body.String()))
		})
	}
}

func TestReadinessFailureAndTimeout(t *testing.T) {
	s, _, _ := setup()
	for _, tc := range []struct {
		name string
		f    readyFunc
	}{{"unavailable", func(context.Context) error { return errors.New("secret") }}, {"timeout", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}} {
		t.Run(tc.name, func(t *testing.T) {
			s.Readiness = tc.f
			start := time.Now()
			w := request(s.Handler(), "GET", "/readyz", "", "")
			if w.Code != 503 || strings.TrimSpace(w.Body.String()) != `{"status":"not_ready"}` || time.Since(start) > 2*time.Second {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			t.Logf("HTTP %d %s", w.Code, strings.TrimSpace(w.Body.String()))
		})
	}
}

func TestRequestIDAndSensitiveLogs(t *testing.T) {
	s, _, logs := setup()
	h := s.Handler()
	ids := map[string]bool{}
	for _, path := range []string{"/healthz", "/unknown-secret", "/v1/events"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("X-Request-ID", "client-secret")
		r.Header.Set("Authorization", "Bearer token-secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		id := w.Header().Get("X-Request-ID")
		b, err := hex.DecodeString(id)
		if err != nil || len(b) != 16 || ids[id] || id == "client-secret" {
			t.Fatalf("неверный ID %q", id)
		}
		ids[id] = true
	}
	r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(strings.Replace(valid, "login denied", "message-secret", 1)))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer token-secret")
	h.ServeHTTP(httptest.NewRecorder(), r)
	for _, secret := range []string{"client-secret", "token-secret", "message-secret", "unknown-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("журнал содержит %s", secret)
		}
	}
	for _, field := range []string{`"request_id"`, `"route"`, `"status"`, `"duration_ms"`} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("нет поля %s", field)
		}
	}
}

func TestContextAndProcessingError(t *testing.T) {
	s, i, _ := setup()
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "marker")
	i.fn = func(ctx context.Context, e events.Event) (events.Decision, error) {
		if ctx.Value(key{}) != "marker" || ctx.Value(requestIDKey{}) == nil {
			t.Error("контекст не передан")
		}
		return events.Decision{}, fmt.Errorf("внутренний секрет: %w", context.Canceled)
	}
	r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(valid)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 500 || strings.Contains(w.Body.String(), "секрет") || !strings.Contains(w.Body.String(), "processing_failed") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestCanceledRequestCannotSucceed(t *testing.T) {
	s, i, _ := setup()
	i.fn = func(context.Context, events.Event) (events.Decision, error) {
		return events.Decision{Action: "observe"}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest("POST", "/v1/events", strings.NewReader(valid)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 500 || !strings.Contains(w.Body.String(), "processing_failed") {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestRequestIDGenerationFailure(t *testing.T) {
	var calls int
	h := withRequestIDSource(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }), slog.New(slog.NewTextHandler(ioDiscard{}, nil)), func([]byte) (int, error) {
		return 0, errors.New("недоступен источник случайности")
	})
	w := request(h, "GET", "/healthz", "", "")
	if w.Code != 503 || calls != 0 || !strings.Contains(w.Body.String(), "request_id_unavailable") {
		t.Fatalf("status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
}

func TestDecodeEventLimits(t *testing.T) {
	if _, err := decodeEvent([]byte(valid), len(valid)); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeEvent([]byte(valid), len(valid)-1); !errors.Is(err, errTooLarge) {
		t.Fatalf("ошибка размера: %v", err)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
