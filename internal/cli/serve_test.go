package cli

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeRejectsExternalAddressAndBadConfig(t *testing.T) {
	for _, args := range [][]string{{"--addr", "0.0.0.0:8080"}, {"--addr", "localhost:8080"}, {"--addr", "127.0.0.1:0"}} {
		var out, errOut bytes.Buffer
		if code := runServe(context.Background(), args, &out, &errOut); code != 2 {
			t.Fatalf("%v: код %d", args, code)
		}
	}
}

func TestServePortInUse(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var out, errOut bytes.Buffer
	code := runServe(context.Background(), []string{"--addr", l.Addr().String()}, &out, &errOut)
	if code != 4 || out.Len() != 0 {
		t.Fatalf("код=%d вывод=%q", code, out.String())
	}
}

func TestShutdownCancelsRequestAndClosesListener(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	canceled := make(chan struct{})
	done := make(chan int, 1)
	srv := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(canceled) })}
	go func() { done <- serveHTTP(ctx, srv, l, io.Discard, 5*time.Second) }()
	go func() {
		resp, err := http.Get("http://" + l.Addr().String())
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("запрос не начался")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("контекст не отменён")
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("код %d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("сервер не остановился")
	}
	c, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if err == nil {
		c.Close()
		t.Fatal("listener принимает соединения после остановки")
	}
}

func TestShutdownDeadline(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan int, 1)
	srv := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-release })}
	go func() { done <- serveHTTP(ctx, srv, l, io.Discard, 5*time.Second) }()
	go func() {
		resp, err := http.Get("http://" + l.Addr().String())
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("запрос не начался")
	}
	start := time.Now()
	cancel()
	select {
	case code := <-done:
		elapsed := time.Since(start)
		if code != 4 || elapsed < 4900*time.Millisecond || elapsed > 6500*time.Millisecond {
			t.Fatalf("код=%d время=%v", code, elapsed)
		}
		t.Logf("Время ожидания остановки: %v", elapsed)
	case <-time.After(7 * time.Second):
		t.Fatal("превышен срок остановки")
	}
	close(release)
	conn, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("listener принимает соединения")
	}
}
