package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"example.org/auguard/internal/events"
	"example.org/auguard/internal/httpapi"
	"example.org/auguard/internal/pipeline"
)

type apiCore struct{ handler pipeline.EventHandler }

func (c *apiCore) Ingest(ctx context.Context, ev events.Event) (events.Decision, error) {
	return c.handler.Handle(ctx, ev)
}
func (c *apiCore) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.handler == nil {
		return errors.New("ядро не инициализировано")
	}
	return nil
}

func runServe(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", "127.0.0.1:8080", "адрес сервера")
	limit := fs.Int("rate-limit", 100, "число запросов за интервал")
	window := fs.Duration("rate-window", time.Second, "интервал ограничения")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *limit < 1 || *window <= 0 {
		fmt.Fprintln(errOut, "Ошибка параметров сервера")
		return 2
	}
	host, port, err := net.SplitHostPort(*addr)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || host != "127.0.0.1" || portErr != nil || portNumber < 1 || portNumber > 65535 {
		fmt.Fprintln(errOut, "Адрес должен иметь вид 127.0.0.1:порт")
		return 2
	}
	core := &apiCore{handler: pipeline.RuleHandler{}}
	api := &httpapi.Server{Ingestor: core, Readiness: core, Logger: slog.New(slog.NewJSONHandler(errOut, nil)), RateLimit: *limit, RateWindow: *window}
	srv := &http.Server{Addr: *addr, Handler: api.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return ctx }}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(errOut, "Не удалось открыть адрес сервера")
		return 4
	}
	fmt.Fprintf(out, "Сервер слушает %s\n", listener.Addr())
	return serveHTTP(ctx, srv, listener, errOut, 5*time.Second)
}

func serveHTTP(ctx context.Context, srv *http.Server, listener net.Listener, errOut io.Writer, timeout time.Duration) int {
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(errOut, "Ошибка HTTP-сервера")
			return 4
		}
		return 0
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			<-done
			fmt.Fprintln(errOut, "Истекло время остановки сервера")
			return 4
		}
		if err := <-done; err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(errOut, "Ошибка HTTP-сервера")
			return 4
		}
		fmt.Fprintln(errOut, "Сервер остановлен")
		return 0
	}
}
