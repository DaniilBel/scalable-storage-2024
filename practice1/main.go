package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	r := http.ServeMux{}

	storage := NewStorage(&r, "storage", []string{}, true)
	router := NewRouter(&r, [][]string{{"storage"}})

	go storage.Run()
	go router.Run()

	l := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: &r,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		for _ = range sigs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l.Shutdown(ctx)
		}
	}()

	defer slog.Info("we are going down")
	slog.Info("listen http://" + l.Addr)
	err := l.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}

	// Handle shutdown signals
	//sigChan := make(chan os.Signal, 1)
	//signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	//sig := <-sigChan
	//slog.Info("Received signal", "signal", sig)

	// Graceful shutdown
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	//if err := server.Shutdown(ctx); err != nil {
	//	slog.Error("Server shutdown error", "err", err)
	//}

	//router.Stop()
	//storage.Stop()
	//slog.Info("Application stopped gracefully")
}
