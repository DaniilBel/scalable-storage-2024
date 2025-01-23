package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"practice3/storage"
	"syscall"
	"time"
)

func main() {
	r := http.ServeMux{}

	storage1 := storage.NewStorage(&r, "node1", []string{"node2", "node3"}, true)
	storage2 := storage.NewStorage(&r, "node2", []string{"node1", "node3"}, false)
	storage3 := storage.NewStorage(&r, "node3", []string{"node1", "node2"}, false)

	nodes := [][]string{
		{"node1", "node2", "node3"},
		{"node2", "node1", "node3"},
		{"node3", "node1", "node2"},
	}

	router := NewRouter(&r, nodes)

	go storage1.Run()
	go storage2.Run()
	go storage3.Run()

	go router.Run()

	defer storage1.Stop()
	defer storage2.Stop()
	defer storage3.Stop()

	l := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: &r,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		for range sigs {
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
}
