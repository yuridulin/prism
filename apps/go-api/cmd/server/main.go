package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"prism/go-api/internal/api"
	"prism/go-api/internal/config"
	"prism/go-api/internal/ingest"
	"prism/go-api/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	st, err := store.New(cfg)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	consumer, err := ingest.Subscribe(cfg.NATSURL, cfg.NATSSubject, st)
	if err != nil {
		log.Printf("nats unavailable, HTTP-only mode: %v", err)
	} else {
		defer consumer.Close()
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("go-api listening on %s storage=%s", cfg.HTTPAddr, st.Name())
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
