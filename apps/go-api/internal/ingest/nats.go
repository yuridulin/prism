package ingest

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"prism/go-api/internal/metrics"
	"prism/go-api/internal/model"
	"prism/go-api/internal/store"
)

type Consumer struct {
	nc      *nats.Conn
	store   store.Store
	subject string
}

func Subscribe(url, subject string, st store.Store) (*Consumer, error) {
	nc, err := nats.Connect(url, nats.Name("prism-go-api"), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	c := &Consumer{nc: nc, store: st, subject: subject}
	if _, err := nc.QueueSubscribe(subject, "prism-go", c.handle); err != nil {
		nc.Close()
		return nil, err
	}
	if err := nc.Flush(); err != nil {
		nc.Close()
		return nil, err
	}
	log.Printf("nats subscribed subject=%s queue=prism-go", subject)
	return c, nil
}

func (c *Consumer) handle(msg *nats.Msg) {
	var req model.WriteRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		var one model.WriteSample
		if err2 := json.Unmarshal(msg.Data, &one); err2 != nil {
			metrics.ObserveBackend(c.store.Name(), "write", "nats", 0, 0, err)
			log.Printf("nats decode error: %v", err)
			return
		}
		req.Samples = []model.WriteSample{one}
	}
	if len(req.Samples) == 0 {
		return
	}
	now := time.Now().UTC()
	samples := make([]model.Sample, 0, len(req.Samples))
	for _, raw := range req.Samples {
		samples = append(samples, raw.Normalize(now))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	err := c.store.Write(ctx, samples)
	metrics.ObserveBackend(c.store.Name(), "write", "nats", len(samples), time.Since(start), err)
	if err != nil {
		log.Printf("nats write error: %v", err)
	}
}

func (c *Consumer) Close() {
	if c != nil && c.nc != nil {
		c.nc.Drain()
	}
}
