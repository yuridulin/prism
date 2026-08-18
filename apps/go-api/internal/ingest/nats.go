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
		var one model.Point
		if err2 := json.Unmarshal(msg.Data, &one); err2 != nil {
			metrics.IngestErrors.WithLabelValues("go", c.store.Name()).Inc()
			log.Printf("nats decode error: %v", err)
			return
		}
		req.Points = []model.Point{one}
	}
	if len(req.Points) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.store.Write(ctx, req.Points); err != nil {
		metrics.IngestErrors.WithLabelValues("go", c.store.Name()).Inc()
		log.Printf("nats write error: %v", err)
		return
	}
	metrics.IngestPoints.WithLabelValues("go", c.store.Name()).Add(float64(len(req.Points)))
}

func (c *Consumer) Close() {
	if c != nil && c.nc != nil {
		c.nc.Drain()
	}
}
