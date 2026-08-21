package ingest

import (
	"bytes"
	"context"
	"errors"
	"log"
	"time"

	json "github.com/goccy/go-json"
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

func decodeWrite(data []byte) []model.WriteItem {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil
	}
	if data[0] == '[' {
		var items []model.WriteItem
		if json.Unmarshal(data, &items) == nil {
			return items
		}
	}
	var wrap model.SamplesWrap
	if json.Unmarshal(data, &wrap) == nil && len(wrap.Samples) > 0 {
		return wrap.Samples
	}
	var one model.WriteItem
	if json.Unmarshal(data, &one) == nil && one.ID != 0 {
		return []model.WriteItem{one}
	}
	return nil
}

func (c *Consumer) handle(msg *nats.Msg) {
	items := decodeWrite(msg.Data)
	if len(items) == 0 {
		metrics.ObserveBackend(c.store.Name(), "write", "nats", 0, 0, errors.New("empty payload"))
		log.Printf("nats decode error: empty payload")
		return
	}
	now := time.Now().UTC()
	samples := make([]model.Sample, 0, len(items))
	for _, raw := range items {
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
