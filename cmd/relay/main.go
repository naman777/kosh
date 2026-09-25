// Command relay publishes outbox rows to Kafka.
//
//	KOSH_DATABASE_URL    Postgres URL (default: the compose database)
//	KOSH_KAFKA_BROKERS   comma-separated brokers (default localhost:19092)
//	KOSH_KAFKA_TOPIC     topic (default kosh.events)
//	KOSH_METRICS_ADDR    /metrics listen address (default :9101)
//	KOSH_CRASHPOINT      crash at a named point, for failure tests
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/db"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/metrics"
	"github.com/naman777/kosh/internal/outbox"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("relay exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, db.URLFromEnv())
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	brokers := env.List("KOSH_KAFKA_BROKERS", "localhost:19092")
	topic := env.String("KOSH_KAFKA_TOPIC", bus.DefaultTopic)
	if err := bus.EnsureTopic(ctx, brokers, topic, 3); err != nil {
		return err
	}
	pub, err := bus.NewKafkaPublisher(brokers, topic)
	if err != nil {
		return err
	}
	defer pub.Close()

	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, age, err := outbox.Lag(ctx, pool, time.Now())
				if err == nil {
					log.Info("outbox lag", "unpublished", n, "oldest_ms", age.Milliseconds())
				}
			}
		}
	}()

	metrics.DBGauge(pool, "kosh_outbox_unpublished", "Outbox rows not yet published.",
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`)
	metrics.DBGauge(pool, "kosh_outbox_oldest_seconds", "Age of the oldest unpublished outbox row.",
		`SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) FROM outbox WHERE published_at IS NULL`)
	metrics.Serve(env.String("KOSH_METRICS_ADDR", ":9101"), log)
	log.Info("relay running", "brokers", brokers, "topic", topic)
	r := &outbox.Relay{Pool: pool, Publisher: pub, Clock: clock.Real{}}
	return r.Run(ctx)
}
