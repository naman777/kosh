// Command dispatcher turns Kafka events into signed webhook deliveries.
//
//	KOSH_DATABASE_URL           Postgres URL (default: the compose database)
//	KOSH_KAFKA_BROKERS          comma-separated brokers (default localhost:19092)
//	KOSH_KAFKA_TOPIC            topic (default kosh.events)
//	KOSH_DISPATCH_GROUP         consumer group (default kosh-dispatcher)
//	KOSH_WEBHOOK_SEND_TIMEOUT   per-attempt timeout (default 10s)
//	KOSH_METRICS_ADDR           /metrics listen address (default :9102)
//	KOSH_CRASHPOINT             crash at a named point, for failure tests
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

	"github.com/naman777/kosh/internal/bus"
	"github.com/naman777/kosh/internal/clock"
	"github.com/naman777/kosh/internal/db"
	"github.com/naman777/kosh/internal/env"
	"github.com/naman777/kosh/internal/metrics"
	"github.com/naman777/kosh/internal/rng"
	"github.com/naman777/kosh/internal/webhooks"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("dispatcher exiting", "err", err)
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
	consumer, err := bus.NewKafkaConsumer(brokers, topic, env.String("KOSH_DISPATCH_GROUP", "kosh-dispatcher"))
	if err != nil {
		return err
	}
	defer consumer.Close()

	timeout := env.Duration("KOSH_WEBHOOK_SEND_TIMEOUT", 10*time.Second)
	d := &webhooks.Dispatcher{
		Pool:        pool,
		Clock:       clock.Real{},
		RNG:         rng.New(uint64(time.Now().UnixNano())),
		Sender:      webhooks.HTTPSender{Client: &http.Client{Timeout: timeout}},
		SendTimeout: timeout,
	}

	metrics.DBGauge(pool, "kosh_webhook_backlog", "Webhook deliveries not yet delivered to enabled merchants.",
		`SELECT count(*) FROM webhook_deliveries d JOIN merchants m ON m.id = d.merchant_id
		 WHERE d.delivered_at IS NULL AND m.webhook_disabled_at IS NULL`)
	metrics.Serve(env.String("KOSH_METRICS_ADDR", ":9102"), log)
	errc := make(chan error, 2)
	go func() { errc <- d.RunIngest(ctx, consumer) }()
	go func() { errc <- d.RunSender(ctx, 100*time.Millisecond) }()
	log.Info("dispatcher running", "brokers", brokers, "topic", topic)
	err = <-errc
	stop()
	return err
}
