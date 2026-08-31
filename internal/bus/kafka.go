package bus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultTopic carries every Kosh event.
const DefaultTopic = "kosh.events"

// EnsureTopic creates topic if it is missing.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	// kadm reports a per-topic failure both in resp.Err and as the returned
	// error, so "already exists" has to be tolerated in both places.
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, partitions, 1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	return nil
}

// KafkaPublisher produces with acks=all and the idempotent producer, so a
// retried produce never duplicates within a session.
type KafkaPublisher struct{ cl *kgo.Client }

// NewKafkaPublisher connects to brokers and produces to topic.
func NewKafkaPublisher(brokers []string, topic string) (*KafkaPublisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{cl: cl}, nil
}

// Publish implements Publisher. It waits for every record's ack.
func (p *KafkaPublisher) Publish(ctx context.Context, msgs []Message) error {
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = &kgo.Record{Key: m.Key, Value: m.Value, Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(strconv.FormatInt(m.EventID, 10))},
		}}
	}
	return p.cl.ProduceSync(ctx, recs...).FirstErr()
}

// Close flushes and disconnects.
func (p *KafkaPublisher) Close() { p.cl.Close() }

// KafkaConsumer reads a topic in a consumer group with manual commits: the
// dispatcher commits only after its database transaction commits.
type KafkaConsumer struct{ cl *kgo.Client }

// NewKafkaConsumer joins group on topic.
func NewKafkaConsumer(brokers []string, topic, group string) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// A crashed dispatcher holds its partitions until its session times
		// out. Kafka's 45 s default would blow the 10 s recovery target; 6 s
		// is Redpanda's minimum.
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{cl: cl}, nil
}

// Poll implements Consumer.
func (c *KafkaConsumer) Poll(ctx context.Context) ([]Record, error) {
	for {
		fetches := c.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("bus: fetch %s[%d]: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}
		var out []Record
		fetches.EachRecord(func(r *kgo.Record) {
			var id int64
			for _, h := range r.Headers {
				if h.Key == "event_id" {
					id, _ = strconv.ParseInt(string(h.Value), 10, 64)
				}
			}
			out = append(out, Record{Message: Message{Key: r.Key, Value: r.Value, EventID: id}, offset: r.Offset, raw: r})
		})
		if len(out) > 0 {
			return out, nil
		}
	}
}

// Commit implements Consumer.
func (c *KafkaConsumer) Commit(ctx context.Context, recs []Record) error {
	krs := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		if kr, ok := r.raw.(*kgo.Record); ok {
			krs = append(krs, kr)
		}
	}
	return c.cl.CommitRecords(ctx, krs...)
}

// Close leaves the group.
func (c *KafkaConsumer) Close() { c.cl.Close() }
