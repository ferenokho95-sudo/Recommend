package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"recogo/internal/app"
	"recogo/internal/reco"
	"time"
)

type Producer struct{ Writer *kafka.Writer }

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{&kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, WriteTimeout: 10 * time.Second, AllowAutoTopicCreation: false}}
}
func (p *Producer) Publish(ctx context.Context, e reco.Event) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return p.Writer.WriteMessages(ctx, kafka.Message{Key: []byte(e.User), Value: raw})
}
func (p *Producer) Close() error { return p.Writer.Close() }

func Consume(ctx context.Context, brokers []string, topic, dlq string, store *app.Store) error {
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: "recogo-v1", MinBytes: 1, MaxBytes: 1e6, CommitInterval: 0, StartOffset: kafka.FirstOffset})
	defer reader.Close()
	dead := NewProducer(brokers, dlq)
	defer dead.Close()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		var event reco.Event
		validation := json.Unmarshal(message.Value, &event)
		if validation == nil {
			validation = event.Validate()
		}
		if validation == nil {
			_, err = store.Add(event)
			if err == app.ErrConflict {
				validation = err
			} else if err != nil {
				return fmt.Errorf("store before commit: %w", err)
			}
		}
		if validation != nil {
			raw, _ := json.Marshal(map[string]any{"error": validation.Error(), "payload": string(message.Value), "topic": message.Topic, "partition": message.Partition, "offset": message.Offset})
			if err = dead.Writer.WriteMessages(ctx, kafka.Message{Key: message.Key, Value: raw}); err != nil {
				return fmt.Errorf("DLQ before commit: %w", err)
			}
			slog.Warn("event_rejected_to_dlq", "error", validation)
		}
		// Never fetch a later message after an unsuccessful commit.
		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}
