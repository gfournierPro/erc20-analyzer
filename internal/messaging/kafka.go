package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type Message struct {
	Key   []byte
	Value []byte

	Headers map[string]string
}

type Publisher struct {
	writer *kafka.Writer
	topic  string
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchTimeout: 50 * time.Millisecond,
			Compression:  kafka.Snappy,
		},
	}
}

func (p *Publisher) PublishJSON(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	})
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

type Handler func(ctx context.Context, msg Message) error
type Consumer struct {
	reader  *kafka.Reader
	topic   string
	groupID string
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		topic:   topic,
		groupID: groupID,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
			MaxWait:        500 * time.Millisecond,
		}),
	}
}

func (c *Consumer) Run(ctx context.Context, handler Handler) error {

	log.Info().Str("topic", c.topic).Str("group", c.groupID).Msg("consumer started")
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Error().Err(err).Msg("fetch message failed")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		msg := Message{Key: m.Key, Value: m.Value}
		if len(m.Headers) > 0 {
			msg.Headers = make(map[string]string, len(m.Headers))
			for _, h := range m.Headers {
				msg.Headers[h.Key] = string(h.Value)
			}
		}

		if err := handler(ctx, msg); err != nil {
			log.Error().Err(err).Str("topic", m.Topic).Int("partition", m.Partition).Int64("offset", m.Offset).Msg("handler failed; will reprocess")
			time.Sleep(time.Second)
			continue
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Error().Err(err).Msg("commit failed")
		}
	}

}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
