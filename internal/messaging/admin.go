package messaging

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

type TopicSpec struct {
	Name             string
	Partitions       int
	ReplicationFactr int
}

func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	if len(brokers) == 0 {
		return errors.New("no brokers")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(specs))
	for _, s := range specs {
		p := s.Partitions
		if p == 0 {
			p = 3
		}
		rf := s.ReplicationFactr
		if rf == 0 {
			rf = 1
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             s.Name,
			NumPartitions:     p,
			ReplicationFactor: rf,
		})
	}

	if err := ctrlConn.CreateTopics(configs...); err != nil {
		return err
	}

	for _, s := range specs {
		log.Info().Str("topic", s.Name).Int("partitions", s.Partitions).Msg("topice ready")
	}

	return nil
}
