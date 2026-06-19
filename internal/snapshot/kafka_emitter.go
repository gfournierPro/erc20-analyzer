package snapshot

import (
	"context"

	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
)

type KafkaEmitter struct {
	results *messaging.Publisher
	status  *messaging.Publisher
}

func NewKafkaEmitter(results, status *messaging.Publisher) *KafkaEmitter {
	return &KafkaEmitter{
		results: results,
		status:  status,
	}
}

func (k *KafkaEmitter) EmitBatch(ctx context.Context, b TransferBatch) error {
	return k.results.PublishJSON(ctx, b.Token, b)
}

func (k *KafkaEmitter) EmitStatus(ctx context.Context, s SnapshotStatus) error {
	return k.status.PublishJSON(ctx, s.Token, s)
}
