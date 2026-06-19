package aggregator

import (
	"context"
	"encoding/json"

	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/gfournierPro/erc20-analyzer/internal/snapshot"
	"github.com/gfournierPro/erc20-analyzer/internal/storage"
	"github.com/rs/zerolog/log"
)

type Aggregator struct {
	repo *storage.Repo
}

func New(repo *storage.Repo) *Aggregator {
	return &Aggregator{
		repo: repo,
	}
}

func (a *Aggregator) HandleBatch(ctx context.Context, msg messaging.Message) error {
	var b snapshot.TransferBatch
	if err := json.Unmarshal(msg.Value, &b); err != nil {
		log.Error().Err(err).Msg("invalid TransferBatch; dropping (poison pill)")
		return nil
	}

	if err := a.repo.IngestBatch(ctx, b); err != nil {
		return err
	}

	log.Debug().
		Str("snapshot", b.JobID).
		Uint64("from", b.ChunkFrom).Uint64("to", b.ChunkTo).
		Int("transfers", len(b.Transfers)).
		Msg("batch ingested")

	return a.tryComplete(ctx, b.JobID)
}

func (a *Aggregator) HandleStatus(ctx context.Context, msg messaging.Message) error {
	var s snapshot.SnapshotStatus
	if err := json.Unmarshal(msg.Value, &s); err != nil {
		log.Error().Err(err).Msg("invalid SnapshotStatus; dropping")
		return nil
	}

	switch s.State {
	case "done":
		if err := a.repo.MarkDone(ctx, s.JobID); err != nil {
			return err
		}
		log.Info().Str("snapshot", s.JobID).Msg("done signal received; no more batches expected")
		return a.tryComplete(ctx, s.JobID)
	case "error":
		log.Warn().Str("snapshot", s.JobID).Str("msg", s.Message).Msg("snapshot reported error")
		return nil
	default:
		return nil
	}
}

func (a *Aggregator) tryComplete(ctx context.Context, snapshotID string) error {
	done, err := a.repo.TryComplete(ctx, snapshotID)
	if err != nil {
		return err
	}
	if done {
		log.Info().Str("snapshot", snapshotID).Msg("snapshot complete: balances recomputed, coverage advanced")
	}
	return nil
}
