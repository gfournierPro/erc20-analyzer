package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gfournierPro/erc20-analyzer/internal/chain"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/rs/zerolog/log"
)

type Runner struct {
	registry *chain.Registry
	scanner  *Scanner
}

func NewRunner(reg *chain.Registry, scanner *Scanner) *Runner {
	return &Runner{
		registry: reg,
		scanner:  scanner,
	}
}

func (r *Runner) Handle(ctx context.Context, msg messaging.Message) error {

	var job SnapshotJob
	if err := json.Unmarshal(msg.Value, &job); err != nil {
		log.Error().Err(err).Msg("invalid Snapshot payload; dropping")
		return nil
	}

	cli, ok := r.registry.Get(job.Chain)
	if !ok {
		return fmt.Errorf("unknow chain: %s", job.Chain)
	}

	log.Info().Str("job", job.JobID).Str("chain", job.Chain).Str("token", job.Token).
		Msg("processing snapshot job")

	return r.scanner.Run(ctx, cli, job)

}
