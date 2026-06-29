package snapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/gfournierPro/erc20-analyzer/internal/chain"
)

type Emitter interface {
	EmitBatch(ctx context.Context, b TransferBatch) error
	EmitStatus(ctx context.Context, s SnapshotStatus) error
}

type ScannerConfig struct {
	Workers        int
	MaxChunkSize   uint64
	MinChunkSize   uint64
	ChunkRetries   int
	ProgressEveryN int
}

func DefaultScannerConfig() ScannerConfig {
	return ScannerConfig{
		Workers:        8,
		MaxChunkSize:   2000,
		MinChunkSize:   50,
		ChunkRetries:   3,
		ProgressEveryN: 10,
	}
}

type Scanner struct {
	cfg     ScannerConfig
	emitter Emitter
}

func NewScanner(cfg ScannerConfig, emitter Emitter) *Scanner {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.MaxChunkSize == 0 {
		cfg.MaxChunkSize = 2000
	}
	if cfg.MinChunkSize == 0 {
		cfg.MinChunkSize = 50
	}
	return &Scanner{cfg: cfg, emitter: emitter}
}

func (s *Scanner) Run(ctx context.Context, cli *chain.Client, job SnapshotJob) error {
	token := common.HexToAddress(job.Token)

	toBlock := job.ToBlock
	if toBlock == 0 {
		head, err := cli.LatestBlock(ctx)
		if err != nil {
			return fmt.Errorf("get latest block: %w", err)
		}
		toBlock = head
	}

	fromBlock := job.FromBlock
	if fromBlock == 0 {
		deploy, err := cli.FindDeployBlock(ctx, token)
		if err != nil {
			log.Warn().Err(err).Str("token", job.Token).Msg("deploy-block detection failed; scanning from block 0")
			fromBlock = 0
		} else {
			fromBlock = deploy
			log.Info().Str("token", job.Token).Uint64("deploy_block", deploy).Msg("deploy block detected")
		}
	}
	if fromBlock > toBlock {
		return fmt.Errorf("from_block (%d) > to_block (%d)", fromBlock, toBlock)
	}

	chunkSize := s.cfg.MaxChunkSize
	if cli.MaxLogRange > 0 && cli.MaxLogRange < chunkSize {
		chunkSize = cli.MaxLogRange
	}

	totalBlocks := toBlock - fromBlock + 1

	log.Info().
		Str("job", job.JobID).
		Str("token", job.Token).
		Uint64("from", fromBlock).Uint64("to", toBlock).
		Uint64("chunk_size", chunkSize).
		Int("workers", s.cfg.Workers).
		Msg("snpashot started")

	_ = s.emitter.EmitStatus(ctx, SnapshotStatus{
		JobID: job.JobID, Token: job.Token, Chain: job.Chain,
		State: "started", BlocksTotal: totalBlocks, UpdatedAt: time.Now(),
	})

	chunks := buildChunks(fromBlock, toBlock, chunkSize)
	totalChunks := len(chunks)

	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, s.cfg.Workers)

	var (
		mu             sync.Mutex
		chunksDone     int
		blocksDone     uint64
		lastProgressAt = time.Now()
	)

	for i, ch := range chunks {
		i, ch := i, ch
		sem <- struct{}{}

		g.Go(func() error {
			defer func() { <-sem }()
			transfers, err := s.fetchChunkAdaptive(gctx, cli, token, ch.From, ch.To)
			if err != nil {
				return fmt.Errorf("chunk [%d..%d]", ch.From, ch.To, err)
			}

			const maxTransferPerBatch = 500

			wire := toWireTransfers(transfers)

			if len(wire) == 0 {
				if err := s.emitter.EmitBatch(gctx, TransferBatch{
					JobID:        job.JobID,
					Chain:        job.Chain,
					Token:        job.Token,
					ChunkFrom:    ch.From,
					ChunkTo:      ch.To,
					ChunksTotal:  uint64(totalChunks),
					Transfers:    nil,
					IsFinalChunk: i == totalChunks-1,
				}); err != nil {
					return fmt.Errorf("emit batch: %w", err)
				}
			} else {
				for start := 0; start < len(wire); start += maxTransferPerBatch {
					end := start + maxTransferPerBatch
					if end > len(wire) {
						end = len(wire)
					}
					if err := s.emitter.EmitBatch(gctx, TransferBatch{
						JobID:        job.JobID,
						Chain:        job.Chain,
						Token:        job.Token,
						ChunkFrom:    ch.From,
						ChunkTo:      ch.To,
						ChunksTotal:  uint64(totalChunks),
						Transfers:    wire[start:end],
						IsFinalChunk: i == totalChunks-1,
					}); err != nil {
						return fmt.Errorf("emit batch: %w", err)
					}
				}
			}

			mu.Lock()
			chunksDone++
			blocksDone += ch.To - ch.From + 1
			emitProgress := chunksDone%s.cfg.ProgressEveryN == 0 || time.Since(lastProgressAt) > 5*time.Second
			cd, bd := chunksDone, blocksDone
			if emitProgress {
				lastProgressAt = time.Now()
			}
			mu.Unlock()

			if emitProgress {
				log.Info().
					Str("job", job.JobID).
					Int("chunks_done", cd).Int("chunks_total", totalChunks).
					Uint64("blocks_done", bd).Uint64("blocks_total", totalBlocks).
					Msg("snapshot progress")

				_ = s.emitter.EmitStatus(gctx, SnapshotStatus{
					JobID: job.JobID, Token: job.Token, Chain: job.Chain,
					State: "progress", BlocksDone: bd, BlocksTotal: totalBlocks,
					UpdatedAt: time.Now(),
				})

			}
			return nil
		})

	}

	if err := g.Wait(); err != nil {
		_ = s.emitter.EmitStatus(ctx, SnapshotStatus{
			JobID: job.JobID, Token: job.Token, Chain: job.Chain,
			State: "error", Message: err.Error(), UpdatedAt: time.Now(),
		})
		return err
	}

	if err := s.emitter.EmitStatus(ctx, SnapshotStatus{
		JobID: job.JobID, Token: job.Token, Chain: job.Chain,
		State: "done", BlocksDone: totalBlocks, BlocksTotal: totalBlocks,
		UpdatedAt: time.Now(),
	}); err != nil {
		log.Error().Err(err).Str("job", job.JobID).Msg("failed to emit done status")
	}
	log.Info().Str("job", job.JobID).Msg("snapshot done")

	return nil
}

type blockRange struct{ From, To uint64 }

func buildChunks(from, to, size uint64) []blockRange {
	var out []blockRange
	for start := from; start <= to; start += size {
		end := start + size - 1
		if end > to {
			end = to
		}
		out = append(out, blockRange{From: start, To: end})
		if end == to {
			break
		}
	}
	return out
}

func (s *Scanner) fetchChunkAdaptive(
	ctx context.Context,
	cli *chain.Client,
	token common.Address,
	from, to uint64,
) ([]chain.RawTransfer, error) {
	var all []chain.RawTransfer
	stack := []blockRange{{from, to}}

	for len(stack) > 0 {
		r := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		var (
			res []chain.RawTransfer
			err error
		)

		for attempt := 0; attempt <= s.cfg.ChunkRetries; attempt++ {
			res, err = cli.FetchTransferLogs(ctx, token, r.From, r.To)
			if err == nil {
				break
			}

			if errors.Is(err, chain.ErrLogRangeTooLarge) {
				break
			}

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(200*attempt+1) * time.Millisecond):
			}

		}
		if err != nil {
			if errors.Is(err, chain.ErrLogRangeTooLarge) {
				span := r.To - r.From + 1
				if span <= s.cfg.MinChunkSize {
					return nil, fmt.Errorf("range too large even at min size [%d..%d]: %w", r.From, r.To, err)
				}
				mid := r.From + span/2

				stack = append(stack, blockRange{From: mid, To: r.To})
				stack = append(stack, blockRange{From: r.From, To: mid - 1})
				continue
			}
			return nil, err
		}
		all = append(all, res...)
	}
	return all, nil
}

func toWireTransfers(in []chain.RawTransfer) []Transfer {
	out := make([]Transfer, len(in))
	for i, t := range in {
		out[i] = Transfer{
			Block:    t.Block,
			TxHash:   t.TxHash.Hex(),
			LogIndex: t.LogIndex,
			From:     t.From.Hex(),
			To:       t.To.Hex(),
			Value:    t.Value.String(),
		}
	}
	return out
}
