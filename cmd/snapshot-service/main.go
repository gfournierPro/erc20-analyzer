package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"

	"github.com/gfournierPro/erc20-analyzer/internal/chain"
	"github.com/gfournierPro/erc20-analyzer/internal/config"
	"github.com/gfournierPro/erc20-analyzer/internal/logging"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
)

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		panic(err)
	}
	logging.Init(cfg.Env, "snapshot-service")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	bootstrapCtx, bsCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer bsCancel()
	if err := messaging.EnsureTopics(bootstrapCtx, cfg.Kafka.Brokers, []messaging.TopicSpec{
		{Name: cfg.Kafka.Topics.SnapshotJobs, Partitions: 6, ReplicationFactr: 1},
		{Name: cfg.Kafka.Topics.SnapshotResults, Partitions: 6, ReplicationFactr: 1},
	}); err != nil {
		log.Warn().Err(err).Msg("ensure topics (may already exist)")
	}

	reg := chain.NewRegistry()

	for _, ch := range cfg.Chains {
		cli, err := chain.NewClient(ctx, chain.ClientConfig{
			Name: ch.Name, ChainID: ch.ChainID, RPCURL: ch.RPCURL,
			IsArchive: ch.IsArchive, MaxLogRange: ch.MaxLogRange, RPSLimit: ch.RPSLimit,
		})
		if err != nil {
			log.Fatal().Err(err).Str("chain", ch.Name).Msg("client init failed")
		}
		reg.Add(cli)
		log.Info().Str("chain", ch.Name).Int64("chain_id", ch.ChainID).Msg("chain client ready")

	}
	defer reg.CloseAll()

	pub := messaging.NewPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topics.SnapshotJobs)
	defer pub.Close()

	if cli, ok := reg.Get("ethereum"); ok {
		usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
		md, err := cli.GetTokenMetadata(ctx, usdc)
		if err != nil {
			log.Error().Err(err).Msg("metadata fetch failed")
		} else {
			log.Info().
				Str("name", md.Name).
				Str("symbol", md.Symbol).
				Uint8("decimals", md.Decimals).
				Str("totalSupply", md.TotalSupply.String()).
				Msg("token metadata OK")

			job := map[string]any{
				"chain": "ethereum",
				"token": usdc.Hex(),
				"name":  md.Name,
			}
			if err := pub.PublishJSON(ctx, usdc.Hex(), job); err != nil {
				log.Error().Err(err).Msg("publish failed")
			} else {
				log.Info().Msg("test job published to Kafka")
			}
		}
	}
	<-ctx.Done()
	log.Info().Msg("shutting down")
}
