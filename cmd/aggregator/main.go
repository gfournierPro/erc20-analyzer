package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/gfournierPro/erc20-analyzer/internal/aggregator"
	"github.com/gfournierPro/erc20-analyzer/internal/config"
	"github.com/gfournierPro/erc20-analyzer/internal/logging"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/gfournierPro/erc20-analyzer/internal/storage"
)

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		panic(err)
	}

	logging.Init(cfg.Env, "aggregator")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := storage.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer pool.Close()

	repo := storage.NewRepo(pool)

	classifyReqPub := messaging.NewPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topics.ClassifyRequests)
	defer classifyReqPub.Close()

	agg := aggregator.New(repo, classifyReqPub)

	resutlsConsumer := messaging.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.SnapshotResults, "aggregator")
	statusConsumer := messaging.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.SnapshotStatus, "aggregator")
	defer resutlsConsumer.Close()
	defer statusConsumer.Close()

	classifyResultConsumer := messaging.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topics.ClassifyResults, "aggregator-classify")
	defer classifyResultConsumer.Close()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info().Str("topic", cfg.Kafka.Topics.SnapshotResults).Msg("consuming results")
		return resutlsConsumer.Run(gctx, agg.HandleBatch)
	})
	g.Go(func() error {
		log.Info().Str("topic", cfg.Kafka.Topics.SnapshotStatus).Msg("consuming status")
		return statusConsumer.Run(gctx, agg.HandleStatus)
	})

	g.Go(func() error {
		log.Info().Str("topic", cfg.Kafka.Topics.ClassifyResults).Msg("consuming classify results")
		return classifyResultConsumer.Run(gctx, agg.HandleClassifyResult)
	})

	log.Info().Msg("aggregator ready")
	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("consumer group stopped")
	}
	_ = os.Stderr.Sync()

}
