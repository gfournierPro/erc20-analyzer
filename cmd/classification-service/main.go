package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/gfournierPro/erc20-analyzer/internal/chain"
	"github.com/gfournierPro/erc20-analyzer/internal/classify"
	"github.com/gfournierPro/erc20-analyzer/internal/config"
	"github.com/gfournierPro/erc20-analyzer/internal/logging"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		panic(err)
	}
	logging.Init(cfg.Env, "classification-service")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal().Err(err).Msg("redis connect failed")
	}
	defer rdb.Close()

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
	}
	defer reg.CloseAll()

	resultsPub := messaging.NewPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topics.ClassifyResults)
	defer resultsPub.Close()

	classifier := classify.NewClassifier(reg, rdb)
	runner := classify.NewRunner(classifier, resultsPub)

	consumer := messaging.NewConsumer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topics.ClassifyRequests,
		"classification-service",
	)
	defer consumer.Close()

	log.Info().Msg("classification-service ready; consuming requests")
	if err := consumer.Run(ctx, runner.Handle); err != nil {
		log.Error().Err(err).Msg("consumer stopped")
	}

	<-ctx.Done()
	log.Info().Msg("shutting down")

}
