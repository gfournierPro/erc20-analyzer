package main

import (
	"context"
	"fmt"
	"net"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics"
	"github.com/gfournierPro/erc20-analyzer/internal/analytics/pb"
	"github.com/gfournierPro/erc20-analyzer/internal/config"
	"github.com/gfournierPro/erc20-analyzer/internal/logging"
	"github.com/gfournierPro/erc20-analyzer/internal/storage"
)

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		panic(err)
	}
	logging.Init(cfg.Env, "analytics-service")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := storage.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer pool.Close()
	repo := storage.NewRepo(pool)

	grpcPort := cfg.Services.GRPCPort
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.Fatal().Err(err).Int("port", grpcPort).Msg("listen failed")
	}

	srv := grpc.NewServer()
	pb.RegisterAnalyticsServiceServer(srv, analytics.NewServer(repo))

	go func() {
		<-ctx.Done()
		log.Info().Msg("shutting down grpc server")
		srv.GracefulStop()
	}()

	log.Info().Int("port", grpcPort).Msg("analytics-service ready (grpc)")
	if err := srv.Serve(lis); err != nil {
		log.Error().Err(err).Msg("grpc servce stopped")
	}

}
