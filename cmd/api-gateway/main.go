package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics/pb"
	"github.com/gfournierPro/erc20-analyzer/internal/config"
	"github.com/gfournierPro/erc20-analyzer/internal/gateway"
	"github.com/gfournierPro/erc20-analyzer/internal/logging"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/gfournierPro/erc20-analyzer/internal/storage"
)

func main() {
	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		panic(err)
	}

	logging.Init(cfg.Env, "api-gateway")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := storage.NewPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	defer pool.Close()
	repo := storage.NewRepo(pool)

	jobsPub := messaging.NewPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topics.SnapshotJobs)
	defer jobsPub.Close()

	analyticsAddr := fmt.Sprintf("localhost:%d", cfg.Services.GRPCPort)
	conn, err := grpc.NewClient(analyticsAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal().Err(err).Msg("analytics grpc dial failed")
	}
	defer conn.Close()
	analyticsClient := pb.NewAnalyticsServiceClient(conn)

	if cfg.Env != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery(), requestLogger())

	gw := gateway.New(jobsPub, repo, analyticsClient)
	gw.Routes(r)

	httpPort := cfg.Services.HTTPPort
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Info().Int("port", httpPort).Msg("api-gateway ready (http)")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error().Err(err).Msg("http server stopeped")
	}
}

func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("latency", time.Since(start)).
			Msg("http request")

	}
}
