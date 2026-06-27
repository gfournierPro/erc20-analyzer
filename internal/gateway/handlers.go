package gateway

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics/pb"
	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/gfournierPro/erc20-analyzer/internal/snapshot"
	"github.com/gfournierPro/erc20-analyzer/internal/storage"
)

type Gateway struct {
	jobs      *messaging.Publisher
	repo      *storage.Repo
	analytics pb.AnalyticsServiceClient
}

func New(jobs *messaging.Publisher, repo *storage.Repo, analytics pb.AnalyticsServiceClient) *Gateway {
	return &Gateway{jobs: jobs, repo: repo, analytics: analytics}
}

func (g *Gateway) Routes(r *gin.Engine) {
	r.POST("/snapshots", g.createSnapshot)
	r.GET("/snapshots/:id", g.getSnapshot)
	r.GET("/snapshots/:id/distribution", g.getDistribution)
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
}

type createSnapshotRequest struct {
	Chain     string `json:"chain" binding:"required"`
	Token     string `json:"token" binding:"required"`
	FromBlock uint64 `json:"from_block"`
	ToBlock   uint64 `json:"to_block"`
}

func (g *Gateway) createSnapshot(c *gin.Context) {
	var req createSnapshotRequest
	if err := c.ShouldBindBodyWithJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "chain and token are required"})
		return
	}

	jobID := uuid.NewString()
	job := snapshot.SnapshotJob{
		JobID:       jobID,
		Chain:       req.Chain,
		Token:       req.Token,
		FromBlock:   req.FromBlock,
		ToBlock:     req.ToBlock,
		RequestedAt: time.Now(),
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	if err := g.jobs.PublishJSON(ctx, req.Token, job); err != nil {
		log.Error().Err(err).Msg("publish job failed")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not enqueu snapshot"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"snapshot_id": jobID,
		"status":      "scanning",
		"poll":        "/snapshots/" + jobID,
	})
}

func (g *Gateway) getSnapshot(c *gin.Context) {
	id := c.Param("id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	status, err := g.repo.SnapshotStatus(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "snapshot not found"})
			return
		}
		log.Error().Err(err).Msg("get snapshot status failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	c.JSON(http.StatusOK, status)
}

func (g *Gateway) getDistribution(c *gin.Context) {
	id := c.Param("id")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	status, err := g.repo.SnapshotStatus(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "snapshot not found"})
			return
		}
		log.Error().Err(err).Msg("get snapshot status failed")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	if status.State != "ready" {
		c.JSON(http.StatusConflict, gin.H{
			"error": "snapshot not ready",
			"state": status.State,
		})
		return
	}

	resp, err := g.analytics.GetDistribution(ctx, &pb.GetDistributionRequest{SnapshotId: id})
	if err != nil {
		log.Error().Err(err).Msg("anaytics call failed")
		c.JSON(http.StatusBadGateway, gin.H{"error": "analytics unavailabe"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"snapshot_id":  resp.SnapshotId,
		"chain":        resp.Chain,
		"token":        resp.Token,
		"block_number": resp.BlockNumber,
		"raw":          metricsToJSON(resp.Raw),
		"filtered":     metricsToJSON(resp.Filtered),
	})
}

func metricsToJSON(m *pb.Metrics) gin.H {
	buckets := make([]gin.H, len(m.Buckets))
	for i, b := range m.Buckets {
		buckets[i] = gin.H{
			"label":         b.Label,
			"min_share":     b.MinShare,
			"holder_count":  b.HolderCount,
			"total_balance": b.TotalBalance,
		}
	}

	return gin.H{
		"holder_count": m.HolderCount,
		"gini":         m.Gini,
		"nakamoto":     m.Nakamoto,
		"hhi":          m.Hhi,
		"buckets":      buckets,
	}
}
