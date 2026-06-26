package analytics

import (
	"context"
	"math/big"
	"strings"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics/pb"
)

type Repo interface {
	SnapshotHolders(ctx context.Context, snapshotId string) ([]Holder, error)
	SnapshotMeta(ctx context.Context, snapshotId string) (chain, token string, block int64, err error)
}

type Server struct {
	pb.UnimplementedAnalyticsServiceServer
	repo Repo
}

func NewServer(repo Repo) *Server {
	return &Server{repo: repo}
}

var burnAddrs = map[string]bool{
	"0x0000000000000000000000000000000000000000": true,
	"0x000000000000000000000000000000000000dEaD": true,
}

func (s *Server) GetDistribution(ctx context.Context, req *pb.GetDistributionRequest) (*pb.GetDistributionResponse, error) {
	holders, err := s.repo.SnapshotHolders(ctx, req.SnapshotId)
	if err != nil {
		return nil, err
	}
	chain, token, block, err := s.repo.SnapshotMeta(ctx, req.SnapshotId)
	if err != nil {
		return nil, err
	}

	rawBalances := make([]*big.Int, 0, len(holders))
	filteredBalances := make([]*big.Int, 0, len(holders))
	for _, h := range holders {
		rawBalances = append(rawBalances, h.Balance)
		if h.AddressType == "contract" || h.AddressType == "delegated" {
			continue
		}
		filteredBalances = append(filteredBalances, h.Balance)
	}

	return &pb.GetDistributionResponse{
		SnapshotId:  req.SnapshotId,
		Chain:       chain,
		Token:       token,
		BlockNumber: block,
		Raw:         toPB(Compute(rawBalances)),
		Filtered:    toPB(Compute(filteredBalances)),
	}, nil
}

func toPB(m Metrics) *pb.Metrics {
	bs := make([]*pb.Bucket, len(m.Buckets))
	for i, b := range m.Buckets {
		bs[i] = &pb.Bucket{
			Label:        b.Label,
			MinShare:     formatShare(b.MinShare),
			HolderCount:  int64(b.HolderCount),
			TotalBalance: b.TotalBalance.String(),
		}
	}

	return &pb.Metrics{
		HolderCount: int64(m.HolderCount),
		Gini:        m.Gini,
		Nakamoto:    int64(m.Nakamoto),
		Hhi:         m.HHI,
		Buckets:     bs,
	}
}

func formatShare(f float64) string {
	return strings.TrimRight(strings.TrimRight(
		big.NewFloat(f).Text('f', 6), "0"), ".")
}
