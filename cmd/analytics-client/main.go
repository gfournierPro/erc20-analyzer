package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics/pb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: analytics-client <snapshot_id>")
		os.Exit(1)
	}
	snapshotID := os.Args[1]

	conn, err := grpc.NewClient("localhost:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewAnalyticsServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetDistribution(ctx, &pb.GetDistributionRequest{SnapshotId: snapshotID})
	if err != nil {
		panic(err)
	}

	fmt.Printf("token %s @ block %d\n\n", resp.Token, resp.BlockNumber)
	printMetrics("RAW (all holders)", resp.Raw)
	fmt.Println()
	printMetrics("FILTERED (excl. contracts/delegated)", resp.Filtered)
}

func printMetrics(title string, m *pb.Metrics) {
	fmt.Println(title)
	fmt.Printf("  holders:  %d\n", m.HolderCount)
	fmt.Printf("  gini:     %.4f\n", m.Gini)
	fmt.Printf("  nakamoto: %d\n", m.Nakamoto)
	fmt.Printf("  hhi:      %.4f\n", m.Hhi)
	for _, b := range m.Buckets {
		fmt.Printf("  %-8s (>=%s): %d holders\n", b.Label, b.MinShare, b.HolderCount)
	}
}
