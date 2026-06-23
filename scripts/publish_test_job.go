//go:build ignore

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gfournierPro/erc20-analyzer/internal/messaging"
	"github.com/gfournierPro/erc20-analyzer/internal/snapshot"
	"github.com/google/uuid"
)

func main() {
	pub := messaging.NewPublisher([]string{"localhost:9092"}, "snapshot.jobs")
	defer pub.Close()

	job := snapshot.SnapshotJob{
		JobID:       uuid.NewString(),
		Chain:       "ethereum",
		Token:       "0x4647e1fE715c9e23959022C2416C71867F5a6E80",
		FromBlock:   0,
		ToBlock:     0,
		RequestedAt: time.Now(),
	}

	if err := pub.PublishJSON(context.Background(), job.Token, job); err != nil {
		panic(err)
	}
	fmt.Println("job published:", job.JobID)
}
