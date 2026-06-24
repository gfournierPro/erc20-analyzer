package classify

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gfournierPro/erc20-analyzer/internal/chain"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const cacheTTL = 30 * 24 * time.Hour

type Classifier struct {
	registry *chain.Registry
	rdb      *redis.Client
}

func NewClassifier(reg *chain.Registry, rdb *redis.Client) *Classifier {
	return &Classifier{registry: reg, rdb: rdb}
}

func cacheKey(chainName, addr string) string {
	return fmt.Sprintf("classify:%s:%s", chainName, addr)
}

func (c *Classifier) Classify(ctx context.Context, chainName, addr string) (string, error) {
	key := cacheKey(chainName, addr)

	cached, err := c.rdb.Get(ctx, key).Result()
	if err == nil {
		return cached, nil
	}
	if !errors.Is(err, redis.Nil) {
		log.Warn().Err(err).Str("key", key).Msg("redis get failed; falling through to rpc")
	}

	cli, ok := c.registry.Get(chainName)
	if !ok {
		return "", fmt.Errorf("unknown chain: %s", chainName)
	}
	head, err := cli.LatestBlock(ctx)
	if err != nil {
		return "", fmt.Errorf("latest block: %w", err)
	}
	hasCode, err := cli.HasCodeAt(ctx, common.HexToAddress(addr), head)
	if err != nil {
		return "", fmt.Errorf("getCode: %w", err)
	}

	addrType := TypeEOA
	if hasCode {
		addrType = TypeContract
	}

	if err := c.rdb.Set(ctx, key, addrType, cacheTTL).Err(); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("redis set failed")
	}

	return addrType, nil
}
