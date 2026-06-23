package chain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

func (c *Client) HasCodeAt(ctx context.Context, addr common.Address, block uint64) (bool, error) {
	var hasCode bool
	err := c.Do(ctx, "eth_getCode", func(ctx context.Context) error {
		code, e := c.eth.CodeAt(ctx, addr, new(big.Int).SetUint64(block))
		if e != nil {
			return e
		}
		hasCode = len(code) > 0
		return nil
	})
	return hasCode, err
}

func (c *Client) FindDeployBlock(ctx context.Context, token common.Address) (uint64, error) {
	latest, err := c.LatestBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("latest block: %w", err)
	}

	hasCodeNow, err := c.HasCodeAt(ctx, token, latest)
	if err != nil {
		return 0, fmt.Errorf("getCode at head: %w", err)
	}
	if !hasCodeNow {
		return 0, fmt.Errorf("address %s has no code at block %d (EOA or self-destruced)", token.Hex(), latest)
	}

	// Edge case: code exists at block 0 (genesis-allocated contract, rare).
	hasCodeAtZero, err := c.HasCodeAt(ctx, token, 0)
	if err != nil {
		return 0, fmt.Errorf("getCode at genesis: %w", err)
	}
	if hasCodeAtZero {
		return 0, nil
	}

	lo, hi := uint64(0), latest
	for lo+1 < hi {
		mid := lo + (hi-lo)/2 // overflow-safe midpoint
		hasCode, err := c.HasCodeAt(ctx, token, mid)
		if err != nil {
			return 0, fmt.Errorf("getCode at %d: %w", mid, err)
		}
		if hasCode {
			hi = mid // deploy is at or before mid
		} else {
			lo = mid // deploy is strictly after mid
		}
	}
	return hi, nil
}
