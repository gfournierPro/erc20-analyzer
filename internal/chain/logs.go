package chain

import (
	"context"
	"errors"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

var TransferEventSig = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

type RawTransfer struct {
	Block    uint64
	TxHash   common.Hash
	LogIndex uint
	From     common.Address
	To       common.Address
	Value    *big.Int
}

func (c *Client) FetchTransferLogs(ctx context.Context, token common.Address, from, to uint64) ([]RawTransfer, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: []common.Address{token},
		Topics:    [][]common.Hash{{TransferEventSig}},
	}

	logs, err := c.eth.FilterLogs(ctx, query)
	if err != nil {
		return nil, classifyLogsError(err)
	}
	out := make([]RawTransfer, 0, len(logs))
	for _, lg := range logs {
		if lg.Removed {
			continue
		}
		t, ok := decodeTransfer(lg)
		if !ok {
			continue
		}
		out = append(out, t)
	}

	return out, nil
}

func decodeTransfer(lg types.Log) (RawTransfer, bool) {

	if len(lg.Topics) < 3 {
		return RawTransfer{}, false
	}
	value := new(big.Int).SetBytes(lg.Data)
	return RawTransfer{
		Block:    lg.BlockNumber,
		TxHash:   lg.TxHash,
		LogIndex: lg.Index,
		From:     common.BytesToAddress(lg.Topics[1].Bytes()),
		To:       common.BytesToAddress(lg.Topics[2].Bytes()),
		Value:    value,
	}, true
}

var ErrLogRangeTooLarge = errors.New("log range too large")

func classifyLogsError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "more than") && strings.Contains(msg, "results"),
		strings.Contains(msg, "query returned more than"),
		strings.Contains(msg, "response size"),
		strings.Contains(msg, "too large"),
		strings.Contains(msg, "limit exceeded"),
		strings.Contains(msg, "block range"):
		return errors.Join(ErrLogRangeTooLarge, err)
	}
	return err
}

func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	return c.eth.BlockNumber(ctx)
}
