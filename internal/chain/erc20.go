package chain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	selName        = mustHexDecode("0x06fdde03")
	selSymbol      = mustHexDecode("0x95d89b41")
	selDecimals    = mustHexDecode("0x313ce567")
	selTotalSupply = mustHexDecode("0x18160ddd")
)

type TokenMetadata struct {
	Address     common.Address
	Name        string
	Symbol      string
	Decimals    uint8
	TotalSupply *big.Int
}

func (c *Client) GetTokenMetadata(ctx context.Context, token common.Address) (*TokenMetadata, error) {
	md := &TokenMetadata{Address: token}

	name, err := c.callString(ctx, token, selName)
	if err != nil {
		return nil, fmt.Errorf("name: %w", err)
	}
	md.Name = name

	sym, err := c.callString(ctx, token, selSymbol)
	if err != nil {
		return nil, fmt.Errorf("symbol: %w", err)
	}
	md.Symbol = sym

	dec, err := c.callUint8(ctx, token, selDecimals)
	if err != nil {
		return nil, fmt.Errorf("decimals: %w", err)
	}
	md.Decimals = dec

	supply, err := c.callBigInt(ctx, token, selTotalSupply)
	if err != nil {
		return nil, fmt.Errorf("totalSupply: %w", err)
	}
	md.TotalSupply = supply

	return md, nil
}

func (c *Client) callRaw(ctx context.Context, to common.Address, data []byte) ([]byte, error) {
	var out []byte
	err := c.Do(ctx, "eth_call", func(ctx context.Context) error {
		var e error
		out, e = c.eth.CallContract(ctx, ethereum.CallMsg{To: &to, Data: data}, nil)
		return e
	})
	return out, err
}

func (c *Client) callBigInt(ctx context.Context, to common.Address, data []byte) (*big.Int, error) {
	b, err := c.callRaw(ctx, to, data)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	return new(big.Int).SetBytes(b), nil
}

func (c *Client) callUint8(ctx context.Context, to common.Address, data []byte) (uint8, error) {
	v, err := c.callBigInt(ctx, to, data)
	if err != nil {
		return 0, err
	}
	return uint8(v.Uint64()), nil
}

func (c *Client) callString(ctx context.Context, to common.Address, data []byte) (string, error) {
	b, err := c.callRaw(ctx, to, data)
	if err != nil {
		return "", err
	}
	return decodeString(b), nil
}

func decodeString(b []byte) string {
	if len(b) < 64 {
		// Some tokens return bytes32 directly
		return string(trimZero(b))
	}
	// ABI: offset(32) + length(32) + data
	length := new(big.Int).SetBytes(b[32:64]).Uint64()
	if 64+length > uint64(len(b)) {
		return ""
	}
	return string(b[64 : 64+length])
}

func trimZero(b []byte) []byte {
	for i, c := range b {
		if c == 0 {
			return b[:i]
		}
	}
	return b
}

func mustHexDecode(s string) []byte {
	b, err := hexutil.Decode(s)
	if err != nil {
		panic(err)
	}
	return b
}
