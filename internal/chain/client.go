package chain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

type Client struct {
	Name        string
	ChainID     int64
	IsArchive   bool
	MaxLogRange uint64

	eth     *ethclient.Client
	rpc     *rpc.Client
	limiter *rate.Limiter
}

type ClientConfig struct {
	Name        string
	ChainID     int64
	RPCURL      string
	IsArchive   bool
	MaxLogRange uint64
	RPSLimit    int
}

func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	if cfg.RPCURL == "" {
		return nil, errors.New("rpc url is required")
	}
	if cfg.RPSLimit <= 0 {
		cfg.RPSLimit = 10
	}
	if cfg.MaxLogRange == 0 {
		cfg.MaxLogRange = 2000
	}

	rpcClient, err := rpc.DialContext(ctx, cfg.RPCURL)
	if err != nil {
		return nil, fmt.Errorf("dial rpc: %w", err)
	}

	ethCli := ethclient.NewClient(rpcClient)

	return &Client{
		Name:        cfg.Name,
		ChainID:     cfg.ChainID,
		IsArchive:   cfg.IsArchive,
		MaxLogRange: cfg.MaxLogRange,
		eth:         ethCli,
		rpc:         rpcClient,
		limiter:     rate.NewLimiter(rate.Limit(cfg.RPSLimit), cfg.RPSLimit),
	}, nil
}

// Eth returns the underlying ethclient (use sparingly; prefer wrapper methods).
func (c *Client) Eth() *ethclient.Client { return c.eth }

// RPC returns the underlying raw rpc client (for batch calls).
func (c *Client) RPC() *rpc.Client { return c.rpc }

// Wait blocks until the rate limiter allows another call.
func (c *Client) Wait(ctx context.Context) error {
	return c.limiter.Wait(ctx)
}

func (c *Client) Do(ctx context.Context, op string, fn func(context.Context) error) error {
	const maxAttempts = 5
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := c.Wait(ctx); err != nil {
			return fmt.Errorf("rate limit wait: %w", err)
		}

		err := fn(ctx)

		if err == nil {
			return nil
		}

		lastErr = fmt.Errorf("%s attempt %d: %w", op, attempt, err)
		if !isRetryable(err) {
			return err
		}

		backoff := time.Duration(attempt*attempt) * 200 * time.Millisecond
		log.Warn().
			Str("op", op).
			Int("attempt", attempt).
			Err(err).
			Dur("backoff", backoff).
			Msg("retrying rpc call")

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("op %s failed after %d attempts: %w", op, maxAttempts, lastErr)
}

func (c *Client) Close() {
	c.eth.Close()
}

type Registry struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewRegistry() *Registry {
	return &Registry{clients: make(map[string]*Client)}
}

func (r *Registry) Add(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c.Name] = c
}

func (r *Registry) Get(name string) (*Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c, ok := r.clients[name]
	return c, ok
}

func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.clients))
	for n := range r.clients {
		names = append(names, n)
	}
	return names
}

func (r *Registry) CloseAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.clients {
		c.Close()
	}
}
