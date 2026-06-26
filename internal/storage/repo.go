package storage

import (
	"context"
	"fmt"
	"math/big"

	"github.com/gfournierPro/erc20-analyzer/internal/analytics"
	"github.com/gfournierPro/erc20-analyzer/internal/snapshot"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const zeroAddr = "0x0000000000000000000000000000000000000000"

type Repo struct {
	pool *pgxpool.Pool
}

func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

func (r *Repo) IngestBatch(ctx context.Context, b snapshot.TransferBatch) error {

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO snapshots (id, chain, token, chunks_total, state)
		VALUES ($1, $2, $3, $4, 'scanning')
		ON CONFLICT (id) DO UPDATE
		SET chunks_total = GREATEST(snapshots.chunks_total, EXCLUDED.chunks_total)
	`, b.JobID, b.Chain, b.Token, int64(b.ChunksTotal))
	if err != nil {
		return fmt.Errorf("upsert snapshot: %w", err)
	}

	if len(b.Transfers) > 0 {
		n := len(b.Transfers)
		txHashes := make([]string, n)
		logIdx := make([]int32, n)
		blocks := make([]int64, n)
		froms := make([]string, n)
		tos := make([]string, n)
		values := make([]string, n)
		for i, t := range b.Transfers {
			txHashes[i] = t.TxHash
			logIdx[i] = int32(t.LogIndex)
			blocks[i] = int64(t.Block)
			froms[i] = t.From
			tos[i] = t.To
			values[i] = t.Value
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO transfers 
				(chain, token, tx_hash, log_index, block_number, from_addr, to_addr, value)
			SELECT $1, $2, u.tx_hash, u.log_index, u.block_number, u.from_addr, u.to_addr, u.value::numeric
			FROM unnest($3::text[], $4::int[], $5::bigint[], $6::text[], $7::text[], $8::text[])
				AS u(tx_hash, log_index, block_number, from_addr, to_addr, value)
			ON CONFLICT (chain, token, tx_hash, log_index) DO NOTHING
		`, b.Chain, b.Token, txHashes, logIdx, blocks, froms, tos, values)
		if err != nil {
			return fmt.Errorf("insert transfers: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO snapshot_chunks (snapshot_id, chunk_from, chunk_to)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING
	`, b.JobID, int64(b.ChunkFrom), int64(b.ChunkTo))
	if err != nil {
		return fmt.Errorf("insert chunk: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repo) MarkDone(ctx context.Context, snapshotID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE snapshots SET done_seen = TRUE WHERE id = $1
	`, snapshotID)
	return err
}

func (r *Repo) TryComplete(ctx context.Context, snapshotID string) (bool, string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx)

	var fromBlock, toBlock int64
	var chain, token string
	err = tx.QueryRow(ctx, `
		WITH agg AS (
			SELECT count(*) AS done, min(chunk_from) as cfrom, max(chunk_to) AS cto
			FROM snapshot_chunks WHERE snapshot_id = $1
		)
		UPDATE snapshots s 
		SET state = 'ready',
			from_block = agg.cfrom,
			block_number = agg.cto,
			completed_at = now()
		FROM agg 
		WHERE s.id = $1
		AND s.state = 'scanning'
		AND s.done_seen = TRUE 
		AND s.chunks_total > 0
		AND agg.done = s.chunks_total
		RETURNING s.from_block, s.block_number, s.chain, s.token
	`, snapshotID).Scan(&fromBlock, &toBlock, &chain, &token)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, "", nil
		}
		return false, "", fmt.Errorf("completion gate: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO balances (snapshot_id, address, balance, last_block)
		SELECT $1, addr, SUM(delta), MAX(blk)
		FROM (
			SELECT to_addr AS addr, value AS delta, block_number AS blk
				FROM transfers WHERE chain = $2 AND token = $3 and block_number <= $4
			UNION ALL
			SELECT from_addr AS addr, -value AS delta, block_number AS blk
				FROM transfers WHERE chain =$2 AND token = $3 AND block_number <= $4
		) t 
		WHERE addr <> $5
		GROUP BY addr
		HAVING SUM(delta) <> 0
	`, snapshotID, chain, token, toBlock, zeroAddr)
	if err != nil {
		return false, "", fmt.Errorf("recompute balances: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO token_coverage (chain, token, covered_from, covered_to)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (chain, token) DO UPDATE
		SET covered_from = LEAST(token_coverage.covered_from, EXCLUDED.covered_from),
			covered_to = GREATEST(token_coverage.covered_to, EXCLUDED.covered_to)
		WHERE token_coverage.covered_from <= EXCLUDED.covered_to +1 
		AND EXCLUDED.covered_from <= token_coverage.covered_to +1 	
	`, chain, token, fromBlock, toBlock)
	if err != nil {
		return false, "", fmt.Errorf("advance coverage: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, "", err
	}
	return true, chain, nil
}

func (r *Repo) HolderAddresses(ctx context.Context, snapshotID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT address FROM balances WHERE snapshot_id = $1`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var addrs []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		addrs = append(addrs, a)
	}

	return addrs, rows.Err()
}

func (r *Repo) UpsertClassification(ctx context.Context, chain, address, addrType string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO address_classification (chain, address, address_type)
		VALUES ($1, $2, $3)
		ON CONFLICT (chain, address) DO UPDATE
		SET address_type = EXCLUDED.address_type, classified_at = now()
	`, chain, address, addrType)
	return err
}

func (r *Repo) SnapshotHolders(ctx context.Context, snapshotID string) ([]analytics.Holder, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT b.balance::text, COALESCE(c.address_type, '')
		FROM balances b
		LEFT JOIN address_classification c
			ON c.address = b.address
		WHERE b.snapshot_id = $1 AND b.balance > 0
	`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []analytics.Holder
	for rows.Next() {
		var balStr, atype string
		if err := rows.Scan(&balStr, &atype); err != nil {
			return nil, err
		}
		bal, ok := new(big.Int).SetString(balStr, 10)
		if !ok {
			continue
		}
		out = append(out, analytics.Holder{Balance: bal, AddressType: atype})
	}
	return out, rows.Err()
}

func (r *Repo) SnapshotMeta(ctx context.Context, snapshotID string) (chain, token string, block int64, err error) {
	err = r.pool.QueryRow(ctx,
		`SELECT chain, token, block_number FROM snapshots WHERE id = $1 AND state = 'ready'`,
		snapshotID).Scan(&chain, &token, &block)
	return
}
