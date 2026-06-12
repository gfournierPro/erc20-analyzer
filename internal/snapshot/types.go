package snapshot

import (
	"math/big"
	"time"
)

type SnapshotJob struct {
	JobID       string    `json:"job_id"`
	Chain       string    `json:"chain"`
	Token       string    `json:"token"`
	FromBlock   uint64    `json:"from_block"`
	ToBlock     uint64    `json:"to_block"`
	RequestedAt time.Time `json:"request_at"`
}

type Transfer struct {
	Block    uint64 `json:"block"`
	TxHash   string `json:"tx_hash"`
	LogIndex uint   `json:"log_index"`
	From     string `json:"from"`
	To       string `json:"to"`
	Value    string `json:"value"`
}

func (t *Transfer) ValueBig() *big.Int {
	v, _ := new(big.Int).SetString(t.Value, 10)
	return v
}

type TransferBatch struct {
	JobID        string     `json:"job_id"`
	Chain        string     `json:"chain"`
	Token        string     `json:"token"`
	ChunkFrom    uint64     `json:"chunk_from"`
	ChunkTo      uint64     `json:"chunk_to"`
	ChunksTotal  uint64     `json:"chunks_total"`
	Transfers    []Transfer `json:"transfers"`
	IsFinalChunk bool       `json:"is_final_chunk"`
}

type SnapshotStatus struct {
	JobID       string    `json:"job_id"`
	Chain       string    `json:"chain"`
	Token       string    `json:"token"`
	State       string    `json:"state"`
	Message     string    `json:"messag,omitempty"`
	BlocksDone  uint64    `json:"blocks_done"`
	BlocksTotal uint64    `json:"blocks_total"`
	UpdatedAt   time.Time `json:"updated_at"`
}
