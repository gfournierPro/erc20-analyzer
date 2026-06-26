package analytics

import (
	"math/big"
	"sort"
)

// Holder is one balance, optionally with its classification.
type Holder struct {
	Balance     *big.Int
	AddressType string // "eoa" | "contract" | "delegated" | "" (unknown)
}

// Metrics holds the four computed distribution metrics.
type Metrics struct {
	HolderCount int
	Gini        float64
	Nakamoto    int
	HHI         float64
	Buckets     []Bucket
}

type Bucket struct {
	Label        string
	MinShare     float64
	HolderCount  int
	TotalBalance *big.Int
}

// Compute calculates all metrics over the given balances.
// balances must be > 0 (zero-balance holders are not holders).
func Compute(balances []*big.Int) Metrics {
	n := len(balances)
	if n == 0 {
		return Metrics{Buckets: emptyBuckets()}
	}

	// Sort ascending once; Gini wants ascending, Nakamoto wants descending
	// (we walk it in reverse).
	sorted := make([]*big.Int, n)
	copy(sorted, balances)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Cmp(sorted[j]) < 0
	})

	total := new(big.Int)
	for _, b := range sorted {
		total.Add(total, b)
	}

	return Metrics{
		HolderCount: n,
		Gini:        gini(sorted, total),
		Nakamoto:    nakamoto(sorted, total),
		HHI:         hhi(sorted, total),
		Buckets:     buckets(sorted, total),
	}
}

// gini computes the Gini coefficient over an ascending-sorted slice.
// Formula: G = ( sum_i (2i - n - 1) * x_i ) / ( n * sum_i x_i ),  i = 1..n
// 0 = perfect equality, 1 = one holder owns everything.
func gini(asc []*big.Int, total *big.Int) float64 {
	n := len(asc)
	if n == 0 || total.Sign() == 0 {
		return 0
	}
	// numerator = sum (2i - n - 1) * x_i, with i 1-based.
	// Use big.Float to avoid overflow on the weighted sum.
	num := new(big.Float)
	for idx, x := range asc {
		i := int64(idx + 1) // 1-based
		coef := big.NewInt(2*i - int64(n) - 1)
		term := new(big.Int).Mul(coef, x)
		num.Add(num, new(big.Float).SetInt(term))
	}
	denom := new(big.Float).Mul(big.NewFloat(float64(n)), new(big.Float).SetInt(total))
	if denom.Sign() == 0 {
		return 0
	}
	g, _ := new(big.Float).Quo(num, denom).Float64()
	return g
}

// nakamoto returns the minimum number of top holders whose combined balance
// exceeds 50% of total supply.
func nakamoto(asc []*big.Int, total *big.Int) int {
	if total.Sign() == 0 {
		return 0
	}
	half := new(big.Int).Div(total, big.NewInt(2)) // floor(total/2)
	acc := new(big.Int)
	count := 0
	// Walk descending: from the end of the ascending slice.
	for i := len(asc) - 1; i >= 0; i-- {
		acc.Add(acc, asc[i])
		count++
		if acc.Cmp(half) > 0 { // strictly more than half
			return count
		}
	}
	return count
}

// hhi: Herfindahl-Hirschman Index = sum of squared shares.
// Each share = balance/total in [0,1]; HHI in (0,1]. 1 = monopoly.
func hhi(asc []*big.Int, total *big.Int) float64 {
	if total.Sign() == 0 {
		return 0
	}
	totalF := new(big.Float).SetInt(total)
	sum := 0.0
	for _, x := range asc {
		share, _ := new(big.Float).Quo(new(big.Float).SetInt(x), totalF).Float64()
		sum += share * share
	}
	return sum
}

// buckets groups holders by share of supply (log-scale-ish thresholds).
func buckets(asc []*big.Int, total *big.Int) []Bucket {
	defs := bucketDefs()
	totalF := new(big.Float).SetInt(total)
	for _, x := range asc {
		share := 0.0
		if total.Sign() != 0 {
			share, _ = new(big.Float).Quo(new(big.Float).SetInt(x), totalF).Float64()
		}
		// Find the highest threshold this holder meets.
		for i := range defs {
			if share >= defs[i].MinShare {
				defs[i].HolderCount++
				defs[i].TotalBalance.Add(defs[i].TotalBalance, x)
				break
			}
		}
	}
	return defs
}

func bucketDefs() []Bucket {
	return []Bucket{
		{Label: "whale", MinShare: 0.01, TotalBalance: new(big.Int)},    // >= 1%
		{Label: "dolphin", MinShare: 0.001, TotalBalance: new(big.Int)}, // >= 0.1%
		{Label: "fish", MinShare: 0.0001, TotalBalance: new(big.Int)},   // >= 0.01%
		{Label: "shrimp", MinShare: 0.0, TotalBalance: new(big.Int)},    // the rest
	}
}

func emptyBuckets() []Bucket { return bucketDefs() }
