package classify

import "time"

type Request struct {
	Chain     string    `json:"chain"`
	Address   string    `json:"address"`
	CreatedAt time.Time `json:"created_at"`
}

type Result struct {
	Chain        string    `json:"chain"`
	Address      string    `json:"address"`
	AddressType  string    `json:"address_type"`
	ClassifiedAt time.Time `json:"classified_at"`
}

const (
	TypeEOA      = "eoa"
	TypeContract = "contract"
)
