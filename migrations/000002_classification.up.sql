CREATE TABLE IF NOT EXISTS address_classification (
    chain TEXT NOT NULL,
    address TEXT NOT NULL,
    address_type TEXT NOT NULL,
    classified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address)
)