-- Tabela de contas (criada primeiro para ser referenciada pelos eventos)
CREATE TABLE IF NOT EXISTS accounts (
    aggregate_id UUID PRIMARY KEY,
    owner_name VARCHAR(255) NOT NULL,
    balance DECIMAL(15,2) NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indices
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_created ON accounts(created_at DESC);
