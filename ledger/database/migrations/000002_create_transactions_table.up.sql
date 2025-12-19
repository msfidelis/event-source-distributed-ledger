-- Tabela de transações (read model para histórico com saldo)
CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY,
    account_id UUID NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    balance_after DECIMAL(15,2) NOT NULL,
    description VARCHAR(500),
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Relação com accounts
    CONSTRAINT fk_transaction_account
        FOREIGN KEY (account_id)
        REFERENCES accounts(aggregate_id)
        ON DELETE CASCADE
);

-- Índices para performance
CREATE INDEX idx_transactions_account ON transactions(account_id, occurred_at DESC);
CREATE INDEX idx_transactions_occurred ON transactions(occurred_at DESC);
