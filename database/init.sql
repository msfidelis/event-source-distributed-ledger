-- Event Sourcing Database Schema
-- Sistema Bancário com Ledger Distribuído

-- Tabela de contas (criada primeiro para ser referenciada pelos eventos)
CREATE TABLE IF NOT EXISTS accounts (
    aggregate_id UUID PRIMARY KEY,
    owner_name VARCHAR(255) NOT NULL,
    balance DECIMAL(15,2) NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_accounts_created ON accounts(created_at DESC);

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

-- Tabela principal de eventos (append-only)
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    metadata JSONB,
    version INT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- Garante consistência: um agregado não pode ter duas versões iguais
    CONSTRAINT unique_aggregate_version UNIQUE(aggregate_id, version),
    
    -- Relação forte: eventos devem pertencer a contas existentes
    CONSTRAINT fk_event_account
        FOREIGN KEY (aggregate_id)
        REFERENCES accounts(aggregate_id)
        ON DELETE CASCADE
);

-- Índices para performance
CREATE INDEX idx_aggregate_stream ON events(aggregate_id, version);
CREATE INDEX idx_event_type ON events(event_type);
CREATE INDEX idx_occurred_at ON events(occurred_at DESC);
CREATE INDEX idx_aggregate_type ON events(aggregate_type);

-- Índice GIN para queries em JSONB
CREATE INDEX idx_event_data_gin ON events USING GIN(event_data);
CREATE INDEX idx_metadata_gin ON events USING GIN(metadata);

-- Tabela de snapshots (otimização para agregados com muitos eventos)
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    version INT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshot_type ON snapshots(aggregate_type);

-- Função para inserir eventos com validação de versão
CREATE OR REPLACE FUNCTION append_event(
    p_aggregate_id UUID,
    p_aggregate_type VARCHAR,
    p_event_type VARCHAR,
    p_event_data JSONB,
    p_metadata JSONB,
    p_expected_version INT
) RETURNS BIGINT AS $$
DECLARE
    v_current_version INT;
    v_event_id BIGINT;
BEGIN
    -- Obtém versão atual
    SELECT COALESCE(MAX(version), 0) INTO v_current_version
    FROM events
    WHERE aggregate_id = p_aggregate_id;
    
    -- Valida versão esperada (optimistic locking)
    IF v_current_version != p_expected_version THEN
        RAISE EXCEPTION 'Concurrency conflict: expected version %, but current is %', 
            p_expected_version, v_current_version;
    END IF;
    
    -- Insere o evento
    INSERT INTO events (
        aggregate_id,
        aggregate_type,
        event_type,
        event_data,
        metadata,
        version
    ) VALUES (
        p_aggregate_id,
        p_aggregate_type,
        p_event_type,
        p_event_data,
        p_metadata,
        v_current_version + 1
    ) RETURNING id INTO v_event_id;
    
    RETURN v_event_id;
END;
$$ LANGUAGE plpgsql;

-- Comentários para documentação
COMMENT ON TABLE events IS 'Event store - append-only log de todos os eventos do sistema';
COMMENT ON TABLE snapshots IS 'Snapshots de agregados para otimização de reconstrução de estado';
