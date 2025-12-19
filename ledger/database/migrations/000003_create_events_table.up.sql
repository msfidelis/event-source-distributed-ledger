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
