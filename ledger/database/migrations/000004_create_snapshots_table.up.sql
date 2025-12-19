-- Tabela de snapshots (otimização para agregados com muitos eventos)
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_id UUID PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    version INT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshot_type ON snapshots(aggregate_type);

-- Comentários para documentação
COMMENT ON TABLE events IS 'Event store - append-only log de todos os eventos do sistema';
COMMENT ON TABLE snapshots IS 'Snapshots de agregados para otimização de reconstrução de estado';
