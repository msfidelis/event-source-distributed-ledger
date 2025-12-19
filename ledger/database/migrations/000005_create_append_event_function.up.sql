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
