-- Corrección de resiliencia (ADR-006/ADR-008): cada medición del SLO registra la causa del
-- incumplimiento y el atraso de los consumidores. Tras una caída, el atraso de la propia caída ya no
-- se confunde con una sobrecarga (no se activa el modo degradado durante la puesta al día).
ALTER TABLE slo_measurements ADD COLUMN IF NOT EXISTS cause TEXT;            -- SOBRECARGA | RECUPERACION
ALTER TABLE slo_measurements ADD COLUMN IF NOT EXISTS consumer_lag BIGINT;    -- mensajes pendientes (ingesta + correlación)
