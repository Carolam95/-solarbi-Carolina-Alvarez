-- =============================================================================
-- PROYECTO SOLARBI PASCUAL - INTELIGENCIA DE NEGOCIOS (2026-II)
-- DDL DE BASE DE DATOS: ESQUEMAS SILVER Y DWH (POSTGRESQL)
-- =============================================================================

-- 1. Creación de Esquemas
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS dwh;

-- 2. Capa Silver: Telemetría limpia a granularidad de 5 minutos
CREATE TABLE IF NOT EXISTS silver.lectura_5min (
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dispositivo_id INTEGER NOT NULL,
    p_ac_kw NUMERIC(8, 3) NOT NULL,
    irradiancia_wm2 NUMERIC(8, 1) NOT NULL,
    temp_modulo_c NUMERIC(5, 1) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_silver_lectura_5min PRIMARY KEY (ts, dispositivo_id),
    CONSTRAINT chk_p_ac_kw_rango CHECK (p_ac_kw >= 0 AND p_ac_kw <= 6.0),
    CONSTRAINT chk_irradiancia_rango CHECK (irradiancia_wm2 >= 0 AND irradiancia_wm2 <= 1400.0),
    CONSTRAINT chk_temp_modulo_rango CHECK (temp_modulo_c >= -10.0 AND temp_modulo_c <= 90.0)
);

-- Índices para optimizar consultas de series de tiempo (Grafana)
CREATE INDEX IF NOT EXISTS idx_silver_lectura_ts ON silver.lectura_5min (ts DESC);
CREATE INDEX IF NOT EXISTS idx_silver_lectura_disp_ts ON silver.lectura_5min (dispositivo_id, ts DESC);

-- 3. Capa DWH (Gold): Resumen diario de generación y calidad
CREATE TABLE IF NOT EXISTS dwh.fact_energia_dia (
    fecha DATE NOT NULL,
    dispositivo_id INTEGER NOT NULL,
    energia_kwh NUMERIC(10, 3) NOT NULL,
    pct_datos_validos NUMERIC(5, 2) NOT NULL,
    filas_validas INTEGER NOT NULL,
    filas_esperadas INTEGER NOT NULL DEFAULT 288,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_dwh_fact_energia_dia PRIMARY KEY (fecha, dispositivo_id),
    CONSTRAINT chk_energia_positiva CHECK (energia_kwh >= 0),
    CONSTRAINT chk_pct_validos CHECK (pct_datos_validos >= 0 AND pct_datos_validos <= 100)
);

-- Índice para acelerar filtros por fecha en Power BI
CREATE INDEX IF NOT EXISTS idx_fact_energia_fecha ON dwh.fact_energia_dia (fecha);
