-- Schema per organizzare il progetto
CREATE SCHEMA nba_analytics;

-- Tabella per i dati RAW, con tutte le colonne presenti nel CSV
CREATE TABLE nba_analytics.raw_player_stats (
    id SERIAL PRIMARY KEY,
    seas_id TEXT,
    season TEXT,
    player_id TEXT,
    player TEXT,
    birth_year TEXT,
    pos TEXT,
    age TEXT,
    experience TEXT,
    lg TEXT,
    tm TEXT,
    g TEXT,
    gs TEXT,
    mp TEXT,
    fg TEXT,
    fga TEXT,
    fg_percent TEXT,
    x3p TEXT,
    x3pa TEXT,
    x3p_percent TEXT,
    x2p TEXT,
    x2pa TEXT,
    x2p_percent TEXT,
    e_fg_percent TEXT,
    ft TEXT,
    fta TEXT,
    ft_percent TEXT,
    orb TEXT,
    drb TEXT,
    trb TEXT,
    ast TEXT,
    stl TEXT,
    blk TEXT,
    tov TEXT,
    pf TEXT,
    pts TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Tabella per le metriche calcolate
CREATE TABLE nba_analytics.processed_metrics (
    player_id INT REFERENCES nba_analytics.raw_player_stats(id),
    pts_per_36_min FLOAT,
    ast_per_36_min FLOAT,
    trb_per_36_min FLOAT,
    tov_per_36_min FLOAT,
    stl_per_36_min FLOAT,
    blk_per_36_min FLOAT,
    mp_per_game FLOAT,
    pts_per_game FLOAT,
    ts_pct_calc FLOAT,
    processing_date TIMESTAMP DEFAULT NOW(),
    processing_version VARCHAR(10) DEFAULT '1.0'
);

-- Tabella per i risultati del clustering
CREATE TABLE nba_analytics.player_clusters (
    player_id INT REFERENCES nba_analytics.raw_player_stats(id),
    cluster_id INT NOT NULL,
    distance_to_centroid FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Tabella per le definizioni e le etichette dei cluster
CREATE TABLE nba_analytics.cluster_definitions (
    id INT PRIMARY KEY,
    label VARCHAR(100) NOT NULL,
    description TEXT
);

-- Una vista completa per rendere facile l'analisi
CREATE VIEW nba_analytics.player_analysis_full AS
SELECT
    r.player,
    r.season,
    r.tm,
    r.pos,
    -- Colonne dalla tabella delle metriche
    pm.pts_per_36_min,
    pm.ast_per_36_min,
    pm.trb_per_36_min,
    pm.tov_per_36_min,
    pm.stl_per_36_min,
    pm.blk_per_36_min,
    pm.mp_per_game,
    pm.ts_pct_calc,
    -- Colonne dalla tabella dei cluster
    pc.cluster_id,
    cd.label AS cluster_label
FROM nba_analytics.raw_player_stats r
LEFT JOIN nba_analytics.processed_metrics pm ON r.id = pm.player_id
LEFT JOIN nba_analytics.player_clusters pc ON r.id = pc.player_id
LEFT JOIN nba_analytics.cluster_definitions cd ON pc.cluster_id = cd.id;

-- Indici per le chiavi esterne
CREATE INDEX idx_processed_metrics_player_id ON nba_analytics.processed_metrics(player_id);
CREATE INDEX idx_player_clusters_player_id ON nba_analytics.player_clusters(player_id);

-- Indici per le colonne usate frequentemente per filtrare
CREATE INDEX idx_raw_player_stats_season ON nba_analytics.raw_player_stats(season);
CREATE INDEX idx_raw_player_stats_tm ON nba_analytics.raw_player_stats(tm);