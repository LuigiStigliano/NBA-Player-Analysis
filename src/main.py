"""
Questo è il mio script principale, che utilizzo per eseguire l'intera pipeline 
di analisi dei giocatori NBA. L'ho progettato per essere eseguito dall'inizio alla fine, 
in modo automatizzato, caricando i dati direttamente su un database PostgreSQL.
"""
import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window
from sqlalchemy import text # Import necessario per eseguire SQL nativo
from config.spark_config import SPARK_CONFIG
from utils.helpers import get_spark_session, get_db_engine, save_pd_to_db
from data_ingestion.download_data import download_nba_dataset
from data_processing.cleaning import standardize_column_names, correct_data_types, handle_missing_values
from data_processing.normalization import per_36_minutes_stats, add_per_game_metrics
from feature_engineering.advanced_metrics import calculate_true_shooting_percentage
from clustering.models import prepare_features_for_clustering, train_kmeans_model, assign_clusters
from reporting.summary import generate_cluster_summary

def run_pipeline():
    """
    In questa funzione, eseguo l'intera pipeline di analisi, passo dopo passo,
    e carico i risultati in un database PostgreSQL.
    """
    # Per prima cosa, carico le variabili d'ambiente dal file .env.
    load_dotenv()
    
    spark = get_spark_session(
        app_name="NBAPipeline",
        driver_memory=SPARK_CONFIG["driver_memory"]
    )
    engine = get_db_engine()

    # --- Configuro i Percorsi e i Nomi delle Tabelle ---
    base_dir = os.path.dirname(os.path.dirname(__file__))
    data_dir = os.path.join(base_dir, "data")
    reports_dir = os.path.join(base_dir, "reports")
    raw_data_folder = os.path.join(data_dir, "raw")
    raw_data_path = os.path.join(raw_data_folder, "Player Totals.csv")
    db_schema = "nba_analytics"

    # --- Fase 0: Eseguo il Download del Dataset ---
    if not os.path.exists(raw_data_path):
        download_nba_dataset(raw_data_folder)
    
    # --- Fase 1: Ingestione, Pulizia e Caricamento dei Dati Grezzi ---
    print("\nFase 1: Carico, pulisco e preparo i dati grezzi")
    raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=False)
    df_std_names = standardize_column_names(raw_df)
    
    print(f"\n--- Caricamento Dati in '{db_schema}.raw_player_stats' ---")
    raw_pd = df_std_names.toPandas()
    # Svuoto la tabella prima del caricamento per rieseguire lo script da zero
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {db_schema}.raw_player_stats RESTART IDENTITY CASCADE"))
        connection.commit()
    save_pd_to_db(raw_pd, "raw_player_stats", db_schema, engine)
    
    # --- Fase 2: Elaborazione dei Dati e Caricamento delle Metriche ---
    print("\nFase 2: Calcolo metriche avanzate e normalizzate")
    df_typed = correct_data_types(df_std_names)
    df_cleaned = handle_missing_values(df_typed, min_games_threshold=SPARK_CONFIG["min_games_threshold"])
    
    stats_to_normalize = ['pts', 'trb', 'ast', 'stl', 'blk', 'tov', 'fga', 'fta']
    df_normalized = per_36_minutes_stats(df_cleaned, stats_to_normalize, minutes_played_col="mp")
    df_advanced = calculate_true_shooting_percentage(df_normalized, points_col="pts", fga_col="fga", fta_col="fta")
    df_full_features = add_per_game_metrics(df_advanced)

    raw_stats_ids = pd.read_sql(f"SELECT id, player, season, tm FROM {db_schema}.raw_player_stats", engine)
    
    metrics_to_load_pd = df_full_features.select(
        "player", "season", "tm", "pts_per_36_min", "ast_per_36_min", "trb_per_36_min",
        "tov_per_36_min", "stl_per_36_min", "blk_per_36_min", "mp_per_game",
        "pts_per_game", "ts_pct_calc"
    ).toPandas()

    metrics_with_ids = pd.merge(metrics_to_load_pd, raw_stats_ids, on=["player", "season", "tm"])
    metrics_with_ids = metrics_with_ids.rename(columns={"id": "player_id"})

    print(f"\n--- Caricamento Dati in '{db_schema}.processed_metrics' ---")
    save_pd_to_db(
        metrics_with_ids.drop(columns=['player', 'season', 'tm']),
        "processed_metrics",
        db_schema,
        engine
    )

    # --- Fase 3: Clustering e Caricamento dei Risultati ---
    print("\nFase 3: Eseguo il clustering dei giocatori")
    window_spec_tot = Window.partitionBy("player", "season").orderBy(when(col("tm") == "TOT", 1).otherwise(2))
    df_unique_season = df_full_features.withColumn("row", row_number().over(window_spec_tot)).filter(col("row") == 1)
    
    window_spec_latest = Window.partitionBy("player").orderBy(col("season").desc())
    df_latest_season = df_unique_season.withColumn("rank", row_number().over(window_spec_latest)).filter(col("rank") == 1)

    feature_cols = [
        'pts_per_36_min', 'trb_per_36_min', 'ast_per_36_min',
        'stl_per_36_min', 'blk_per_36_min', 'tov_per_36_min',
        'ts_pct_calc'
    ]
    df_for_clustering = df_latest_season.select(["player", "season", "tm"] + feature_cols).na.drop()

    df_prepared = prepare_features_for_clustering(df_for_clustering, feature_cols)
    optimal_k = 6
    kmeans_model = train_kmeans_model(df_prepared, k=optimal_k)
    df_clustered = assign_clusters(kmeans_model, df_prepared)

    df_clustered = df_clustered.withColumn("cluster_id", col("cluster_id") + 1)

    clusters_pd = df_clustered.select("player", "season", "tm", "cluster_id").toPandas()
    clusters_with_ids = pd.merge(clusters_pd, raw_stats_ids, on=["player", "season", "tm"])
    clusters_with_ids = clusters_with_ids.rename(columns={"id": "player_id"})
    
    clusters_with_ids['distance_to_centroid'] = None 

    print(f"\n--- Caricamento Dati in '{db_schema}.player_clusters' ---")
    save_pd_to_db(
        clusters_with_ids[['player_id', 'cluster_id', 'distance_to_centroid']],
        "player_clusters",
        db_schema,
        engine
    )

    # --- Fase 4: Caricamento delle Definizioni dei Cluster ---
    print(f"\n--- Caricamento Dati in '{db_schema}.cluster_definitions' ---")
    cluster_profiles_map = {
        1: ("Ali Forti Moderne / Marcatori-Rimbalzisti", "Molto bilanciato, con punti alti, ottimi rimbalzi e eccellente efficienza al tiro."),
        2: ("Playmaker Puri / Organizzatori di Gioco", "La loro forza è la distribuzione del gioco, con un alto numero di assist e palle rubate."),
        3: ("Giocatori di Ruolo a Basso Utilizzo", "Il gruppo più numeroso, con impatto statistico basso ma anche poche palle perse."),
        4: ("All-Around Stars / Motori Offensivi", "Eccezionali in attacco, con i valori più alti di punti e assist."),
        5: ("Giocatori Affidabili a Controllo Rischio", "Caratterizzati da un bassissimo numero di palle perse e alta efficienza."),
        6: ("Ancore Difensive / Specialisti del Canestro", "Dominano in difesa, con valori altissimi di rimbalzi e stoppate.")
    }
    definitions_pd = pd.DataFrame([
        {"id": k, "label": v[0], "description": v[1]} for k, v in cluster_profiles_map.items()
    ])
    
    # Svuoto la tabella prima di inserire i nuovi dati per risolvere l'errore di dipendenza.
    # RESTART IDENTITY resetta l'autoincremento, CASCADE propaga l'operazione agli oggetti dipendenti.
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {db_schema}.cluster_definitions RESTART IDENTITY CASCADE"))
        connection.commit()

    # Salvo le nuove definizioni usando 'append' su una tabella ora vuota.
    save_pd_to_db(definitions_pd, "cluster_definitions", db_schema, engine)


    # --- Fase 5: Generazione del Report Finale dal Database ---
    print("\nFase 5: Genero il report finale leggendo i dati dal database")
    generate_cluster_summary(engine, reports_dir)

    print("\nPipeline completata con successo. Il database è stato popolato.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()