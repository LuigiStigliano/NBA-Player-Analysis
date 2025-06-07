"""
Script principale per l'esecuzione della pipeline di analisi dei giocatori NBA.
"""
import os
from pyspark.sql.functions import col, max
# MODIFICA: Importazione della configurazione
from config.spark_config import SPARK_CONFIG
from utils.helpers import get_spark_session, save_dataframe
from data_ingestion.download_data import download_nba_dataset
from data_processing.cleaning import standardize_column_names, correct_data_types, handle_missing_values
from data_processing.normalization import per_36_minutes_stats, add_per_game_metrics
from feature_engineering.advanced_metrics import calculate_true_shooting_percentage
from clustering.models import prepare_features_for_clustering, train_kmeans_model, assign_clusters, evaluate_clustering, get_cluster_profiles, determine_optimal_k
from reporting.summary import generate_cluster_summary

def run_pipeline():
    """
    Esegue l'intera pipeline di analisi dei dati dei giocatori NBA.
    """
    # MODIFICA: Uso della configurazione per inizializzare Spark
    spark = get_spark_session(
        app_name="NBAPipeline",
        driver_memory=SPARK_CONFIG["driver_memory"]
    )

    # --- Configurazione dei Percorsi ---
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    processed_data_dir = os.path.join(base_dir, "data", "processed")
    reports_dir = os.path.join(base_dir, "reports")

    raw_data_path = os.path.join(raw_data_dir, "Player Totals.csv")
    final_output_path = os.path.join(processed_data_dir, "player_clusters.parquet")

    # --- Fase 0: Download del Dataset ---
    if not os.path.exists(raw_data_path):
        download_nba_dataset(raw_data_dir)
    
    # --- Fase 1: Ingestione e Pulizia ---
    print("\nFase 1: Caricamento, Pulizia e Preparazione Dati...")
    raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    df_std_names = standardize_column_names(raw_df)
    df_typed = correct_data_types(df_std_names)
    # MODIFICA: Uso della soglia di partite dalla configurazione
    df_cleaned = handle_missing_values(
        df_typed, 
        min_games_threshold=SPARK_CONFIG["min_games_threshold"]
    )

    # --- Fase 2: Normalizzazione e Feature Engineering ---
    print("\nFase 2: Normalizzazione e Feature Engineering...")
    stats_to_normalize = ['pts', 'trb', 'ast', 'stl', 'blk', 'tov', 'fga', 'fta']
    df_normalized = per_36_minutes_stats(df_cleaned, stats_to_normalize, minutes_played_col="mp")
    df_advanced = calculate_true_shooting_percentage(df_normalized, points_col="pts", fga_col="fga", fta_col="fta")
    df_full_features = add_per_game_metrics(df_advanced)

    # --- Fase 3: Clustering ---
    print("\nFase 3: Raggruppamento dei Giocatori (Clustering)...")
    df_latest_season = df_full_features.groupBy("player").agg(max(col("season")).alias("latest_year"))
    df_for_clustering_input = df_full_features.join(
        df_latest_season,
        (df_full_features.player == df_latest_season.player) & (df_full_features.season == df_latest_season.latest_year),
        "inner"
    ).select(df_full_features["*"])

    feature_cols = [
        'pts_per_36_min', 'trb_per_36_min', 'ast_per_36_min', 
        'stl_per_36_min', 'blk_per_36_min', 'tov_per_36_min',
        'ts_pct_calc'
    ]
    df_for_clustering = df_for_clustering_input.select(["player"] + feature_cols).na.drop()
    
    df_prepared = prepare_features_for_clustering(df_for_clustering, feature_cols)
    optimal_k = determine_optimal_k(df_prepared)
    kmeans_model = train_kmeans_model(df_prepared, k=optimal_k)
    df_clustered = assign_clusters(kmeans_model, df_prepared)

    # --- Fase 4: Valutazione e Analisi ---
    print("\nFase 4: Valutazione e Analisi dei Risultati...")
    generate_cluster_summary(df_clustered, df_for_clustering_input, optimal_k, reports_dir)

    # --- Fase 5: Salvataggio Risultati ---
    print(f"\nFase 5: Salvataggio dei risultati numerici...")
    columns_to_save = ["player", "season"] + feature_cols + ["cluster_id"]
    df_to_save = df_clustered.select(columns_to_save)
    save_dataframe(df_to_save, final_output_path)

    print("\nPipeline completata con successo.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()
