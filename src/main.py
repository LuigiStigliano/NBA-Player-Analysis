"""
Script principale per l'esecuzione della pipeline di analisi dei giocatori NBA.
"""
import os
from pyspark.sql.functions import col, max, row_number, lit, when
from pyspark.sql.window import Window
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
    raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=False)
    df_std_names = standardize_column_names(raw_df)
    df_typed = correct_data_types(df_std_names)
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

    window_spec = Window.partitionBy("player").orderBy(col("season").desc(), col("g").desc())

    df_with_rank = df_full_features.withColumn("rank", row_number().over(window_spec))

    df_for_clustering_input = df_with_rank.withColumn(
        "priority",
        when(col("tm") == "TOT", 1).otherwise(2)
    )

    final_window_spec = Window.partitionBy("player", "season").orderBy("priority", "g")
    df_unique_latest_season = df_for_clustering_input.withColumn("final_rank", row_number().over(final_window_spec))

    df_for_clustering_input = df_unique_latest_season.filter(col("rank") == 1).filter(col("final_rank") == 1)

    feature_cols = [
        'pts_per_36_min', 'trb_per_36_min', 'ast_per_36_min',
        'stl_per_36_min', 'blk_per_36_min', 'tov_per_36_min',
        'ts_pct_calc'
    ]

    df_for_clustering = df_for_clustering_input.select(["player", "season"] + feature_cols).na.drop()

    df_prepared = prepare_features_for_clustering(df_for_clustering, feature_cols)
    # Usiamo il valore k=6 validato nell'analisi esplorativa per ottenere cluster più significativi
    optimal_k = 6
    print(f"Valore di k impostato manualmente a {optimal_k} sulla base dell'analisi esplorativa per una migliore interpretabilità.")
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
