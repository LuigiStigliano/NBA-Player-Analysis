"""
Script principale per l'esecuzione della pipeline di analisi dei giocatori NBA.

Questo script orchestra l'intero processo:
1.  **Ingestione e Pulizia**: Scarica il dataset da Kaggle (se non presente),
    lo carica, standardizza i nomi delle colonne, corregge i tipi di dato e gestisce
    i valori mancanti.
2.  **Feature Engineering e Normalizzazione**: Calcola le statistiche
    normalizzate (per 36 minuti) e metriche avanzate come il True Shooting Percentage.
3.  **Clustering**: Determina il numero ottimale di cluster (k) e poi applica
    l'algoritmo K-Means per raggruppare i giocatori in base allo stile di gioco.
4.  **Valutazione e Salvataggio**: Valuta la qualità dei cluster, genera un riassunto
    qualitativo e salva i risultati finali.
"""
import os
from pyspark.sql.functions import col, max
from utils.helpers import get_spark_session, save_dataframe
from data_ingestion.download_data import download_nba_dataset
from data_processing.cleaning import standardize_column_names, correct_data_types, handle_missing_values
from data_processing.normalization import per_36_minutes_stats
from feature_engineering.advanced_metrics import calculate_true_shooting_percentage
from clustering.models import prepare_features_for_clustering, train_kmeans_model, assign_clusters, evaluate_clustering, get_cluster_profiles, determine_optimal_k
# MODIFICA: Importazione della nuova funzione di summary
from reporting.summary import generate_cluster_summary

def run_pipeline():
    """
    Esegue l'intera pipeline di analisi dei dati dei giocatori NBA.
    """
    spark = get_spark_session(app_name="NBAPipeline")

    # --- Configurazione dei Percorsi ---
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    processed_data_dir = os.path.join(base_dir, "data", "processed")
    reports_dir = os.path.join(base_dir, "reports") # Percorso per i report

    raw_data_path = os.path.join(raw_data_dir, "Player Totals.csv")
    final_output_path = os.path.join(processed_data_dir, "player_clusters.parquet")

    # --- Fase 0: Download del Dataset (se necessario) ---
    if not os.path.exists(raw_data_path):
        try:
            print(f"Dataset non trovato. Avvio del download in '{raw_data_dir}'...")
            download_nba_dataset(raw_data_dir)
        except Exception as e:
            print(f"Pipeline interrotta: errore durante il download. {e}")
            spark.stop()
            return
    else:
        print(f"Dataset già presente. Salto il download.")

    # --- Fase 1: Ingestione e Pulizia Dati ---
    print("\nFase 1: Caricamento, Pulizia e Preparazione Dati...")
    raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    df_std_names = standardize_column_names(raw_df)
    df_typed = correct_data_types(df_std_names)
    df_cleaned = handle_missing_values(df_typed)

    # --- Fase 2: Normalizzazione e Feature Engineering ---
    print("\nFase 2: Normalizzazione e Feature Engineering...")
    stats_to_normalize = ['pts', 'trb', 'ast', 'stl', 'blk', 'tov', 'fga', 'fta']
    df_normalized = per_36_minutes_stats(df_cleaned, stats_to_normalize, minutes_played_col="mp")
    df_advanced = calculate_true_shooting_percentage(df_normalized, points_col="pts", fga_col="fga", fta_col="fta")

    # --- Fase 3: Clustering dei Giocatori ---
    print("\nFase 3: Raggruppamento dei Giocatori (Clustering)...")
    df_latest_season = df_advanced.groupBy("player").agg(max(col("season")).alias("latest_year"))
    df_for_clustering_input = df_advanced.join(
        df_latest_season,
        (df_advanced.player == df_latest_season.player) & (df_advanced.season == df_latest_season.latest_year),
        "inner"
    ).select(df_advanced["*"])

    feature_cols = [
        'pts_per_36_min', 'trb_per_36_min', 'ast_per_36_min', 
        'stl_per_36_min', 'blk_per_36_min', 'tov_per_36_min',
        'ts_pct_calc'
    ]
    df_for_clustering = df_for_clustering_input.select(["player", "season"] + feature_cols).na.drop()
    
    if df_for_clustering.count() == 0:
        print("Nessun dato valido per il clustering. Pipeline interrotta.")
        spark.stop()
        return

    df_prepared = prepare_features_for_clustering(df_for_clustering, feature_cols)
    optimal_k = determine_optimal_k(df_prepared)
    kmeans_model = train_kmeans_model(df_prepared, k=optimal_k)
    df_clustered = assign_clusters(kmeans_model, df_prepared)

    # --- Fase 4: Valutazione e Analisi dei Cluster ---
    print("\nFase 4: Valutazione e Analisi dei Risultati...")
    silhouette_score = evaluate_clustering(df_clustered)
    print(f"Punteggio di validità del clustering finale (Silhouette Score): {silhouette_score:.3f}")

    cluster_profiles = get_cluster_profiles(df_clustered, feature_cols)
    print("Profili medi (caratteristiche) di ciascun cluster:")
    cluster_profiles.show(truncate=False)
    
    # MODIFICA: Aggiunta la chiamata per generare il summary qualitativo
    generate_cluster_summary(df_clustered, df_for_clustering_input, optimal_k, reports_dir)

    # --- Fase 5: Salvataggio Risultati ---
    print(f"\nFase 5: Salvataggio dei risultati numerici in '{final_output_path}'...")
    columns_to_save = ["player", "season"] + feature_cols + ["cluster_id"]
    df_to_save = df_clustered.select(columns_to_save)
    save_dataframe(df_to_save, final_output_path)

    print("\nPipeline completata con successo.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()
