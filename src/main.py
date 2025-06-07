"""
Script principale per l'esecuzione della pipeline di analisi dei giocatori NBA.

Questo script orchestra l'intero processo:
1.  **Ingestione e Pulizia**: Scarica il dataset da Kaggle (se non presente),
    lo carica, standardizza i nomi delle colonne, corregge i tipi di dato e gestisce
    i valori mancanti.
2.  **Feature Engineering e Normalizzazione**: Calcola le statistiche
    normalizzate (per 36 minuti) e metriche avanzate come il True Shooting Percentage.
3.  **Clustering**: Determina il numero ottimale di cluster (k) e poi applica
    l'algoritmo K-Means per raggruppare i giocatori in base allo stile di gioco,
    utilizzando i dati della loro stagione più recente.
4.  **Valutazione e Salvataggio**: Valuta la qualità dei cluster tramite il
    punteggio Silhouette e salva i risultati finali in formato Parquet.
"""
import os
from pyspark.sql.functions import col, max
from utils.helpers import get_spark_session, save_dataframe
from data_ingestion.download_data import download_nba_dataset
from data_processing.cleaning import standardize_column_names, correct_data_types, handle_missing_values
from data_processing.normalization import per_36_minutes_stats
from feature_engineering.advanced_metrics import calculate_true_shooting_percentage
from clustering.models import prepare_features_for_clustering, train_kmeans_model, assign_clusters, evaluate_clustering, get_cluster_profiles, determine_optimal_k

def run_pipeline():
    """
    Esegue l'intera pipeline di analisi dei dati dei giocatori NBA.
    """
    spark = get_spark_session(app_name="NBAPipeline")

    # --- Configurazione dei Percorsi ---
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    processed_data_path = os.path.join(base_dir, "data", "processed")

    correct_csv_name = "Player Totals.csv"
    raw_data_path = os.path.join(raw_data_dir, correct_csv_name)
    final_output_path = os.path.join(processed_data_path, "player_clusters.parquet")

    # --- Fase 0: Download del Dataset (se necessario) ---
    if not os.path.exists(raw_data_path):
        try:
            print(f"Dataset non trovato. Avvio del download in '{raw_data_dir}'...")
            download_nba_dataset(raw_data_dir)
        except Exception as e:
            print(f"Pipeline interrotta: errore durante il download del dataset. {e}")
            spark.stop()
            return
    else:
        print(f"Dataset già presente in '{raw_data_path}'. Salto il download.")

    # --- Fase 1: Ingestione e Pulizia Dati ---
    print("\nFase 1: Caricamento, Pulizia e Preparazione Dati...")
    try:
        raw_df = spark.read.csv(raw_data_path, header=True, inferSchema=True)
        print(f"Dataset caricato con {raw_df.count()} righe.")
    except Exception as e:
        print(f"Errore critico nel caricamento del dataset da '{raw_data_path}': {e}")
        spark.stop()
        return

    df_std_names = standardize_column_names(raw_df)
    df_typed = correct_data_types(df_std_names)
    df_cleaned = handle_missing_values(df_typed)
    print(f"Dati dopo pulizia e filtraggio: {df_cleaned.count()} righe.")

    # --- Fase 2: Normalizzazione e Feature Engineering ---
    print("\nFase 2: Normalizzazione Statistiche e Calcolo Metriche Avanzate...")
    
    stats_to_normalize = ['pts', 'trb', 'ast', 'stl', 'blk', 'tov', 'fga', 'fta']
    df_normalized = per_36_minutes_stats(df_cleaned, stats_to_normalize, minutes_played_col="mp")

    df_advanced = calculate_true_shooting_percentage(df_normalized, points_col="pts", fga_col="fga", fta_col="fta")
    
    print("Statistiche normalizzate e metriche avanzate calcolate.")
    df_advanced.select("player", "season", "pts_per_36_min", "ast_per_36_min", "ts_pct_calc").show(5)

    # --- Fase 3: Clustering dei Giocatori ---
    print("\nFase 3: Raggruppamento dei Giocatori (Clustering)...")
    
    df_latest_season = df_advanced.groupBy("player").agg(max(col("season")).alias("latest_year"))

    df_advanced_aliased = df_advanced.alias("adv")
    df_latest_season_aliased = df_latest_season.alias("latest")

    df_for_clustering_input = df_advanced_aliased.join(
        df_latest_season_aliased,
        (col("adv.player") == col("latest.player")) & (col("adv.season") == col("latest.latest_year")),
        "inner"
    ).select("adv.*")

    feature_cols = [
        'pts_per_36_min', 'trb_per_36_min', 'ast_per_36_min', 
        'stl_per_36_min', 'blk_per_36_min', 'tov_per_36_min',
        'ts_pct_calc'
    ]
    
    df_for_clustering = df_for_clustering_input.select(["player", "season"] + feature_cols).na.drop()
    
    if df_for_clustering.count() == 0:
        print("Nessun dato valido per il clustering dopo il filtraggio. Pipeline interrotta.")
        spark.stop()
        return

    print(f"Dati pronti per il clustering: {df_for_clustering.count()} giocatori.")
    df_prepared = prepare_features_for_clustering(df_for_clustering, feature_cols)

    # Determina k dinamicamente invece di usare un valore hardcoded
    optimal_k = determine_optimal_k(df_prepared)
    
    # Addestramento del modello K-Means con il k ottimale
    kmeans_model = train_kmeans_model(df_prepared, k=optimal_k)
    df_clustered = assign_clusters(kmeans_model, df_prepared)
    
    print("Primi 10 giocatori con cluster assegnato:")
    df_clustered.select("player", "season", "cluster_id").show(10)

    # --- Fase 4: Valutazione e Analisi dei Cluster ---
    print("\nFase 4: Valutazione e Analisi dei Risultati...")
    silhouette_score = evaluate_clustering(df_clustered)
    print(f"Punteggio di validità del clustering finale (Silhouette Score): {silhouette_score:.3f}")

    cluster_profiles = get_cluster_profiles(df_clustered, feature_cols)
    print("Profili medi (caratteristiche) di ciascun cluster:")
    cluster_profiles.show(truncate=False)

    # --- Fase 5: Salvataggio Risultati ---
    print(f"\nFase 5: Salvataggio dei risultati finali in '{final_output_path}'...")

    columns_to_save = ["player", "season"] + feature_cols + ["cluster_id"]
    df_to_save = df_clustered.select(columns_to_save)

    save_dataframe(df_to_save, final_output_path)
    print("\nPipeline completata con successo.")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()
