"""
Funzioni per la normalizzazione e la creazione di feature per i giocatori.

Questo modulo contiene la logica per:
- Convertire le statistiche totali in medie "per 36 minuti".
- Calcolare nuove metriche "per partita" utili per l'analisi.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, when

def per_36_minutes_stats(df: DataFrame, stats_cols: list, minutes_played_col: str = "mp") -> DataFrame:
    """
    Normalizza le statistiche di un giocatore su una base di 36 minuti.

    Args:
        df (DataFrame): Il DataFrame di input.
        stats_cols (list): Lista di colonne da normalizzare.
        minutes_played_col (str): Colonna dei minuti totali giocati.

    Returns:
        DataFrame: Un nuovo DataFrame con le colonne normalizzate aggiunte.
    """
    print(f"Normalizzazione 'per 36 minuti' per le colonne: {stats_cols}")
    df_normalized = df
    if minutes_played_col not in df.columns:
        print(f"Attenzione: la colonna dei minuti '{minutes_played_col}' non è stata trovata.")
        return df

    for stat_col in stats_cols:
        if stat_col in df.columns:
            new_col_name = f"{stat_col}_per_36_min"
            df_normalized = df_normalized.withColumn(
                new_col_name,
                when(col(minutes_played_col) == 0, 0.0)
                .otherwise(round((col(stat_col) / col(minutes_played_col)) * 36, 2))
            )
        else:
            print(f"Attenzione: la colonna statistica '{stat_col}' non è stata trovata.")
    return df_normalized

def add_per_game_metrics(df: DataFrame) -> DataFrame:
    """
    Aggiunge metriche per partita (minuti e punti) al DataFrame.

    Args:
        df (DataFrame): Il DataFrame di input che deve contenere le colonne 'g', 'mp', 'pts'.

    Returns:
        DataFrame: Un nuovo DataFrame con le colonne 'mp_per_game' e 'pts_per_game'.
    """
    print("Calcolo delle metriche 'per partita' (minuti e punti)...")
    
    # Calcola i minuti per partita (mp_per_game)
    df_with_metrics = df.withColumn("mp_per_game", 
                      when(col("g") > 0, col("mp") / col("g"))
                      .otherwise(0.0))
    
    # Calcola i punti per partita (pts_per_game)
    df_with_metrics = df_with_metrics.withColumn("pts_per_game",
                      when(col("g") > 0, col("pts") / col("g")) 
                      .otherwise(0.0))
    
    return df_with_metrics
