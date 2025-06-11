"""
Questo modulo contiene le funzioni che ho impiegato per normalizzare le statistiche
e creare nuove metriche utili per la mia analisi.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, when

def per_36_minutes_stats(df: DataFrame, stats_cols: list, minutes_played_col: str = "mp") -> DataFrame:
    """
    Ho deciso di normalizzare le statistiche su una base di 36 minuti. Questo mi permette 
    di confrontare in modo equo giocatori con minutaggi diversi, come titolari e riserve.
    """
    print(f"Normalizzo 'per 36 minuti' le colonne: {stats_cols}")
    df_normalized = df
    if minutes_played_col not in df.columns:
        print(f"Attenzione: la colonna dei minuti '{minutes_played_col}' non è stata trovata.")
        return df

    for stat_col in stats_cols:
        if stat_col in df.columns:
            new_col_name = f"{stat_col}_per_36_min"
            # La formula che uso è semplice: (statistica / minuti giocati) * 36.
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
    Ho aggiunto anche le metriche 'per partita', come minuti e punti, perché
    le trovo utili per avere un contesto rapido sulle performance di un giocatore.
    """
    print("Calcolo le metriche 'per partita' (minuti e punti)")
    
    # Calcolo i minuti per partita.
    df_with_metrics = df.withColumn("mp_per_game", 
                      when(col("g") > 0, col("mp") / col("g"))
                      .otherwise(0.0))
    
    # Calcolo i punti per partita.
    df_with_metrics = df_with_metrics.withColumn("pts_per_game",
                      when(col("g") > 0, col("pts") / col("g")) 
                      .otherwise(0.0))
    
    return df_with_metrics