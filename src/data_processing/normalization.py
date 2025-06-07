"""
Funzioni per la normalizzazione delle statistiche dei giocatori.

Questo modulo contiene la logica per convertire le statistiche totali
di un giocatore in medie "per 36 minuti", permettendo un confronto più
equo tra giocatori con minutaggi differenti (es. titolari vs. riserve).
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, when

def per_36_minutes_stats(df: DataFrame, stats_cols: list, minutes_played_col: str = "mp") -> DataFrame:
    """
    Normalizza le statistiche di un giocatore su una base di 36 minuti.

    La formula utilizzata è: (Statistica / Minuti Giocati) * 36.
    Questo proietta la produzione di un giocatore su un tempo di gioco standard,
    facilitando i confronti.

    Args:
        df (DataFrame): Il DataFrame di input.
        stats_cols (list): Una lista dei nomi delle colonne da normalizzare
                           (es. ['pts', 'ast', 'trb']).
        minutes_played_col (str): Il nome della colonna che contiene i minuti
                                  totali giocati.

    Returns:
        DataFrame: Un nuovo DataFrame con le colonne normalizzate aggiunte.
                   Le nuove colonne avranno il suffisso '_per_36_min'.
    """
    print(f"Normalizzazione 'per 36 minuti' per le colonne: {stats_cols}")
    df_normalized = df
    if minutes_played_col not in df.columns:
        print(f"Attenzione: la colonna dei minuti '{minutes_played_col}' non è stata trovata.")
        return df

    for stat_col in stats_cols:
        if stat_col in df.columns:
            new_col_name = f"{stat_col}_per_36_min"
            # Calcola la statistica per 36 minuti, gestendo il caso di minuti giocati pari a zero
            df_normalized = df_normalized.withColumn(
                new_col_name,
                when(col(minutes_played_col) == 0, 0.0)
                .otherwise(round((col(stat_col) / col(minutes_played_col)) * 36, 2))
            )
        else:
            print(f"Attenzione: la colonna statistica '{stat_col}' non è stata trovata.")
    return df_normalized