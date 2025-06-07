"""
Funzioni per il calcolo di metriche statistiche avanzate.

Questo modulo si concentra sulla creazione di nuove feature (metriche) che
possono catturare aspetti più sottili della performance di un giocatore
rispetto alle statistiche di base.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def calculate_true_shooting_percentage(df: DataFrame,
                                       points_col: str = "pts",
                                       fga_col: str = "fga",
                                       fta_col: str = "fta") -> DataFrame:
    """
    Calcola il True Shooting Percentage (TS%), una metrica di efficienza al tiro.

    A differenza della semplice percentuale dal campo (FG%), il TS% tiene conto
    del valore dei tiri da tre punti e dei tiri liberi, fornendo una misura
    più completa dell'efficienza realizzativa di un giocatore.
    La formula è: TS% = Punti / (2 * (Tiri dal Campo Tentati + 0.44 * Tiri Liberi Tentati)).

    Args:
        df (DataFrame): Il DataFrame di input.
        points_col (str): Nome della colonna dei punti totali (PTS).
        fga_col (str): Nome della colonna dei tiri dal campo tentati (FGA).
        fta_col (str): Nome della colonna dei tiri liberi tentati (FTA).

    Returns:
        DataFrame: Il DataFrame originale con una nuova colonna 'ts_pct_calc' aggiunta.
    """
    print("Calcolo del True Shooting Percentage (TS%)...")
    
    # Il denominatore rappresenta il numero di "possessi di tiro"
    denominator = (2 * (col(fga_col) + 0.44 * col(fta_col)))
    
    # Aggiunge la colonna calcolata, gestendo il caso di divisione per zero
    df_with_ts = df.withColumn(
        "ts_pct_calc",
        when(denominator == 0, 0.0)
        .otherwise(col(points_col) / denominator)
    )
    return df_with_ts