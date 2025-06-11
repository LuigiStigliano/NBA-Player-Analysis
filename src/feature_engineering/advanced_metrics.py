"""
In questo modulo, ho implementato la funzione per calcolare una metrica statistica 
più avanzata. Volevo andare oltre le statistiche di base per catturare 
meglio la performance di un giocatore.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def calculate_true_shooting_percentage(df: DataFrame,
                                       points_col: str = "pts",
                                       fga_col: str = "fga",
                                       fta_col: str = "fta") -> DataFrame:
    """
    Ho scelto di calcolare il True Shooting Percentage (TS%) perché è una metrica di 
    efficienza al tiro molto più completa della semplice percentuale dal campo.
    Tiene conto sia dei tiri da tre punti che dei tiri liberi.
    La formula che uso è: TS% = Punti / (2 * (Tiri dal Campo Tentati + 0.44 * Tiri Liberi Tentati)).
    """
    print("Calcolo il True Shooting Percentage (TS%)")
    
    # Il denominatore rappresenta il numero di "possessi di tiro".
    denominator = (2 * (col(fga_col) + 0.44 * col(fta_col)))
    
    # Aggiungo la nuova colonna, gestendo anche il caso di divisione per zero.
    df_with_ts = df.withColumn(
        "ts_pct_calc",
        when(denominator == 0, 0.0)
        .otherwise(col(points_col) / denominator)
    )
    return df_with_ts