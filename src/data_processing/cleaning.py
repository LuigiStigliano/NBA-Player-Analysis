"""
Funzioni per la pulizia e la preparazione dei dati grezzi dei giocatori NBA.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizza i nomi delle colonne del DataFrame.
    """
    print("Standardizzazione dei nomi delle colonne...")
    new_columns = [c.lower().replace("%", "_pct") for c in df.columns]
    return df.toDF(*new_columns)

def correct_data_types(df: DataFrame) -> DataFrame:
    """
    Converte le colonne numeriche al tipo di dato corretto (Double) usando un cast sicuro.
    I valori non numerici (incluso 'NA') vengono convertiti in 0.0.
    """
    print("Correzione dei tipi di dato per le colonne numeriche...")
    numeric_cols = [
        'g', 'gs', 'mp', 'per', 'ts_pct', '3par', 'ftr', 'orb_pct', 'drb_pct', 'trb_pct',
        'ast_pct', 'stl_pct', 'blk_pct', 'tov_pct', 'usg_pct', 'ows', 'dws', 'ws', 'ws/48',
        'obpm', 'dbpm', 'bpm', 'vorp', 'fg', 'fga', 'fg_pct', '3p', '3pa', '3p_pct',
        '2p', '2pa', '2p_pct', 'efg_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb',
        'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts'
    ]
    
    df_casted = df
    for col_name in numeric_cols:
        if col_name in df_casted.columns:
            # Cast sicuro: se il valore è 'NA' o non può essere convertito in double, imposta a 0.0
            # Altrimenti, esegui il cast a double.
            df_casted = df_casted.withColumn(
                col_name,
                when(col(col_name) == 'NA', 0.0)
                .otherwise(col(col_name).cast("double"))
            ).na.fill(0.0, subset=[col_name]) # Aggiunto na.fill per gestire eventuali nulli post-cast

    return df_casted

def handle_missing_values(df: DataFrame, min_games_threshold: int) -> DataFrame:
    """
    Gestisce i valori mancanti e filtra i record per coerenza.
    """
    print(f"Gestione dei valori mancanti e applicazione filtri (min partite giocate: {min_games_threshold})...")
    
    # Filtra per record rilevanti e statisticamente significativi
    df_filtered = df.filter(
        (col("season") >= 1980) &
        (col("mp").isNotNull()) & 
        (col("mp") > 0) &
        (col("g") > min_games_threshold) # Usa la soglia parametrizzata
    )
    
    return df_filtered
