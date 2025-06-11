"""
Qui ho raggruppato le funzioni che utilizzo per pulire e preparare i dati grezzi.
Considero la pulizia una fase fondamentale per assicurarmi che la mia 
analisi sia affidabile.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Ho creato questa funzione per standardizzare i nomi delle colonne.
    Li trasformo tutti in minuscolo e sostituisco caratteri speciali, 
    come '%' con '_pct'. In questo modo, mi risulta più facile lavorarci dopo.
    """
    print("Standardizzo i nomi delle colonne")
    new_columns = [c.lower().replace("%", "_pct") for c in df.columns]
    return df.toDF(*new_columns)

def correct_data_types(df: DataFrame) -> DataFrame:
    """
    Dato che i dati vengono caricati come stringhe, qui li converto nei tipi corretti.
    Utilizzo un cast sicuro: se un valore non è un numero (ad esempio, contiene 'NA'), 
    lo imposto a 0.0.
    """
    print("Correggo i tipi di dato per le colonne numeriche")
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
            # Se il valore è 'NA' o non può essere convertito in double, lo imposto a 0.0.
            df_casted = df_casted.withColumn(
                col_name,
                when(col(col_name) == 'NA', 0.0)
                .otherwise(col(col_name).cast("double"))
            ).na.fill(0.0, subset=[col_name]) # Gestisco anche eventuali valori nulli che potrebbero rimanere.

    return df_casted

def handle_missing_values(df: DataFrame, min_games_threshold: int) -> DataFrame:
    """
    In questa funzione, gestisco i valori mancanti e applico alcuni filtri
    per rendere i dati più coerenti e significativi per la mia analisi.
    """
    print(f"Gestisco i valori mancanti e applico i filtri (min partite giocate: {min_games_threshold})")
    
    # Filtro i dati per tenere solo le stagioni dall'era del tiro da 3 punti (dal 1980),
    # i giocatori con minuti giocati > 0 e che hanno disputato un numero minimo di partite.
    df_filtered = df.filter(
        (col("season") >= 1980) &
        (col("mp").isNotNull()) & 
        (col("mp") > 0) &
        (col("g") > min_games_threshold)
    )
    
    return df_filtered