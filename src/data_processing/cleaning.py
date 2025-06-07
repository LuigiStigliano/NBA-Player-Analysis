"""
Funzioni per la pulizia e la preparazione dei dati grezzi dei giocatori NBA.

Questo modulo fornisce una pipeline di funzioni per:
1.  Standardizzare i nomi delle colonne.
2.  Correggere i tipi di dato, convertendo le colonne numeriche in Double.
3.  Gestire i valori mancanti e applicare filtri per garantire la qualità
    e la coerenza dei dati per l'analisi successiva.
"""
from pyspark.sql import DataFrame, types
from pyspark.sql.functions import col, when

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizza i nomi delle colonne del DataFrame.

    Converte tutti i nomi in minuscolo e sostituisce caratteri speciali
    come '%' con '_pct' per renderli più facili da usare nel codice.

    Args:
        df (DataFrame): Il DataFrame di input.

    Returns:
        DataFrame: Un nuovo DataFrame con i nomi delle colonne standardizzati.
    """
    print("Standardizzazione dei nomi delle colonne...")
    new_columns = [c.lower().replace("%", "_pct") for c in df.columns]
    return df.toDF(*new_columns)

def correct_data_types(df: DataFrame) -> DataFrame:
    """
    Converte le colonne numeriche al tipo di dato corretto (Double).

    Itera su una lista predefinita di colonne statistiche e tenta di
    convertirle in DoubleType. Se un valore non è un numero valido,
    viene sostituito con NULL per essere gestito successivamente.

    Args:
        df (DataFrame): Il DataFrame con nomi di colonna standardizzati.

    Returns:
        DataFrame: Un nuovo DataFrame con i tipi di dato corretti.
    """
    print("Correzione dei tipi di dato per le colonne numeriche...")
    # Lista di colonne che dovrebbero essere numeriche
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
            # Converte la colonna in DoubleType solo se il valore è un formato numerico valido.
            # Altrimenti, imposta il valore a None (NULL in Spark).
            df_casted = df_casted.withColumn(
                col_name,
                when(col(col_name).rlike(r"^-?\d*\.?\d+$"), col(col_name).cast(types.DoubleType()))
                .otherwise(None)
            )
    return df_casted

def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Gestisce i valori mancanti e filtra i record per coerenza.

    - Riempie i valori NULL con 0.0 per le statistiche di base (punti, rimbalzi, etc.),
      assumendo che un'assenza di valore corrisponda a un'assenza di prestazione.
    - Filtra i dati per mantenere solo le stagioni dall'era del tiro da 3 punti (1980+),
      giocatori con minuti giocati > 0 e un numero minimo di partite giocate (g > 10)
      per garantire che l'analisi si basi su dati significativi.

    Args:
        df (DataFrame): Il DataFrame con i tipi di dato corretti.

    Returns:
        DataFrame: Un nuovo DataFrame pulito e filtrato.
    """
    print("Gestione dei valori mancanti e applicazione filtri di coerenza...")
    
    # Colonne per cui è ragionevole che NULL significhi zero (es. tiri non tentati).
    # Metriche complesse come 'per' o 'ws' sono escluse per non alterarle.
    cols_to_fill_zero = [
        'g', 'gs', 'mp', 'fg', 'fga', 'fg_pct', '3p', '3pa', '3p_pct',
        '2p', '2pa', '2p_pct', 'efg_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 
        'ast', 'stl', 'blk', 'tov', 'pf', 'pts'
    ]
    
    # Applica la sostituzione con 0 solo alle colonne definite
    safe_cols_to_fill = [c for c in cols_to_fill_zero if c in df.columns]
    df_filled = df.na.fill(0.0, subset=safe_cols_to_fill)
    
    # Applica filtri per mantenere solo record rilevanti e statisticamente significativi
    df_filtered = df_filled.filter(
        (col("season") >= 1980) &  # Analisi dall'era del tiro da 3 punti
        (col("mp").isNotNull()) & 
        (col("mp") > 0) &          # Esclude giocatori che non hanno mai giocato
        (col("g") > 10)            # Esclude "small sample size"
    )
    
    return df_filtered