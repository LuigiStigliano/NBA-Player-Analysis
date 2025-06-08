"""
Qui ho raggruppato alcune funzioni di utilità che uso in diverse parti del progetto.
Mi aiuta a non ripetere codice.
"""
import os
from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str, driver_memory: str, master: str = "local[*]") -> SparkSession:
    """
    Questa funzione mi serve per inizializzare Spark. Se c'è già una sessione attiva,
    la riutilizzo, altrimenti ne creo una nuova.
    """
    print(f"Creo o recupero una SparkSession: {app_name} con master {master}")
    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.driver.memory", driver_memory)
             .getOrCreate())
    return spark

def save_dataframe(df: DataFrame, path: str, file_format: str = "parquet"):
    """
    Uso questa funzione per salvare i DataFrame. Ho scelto Parquet come formato di default
    perché è molto efficiente con Spark.
    """
    print(f"Salvo il DataFrame in '{path}' (formato: {file_format})")
    
    output_dir = os.path.dirname(path)
    if not os.path.exists(output_dir):
         os.makedirs(output_dir)

    if file_format == "parquet":
        df.write.mode("overwrite").parquet(path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", "true").csv(path)
    else:
        raise ValueError(f"Il formato file '{file_format}' non è supportato.")