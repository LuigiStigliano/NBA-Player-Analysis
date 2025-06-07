"""
Funzioni di utilità per il progetto di analisi dei giocatori NBA.
"""
import os
from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str, driver_memory: str, master: str = "local[*]") -> SparkSession:
    """
    Inizializza e restituisce una sessione Spark, o ne recupera una esistente.
    """
    print(f"Creazione o recupero di SparkSession: {app_name} con master {master}")
    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.driver.memory", driver_memory)
             .getOrCreate())
    return spark

def save_dataframe(df: DataFrame, path: str, file_format: str = "parquet"):
    """
    Salva un DataFrame Spark in un percorso specificato usando le API native di Spark.
    """
    print(f"Salvataggio del DataFrame in '{path}' (formato: {file_format})...")
    
    output_dir = os.path.dirname(path)
    if not os.path.exists(output_dir):
         os.makedirs(output_dir)

    if file_format == "parquet":
        df.write.mode("overwrite").parquet(path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", "true").csv(path)
    else:
        raise ValueError(f"Formato file '{file_format}' non supportato.")
