"""
Funzioni di utilità per il progetto di analisi dei giocatori NBA.

Questo modulo contiene funzioni helper per la gestione della sessione Spark
e per il salvataggio dei DataFrame.
"""
import os
from pyspark.sql import SparkSession, DataFrame

def get_spark_session(app_name: str = "NBAPlayerAnalysis", master: str = "local[*]") -> SparkSession:
    """
    Inizializza e restituisce una sessione Spark, o ne recupera una esistente.

    Args:
        app_name (str): Il nome da assegnare all'applicazione Spark.
        master (str): L'URL del master Spark. 'local[*]' è usato per l'esecuzione
                    locale utilizzando tutti i core disponibili.

    Returns:
        SparkSession: L'oggetto SparkSession configurato.
    """
    print(f"Creazione o recupero di SparkSession: {app_name} con master {master}")
    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    return spark

def save_dataframe(df: DataFrame, path: str, file_format: str = "parquet"):
    """
    Salva un DataFrame Spark in un percorso specificato.

    Questa funzione converte il DataFrame Spark in un DataFrame Pandas prima di
    salvarlo. Questo approccio è stato scelto per aggirare potenziali problemi di
    configurazione con `winutils.exe` su sistemi Windows, ma non è scalabile
    per dataset molto grandi.

    La funzione crea le cartelle di destinazione se non esistono.

    Args:
        df (DataFrame): Il DataFrame Spark da salvare.
        path (str): Il percorso completo (inclusa la cartella e il nome del file)
                    dove salvare il file.
        file_format (str): Il formato di salvataggio. Supporta 'parquet' (default)
                           e 'csv'.
    """
    print(f"Salvataggio del DataFrame in '{path}' (formato: {file_format})...")

    # Estrae il percorso della cartella e la crea se non esiste
    output_dir = os.path.dirname(path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Salva direttamente usando le API native di Spark, che operano in modo distribuito.
    # Spark creerà una cartella al percorso specificato contenente i file di dati partizionati.
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(path)
    elif file_format == "csv":
        # Per il CSV, è buona norma specificare l'header
        df.write.mode("overwrite").option("header", "true").csv(path)
    else:
        raise ValueError(f"Formato file '{file_format}' non supportato.")

if __name__ == '__main__':
    print("Test delle funzioni di utilità...")
    spark_session = get_spark_session("HelperTest")
    print(f"SparkSession creata con successo: {spark_session.sparkContext.appName}")
    spark_session.stop()
    print("Funzioni di utilità verificate.")
