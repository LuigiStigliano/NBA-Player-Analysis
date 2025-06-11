"""
Qui ho raggruppato alcune funzioni di utilità che riutilizzo in diverse 
parti del progetto. In questo modo, evito di ripetere il codice.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import DataFrame, SparkSession
from ..config.db_config import DB_CONFIG

def get_spark_session(app_name: str, driver_memory: str, master: str = "local[*]") -> SparkSession:
    """
    Uso questa funzione per inizializzare Spark. Se trovo una sessione già attiva,
    la riutilizzo, altrimenti ne creo una nuova.
    """
    print(f"Creo o recupero una SparkSession: {app_name} con master {master}")
    spark = (SparkSession.builder
             .appName(app_name)
             .master(master)
             .config("spark.driver.memory", driver_memory)
             .getOrCreate())
    return spark

def save_dataframe(df: DataFrame, path: str):
    """
    Salva un DataFrame Spark in formato Parquet, sovrascrivendo se il file esiste già.
    """
    print(f"Salvo il DataFrame Spark in formato Parquet nel percorso: {path}")
    df.write.mode("overwrite").parquet(path)

def get_db_engine():
    """
    Con questa funzione, creo un motore di connessione SQLAlchemy per interagire 
    con PostgreSQL. Carico le credenziali in modo sicuro dalla configurazione.
    """
    db_url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
              f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    return create_engine(db_url)

def save_pd_to_db(df_pandas: pd.DataFrame, table_name: str, schema: str, engine):
    """
    Mi avvalgo di questa funzione per salvare i DataFrame di Pandas in una tabella del database.
    Per questo tipo di operazioni di inserimento, la trovo più efficiente rispetto a Spark.
    """
    print(f"Salvo il DataFrame Pandas nella tabella '{schema}.{table_name}'")
    try:
        df_pandas.to_sql(
            table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False
        )
        print(f"Salvataggio di {len(df_pandas)} righe completato con successo.")
    except Exception as e:
        print(f"Errore durante il salvataggio nella tabella '{schema}.{table_name}': {e}")
        raise e