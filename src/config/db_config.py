"""
Qui ho centralizzato la configurazione per la connessione al mio database PostgreSQL.
Utilizzo le variabili d'ambiente per evitare di esporre le credenziali nel codice.
"""
import os

DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "sys"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "nba_db")
}