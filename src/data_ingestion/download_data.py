"""
Modulo per il download del dataset NBA da Kaggle.

Questo script contiene la logica per autenticarsi all'API di Kaggle e
scaricare il dataset necessario per l'analisi, decomprimendolo nella
directory specificata.
"""
import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_nba_dataset(save_path: str):
    """
    Scarica e decomprime il dataset "nba-aba-baa-stats" da Kaggle.

    Utilizza l'API di Kaggle per scaricare i file del dataset e li estrae
    nel percorso di destinazione. La funzione crea la cartella di destinazione
    se questa non esiste.

    Args:
        save_path (str): Il percorso della cartella dove salvare e decomprimere
                         i file del dataset.

    Raises:
        Exception: Solleva un'eccezione se si verifica un errore durante
                   l'autenticazione all'API di Kaggle o il download.
    """
    print(f"Tentativo di download del dataset NBA in '{save_path}'...")
    os.makedirs(save_path, exist_ok=True)

    try:
        # Inizializza l'API di Kaggle e si autentica
        api = KaggleApi()
        api.authenticate()
        
        # Scarica e decomprime i file
        api.dataset_download_files(
            "sumitrodatta/nba-aba-baa-stats",
            path=save_path,
            unzip=True
        )
        print("Download e decompressione completati con successo.")
    except Exception as e:
        print(f"Errore critico durante il download da Kaggle: {e}")
        print("Assicurarsi che il file 'kaggle.json' sia configurato correttamente.")
        raise e

if __name__ == "__main__":
    # Esempio di esecuzione per testare il download
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    
    # Controlla se il file principale esiste per evitare download ripetuti
    if not os.path.exists(os.path.join(raw_data_dir, "Player Totals.csv")):
         print("File del dataset non trovato. Avvio del download...")
         download_nba_dataset(raw_data_dir)
    else:
        print("Il dataset sembra essere già presente in locale. Nessun download necessario.")