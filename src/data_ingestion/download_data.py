"""
Ho creato questo modulo per gestire il download del dataset direttamente da Kaggle.
In questo modo, lo script è autonomo e non mi richiede di scaricare i dati manualmente.
"""
import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_nba_dataset(save_path: str):
    """
    Questa funzione si occupa di scaricare e decomprimere il dataset da Kaggle.
    Utilizza l'API di Kaggle, quindi ho bisogno che il mio file 'kaggle.json' sia configurato.
    """
    print(f"Provo a scaricare il dataset NBA in '{save_path}'")
    os.makedirs(save_path, exist_ok=True)

    try:
        # Inizializzo l'API di Kaggle e mi autentico.
        api = KaggleApi()
        api.authenticate()
        
        # Scarico e decomprimo i file.
        api.dataset_download_files(
            "sumitrodatta/nba-aba-baa-stats",
            path=save_path,
            unzip=True
        )
        print("Download e decompressione completati con successo.")
    except Exception as e:
        print(f"Errore durante il download da Kaggle: {e}")
        print("Mi assicuro che il file 'kaggle.json' sia configurato correttamente.")
        raise e

if __name__ == "__main__":
    # Ho aggiunto questo blocco per poter testare il download eseguendo direttamente questo script.
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    
    # Controllo se il file esiste già per non scaricarlo di nuovo inutilmente.
    if not os.path.exists(os.path.join(raw_data_dir, "Player Totals.csv")):
         print("File del dataset non trovato. Avvio del download")
         download_nba_dataset(raw_data_dir)
    else:
        print("Il dataset sembra essere già presente. Salto il download.")