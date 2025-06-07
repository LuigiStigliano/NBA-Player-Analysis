# Analisi delle Prestazioni dei Giocatori NBA e Clustering per Stile di Gioco

Questo progetto analizza le statistiche storiche dei giocatori NBA per identificare diversi profili di giocatori e comprendere come le loro statistiche definiscono il loro ruolo e stile di gioco. L'analisi utilizza PySpark per l'elaborazione di dati su larga scala e il machine learning per il clustering.

## Dataset

Il dataset utilizzato è "NBA ABA BAA Stats" disponibile su Kaggle: https://www.kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats.

**Nota bene:** Non è necessario scaricare manualmente il dataset. L'archivio verrà scaricato e decompresso automaticamente. Il file specifico utilizzato per l'analisi è **`Player Totals.csv`**. Sia lo script principale (`src/main.py`) sia il primo notebook (`01_data_ingestion_and_preparation.ipynb`) sono configurati per usare questo file, posizionato nella cartella `data/raw/`.

## Setup

### 1. Clonare il repository

```bash
git clone https://github.com/LuigiStigliano/NBA-Player-Analysis.git
cd NBA-Player-Analysis
```

### 2. Configurare le credenziali API di Kaggle

Per permettere il download automatico del dataset, è necessario configurare le proprie credenziali API di Kaggle. Il metodo standard consiste nel posizionare il file `kaggle.json` (scaricabile dal proprio profilo Kaggle) nella cartella:
- `~/.kaggle/` (su Linux/macOS) 
- `C:\Users\<Windows-username>\.kaggle\` (su Windows)

### 3. Creare un ambiente virtuale

```bash
# Su Windows
python -m venv .venv
.venv\Scripts\activate

# Su macOS/Linux
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Installare le dipendenze

Il file `requirements.txt` include tutte le librerie necessarie, compresa quella per l'interazione con Kaggle.

```bash
pip install -r requirements.txt
```

### 5. Configurare Spark

Assicurarsi di avere Apache Spark installato e configurato correttamente nel proprio sistema. Le variabili d'ambiente come `SPARK_HOME` e `PYSPARK_PYTHON` dovrebbero essere impostate.

## Esecuzione

L'analisi può essere eseguita in due modi:

### 1. Tramite Jupyter Notebooks (consigliato per l'esplorazione)

Eseguire sequenzialmente i notebook nella cartella `notebooks/` per seguire ogni passo dell'analisi, dalla preparazione dei dati alla visualizzazione dei cluster:

1. `01_data_ingestion_and_preparation.ipynb`
2. `02_exploratory_data_analysis.ipynb`
3. `03_advanced_metrics_calculation.ipynb`
4. `04_player_clustering.ipynb`
5. `05_cluster_analysis_and_visualization.ipynb`
6. `06_presentation_visuals.ipynb` (Opzionale, per generare i grafici per la presentazione)

### 2. Tramite lo script principale (per una pipeline automatizzata)

Lo script `src/main.py` esegue l'intera pipeline di analisi in un'unica esecuzione.

```bash
python src/main.py
```

## Obiettivi

- **Pulire e preparare** i dati storici dei giocatori NBA, **filtrando per le stagioni dall'era del tiro da 3 punti (dal 1980 in poi)** per un'analisi più moderna e coerente
- **Condurre un'analisi esplorativa** per identificare trend e leader statistici
- **Calcolare metriche avanzate** (es. True Shooting Percentage, PER semplificato)
- **Normalizzare le statistiche** (es. per 36 minuti) per un confronto equo
- **Applicare l'algoritmo di clustering K-Means** per raggruppare i giocatori in base allo stile di gioco
- **Analizzare e visualizzare** le caratteristiche distintive di ciascun cluster per definire i profili dei giocatori

## Risultati Attesi

Al termine dell'analisi, il progetto fornirà:

- **Cluster di giocatori** raggruppati per stile di gioco
- **Profile dettagliati** per ogni tipologia di giocatore
- **Visualizzazioni** delle caratteristiche distintive
- **Metriche avanzate** per la valutazione delle performance