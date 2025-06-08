# Analisi delle Prestazioni dei Giocatori NBA e Clustering per Stile di Gioco

In questo progetto, ho analizzato le statistiche storiche dei giocatori NBA per scoprire diversi profili di giocatori e capire come le loro statistiche definiscano il loro stile di gioco. Per farlo, ho utilizzato PySpark, che mi ha permesso di gestire una grande mole di dati e di applicare algoritmi di machine learning per il clustering.

## Dataset

Ho utilizzato il dataset "NBA ABA BAA Stats" che ho trovato su Kaggle: https://www.kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats.

**Nota:** Non devi scaricare il dataset manualmente. Ho configurato gli script per scaricarlo e decomprimerlo in automatico. Il file che uso per l'analisi è **`Player Totals.csv`**, che verrà posizionato nella cartella `data/raw/`.

## Setup

### 1. Clona il repository

```bash
git clone https://github.com/LuigiStigliano/NBA-Player-Analysis.git
cd NBA-Player-Analysis
```

### 2. Configura le tue credenziali API di Kaggle

Per scaricare i dati, hai bisogno di configurare le tue credenziali API di Kaggle. Il modo più semplice è mettere il file `kaggle.json` (che puoi scaricare dal tuo profilo Kaggle) in una di queste cartelle:

- `~/.kaggle/` (su Linux/macOS)
- `C:\Users\<Windows-username>\.kaggle\` (su Windows)

### 3. Crea un ambiente virtuale

```bash
# Su Windows
python -m venv .venv
.venv\Scripts\activate

# Su macOS/Linux
python3 -m venv .venv
source .venv/bin/activate
```

### 4. Installa le dipendenze

Ho elencato tutte le librerie necessarie nel file `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 5. Requisiti di Spark e Hadoop

Grazie all'inclusione di pyspark nel file `requirements.txt`, non è necessaria un'installazione separata di Apache Spark. La libreria gestirà autonomamente l'esecuzione in modalità locale.

#### Opzionale ma Raccomandato per Utenti Windows

Per evitare errori comuni su Windows legati a `winutils.exe`:

1. Scaricare il file `winutils.exe` corrispondente alla versione di Hadoop utilizzata da PySpark dal repository [winutils](https://github.com/cdarlint/winutils) (io ho utilizzato la versione 3.3.6)
2. Creare una cartella, ad esempio `C:\hadoop\bin`, e inserirvi il file `winutils.exe`
3. Impostare la variabile d'ambiente `HADOOP_HOME` in modo che punti alla cartella `C:\hadoop`

## Esecuzione

Puoi eseguire l'analisi in due modi.

### 1. Tramite Jupyter Notebooks

Per seguire passo dopo passo il mio processo di analisi, puoi eseguire i notebook che ho creato nella cartella `notebooks/`:

1. `01_data_ingestion_and_preparation.ipynb`
2. `02_exploratory_data_analysis.ipynb`
3. `03_advanced_metrics_calculation.ipynb`
4. `04_player_clustering.ipynb`
5. `05_cluster_analysis_and_visualization.ipynb`
6. `06_presentation_visuals.ipynb` (Opzionale, l'ho creato e usato per generare i grafici per la presentazione)

### 2. Tramite lo script principale

Se vuoi eseguire l'intera pipeline in una volta sola, puoi lanciare lo script `src/main.py`.

```bash
python src/main.py
```

## I miei obiettivi

- **Pulire e preparare** i dati storici, **filtrando le stagioni dal 1980 in poi** (l'era del tiro da 3 punti) per avere un'analisi più moderna
- **Fare un'analisi esplorativa** per trovare trend e leader statistici
- **Calcolare metriche avanzate** come il True Shooting Percentage
- **Normalizzare le statistiche** per 36 minuti, per poter confrontare i giocatori in modo equo
- **Usare l'algoritmo K-Means** per raggruppare i giocatori in base allo stile di gioco
- **Analizzare e visualizzare** le caratteristiche di ogni gruppo per definire i profili dei giocatori

## Risultati che ho ottenuto

Alla fine dell'analisi, ho prodotto:

- **Cluster di giocatori** che rappresentano diversi stili di gioco
- **Profili dettagliati** per ogni tipo di giocatore
- **Visualizzazioni** che mostrano le loro caratteristiche principali
- **Metriche avanzate** per valutare le loro performance