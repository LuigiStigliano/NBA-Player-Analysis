# Analisi delle Prestazioni dei Giocatori NBA e Clustering per Stile di Gioco

In questo progetto, ho analizzato le statistiche storiche dei giocatori NBA per scoprire diversi profili di giocatori e capire come le loro statistiche definiscano il loro stile di gioco. Per farlo, ho utilizzato PySpark, che mi ha permesso di gestire una grande mole di dati e di applicare algoritmi di machine learning per il clustering.

## Dataset

Ho utilizzato il dataset "NBA ABA BAA Stats" che ho trovato su Kaggle: https://www.kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats.

**Nota:** Non devi scaricare il dataset manualmente. Ho configurato gli script per scaricarlo e decomprimerlo in automatico. Il file che uso per l'analisi è **`Player Totals.csv`**, che verrà posizionato nella cartella `data`.

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

### 5. Configurazione dell'Ambiente e del Database (per src/main.py)

Lo script principale `src/main.py` è progettato per caricare i dati e i risultati dell'analisi in un database PostgreSQL. Per eseguirlo correttamente, è necessaria una configurazione preliminare.

#### Prerequisiti

Assicurati di avere un'istanza di PostgreSQL in esecuzione e accessibile. Lo script non creerà il database, quindi è necessario che esista già.

#### Creazione del file .env

Lo script utilizza un file `.env` per gestire le credenziali del database in modo sicuro, senza esporle nel codice.

Crea un file chiamato `.env` nella cartella principale del progetto (`NBA-Player-Analysis/.env`) e inserisci le tue credenziali, seguendo questo modello:

```env
# Credenziali per la connessione al database PostgreSQL
# Queste variabili vengono lette da src/config/db_config.py
DB_USER="il_tuo_username"
DB_PASSWORD="la_tua_password"
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="nba_db"
```

#### Creazione dello Schema e delle Tabelle

Prima di eseguire lo script, è necessario creare lo schema e le tabelle nel tuo database. Puoi utilizzare lo script SQL fornito nel repository:

`schema/nba_schema.sql`

Esegui questo script nel tuo database `nba_db` per preparare la struttura che ospiterà i dati.

Una volta completati questi passaggi, sei pronto per lanciare la pipeline completa con `python src/main.py`.

### 6. Requisiti di Spark e Hadoop

Grazie all'inclusione di `pyspark` nel file `requirements.txt`, non è necessaria un'installazione separata di Apache Spark. La libreria gestirà autonomamente l'esecuzione in modalità locale.

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

**Nota sull'Archiviazione dei Dati:**

Ho scelto due approcci diversi per la gestione dei dati, a seconda dello scopo:

- I Jupyter Notebooks utilizzano file Parquet (`.parquet`) per salvare i dati processati tra uno step e l'altro. Questo approche è ideale per l'analisi esplorativa e interattiva, poiché è veloce e non richiede la configurazione di un database.
- Lo script `main.py` è pensato come una pipeline automatizzata e carica tutti i risultati (dati grezzi, metriche e cluster) in un database PostgreSQL. Questo rende i dati finali persistenti, strutturati e accessibili per future analisi o applicazioni.

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

## Sviluppi Futuri

Questo progetto pone le basi per analisi ancora più avanzate. Ecco alcune direzioni future che ho pianificato per espandere questa ricerca:

1. **Analisi della Composizione delle Squadre Vincenti:** Espandere l'analisi dal singolo giocatore all'ecosistema di squadra, per identificare i "blueprint" dei roster che hanno avuto successo storicamente.

2. **Creazione di un Motore di Raccomandazione:** Sviluppare un algoritmo in grado di suggerire mosse di mercato, identificando i profili di giocatori mancanti in un roster per raggiungere un archetipo vincente.

3. **Sviluppo di un Sistema Predittivo:** Creare un modello avanzato per prevedere le performance di una squadra (es. numero di vittorie) basandosi sulla sua composizione di cluster, con applicazioni pratiche come simulatori di trade e ottimizzatori per la free agency.