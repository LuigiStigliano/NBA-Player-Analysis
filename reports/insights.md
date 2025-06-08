# I Risultati della mia Analisi e i Profili dei Giocatori che ho Scoperto

## Introduzione

In questo documento, ho riassunto le principali scoperte che ho fatto con la mia analisi di clustering sui giocatori NBA. Il mio obiettivo era quello di identificare dei profili di gioco distinti basandomi puramente sulle loro statistiche. Per farlo, ho usato PySpark per elaborare i dati e l'algoritmo K-Means per il clustering.

## Come ho Proceduto

1.  **Preparazione dei Dati**: Ho pulito i dati storici, concentrandomi sulle stagioni dal 1980 in poi (l'era del tiro da 3) e sui giocatori con un minutaggio significativo. Ho normalizzato le statistiche "per 36 minuti" per poter fare confronti equi.
2.  **Feature Engineering**: Ho calcolato la metrica avanzata **True Shooting Percentage (TS%)** per avere una misura migliore dell'efficienza al tiro.
3.  **Clustering**: Ho applicato K-Means sulle statistiche normalizzate e sul TS% dell'ultima stagione di ogni giocatore. Dopo aver usato il metodo "Elbow", ho deciso che **6 cluster** era il numero giusto per un buon equilibrio tra dettaglio e interpretabilità.

## I Profili dei Cluster che ho Identificato

Qui di seguito, descrivo in dettaglio ognuno dei 6 cluster che ho trovato. Ho assegnato i nomi basandomi sulle caratteristiche medie di ogni gruppo.

*(Nota: L'assegnazione dei giocatori può cambiare leggermente a ogni esecuzione. I profili che descrivo si basano su un'esecuzione rappresentativa dell'analisi.)*

### Cluster 1: Ali Forti Moderne / Marcatori-Rimbalzisti
* **Caratteristiche**: Questo gruppo è molto bilanciato, con **punti alti (15.5 per 36 min)**, **ottimi rimbalzi (9.2)** e un'eccellente efficienza al tiro (**TS% 0.536**).
* **Stile di Gioco**: Li vedo come ali o lunghi moderni che sanno segnare e allo stesso tempo essere presenti a rimbalzo. Sono finalizzatori efficaci.
* **Esempi di Giocatori**: Giannis Antetokounmpo, Zion Williamson, Pascal Siakam.

### Cluster 2: Playmaker Puri / Organizzatori di Gioco
* **Caratteristiche**: La loro forza è la distribuzione del gioco, con un **alto numero di assist (4.4)** e **palle rubate (1.7)**. Non segnano tantissimo (9.9 punti), ma gestiscono l'attacco.
* **Stile di Gioco**: Sono le point guard classiche. Il loro compito è orchestrare l'attacco e mettere i compagni in condizione di segnare.
* **Esempi di Giocatori**: Chris Paul, Tyus Jones, Mike Conley.

### Cluster 3: Giocatori di Ruolo a Basso Utilizzo
* **Caratteristiche**: Questo è il gruppo più numeroso e rappresenta i giocatori con l'impatto statistico più basso. Hanno valori bassi in quasi tutto, ma anche poche **palle perse (1.8)**.
* **Stile di Gioco**: Li considero giocatori di rotazione o specialisti di nicchia. Il loro ruolo è limitato e non stanno in campo per molti minuti.
* **Esempi di Giocatori**: I classici giocatori da fondo panchina o specialisti difensivi con poco apporto in attacco.

### Cluster 4: All-Around Stars / Motori Offensivi
* **Caratteristiche**: Questo gruppo è eccezionale in attacco. Hanno i valori più alti di **punti (15.9)** e **assist (5.7)**. Sono i veri motori offensivi delle loro squadre.
* **Stile di Gioco**: Sono le superstar della lega, giocatori capaci di segnare tanto e creare gioco per gli altri. Tutto l'attacco passa da loro.
* **Esempi di Giocatori**: Luka Dončić, James Harden, Trae Young.

### Cluster 5: Giocatori Affidabili a Controllo Rischio
* **Caratteristiche**: La loro caratteristica principale è il **bassissimo numero di palle perse (1.5)**, unito a un'alta efficienza **(TS% 0.533)**. Le altre statistiche sono nella media.
* **Stile di Gioco**: Sono giocatori solidi che raramente commettono errori. Sono quelli di cui ti fidi nei momenti cruciali. Non sono i creatori di gioco principali, ma fanno il loro compito senza rischi.
* **Esempi di Giocatori**: Veterani, tiratori specializzati e in generale giocatori con un alto QI cestistico.

### Cluster 6: Ancore Difensive / Specialisti del Canestro
* **Caratteristiche**: Dominano in difesa, con valori altissimi di **rimbalzi (9.5)** e **stoppate (2.0)**. Il loro contributo in attacco è limitato (10.7 punti).
* **Stile di Gioco**: Sono i "big men" il cui compito è proteggere il ferro e controllare i tabelloni. L'attacco non è la loro specialità.
* **Esempi di Giocatori**: Rudy Gobert, Mitchell Robinson, Walker Kessler.

## Le mie Conclusioni

Sono soddisfatto di come l'analisi di clustering sia riuscita a raggruppare i giocatori in profili coerenti e interpretabili, che rispecchiano i ruoli del basket moderno. Le statistiche normalizzate e le metriche di efficienza si sono rivelate efficaci nel differenziare gli stili di gioco. Penso che questo tipo di analisi possa essere davvero utile per lo scouting, la valutazione dei giocatori e la costruzione di una squadra.