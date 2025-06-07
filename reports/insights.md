# Risultati dell'Analisi e Profili dei Giocatori NBA

## Introduzione

Questo documento riassume le principali scoperte emerse dall'analisi di clustering sulle statistiche dei giocatori NBA. L'obiettivo era identificare profili di giocatori distinti (stili di gioco) basandosi sulle loro performance statistiche. Abbiamo utilizzato un approccio basato su PySpark per l'elaborazione dei dati e l'algoritmo K-Means per il clustering.

## Metodologia

1.  **Preparazione dei Dati**: I dati storici sono stati puliti, filtrando per le stagioni dall'era del tiro da 3 punti (dal 1980 in poi) e per giocatori con un minutaggio significativo. Le statistiche sono state normalizzate "per 36 minuti" per consentire un confronto equo tra giocatori con minutaggi diversi.
2.  **Feature Engineering**: È stata calcolata la metrica avanzata **True Shooting Percentage (TS%)** per misurare l'efficienza realizzativa.
3.  **Clustering**: È stato applicato l'algoritmo K-Means sulle statistiche per 36 minuti e sul TS% dell'ultima stagione giocata da ogni giocatore. Dopo un'analisi con il metodo "Elbow", sono stati scelti **6 cluster**.

## Profili dei Cluster Identificati

Di seguito, una descrizione dettagliata di ciascuno dei 6 cluster identificati. I nomi sono stati assegnati in base alle caratteristiche statistiche medie di ogni gruppo.

*(Nota: L'assegnazione dei giocatori può variare leggermente a ogni esecuzione. I profili descritti si basano su un'esecuzione rappresentativa).*

### Cluster 0: All-Around Stars / Creatori Primari
* **Caratteristiche**: Questo gruppo eccelle in quasi tutte le categorie offensive. Registra i valori più alti di **punti** e **assist** per 36 minuti, indicando che sono i principali motori offensivi della squadra. Mantengono anche un'ottima efficienza (alto TS%) nonostante l'elevato volume di tiri.
* **Stile di Gioco**: Sono le superstar della lega, giocatori in grado di segnare in grande quantità e allo stesso tempo creare opportunità per i compagni. Gestiscono un alto numero di possessi e sono il fulcro del gioco offensivo.
* **Esempi di Giocatori**: LeBron James, Luka Dončić, James Harden.

### Cluster 1: Specialisti Difensivi e Rimbalzisti
* **Caratteristiche**: Dominano nelle categorie difensive. Hanno valori molto alti di **rimbalzi**, **stoppate** e **palle rubate** per 36 minuti. Il loro contributo offensivo (punti e assist) è limitato e l'efficienza al tiro è modesta.
* **Stile di Gioco**: Sono i "big men" o le ali forti il cui ruolo primario è proteggere il ferro, controllare i tabelloni e disturbare le linee di passaggio avversarie. L'attacco non è la loro specialità.
* **Esempi di Giocatori**: Rudy Gobert, Andre Drummond, Mitchell Robinson.

### Cluster 2: Playmaker Puri / Organizzatori di Gioco
* **Caratteristiche**: Questo gruppo eccelle nella distribuzione del gioco, registrando un alto numero di **assist** per 36 minuti, secondo solo alle "All-Around Stars". Hanno un buon numero di palle rubate, ma sono scarsi a rimbalzo e nelle stoppate. Il loro punteggio è moderato ma efficiente.
* **Stile di Gioco**: Sono le point guard tradizionali. Il loro compito principale è orchestrare l'attacco, controllare il ritmo del gioco e servire i compagni. Mantengono un basso numero di palle perse grazie al controllo superiore del pallone.
* **Esempi di Giocatori**: Chris Paul, Ricky Rubio, Tyus Jones.

### Cluster 3: Giocatori di Ruolo a Basso Utilizzo
* **Caratteristiche**: Questo è il cluster più numeroso e rappresenta i giocatori con il minor impatto statistico. Hanno i valori più bassi in quasi tutte le categorie: punti, rimbalzi, assist, palle rubate e stoppate.
* **Stile di Gioco**: Sono giocatori di rotazione o specialisti di nicchia (es. solo tiro da tre punti senza altre abilità di spicco) che riempiono il roster. Il loro ruolo è limitato e il loro utilizzo in campo è basso.
* **Esempi di Giocatori**: Giocatori di fondo panchina o specialisti difensivi perimetrali con scarso apporto offensivo.

### Cluster 4: Giocatori di Ruolo a Controllo Rischio
* **Caratteristiche**: Questo gruppo si distingue per il bassissimo numero di **palle perse (TOV)** per 36 minuti, indicando un controllo eccellente del pallone. Le loro statistiche offensive e difensive sono moderate, senza particolari eccellenze ma anche senza gravi debolezze.
* **Stile di Gioco**: Sono giocatori affidabili che raramente commettono errori. Spesso utilizzati in momenti cruciali dove la conservazione del possesso è fondamentale. Non sono creatori primari ma svolgono il loro ruolo senza rischi eccessivi.
* **Esempi di Giocatori**: Veterani esperti, specialisti nel controllo del tempo e giocatori "smart" con alto QI cestistico.

### Cluster 5: Finalizzatori Interni / Marcatori Efficienti
* **Caratteristiche**: Questi giocatori mostrano un profilo bilanciato con buoni punteggi, ottimi rimbalzi per il loro ruolo e un numero di assist discreto. La loro caratteristica distintiva è un'altissima efficienza al tiro (**TS%**), spesso la più alta tra tutti i cluster, derivante da tiri ad alta probabilità vicino al canestro.
* **Stile di Gioco**: Sono ali o lunghi moderni che giocano principalmente vicino al canestro, tagliando, ricevendo lob o giocando in post. Sono finalizzatori efficaci ma non i creatori primari di gioco. Combinano volume di tiri con efficienza eccezionale.
* **Esempi di Giocatori**: Giannis Antetokounmpo, Zion Williamson, Anthony Davis.

## Conclusioni

L'analisi di clustering ha raggruppato con successo i giocatori NBA in profili statisticamente coerenti e interpretabili, che riflettono i ruoli moderni del gioco. Dalle superstar complete ai giocatori di ruolo specializzati, le statistiche per 36 minuti e le metriche di efficienza si sono dimostrate efficaci nel differenziare gli stili di gioco. Questo tipo di analisi può essere utile per lo scouting, la valutazione dei giocatori e la costruzione di una squadra equilibrata.