"""
Modulo per la generazione di output qualitativi e riassunti dell'analisi.
"""
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def generate_cluster_summary(df_clustered: DataFrame, df_original_stats: DataFrame, k: int, output_dir: str):
    """
    Genera un riassunto qualitativo dei cluster, mostrando profili,
    caratteristiche e giocatori rappresentativi. Salva il riassunto in un file .txt.

    Args:
        df_clustered (DataFrame): DataFrame con i cluster assegnati.
        df_original_stats (DataFrame): DataFrame contenente le statistiche originali per il join.
        k (int): Il numero di cluster generati.
        output_dir (str): La cartella dove salvare il file di riassunto.
    """
    print("\n" + "="*50)
    print("ANALISI QUALITATIVA DEI CLUSTER")
    print("="*50)
    
    # Mappa dei profili e delle caratteristiche per ogni cluster.
    # Questi sono basati sull'analisi esplorativa fatta nei notebook.
    cluster_profiles = {
        0: "All-Around Stars / Creatori Primari",
        1: "Specialisti Difensivi e Rimbalzisti",
        2: "Playmaker Puri / Organizzatori di Gioco",
        3: "Giocatori di Ruolo a Basso Utilizzo",
        4: "Giocatori di Ruolo a Controllo Rischio",
        5: "Finalizzatori Interni ed Efficienti"
    }
    cluster_characteristics = {
        0: "Eccellenti in tutte le statistiche offensive, leader naturali.",
        1: "Dominano a rimbalzo e difesa, impatto oltre le statistiche.",
        2: "Maestri negli assist, pochissimi turnover, orchestrano il gioco.",
        3: "Contributo limitato ma costante, minutaggio ridotto.",
        4: "Affidabili, pochi errori, non perdono palloni.",
        5: "Altissima efficienza al tiro, finalizzatori letali."
    }
    
    # Logica di ordinamento specifica per identificare i giocatori più rappresentativi di ogni cluster
    sort_logic = {
        0: {"column": "pts_per_36_min", "ascending": False},  # All-Around Stars per punti
        1: {"column": "trb_per_36_min", "ascending": False},  # Specialisti Difensivi per rimbalzi
        2: {"column": "ast_per_36_min", "ascending": False},  # Playmaker per assist
        3: {"column": "pts_per_36_min", "ascending": True},   # Basso Utilizzo per minor impatto
        4: {"column": "tov_per_36_min", "ascending": True},   # Controllo Rischio per turnover più bassi
        5: {"column": "ts_pct_calc", "ascending": False}      # Finalizzatori per efficienza al tiro
    }

    # Prepara il percorso per il file di output
    os.makedirs(output_dir, exist_ok=True)
    summary_file_path = os.path.join(output_dir, "cluster_summary.txt")

    with open(summary_file_path, "w", encoding='utf-8') as f:
        f.write("ANALISI CLUSTER NBA - PROFILI GIOCATORI\n")
        f.write("="*50 + "\n\n")

        # Unisce i dati originali con i risultati del clustering
        # Selezioniamo solo le colonne necessarie per evitare ambiguità
        df_joined = df_clustered.select("player", "cluster_id", *sort_logic[0].keys()) # prende le colonne di sorting
        df_with_stats = df_original_stats.join(
            df_clustered.select("player", "cluster_id"),
            "player",
            "inner"
        )
        
        for i in range(k):
            profile = cluster_profiles.get(i, "Profilo Sconosciuto")
            characteristics = cluster_characteristics.get(i, "N/A")
            
            summary_str = f"\n--- CLUSTER {i}: {profile} ---\n"
            summary_str += f"    Caratteristiche: {characteristics}\n"
            summary_str += "    Giocatori Rappresentativi:\n"
            
            # Applica l'ordinamento corretto per il cluster corrente
            sort_info = sort_logic.get(i, {"column": "pts_per_36_min", "ascending": False})
            
            top_players_df = df_with_stats.filter(col('cluster_id') == i) \
                                          .orderBy(col(sort_info["column"]).desc() if not sort_info["ascending"] else col(sort_info["column"]).asc()) \
                                          .limit(5)
            
            top_players = top_players_df.collect()

            if not top_players:
                summary_str += "      Nessun giocatore trovato per questo cluster.\n"
            else:
                for idx, player_row in enumerate(top_players, 1):
                    summary_str += f"      {idx}. {player_row['player']}\n"
            
            print(summary_str)
            f.write(summary_str + "\n")

    print(f"\nRiassunto qualitativo salvato con successo in '{summary_file_path}'")
