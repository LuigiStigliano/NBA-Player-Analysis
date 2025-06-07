"""
Modulo per la generazione di output qualitativi e riassunti dell'analisi.
"""
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, format_number

def generate_cluster_summary(df_clustered: DataFrame, df_full_stats: DataFrame, k: int, output_dir: str):
    """
    Genera un riassunto qualitativo dei cluster con ordinamento specifico per profilo,
    mostrando caratteristiche e giocatori rappresentativi. Salva il riassunto in un file .txt.

    Args:
        df_clustered (DataFrame): DataFrame con i cluster assegnati.
        df_full_stats (DataFrame): DataFrame con tutte le statistiche, incluse quelle "per partita".
        k (int): Il numero di cluster generati.
        output_dir (str): La cartella dove salvare il file di riassunto.
    """
    print("\n" + "="*50)
    print("ANALISI QUALITATIVA DEI CLUSTER (CON ORDINAMENTO SPECIFICO)")
    print("="*50)
    
    cluster_profiles = {
        0: "All-Around Stars / Creatori Primari",
        1: "Specialisti Difensivi e Rimbalzisti",
        2: "Playmaker Puri / Organizzatori di Gioco",
        3: "Giocatori di Ruolo a Basso Utilizzo",
        4: "Giocatori di Ruolo a Controllo Rischio",
        5: "Finalizzatori Interni ed Efficienti"
    }
    
    # Logica di ordinamento definitiva e specifica per ogni cluster
    sort_logic = {
        0: {"column": "pts_per_36_min", "ascending": False},  # All-Around Stars per punti (DESC)
        1: {"column": "trb_per_36_min", "ascending": False},  # Specialisti Difensivi per rimbalzi (DESC)
        2: {"column": "ast_per_36_min", "ascending": False},  # Playmaker per assist (DESC)
        3: {"column": "mp_per_game", "ascending": True},      # Basso Utilizzo per minuti (ASC - meno minuti)
        4: {"column": "tov_per_36_min", "ascending": True},   # Controllo Rischio per pochi turnover (ASC)
        5: {"column": "ts_pct_calc", "ascending": False}      # Finalizzatori per efficienza (DESC)
    }

    os.makedirs(output_dir, exist_ok=True)
    summary_file_path = os.path.join(output_dir, "cluster_summary.txt")

    # Unisce i risultati del clustering con il DataFrame che contiene TUTTE le statistiche
    df_with_stats = df_full_stats.join(
        df_clustered.select("player", "cluster_id"),
        "player",
        "inner"
    )

    with open(summary_file_path, "w", encoding='utf-8') as f:
        f.write("ANALISI CLUSTER NBA - PROFILI GIOCATORI (ORDINAMENTO DEFINITIVO)\n")
        f.write("="*50 + "\n")

        for i in range(k):
            profile = cluster_profiles.get(i, f"Profilo Sconosciuto {i}")
            
            # Applica l'ordinamento corretto
            sort_info = sort_logic.get(i, {"column": "pts_per_36_min", "ascending": False})
            order_col = sort_info["column"]
            is_ascending = sort_info["ascending"]

            top_players_df = df_with_stats.filter(col('cluster_id') == i) \
                                          .orderBy(col(order_col).asc() if is_ascending else col(order_col).desc()) \
                                          .limit(5)
            
            top_players = top_players_df.collect()

            # Costruisce la stringa di output per il terminale e per il file
            summary_str = f"\n--- CLUSTER {i}: {profile} (Ordinato per: {order_col} {'ASC' if is_ascending else 'DESC'}) ---\n"
            summary_str += "    Giocatori Rappresentativi:\n"

            if not top_players:
                summary_str += "      Nessun giocatore trovato per questo cluster.\n"
            else:
                for idx, p in enumerate(top_players, 1):
                    summary_str += (f"      {idx}. {p['player']:<25} | "
                                    f"PTS: {p['pts_per_36_min']:.1f}, "
                                    f"REB: {p['trb_per_36_min']:.1f}, "
                                    f"AST: {p['ast_per_36_min']:.1f}, "
                                    f"TOV: {p['tov_per_36_min']:.1f}, "
                                    f"TS%: {p['ts_pct_calc']:.3f}, "
                                    f"MIN/G: {p['mp_per_game']:.1f}\n")
            
            print(summary_str)
            f.write(summary_str)

    print(f"\nRiassunto qualitativo aggiornato salvato con successo in '{summary_file_path}'")
