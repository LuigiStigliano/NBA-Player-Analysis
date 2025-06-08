"""
In questo modulo, ho messo il codice per generare i riassunti testuali dei cluster.
Volevo un output facile da leggere che descrivesse ogni profilo.
"""
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, format_number

def generate_cluster_summary(df_final_analysis: DataFrame, k: int, output_dir: str):
    """
    Questa funzione genera un riassunto testuale per ogni cluster.
    Per ogni gruppo, ho definito un profilo e una logica di ordinamento specifica
    per mostrare i giocatori più rappresentativi.
    Salvo tutto in un file .txt.
    """
    print("\n" + "="*50)
    print("ANALISI QUALITATIVA DEI CLUSTER")
    print("="*50)
    
    # Ho definito qui i nomi dei profili per ogni cluster.
    cluster_profiles = {
        1: "Ali Forti Moderne / Marcatori-Rimbalzisti",
        2: "Playmaker Puri / Organizzatori di Gioco",
        3: "Giocatori di Ruolo a Basso Utilizzo",
        4: "All-Around Stars / Motori Offensivi",
        5: "Giocatori Affidabili a Controllo Rischio",
        6: "Ancore Difensive / Specialisti del Canestro"
    }
    
    # Per ogni cluster, ho deciso qual è la statistica più importante
    # per far emergere i giocatori più rappresentativi.
    sort_logic = {
        1: {"column": "pts_per_36_min", "ascending": False},  # I marcatori li ordino per punti.
        2: {"column": "ast_per_36_min", "ascending": False},  # I playmaker per assist.
        3: {"column": "mp_per_game", "ascending": True},      # I giocatori a basso utilizzo per minuti giocati (dal più basso).
        4: {"column": "pts_per_36_min", "ascending": False},  # Le superstar per punti.
        5: {"column": "tov_per_36_min", "ascending": True},   # I giocatori affidabili per palle perse (dal più basso).
        6: {"column": "blk_per_36_min", "ascending": False}   # Le ancore difensive per stoppate.
    }

    os.makedirs(output_dir, exist_ok=True)
    summary_file_path = os.path.join(output_dir, "cluster_summary.txt")

    with open(summary_file_path, "w", encoding='utf-8') as f:
        f.write("ANALISI CLUSTER NBA - PROFILI GIOCATORI\n")
        f.write("="*50 + "\n")

        for i in range(1, k + 1):
            profile = cluster_profiles.get(i, f"Profilo Sconosciuto {i}")
            
            # Applico la logica di ordinamento che ho definito sopra.
            sort_info = sort_logic.get(i, {"column": "pts_per_36_min", "ascending": False})
            order_col = sort_info["column"]
            is_ascending = sort_info["ascending"]

            top_players_df = df_final_analysis.filter(col('cluster_id') == i) \
                                              .orderBy(col(order_col).asc() if is_ascending else col(order_col).desc()) \
                                              .limit(5)
            
            top_players = top_players_df.collect()

            # Costruisco la stringa di output per il terminale e per il file.
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

    print(f"\nRiassunto qualitativo salvato con successo in '{summary_file_path}'")