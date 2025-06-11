import os
import pandas as pd

def generate_cluster_summary(engine, output_dir: str):
    """
    Con questa funzione, genero un riassunto testuale per ogni cluster.
    Applico una logica di ordinamento personalizzata per ciascuno,
    in modo da mostrare i giocatori più rappresentativi.
    """
    print("\n" + "="*50)
    print("ANALISI QUALITATIVA DEI CLUSTER")
    print("="*50)

    # Qui definisco la logica e le etichette, come facevo nei notebook.
    cluster_definitions = {
        1: "Ali Forti Moderne / Marcatori-Rimbalzisti",
        2: "Playmaker Puri / Organizzatori di Gioco",
        3: "Giocatori di Ruolo a Basso Utilizzo",
        4: "All-Around Stars / Motori Offensivi",
        5: "Giocatori Affidabili a Controllo Rischio",
        6: "Ancore Difensive / Specialisti del Canestro"
    }

    sort_logic = {
        1: {"column": "pts_per_36_min", "ascending": False},
        2: {"column": "ast_per_36_min", "ascending": False},
        3: {"column": "mp_per_game", "ascending": True},
        4: {"column": "pts_per_36_min", "ascending": False},
        5: {"column": "tov_per_36_min", "ascending": True},
        6: {"column": "blk_per_36_min", "ascending": False}
    }

    os.makedirs(output_dir, exist_ok=True)
    summary_file_path = os.path.join(output_dir, "riepilogo_cluster.txt")

    # Seleziono le colonne che mi servono per l'ordinamento e la visualizzazione.
    all_metrics = "player, season, tm, cluster_label, pts_per_36_min, ast_per_36_min, trb_per_36_min, tov_per_36_min, stl_per_36_min, blk_per_36_min, mp_per_game, ts_pct_calc"

    with open(summary_file_path, "w", encoding='utf-8') as f:
        f.write("ANALISI CLUSTER NBA - PROFILI GIOCATORI\n")
        f.write("="*50 + "\n")

        # Itero su ogni cluster che ho definito.
        for cluster_id, label in cluster_definitions.items():
            logic = sort_logic.get(cluster_id)
            if not logic:
                continue

            order_col = logic["column"]
            order_dir = "ASC" if logic["ascending"] else "DESC"
            
            # Costruisco una query specifica per questo cluster.
            query = f"""
                WITH RankedPlayers AS (
                    SELECT
                        {all_metrics},
                        ROW_NUMBER() OVER(PARTITION BY player, season ORDER BY (CASE WHEN tm = 'TOT' THEN 1 ELSE 2 END)) as rn
                    FROM nba_analytics.player_analysis_full -- Mi assicuro che esista una vista con tutte le colonne
                    WHERE cluster_label = '{label}'
                )
                SELECT * FROM RankedPlayers
                WHERE rn = 1
                ORDER BY {order_col} {order_dir}
                LIMIT 5;
            """

            try:
                # Eseguo la query per ottenere i top 5 giocatori per questo cluster.
                top_players_df = pd.read_sql(query, engine)
                
                summary_str = f"\n--- CLUSTER: {label} ---\n"
                summary_str += f"    Giocatori Rappresentativi (Ordinati per: {order_col} {order_dir}):\n"

                if top_players_df.empty:
                    summary_str += "      Nessun giocatore trovato per questo cluster.\n"
                else:
                    for _, row in top_players_df.iterrows():
                        # Mostro la metrica di ordinamento per chiarezza.
                        metric_value = row[order_col]
                        summary_str += (f"      - {row['player']:<25} ({row['season']}) | "
                                        f"{order_col.replace('_per_36_min', '').upper()}: {metric_value:.1f}, "
                                        f"TS%: {row['ts_pct_calc']:.3f}\n")
                
                print(summary_str)
                f.write(summary_str)

            except Exception as e:
                print(f"Errore durante l'elaborazione del cluster '{label}': {e}")
                f.write(f"\n--- Errore durante l'elaborazione del cluster: {label} ---\n")

    print(f"\nRiassunto qualitativo salvato con successo in '{summary_file_path}'")