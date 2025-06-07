"""
Funzioni per l'addestramento e la valutazione di modelli di clustering.

Questo modulo contiene le funzioni necessarie per eseguire il clustering K-Means
sui dati dei giocatori NBA, utilizzando la libreria MLlib di PySpark.
Le operazioni includono:
- Preparazione e scaling delle feature.
- Addestramento del modello K-Means.
- Assegnazione dei cluster ai giocatori.
- Valutazione della qualità del clustering.
- Calcolo dei profili medi per l'interpretazione dei cluster.
"""
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count

def prepare_features_for_clustering(df: DataFrame, feature_cols: list, output_col: str = "features_scaled") -> DataFrame:
    """
    Assembla e scala le feature per il modello di clustering.

    L'assemblaggio combina le colonne delle feature in un unico vettore.
    Lo scaling (StandardScaler) normalizza ogni feature per avere media 0 e
    deviazione standard 1, garantendo che nessuna feature domini le altre
    a causa della sua scala.

    Args:
        df (DataFrame): Il DataFrame di input contenente i dati dei giocatori.
        feature_cols (list): La lista dei nomi delle colonne da usare come feature.
        output_col (str): Il nome della colonna di output per le feature scalate.

    Returns:
        DataFrame: Il DataFrame con una colonna vettoriale di feature scalate.
    """
    print(f"Preparazione features per clustering dalle colonne: {feature_cols}")

    # Assembla le feature in un unico vettore denso
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    df_assembled = assembler.transform(df)
    
    # Scala le feature per normalizzarle
    scaler = StandardScaler(inputCol="features_raw", outputCol=output_col,
                            withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)

    return df_scaled

def train_kmeans_model(df_features: DataFrame, k: int, features_col: str = "features_scaled", seed: int = 42) -> KMeansModel:
    """
    Addestra un modello di clustering K-Means.

    Args:
        df_features (DataFrame): DataFrame con la colonna delle feature preparata.
        k (int): Il numero di cluster (gruppi) da creare.
        features_col (str): Il nome della colonna contenente le feature scalate.
        seed (int): Un seme per la riproducibilità dei risultati.

    Returns:
        KMeansModel: Il modello K-Means addestrato.
    """
    print(f"Addestramento del modello K-Means con k={k}...")
    kmeans = KMeans(featuresCol=features_col, k=k, seed=seed, predictionCol="cluster_id")
    model = kmeans.fit(df_features)
    return model

def assign_clusters(model: KMeansModel, df_features: DataFrame) -> DataFrame:
    """
    Assegna i giocatori ai cluster utilizzando un modello K-Means addestrato.

    Args:
        model (KMeansModel): Il modello K-Means addestrato.
        df_features (DataFrame): Il DataFrame con la colonna delle feature preparata.

    Returns:
        DataFrame: Il DataFrame originale con una colonna 'cluster_id' aggiunta.
    """
    print("Assegnazione dei giocatori ai cluster...")
    predictions = model.transform(df_features)
    return predictions

def evaluate_clustering(predictions: DataFrame, features_col: str = "features_scaled", prediction_col: str = "cluster_id") -> float:
    """
    Valuta la qualità del clustering usando il Silhouette Score.

    Il Silhouette Score misura quanto un oggetto sia simile al proprio cluster
    rispetto agli altri cluster. Un valore vicino a 1 indica un buon clustering.

    Args:
        predictions (DataFrame): Il DataFrame con le predizioni del cluster.
        features_col (str): Il nome della colonna delle feature.
        prediction_col (str): Il nome della colonna con l'ID del cluster predetto.

    Returns:
        float: Il punteggio Silhouette.
    """
    print("Valutazione del clustering (Silhouette Score)...")
    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol=prediction_col, metricName="silhouette")
    silhouette_score = evaluator.evaluate(predictions)
    return silhouette_score

def get_cluster_profiles(predictions: DataFrame, feature_cols: list) -> DataFrame:
    """
    Calcola le statistiche medie delle feature per ogni cluster.

    Questo è un passo cruciale per l'interpretazione: analizzando i valori medi
    di ogni feature, possiamo definire il "profilo" o lo "stile di gioco"
    tipico di ogni cluster.

    Args:
        predictions (DataFrame): DataFrame con le assegnazioni dei cluster.
        feature_cols (list): La lista originale dei nomi delle feature.

    Returns:
        DataFrame: Un DataFrame Spark con le medie per cluster e il conteggio
                   dei giocatori in ciascuno.
    """
    print("Calcolo dei profili medi per l'interpretazione dei cluster...")
    agg_expressions = [avg(c).alias(f"avg_{c}") for c in feature_cols]
    agg_expressions.append(count("*").alias("num_players"))

    cluster_profiles = predictions.groupBy("cluster_id").agg(*agg_expressions).orderBy("cluster_id")
    return cluster_profiles

def determine_optimal_k(df_features: DataFrame, k_range: range = range(2, 11), features_col: str = "features_scaled") -> int:
    """
    Determina il k ottimale usando il Silhouette Score.

    Itera su un intervallo di valori di k, calcola il Silhouette Score per ciascuno
    e restituisce il k che ha ottenuto il punteggio più alto.

    Args:
        df_features (DataFrame): DataFrame con la colonna delle feature preparata.
        k_range (range): L'intervallo di valori k da testare (default: da 2 a 10).
        features_col (str): Il nome della colonna delle feature.

    Returns:
        int: Il valore di k che massimizza il Silhouette Score.
    """
    print(f"Avvio della determinazione del k ottimale nell'intervallo {list(k_range)}...")
    silhouette_scores = []
    
    for k in k_range:
        print(f"  -> Valutazione per k={k}...")
        kmeans = KMeans(featuresCol=features_col, k=k, seed=42, predictionCol="cluster_id")
        model = kmeans.fit(df_features)
        
        predictions = model.transform(df_features)
        evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol="cluster_id", metricName="silhouette")
        silhouette = evaluator.evaluate(predictions)
        silhouette_scores.append(silhouette)
        print(f"     Silhouette Score: {silhouette:.4f}")

    # Sceglie il k che massimizza il Silhouette Score
    max_score = max(silhouette_scores)
    optimal_k = k_range[silhouette_scores.index(max_score)]
    print(f"\nValore ottimale di k determinato: {optimal_k} (Silhouette Score più alto: {max_score:.4f})")
    
    return optimal_k
