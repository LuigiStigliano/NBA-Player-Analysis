"""
Questo è il modulo dove ho messo tutte le funzioni per il clustering.
Contiene la logica per preparare i dati, addestrare il modello K-Means
e valutare i risultati.
"""
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count

def prepare_features_for_clustering(df: DataFrame, feature_cols: list, output_col: str = "features_scaled") -> DataFrame:
    """
    Prima di fare il clustering, devo preparare le feature.
    Le assemblo in un unico vettore e poi le standardizzo (scaling).
    Lo scaling è importante perché K-Means è sensibile alla scala delle variabili,
    e non volevo che una feature dominasse le altre solo perché ha valori più grandi.
    """
    print(f"Preparo le feature per il clustering dalle colonne: {feature_cols}")

    # Assemblo le feature in un unico vettore.
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_assembled = assembler.transform(df)
    
    # Standardizzo le feature per avere media 0 e deviazione standard 1.
    scaler = StandardScaler(inputCol="features_raw", outputCol=output_col,
                            withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)

    return df_scaled

def train_kmeans_model(df_features: DataFrame, k: int, features_col: str = "features_scaled", seed: int = 42) -> KMeansModel:
    """
    Questa funzione addestra il modello K-Means.
    Ho usato un 'seed' per rendere i risultati riproducibili.
    """
    print(f"Addestro il modello K-Means con k={k}")
    kmeans = KMeans(featuresCol=features_col, k=k, seed=seed, predictionCol="cluster_id")
    model = kmeans.fit(df_features)
    return model

def assign_clusters(model: KMeansModel, df_features: DataFrame) -> DataFrame:
    """
    Una volta che il modello è addestrato, lo uso per assegnare ogni giocatore a un cluster.
    """
    print("Assegno i giocatori ai cluster")
    predictions = model.transform(df_features)
    return predictions

def evaluate_clustering(predictions: DataFrame, features_col: str = "features_scaled", prediction_col: str = "cluster_id") -> float:
    """
    Per valutare la qualità del clustering, ho usato il Silhouette Score.
    Questo punteggio mi dice quanto i cluster sono ben separati e coesi.
    Un valore vicino a 1 è ottimo.
    """
    print("Valuto il clustering (Silhouette Score)")
    evaluator = ClusteringEvaluator(featuresCol=features_col, predictionCol=prediction_col, metricName="silhouette")
    silhouette_score = evaluator.evaluate(predictions)
    return silhouette_score

def get_cluster_profiles(predictions: DataFrame, feature_cols: list) -> DataFrame:
    """
    Questa è una funzione chiave per interpretare i risultati.
    Calcolo le statistiche medie per ogni cluster, così posso capire
    cosa definisce ogni "stile di gioco".
    """
    print("Calcolo i profili medi per interpretare i cluster")
    agg_expressions = [avg(c).alias(f"avg_{c}") for c in feature_cols]
    agg_expressions.append(count("*").alias("num_players"))

    cluster_profiles = predictions.groupBy("cluster_id").agg(*agg_expressions).orderBy("cluster_id")
    return cluster_profiles