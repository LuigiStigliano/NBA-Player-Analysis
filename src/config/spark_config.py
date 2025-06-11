import os

# Qui ho centralizzato alcune configurazioni che riutilizzo nel progetto.
# In questo modo, se devo apportare delle modifiche, le faccio solo in un unico posto.
SPARK_CONFIG = {
    # Ho impostato 4g di memoria per il driver di Spark, ma posso cambiarla con una variabile d'ambiente.
    "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
    # Ho deciso di considerare solo i giocatori con almeno 10 partite giocate.
    "min_games_threshold": int(os.getenv("MIN_GAMES", "10"))
}