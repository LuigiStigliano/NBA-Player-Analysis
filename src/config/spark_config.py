import os

SPARK_CONFIG = {
    "driver_memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "executor_memory": os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
    "min_games_threshold": int(os.getenv("MIN_GAMES", "10"))
}
