from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    Creates or retrieves a SparkSession with environment-specific configs.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
