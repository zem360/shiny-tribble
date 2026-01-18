from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates a SparkSession.

    Parameters:
    - app_name: Name of the Spark application
    
    Returns:
    - SparkSession object
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
