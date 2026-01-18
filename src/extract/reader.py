from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from utils.schema import manufacturing_schema, maintenance_schema, operator_schema

def read_csv(spark: SparkSession, file_path: str, schema: StructType):
    """
    Reads a CSV file into a Spark DataFrame with the specified schema.
    
    Parameters:
    - spark: SparkSession object
    - file_path: Path to the CSV file
    - schema: One of 'manufacturing', 'maintenance', 'operator'
    
    Returns:
    - DataFrame with the specified schema
    """
    if schema == 'manufacturing':
        selected_schema = manufacturing_schema
    elif schema == 'maintenance':
        selected_schema = maintenance_schema
    elif schema == 'operator':
        selected_schema = operator_schema
    else:
        raise ValueError("Invalid schema type. Choose from 'manufacturing', 'maintenance', 'operator'.")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(selected_schema)
        .csv(file_path)
    )
    
    return df