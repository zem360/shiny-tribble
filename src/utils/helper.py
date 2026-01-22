from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Generic helper to align a DataFrame with a target StructType.
    
    Parameters:
    - df: Input DataFrame
    - schema: Target StructType schema

    Returns:
    - DataFrame aligned to the target schema
    """

    expected_fields = schema.fields
    
    select_exprs = []
    for field in expected_fields:
        if field.name in df.columns:
            select_exprs.append(F.col(field.name).cast(field.dataType))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
            
    return df.select(*select_exprs)