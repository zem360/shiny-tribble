from pyspark.sql import functions as F

def clean_and_calculate_metrics(df_factory):
    """
    Cleans raw manufacturing data and calculates quality metrics.
    """
    return df_factory.withColumn(
        "defect_type", F.coalesce(F.col("defect_type"), F.lit("None"))
    ).withColumn(
        "scrap_rate", 
        F.when(F.col("produced_qty") > 0, F.col("scrap_qty") / F.col("produced_qty"))
        .otherwise(0.0)
    ).withColumn(
        "timestamp", F.to_timestamp("timestamp")
    ).select(
        "timestamp", "factory_id", "line_id", "operator_id", 
        "product_id", "produced_qty", "scrap_qty", "scrap_rate", 
        "oee", "vibration_mm_s", "energy_kwh"
    )

def enrich_operator_context(df_factory_cleaned, df_roster):
    """
    Joins with operator roster and calculates tenure-based metrics.
    """
    roster_enriched = df_roster.withColumn(
        "operator_tenure_days", 
        F.datediff(F.current_date(), F.to_date("hire_date"))
    )
    
    return df_factory_cleaned.join(
        roster_enriched, 
        on="operator_id", 
        how="left"
    ).select(
        df_factory_cleaned["*"],
        F.col("skill_level").alias("operator_skill"),
        F.col("operator_tenure_days")
    )

def build_fact_table(factory_df, maintenance_df):
    """
    Constructs the fact table by joining manufacturing data with operator 
    details and maintenance event intervals.
    """
    
    maintenance_cols = maintenance_df.select(
        F.col("event_id").alias("maintenance_event_id"),
        F.col("factory_id").alias("m_factory_id"),
        F.col("line_id").alias("m_line_id"),
        "start_time",
        "end_time",
        F.col("maintenance_type").alias("m_type")
    )

    fact_table = factory_df.join(
        maintenance_cols,
        (factory_df.factory_id == maintenance_cols.m_factory_id) & 
        (factory_df.line_id == maintenance_cols.m_line_id) & 
        (factory_df.timestamp >= maintenance_cols.start_time) & 
        (factory_df.timestamp <= maintenance_cols.end_time),
        how="left"
    )

    final_fact = fact_table.withColumn(
        "is_maintenance_active", 
        F.when(F.col("maintenance_event_id").isNotNull(), True).otherwise(False)
    ).withColumn(
        "production_event_key", 
        F.sha2(F.concat_ws("|", "timestamp", "line_id"), 256)
    ).select(
        "production_event_key",
        "timestamp",
        "factory_id",
        "line_id",
        "operator_id",
        "product_id",
        "produced_qty",
        "scrap_qty",
        "oee",
        "energy_kwh",
        "vibration_mm_s",
        "operator_skill",
        "is_maintenance_active",
        "maintenance_event_id",
        F.col("m_type").alias("maintenance_type")
    )

    return final_fact
