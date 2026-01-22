from pyspark.sql import functions as F
from pyspark.sql import Row 
from src.transform.transformer import clean_and_calculate_metrics, build_fact_table
from datetime import datetime

def test_clean_and_calculate_metrics_handles_zero_qty(spark):
    """Ensure scrap_rate calculation doesn't crash on zero production."""
    columns = [
        "timestamp", "factory_id", "line_id", "operator_id", 
        "product_id", "produced_qty", "scrap_qty", 
        "oee", "vibration_mm_s", "energy_kwh", "defect_type"
    ]

    data = [
        ("2026-01-01 08:00:00", "F1", "L1", "O1", "P1", 100, 5, 0.9, 2.0, 10.0, None),
        ("2026-01-01 08:15:00", "F1", "L1", "O1", "P1", 0, 0, 0.0, 0.0, 5.0, "None")
    ]
    
    df = spark.createDataFrame(data, columns)
    df = spark.createDataFrame(data, columns)
    
    result_df = clean_and_calculate_metrics(df)
    results = result_df.collect()
    
    assert results[0]["scrap_rate"] == 0.05
    assert results[1]["scrap_rate"] == 0.0 
    assert "timestamp" in result_df.columns


def test_build_fact_table(spark):
    factory_data = [
        Row(timestamp=datetime(2026, 1, 1, 10, 30), factory_id="F1", line_id="L1", 
            operator_id="O1", product_id="P1", produced_qty=100, scrap_qty=2, 
            oee=0.9, energy_kwh=15.0, vibration_mm_s=1.2, operator_skill="Senior"),
        Row(timestamp=datetime(2026, 1, 1, 14, 00), factory_id="F1", line_id="L1", 
            operator_id="O1", product_id="P1", produced_qty=100, scrap_qty=2, 
            oee=0.9, energy_kwh=15.0, vibration_mm_s=1.2, operator_skill="Senior")
    ]
    factory_df = spark.createDataFrame(factory_data)

    maintenance_data = [
        Row(event_id="M-99", factory_id="F1", line_id="L1", 
            start_time=datetime(2026, 1, 1, 10, 00), 
            end_time=datetime(2026, 1, 1, 11, 00), 
            maintenance_type="Preventive")
    ]
    maintenance_df = spark.createDataFrame(maintenance_data)

    result_df = build_fact_table(factory_df, maintenance_df)
    results = result_df.collect()

    assert len(results) == 2
    
    results_sorted = sorted(results, key=lambda x: x["timestamp"])
    
    assert results_sorted[0]["is_maintenance_active"] is True
    assert results_sorted[0]["maintenance_event_id"] == "M-99"
    assert results_sorted[0]["maintenance_type"] == "Preventive"
    assert results_sorted[0]["production_event_key"] is not None 
    
    assert results_sorted[1]["is_maintenance_active"] is False
    assert results_sorted[1]["maintenance_event_id"] is None
    assert results_sorted[1]["maintenance_type"] is None