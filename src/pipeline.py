from utils.spark_session import create_spark_session
from extract.extractor import read_csv
from transform.transformer import (
    clean_and_calculate_metrics, 
    enrich_operator_context, 
    build_fact_table
)
from load.loader import load_data
from utils.helper import enforce_schema
from utils.schema import fact_production_schema


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    try:
        logging.info("Starting Manufacturing Data Pipeline...")
        spark = create_spark_session("Manufacturing Data Pipeline")

        logging.info("Reading raw data...")
        mnt_df = read_csv(spark, "./data/raw/maintenance_events.csv", "maintenance")
        mfg_df = read_csv(spark, "./data/raw/manufacturing_factory_dataset.csv", "manufacturing")
        ops_df = read_csv(spark, "./data/raw/operators_roster.csv", "operator")

        logging.info("Transforming and enriching data...")
        cleaned_mfg_df = clean_and_calculate_metrics(mfg_df)
        enrich_df = enrich_operator_context(cleaned_mfg_df, ops_df)
        
        logging.info("Building fact table...")
        fact_df = build_fact_table(enrich_df, mnt_df)

        logging.info("Enforcing final schema...")
        final_fact_df = enforce_schema(fact_df, fact_production_schema)

        logging.info("Loading final data...")
        load_data(final_fact_df, "./data/output/fact_production_performance", file_format="parquet")

        logging.info("Manufacturing data pipeline completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()