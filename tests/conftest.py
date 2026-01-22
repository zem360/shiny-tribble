import pytest
from src.utils.spark_session import create_spark_session

@pytest.fixture(scope="session")
def spark():
    """
    Initializes a SparkSession for the entire test.
    """

    spark_session = create_spark_session("Unit-Testing-Suite")
    
    yield spark_session
    
    spark_session.stop()