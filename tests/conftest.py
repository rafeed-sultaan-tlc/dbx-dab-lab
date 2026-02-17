import sys
import os
import pytest


#Run the test from the root directory
sys.path.append(os.getcwd())

@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
        except ImportError:        
            print("Neither Databricks Session or SparkSession are available")
    return spark 