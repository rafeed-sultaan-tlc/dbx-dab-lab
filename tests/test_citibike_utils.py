# test_citibike_utils.py
import os
import sys
sys.path.append(os.getcwd())

#Run the test from the root directory
import datetime
from src.citibike.citibike_utils import get_trip_duration_mins
from pyspark.sql import SparkSession


#Adjust the sys.path if needed (usually in conftest.py or at the top of your test files)


def test_get_trip_duration_mins():
    # Create a SparkSession for testing
    spark = SparkSession.builder.getOrCreate()

    data = [
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 10, 0)), #10 mins
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 30, 0)), #30 mins
    ]

    schema = "start_time timestamp, end_time timestamp"
    df = spark.createDataFrame(data, schema)

    # Apply the function to calculate trip duration in minutes
    result_df = get_trip_duration_mins(spark, df, "start_time", "end_time", "trip_duration_mins")

    # Collect the results for the assertions
    result = result_df.select("trip_duration_mins").collect()

    assert result[0]["trip_duration_mins"] == 10
    assert result[1]["trip_duration_mins"] == 30