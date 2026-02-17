# test_citibike_utils.py

import datetime
from src.citibike.citibike_utils import get_trip_duration_mins

def test_get_trip_duration_mins(spark):

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