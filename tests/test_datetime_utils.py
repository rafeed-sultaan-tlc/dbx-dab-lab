# test_datetime_utils.py

import datetime
import src.utils.datetime_utils as datetime_utils



def test_timestamp_to_date_col(spark):
    # Define data as a list of tuples
    # String format 'yyyy-MM-dd HH:mm:ss' works perfectly with the 'timestamp' type
    data = [(datetime.datetime(2025, 4, 10, 10, 30, 0),)]
    schema = "ride_timestamp timestamp"

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    # Run the transformation
    result_df = datetime_utils.timestamp_to_date_col(df, "ride_timestamp", "ride_date")

    # Collect and Assert
    row = result_df.select("ride_date").collect()[0]
    expected_date = datetime.date(2025, 4, 10)
    
    # Using row["ride_date"] or row[0] both work
    assert row["ride_date"] == expected_date