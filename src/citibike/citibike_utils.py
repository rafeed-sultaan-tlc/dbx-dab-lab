from pyspark.sql.functions import unix_timestamp, col

def get_trip_duration_mins(spark, df, start_col, end_col, output_col):
    """
    Calculate trip duration in minutes and add it as a new column to the DataFrame.

    Parameters:
    spark (SparkSession): The Spark session object.
    df (DataFrame): The input DataFrame containing the trip data.
    start_col (str): The name of the column containing the trip start time.
    end_col (str): The name of the column containing the trip end time.
    output_col (str): The name of the new column to store the trip duration in minutes.

    Returns:
    DataFrame: A new DataFrame with the trip duration in minutes added as a new column.
    """
    return df.withColumn(
        output_col,
        (unix_timestamp(col(end_col)) - unix_timestamp(col(start_col))) / 60
    )
