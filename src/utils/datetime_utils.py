from pyspark.sql.functions import col, to_date


def timestamp_to_date_col(df, timestamp_col, output_col):
    """
    Convert a timestamp column to a date column in the given DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame containing the timestamp column.
    timestamp_col (str): The name of the column containing the timestamp data.
    date_col (str): The name of the new column to store the converted date data.

    Returns:
    DataFrame: A new DataFrame with the converted date column added.
    """
    return df.withColumn(output_col, to_date(col(timestamp_col)))