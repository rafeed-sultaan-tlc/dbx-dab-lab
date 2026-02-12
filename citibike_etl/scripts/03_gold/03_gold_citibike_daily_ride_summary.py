from pyspark.sql.functions import max, min, avg, count, round
import sys


#ENVIRONMENT SPECIFIC
environment = sys.argv[5]
region = sys.argv[6]
catalog_name = f"citibike_{environment}"
#TABLE NAMES
src_table_name = f"{catalog_name}.02_silver.jc_citibike"
tgt_table_name = f"{catalog_name}.03_gold.daily_ride_summary"


df = spark.read.table(src_table_name)


df = df.groupBy("trip_start_date").agg(
    round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
    round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
    round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
    count("ride_id").alias("total_trips")
)


(df.write
    .mode('overwrite')
    .option("overwriteSchema","true")
    .saveAsTable(tgt_table_name)
)