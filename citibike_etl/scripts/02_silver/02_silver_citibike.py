import sys
from citibike.citibike_utils import get_trip_duration_mins
from utils.datetime_utils import timestamp_to_date_col
from pyspark.sql.functions import create_map, lit


# META-DATA
pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
#ENVIRONMENT SPECIFIC
environment = sys.argv[5]
region = sys.argv[6]
catalog_name = f"citibike_{environment}"
# TABLES_NAMES
src_table_name = f'{catalog_name}.01_bronze.jc_citibike'
tgt_table_name = f'{catalog_name}.02_silver.jc_citibike'


df = spark.read.table(src_table_name)

df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
df = timestamp_to_date_col(df, "started_at", "trip_start_date")
df = df.withColumn('metadata', 
    create_map(
        lit('pipeline_id'), lit(pipeline_id),
        lit('run_id'), lit(run_id),
        lit('task_id'), lit(task_id),
        lit('processed_timestamp'), lit(processed_timestamp)
    )
)
df = df.select(
    "ride_id",
    "trip_start_date",
    "started_at",
    "ended_at",
    "start_station_name",
    "end_station_name",
    "trip_duration_mins",
    "metadata"
)

(df.write
    .mode('overwrite')
    .option("overwriteSchema","true")
    .saveAsTable(tgt_table_name)
)