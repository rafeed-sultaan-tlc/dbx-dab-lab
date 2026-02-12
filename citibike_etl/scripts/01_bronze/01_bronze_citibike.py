from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import create_map, lit, col 
import sys

# META-DATA
pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
#ENVIRONMENT SPECIFIC
environment = sys.argv[5]
region = sys.argv[6]
catalog_name = f"citibike_{environment}"
#TABLE_NAMES
src_data_path = f'/Volumes/{catalog_name}/00_landing/source_citibike_data/JC-202503-citibike-tripdata.csv'
tgt_table_name = f"{catalog_name}.01_bronze.jc_citibike"


schema = StructType([
    StructField("ride_id", StringType(), True), 
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),   
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DecimalType(10, 7), True),
    StructField("start_lng", DecimalType(10, 7), True),
    StructField("end_lat", DecimalType(10, 7), True),
    StructField("end_lng", DecimalType(10, 7), True),
    StructField("member_casual", StringType(), True)
])

df = spark.read.csv(src_data_path, header=True, schema=schema)


df = df.withColumn('metadata', 
    create_map(
        lit('pipeline_id'), lit(pipeline_id),
        lit('run_id'), lit(run_id),
        lit('task_id'), lit(task_id),
        lit('processed_timestamp'), lit(processed_timestamp)
    )
)

(df.write
    .mode('overwrite')
    .option("overwriteSchema","true")
    .saveAsTable(tgt_table_name)
)