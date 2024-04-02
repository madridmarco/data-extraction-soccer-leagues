from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as f 
from pyspark.sql.window import Window
from itertools import product
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import relativedelta

spark: SparkSession = (
    SparkSession.builder.appName('leagues')
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()
)

season = '2023'

def clear_text(col):
    pattern = "[^a-zA-Z0-9 ]"
    return f.regexp_replace(f.col(col), pattern, "").alias(col)

leagues = (
    spark
    .table('football_bronce.bronce_leagues')
    .filter(f.col('season') == season)
    .select(
        f.col('team_id'),
        clear_text('team_name'),
        clear_text('team_code'),
        clear_text('team_country'),
        f.col('team_founded'),
        f.col('team_national'),
        f.col('team_logo'),
        f.col('venue_id'),
        clear_text('venue_name'),
        clear_text('venue_address'),
        clear_text('venue_city'),
        f.col('venue_capacity'),
        clear_text('venue_surface'),
        f.col('venue_image'),
        f.col('season')
    )
)

hudi_options = {
    "className": "org.apache.hudi",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.database": "football_silver",
    "hoodie.datasource.hive_sync.table": "silver_leagues",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.partition_fields": "season",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.table.name": "silver_leagues",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "team_id",
    "hoodie.datasource.write.partitionpath.field": "season",
    "hoodie.datasource.write.table.name": "silver_leagues",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.metadata.enable": False,
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.write.hive_style_partitioning": True,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.precombine.field": "team_id",
    "hoodie.upsert.shuffle.parallelism": 120,
    "hoodie.insert.shuffle.parallelism": 120,
    "hoodie.bulkinsert.shuffle.parallelism": 120,
    "hoodie.write.concurrency.mode": "single_writer",
    "hoodie.cleaner.policy.failed.writes": "EAGER"
}

path_s3 = 's3://football-hist-silver/silver_leagues/'
leagues.write.format("hudi").mode("append").options(**hudi_options).save(path_s3)