from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from pyspark.sql.window import Window

spark: SparkSession = (
    SparkSession.builder.appName('players')
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()
)

pattern = "[^a-zA-Z0-9 ]"
     
players = (
    spark
    .table('football_bronce.bronce_players')
    .select(
        (
            f.when(
                f.upper(f.col('id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('id'))
            .alias('id')
        ),

        f.col('team'),

        (
            f.when(
                f.upper(f.col('name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.regexp_replace(f.col('name'), pattern, ""))
            .alias('name')
        ),

    
        (
            f.when(
                f.upper(f.col('age')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('age').cast('int'))
            .alias('age')
        ),

    
        (
            f.when(
                f.upper(f.col('number')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('number'))
            .alias('number')
        ),

    
        (
            f.when(
                f.upper(f.col('position')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('position'))
            .alias('position')
        ),

    
        (
            f.when(
                f.upper(f.col('photo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('photo'))
            .alias('photo')
        )
    )
    .withColumn('team_player', f.concat('team','id'))
)

hudi_options = {
    "className": "org.apache.hudi",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.database": "football_silver",
    "hoodie.datasource.hive_sync.table": "silver_players",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.partition_fields": "team",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.table.name": "silver_players",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "team_player",
    "hoodie.datasource.write.partitionpath.field": "team",
    "hoodie.datasource.write.table.name": "silver_players",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.metadata.enable": False,
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.write.hive_style_partitioning": True,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.precombine.field": "team_player",
    "hoodie.upsert.shuffle.parallelism": 120,
    "hoodie.insert.shuffle.parallelism": 120,
    "hoodie.bulkinsert.shuffle.parallelism": 120,
    "hoodie.write.concurrency.mode": "single_writer",
    "hoodie.cleaner.policy.failed.writes": "EAGER"
}

path_s3 = 's3://football-hist-silver/silver_players/'
players.write.format("hudi").mode("append").options(**hudi_options).save(path_s3)