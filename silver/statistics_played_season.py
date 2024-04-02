from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from pyspark.sql.window import Window

spark: SparkSession = (
    SparkSession.builder.appName('players')
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()
)

season = '2023'
statistics_played_season = (
    spark
    .table('football_bronce.bronce_statistics_played_season')
    .filter(f.col('season') == season)
    .select( 
        (
            f.when(
                f.upper(f.col('team_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('team_id').cast('int'))
            .alias('team_id')
        ),

        (
            f.when(
                f.upper(f.col('team_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('team_name'))
            .alias('team_name')
        ),

    
        (
            f.when(
                f.upper(f.col('team_logo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('team_logo'))
            .alias('team_logo')
        ),

    
        (
            f.when(
                f.upper(f.col('league_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_id').cast('decimal(10,7)'))
            .alias('league_id')
        ),

    
        (
            f.when(
                f.upper(f.col('league_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_name'))
            .alias('league_name')
        ),

    
        (
            f.when(
                f.upper(f.col('league_country')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_country'))
            .alias('league_country')
        ),

    
        (
            f.when(
                f.upper(f.col('league_logo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_logo'))
            .alias('league_logo')
        ),

    
        (
            f.when(
                f.upper(f.col('league_flag')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_flag'))
            .alias('league_flag')
        ),

    
        (
            f.when(
                f.upper(f.col('league_season')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_season'))
            .alias('league_season')
        ),

    
        (
            f.when(
                f.upper(f.col('games_appearences')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_appearences').cast('decimal(10,7)'))
            .alias('games_appearences')
        ),

    
        (
            f.when(
                f.upper(f.col('games_lineups')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_lineups').cast('decimal(10,7)'))
            .alias('games_lineups')
        ),

    
        (
            f.when(
                f.upper(f.col('games_minutes')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_minutes').cast('decimal(10,7)'))
            .alias('games_minutes')
        ),

    
        (
            f.when(
                f.upper(f.col('games_number')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_number'))
            .alias('games_number')
        ),

    
        (
            f.when(
                f.upper(f.col('games_position')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_position'))
            .alias('games_position')
        ),

    
        (
            f.when(
                f.upper(f.col('games_rating')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_rating'))
            .alias('games_rating')
        ),

    
        (
            f.when(
                f.upper(f.col('games_captain')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('games_captain'))
            .alias('games_captain')
        ),

    
        (
            f.when(
                f.upper(f.col('substitutes_in')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('substitutes_in').cast('decimal(10,7)'))
            .alias('substitutes_in')
        ),

    
        (
            f.when(
                f.upper(f.col('substitutes_out')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('substitutes_out').cast('decimal(10,7)'))
            .alias('substitutes_out')
        ),

    
        (
            f.when(
                f.upper(f.col('substitutes_bench')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('substitutes_bench').cast('decimal(10,7)'))
            .alias('substitutes_bench')
        ),

    
        (
            f.when(
                f.upper(f.col('shots_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('shots_total').cast('decimal(10,7)'))
            .alias('shots_total')
        ),

    
        (
            f.when(
                f.upper(f.col('shots_on')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('shots_on').cast('decimal(10,7)'))
            .alias('shots_on')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_total').cast('decimal(10,7)'))
            .alias('goals_total')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_conceded')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_conceded').cast('decimal(10,7)'))
            .alias('goals_conceded')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_assists')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_assists').cast('decimal(10,7)'))
            .alias('goals_assists')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_saves')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_saves').cast('decimal(10,7)'))
            .alias('goals_saves')
        ),

    
        (
            f.when(
                f.upper(f.col('passes_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('passes_total').cast('decimal(10,7)'))
            .alias('passes_total')
        ),

        (
            f.when(
                f.upper(f.col('`player.id`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`player.id`'))
            .alias('player_id')
        ),

        (
            f.when(
                f.upper(f.col('`player.name`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`player.name`'))
            .alias('player_name')
        ),

        f.col('season')
    )
    .withColumn('rn',f.row_number().over(Window.partitionBy('team_id','league_id').orderBy(f.col('season'))))
    .withColumn('team_league', f.concat_ws('','rn','league_id','team_id'))
)

hudi_options = {
    "className": "org.apache.hudi",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.database": "football_silver",
    "hoodie.datasource.hive_sync.table": "statistics_played_season",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.partition_fields": "season",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.table.name": "statistics_played_season",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "team_league",
    "hoodie.datasource.write.partitionpath.field": "season",
    "hoodie.datasource.write.table.name": "statistics_played_season",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.metadata.enable": False,
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.write.hive_style_partitioning": True,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.precombine.field": "team_league",
    "hoodie.upsert.shuffle.parallelism": 120,
    "hoodie.insert.shuffle.parallelism": 120,
    "hoodie.bulkinsert.shuffle.parallelism": 120,
    "hoodie.write.concurrency.mode": "single_writer",
    "hoodie.cleaner.policy.failed.writes": "EAGER"
}

path_s3 = 's3://football-hist-silver/statistics_played_season/'
statistics_played_season.write.format("hudi").mode("append").options(**hudi_options).save(path_s3)