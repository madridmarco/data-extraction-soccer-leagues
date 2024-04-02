from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from pyspark.sql.window import Window

spark: SparkSession = (
    SparkSession.builder.appName('leagues')
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()
)

def clear_text(col):
    pattern = "[^a-zA-Z0-9 ]"
    return f.regexp_replace(f.col(col), pattern, "").alias(col)

season = '2023'

statistics_played_season = (
    spark
    .table('football_bronce.bronce_matchday_statistics')
    .filter(f.col('season') == season)
    .select( 
        (
            f.when(
                f.upper(f.col('fixture_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_id').cast('int64'))
            .alias('fixture_id')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_referee')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_referee').cast('object'))
            .alias('fixture_referee')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_timezone')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_timezone').cast('object'))
            .alias('fixture_timezone')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_date')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_date').cast('object'))
            .alias('fixture_date')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_timestamp')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_timestamp').cast('int64'))
            .alias('fixture_timestamp')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_periods_first')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_periods_first').cast('float64'))
            .alias('fixture_periods_first')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_periods_second')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_periods_second').cast('float64'))
            .alias('fixture_periods_second')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_venue_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_venue_id').cast('float64'))
            .alias('fixture_venue_id')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_venue_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_venue_name').cast('object'))
            .alias('fixture_venue_name')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_venue_city')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_venue_city').cast('object'))
            .alias('fixture_venue_city')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_status_long')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_status_long').cast('object'))
            .alias('fixture_status_long')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_status_short')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_status_short').cast('object'))
            .alias('fixture_status_short')
        ),

    
        (
            f.when(
                f.upper(f.col('fixture_status_elapsed')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixture_status_elapsed').cast('float64'))
            .alias('fixture_status_elapsed')
        ),

    
        (
            f.when(
                f.upper(f.col('league_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_id').cast('int64'))
            .alias('league_id')
        ),

    
        (
            f.when(
                f.upper(f.col('league_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_name').cast('object'))
            .alias('league_name')
        ),

    
        (
            f.when(
                f.upper(f.col('league_country')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_country').cast('object'))
            .alias('league_country')
        ),

    
        (
            f.when(
                f.upper(f.col('league_logo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_logo').cast('object'))
            .alias('league_logo')
        ),

    
        (
            f.when(
                f.upper(f.col('league_flag')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_flag').cast('object'))
            .alias('league_flag')
        ),

    
        (
            f.when(
                f.upper(f.col('league_season')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_season').cast('int64'))
            .alias('league_season')
        ),

    
        (
            f.when(
                f.upper(f.col('league_round')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('league_round').cast('object'))
            .alias('league_round')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_home_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_home_id').cast('int64'))
            .alias('teams_home_id')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_home_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_home_name').cast('object'))
            .alias('teams_home_name')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_home_logo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_home_logo').cast('object'))
            .alias('teams_home_logo')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_home_winner')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_home_winner').cast('object'))
            .alias('teams_home_winner')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_away_id')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_away_id').cast('int64'))
            .alias('teams_away_id')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_away_name')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_away_name').cast('object'))
            .alias('teams_away_name')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_away_logo')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_away_logo').cast('object'))
            .alias('teams_away_logo')
        ),

    
        (
            f.when(
                f.upper(f.col('teams_away_winner')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('teams_away_winner').cast('object'))
            .alias('teams_away_winner')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_home').cast('float64'))
            .alias('goals_home')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_away').cast('float64'))
            .alias('goals_away')
        ),

    
        (
            f.when(
                f.upper(f.col('score_halftime_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_halftime_home').cast('float64'))
            .alias('score_halftime_home')
        ),

    
        (
            f.when(
                f.upper(f.col('score_halftime_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_halftime_away').cast('float64'))
            .alias('score_halftime_away')
        ),

    
        (
            f.when(
                f.upper(f.col('score_fulltime_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_fulltime_home').cast('float64'))
            .alias('score_fulltime_home')
        ),

    
        (
            f.when(
                f.upper(f.col('score_fulltime_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_fulltime_away').cast('float64'))
            .alias('score_fulltime_away')
        ),

    
        (
            f.when(
                f.upper(f.col('score_extratime_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_extratime_home').cast('object'))
            .alias('score_extratime_home')
        ),

    
        (
            f.when(
                f.upper(f.col('score_extratime_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_extratime_away').cast('object'))
            .alias('score_extratime_away')
        ),

    
        (
            f.when(
                f.upper(f.col('score_penalty_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_penalty_home').cast('object'))
            .alias('score_penalty_home')
        ),

    
        (
            f.when(
                f.upper(f.col('score_penalty_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('score_penalty_away').cast('object'))
            .alias('score_penalty_away')
        ),

        f.col('season')
    )
)