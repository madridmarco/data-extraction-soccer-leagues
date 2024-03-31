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

teams = (
    spark
    .table('football_bronce.bronce_team_statistics')
    .filter(f.col('season') == season)
    .select(
        f.col('league'),
        f.col('team'),
 
        (
            f.when(
                f.upper(f.col('form')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('form'))
            .alias('form')
        ),

    
        (
            f.when(
                f.upper(f.col('lineups')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('lineups'))
            .alias('lineups')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.id`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.id`').cast('decimal(10,7)'))
            .alias('league__id')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.name`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.name`'))
            .alias('league__name')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.country`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.country`'))
            .alias('league__country')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.logo`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.logo`'))
            .alias('league__logo')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.flag`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.flag`'))
            .alias('league__flag')
        ),

    
        (
            f.when(
                f.upper(f.col('`league.season`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`league.season`').cast('decimal(10,7)'))
            .alias('league__season')
        ),

    
        (
            f.when(
                f.upper(f.col('`team.id`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`team.id`').cast('decimal(10,7)'))
            .alias('team__id')
        ),

    
        (
            f.when(
                f.upper(f.col('`team.name`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`team.name`'))
            .alias('team__name')
        ),

    
        (
            f.when(
                f.upper(f.col('`team.logo`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`team.logo`'))
            .alias('team__logo')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.played.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.played.home`').cast('decimal(10,7)'))
            .alias('fixtures__played__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.played.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.played.away`').cast('decimal(10,7)'))
            .alias('fixtures__played__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.played.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.played.total`').cast('decimal(10,7)'))
            .alias('fixtures__played__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.wins.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.wins.home`').cast('decimal(10,7)'))
            .alias('fixtures__wins__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.wins.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.wins.away`').cast('decimal(10,7)'))
            .alias('fixtures__wins__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.wins.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.wins.total`').cast('decimal(10,7)'))
            .alias('fixtures__wins__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.draws.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.draws.home`').cast('decimal(10,7)'))
            .alias('fixtures__draws__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.draws.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.draws.away`').cast('decimal(10,7)'))
            .alias('fixtures__draws__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.draws.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.draws.total`').cast('decimal(10,7)'))
            .alias('fixtures__draws__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.loses.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.loses.home`').cast('decimal(10,7)'))
            .alias('fixtures__loses__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.loses.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.loses.away`').cast('decimal(10,7)'))
            .alias('fixtures__loses__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`fixtures.loses.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`fixtures.loses.total`').cast('decimal(10,7)'))
            .alias('fixtures__loses__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.total.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.total.home`').cast('decimal(10,7)'))
            .alias('goals__for__total__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.total.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.total.away`').cast('decimal(10,7)'))
            .alias('goals__for__total__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.total.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.total.total`').cast('decimal(10,7)'))
            .alias('goals__for__total__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.average.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.average.home`'))
            .alias('goals__for__average__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.average.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.average.away`'))
            .alias('goals__for__average__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.average.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.average.total`'))
            .alias('goals__for__average__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.0-15.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.0-15.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__0_15__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.0-15.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.0-15.percentage`'))
            .alias('goals__for__minute__0_15__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.16-30.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.16-30.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__16_30__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.16-30.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.16-30.percentage`'))
            .alias('goals__for__minute__16_30__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.31-45.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.31-45.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__31_45__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.31-45.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.31-45.percentage`'))
            .alias('goals__for__minute__31_45__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.46-60.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.46-60.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__46_60__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.46-60.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.46-60.percentage`'))
            .alias('goals__for__minute__46_60__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.61-75.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.61-75.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__61_75__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.61-75.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.61-75.percentage`'))
            .alias('goals__for__minute__61_75__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.76-90.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.76-90.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__76_90__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.76-90.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.76-90.percentage`'))
            .alias('goals__for__minute__76_90__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.91-105.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.91-105.total`').cast('decimal(10,7)'))
            .alias('goals__for__minute__91_105__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.91-105.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.91-105.percentage`'))
            .alias('goals__for__minute__91_105__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.106-120.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.106-120.total`'))
            .alias('goals__for__minute__106_120__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.for.minute.106-120.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.for.minute.106-120.percentage`'))
            .alias('goals__for__minute__106_120__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.total.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.total.home`').cast('decimal(10,7)'))
            .alias('goals__against__total__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.total.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.total.away`').cast('decimal(10,7)'))
            .alias('goals__against__total__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.total.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.total.total`').cast('decimal(10,7)'))
            .alias('goals__against__total__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.average.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.average.home`'))
            .alias('goals__against__average__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.average.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.average.away`'))
            .alias('goals__against__average__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.average.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.average.total`'))
            .alias('goals__against__average__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.0-15.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.0-15.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__0_15__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.0-15.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.0-15.percentage`'))
            .alias('goals__against__minute__0_15__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.16-30.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.16-30.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__16_30__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.16-30.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.16-30.percentage`'))
            .alias('goals__against__minute__16_30__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.31-45.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.31-45.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__31_45__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.31-45.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.31-45.percentage`'))
            .alias('goals__against__minute__31_45__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.46-60.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.46-60.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__46_60__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.46-60.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.46-60.percentage`'))
            .alias('goals__against__minute__46_60__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.61-75.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.61-75.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__61_75__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.61-75.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.61-75.percentage`'))
            .alias('goals__against__minute__61_75__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.76-90.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.76-90.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__76_90__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.76-90.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.76-90.percentage`'))
            .alias('goals__against__minute__76_90__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.91-105.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.91-105.total`').cast('decimal(10,7)'))
            .alias('goals__against__minute__91_105__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.91-105.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.91-105.percentage`'))
            .alias('goals__against__minute__91_105__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.106-120.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.106-120.total`'))
            .alias('goals__against__minute__106_120__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals.against.minute.106-120.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals.against.minute.106-120.percentage`'))
            .alias('goals__against__minute__106_120__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.streak.wins`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.streak.wins`').cast('decimal(10,7)'))
            .alias('biggest__streak__wins')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.streak.draws`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.streak.draws`').cast('decimal(10,7)'))
            .alias('biggest__streak__draws')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.streak.loses`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.streak.loses`').cast('decimal(10,7)'))
            .alias('biggest__streak__loses')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.wins.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.wins.home`'))
            .alias('biggest__wins__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.wins.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.wins.away`'))
            .alias('biggest__wins__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.loses.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.loses.home`'))
            .alias('biggest__loses__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.loses.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.loses.away`'))
            .alias('biggest__loses__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.goals.for.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.goals.for.home`').cast('decimal(10,7)'))
            .alias('biggest__goals__for__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.goals.for.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.goals.for.away`').cast('decimal(10,7)'))
            .alias('biggest__goals__for__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.goals.against.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.goals.against.home`').cast('decimal(10,7)'))
            .alias('biggest__goals__against__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`biggest.goals.against.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`biggest.goals.against.away`').cast('decimal(10,7)'))
            .alias('biggest__goals__against__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`clean_sheet.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`clean_sheet.home`').cast('decimal(10,7)'))
            .alias('clean_sheet__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`clean_sheet.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`clean_sheet.away`').cast('decimal(10,7)'))
            .alias('clean_sheet__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`clean_sheet.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`clean_sheet.total`').cast('decimal(10,7)'))
            .alias('clean_sheet__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`failed_to_score.home`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`failed_to_score.home`').cast('decimal(10,7)'))
            .alias('failed_to_score__home')
        ),

    
        (
            f.when(
                f.upper(f.col('`failed_to_score.away`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`failed_to_score.away`').cast('decimal(10,7)'))
            .alias('failed_to_score__away')
        ),

    
        (
            f.when(
                f.upper(f.col('`failed_to_score.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`failed_to_score.total`').cast('decimal(10,7)'))
            .alias('failed_to_score__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`penalty.scored.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`penalty.scored.total`').cast('decimal(10,7)'))
            .alias('penalty__scored__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`penalty.scored.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`penalty.scored.percentage`'))
            .alias('penalty__scored__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`penalty.missed.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`penalty.missed.total`').cast('decimal(10,7)'))
            .alias('penalty__missed__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`penalty.missed.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`penalty.missed.percentage`'))
            .alias('penalty__missed__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`penalty.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`penalty.total`').cast('decimal(10,7)'))
            .alias('penalty__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.0-15.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.0-15.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__0_15__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.0-15.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.0-15.percentage`'))
            .alias('cards__yellow__0_15__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.16-30.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.16-30.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__16_30__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.16-30.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.16-30.percentage`'))
            .alias('cards__yellow__16_30__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.31-45.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.31-45.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__31_45__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.31-45.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.31-45.percentage`'))
            .alias('cards__yellow__31_45__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.46-60.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.46-60.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__46_60__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.46-60.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.46-60.percentage`'))
            .alias('cards__yellow__46_60__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.61-75.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.61-75.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__61_75__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.61-75.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.61-75.percentage`'))
            .alias('cards__yellow__61_75__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.76-90.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.76-90.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__76_90__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.76-90.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.76-90.percentage`'))
            .alias('cards__yellow__76_90__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.91-105.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.91-105.total`').cast('decimal(10,7)'))
            .alias('cards__yellow__91_105__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.91-105.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.91-105.percentage`'))
            .alias('cards__yellow__91_105__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.106-120.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.106-120.total`'))
            .alias('cards__yellow__106_120__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow.106-120.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow.106-120.percentage`'))
            .alias('cards__yellow__106_120__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.0-15.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.0-15.total`').cast('decimal(10,7)'))
            .alias('cards__red__0_15__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.0-15.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.0-15.percentage`'))
            .alias('cards__red__0_15__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.16-30.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.16-30.total`').cast('decimal(10,7)'))
            .alias('cards__red__16_30__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.16-30.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.16-30.percentage`'))
            .alias('cards__red__16_30__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.31-45.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.31-45.total`').cast('decimal(10,7)'))
            .alias('cards__red__31_45__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.31-45.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.31-45.percentage`'))
            .alias('cards__red__31_45__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.46-60.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.46-60.total`').cast('decimal(10,7)'))
            .alias('cards__red__46_60__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.46-60.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.46-60.percentage`'))
            .alias('cards__red__46_60__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.61-75.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.61-75.total`').cast('decimal(10,7)'))
            .alias('cards__red__61_75__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.61-75.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.61-75.percentage`'))
            .alias('cards__red__61_75__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.76-90.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.76-90.total`').cast('decimal(10,7)'))
            .alias('cards__red__76_90__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.76-90.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.76-90.percentage`'))
            .alias('cards__red__76_90__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.91-105.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.91-105.total`').cast('decimal(10,7)'))
            .alias('cards__red__91_105__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.91-105.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.91-105.percentage`'))
            .alias('cards__red__91_105__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.106-120.total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.106-120.total`'))
            .alias('cards__red__106_120__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red.106-120.percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red.106-120.percentage`'))
            .alias('cards__red__106_120__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_played_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_played_home').cast('decimal(10,7)'))
            .alias('fixtures_played_home')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_played_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_played_away').cast('decimal(10,7)'))
            .alias('fixtures_played_away')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_played_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_played_total').cast('decimal(10,7)'))
            .alias('fixtures_played_total')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_wins_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_wins_home').cast('decimal(10,7)'))
            .alias('fixtures_wins_home')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_wins_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_wins_away').cast('decimal(10,7)'))
            .alias('fixtures_wins_away')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_wins_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_wins_total').cast('decimal(10,7)'))
            .alias('fixtures_wins_total')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_draws_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_draws_home').cast('decimal(10,7)'))
            .alias('fixtures_draws_home')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_draws_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_draws_away').cast('decimal(10,7)'))
            .alias('fixtures_draws_away')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_draws_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_draws_total').cast('decimal(10,7)'))
            .alias('fixtures_draws_total')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_loses_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_loses_home').cast('decimal(10,7)'))
            .alias('fixtures_loses_home')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_loses_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_loses_away').cast('decimal(10,7)'))
            .alias('fixtures_loses_away')
        ),

    
        (
            f.when(
                f.upper(f.col('fixtures_loses_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('fixtures_loses_total').cast('decimal(10,7)'))
            .alias('fixtures_loses_total')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_total_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_total_home').cast('decimal(10,7)'))
            .alias('goals_for_total_home')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_total_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_total_away').cast('decimal(10,7)'))
            .alias('goals_for_total_away')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_total_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_total_total').cast('decimal(10,7)'))
            .alias('goals_for_total_total')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_average_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_average_home'))
            .alias('goals_for_average_home')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_average_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_average_away'))
            .alias('goals_for_average_away')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_for_average_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_for_average_total'))
            .alias('goals_for_average_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_0-15_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_0-15_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_0_15_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_0-15_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_0-15_percentage`'))
            .alias('goals_for_minute_0_15_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_16-30_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_16-30_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_16_30_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_16-30_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_16-30_percentage`'))
            .alias('goals_for_minute_16_30_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_31-45_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_31-45_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_31_45_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_31-45_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_31-45_percentage`'))
            .alias('goals_for_minute_31_45_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_46-60_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_46-60_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_46_60_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_46-60_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_46-60_percentage`'))
            .alias('goals_for_minute_46_60_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_61-75_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_61-75_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_61_75_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_61-75_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_61-75_percentage`'))
            .alias('goals_for_minute_61_75_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_76-90_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_76-90_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_76_90_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_76-90_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_76-90_percentage`'))
            .alias('goals_for_minute_76_90_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_91-105_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_91-105_total`').cast('decimal(10,7)'))
            .alias('goals_for_minute_91_105_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_91-105_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_91-105_percentage`'))
            .alias('goals_for_minute_91_105_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_106-120_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_106-120_total`'))
            .alias('goals_for_minute_106_120_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_for_minute_106-120_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_for_minute_106-120_percentage`'))
            .alias('goals_for_minute_106_120_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_total_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_total_home').cast('decimal(10,7)'))
            .alias('goals_against_total_home')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_total_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_total_away').cast('decimal(10,7)'))
            .alias('goals_against_total_away')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_total_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_total_total').cast('decimal(10,7)'))
            .alias('goals_against_total_total')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_average_home')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_average_home'))
            .alias('goals_against_average_home')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_average_away')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_average_away'))
            .alias('goals_against_average_away')
        ),

    
        (
            f.when(
                f.upper(f.col('goals_against_average_total')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('goals_against_average_total'))
            .alias('goals_against_average_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_0-15_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_0-15_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_0_15_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_0-15_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_0-15_percentage`'))
            .alias('goals_against_minute_0_15_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_16-30_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_16-30_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_16_30_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_16-30_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_16-30_percentage`'))
            .alias('goals_against_minute_16_30_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_31-45_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_31-45_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_31_45_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_31-45_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_31-45_percentage`'))
            .alias('goals_against_minute_31_45_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_46-60_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_46-60_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_46_60_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_46-60_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_46-60_percentage`'))
            .alias('goals_against_minute_46_60_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_61-75_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_61-75_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_61_75_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_61-75_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_61-75_percentage`'))
            .alias('goals_against_minute_61_75_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_76-90_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_76-90_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_76_90_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_76-90_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_76-90_percentage`'))
            .alias('goals_against_minute_76_90_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_91-105_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_91-105_total`').cast('decimal(10,7)'))
            .alias('goals_against_minute_91_105_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_91-105_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_91-105_percentage`'))
            .alias('goals_against_minute_91_105_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_106-120_total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_106-120_total`'))
            .alias('goals_against_minute_106_120_total')
        ),

    
        (
            f.when(
                f.upper(f.col('`goals_against_minute_106-120_percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`goals_against_minute_106-120_percentage`'))
            .alias('goals_against_minute_106_120_percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('lineups_formation')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('lineups_formation'))
            .alias('lineups_formation')
        ),

    
        (
            f.when(
                f.upper(f.col('lineups_played')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('lineups_played').cast('decimal(10,7)'))
            .alias('lineups_played')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow..total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow..total`').cast('decimal(10,7)'))
            .alias('cards__yellow__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.yellow..percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.yellow..percentage`'))
            .alias('cards__yellow__percentage')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red..total`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red..total`').cast('decimal(10,7)'))
            .alias('cards__red__total')
        ),

    
        (
            f.when(
                f.upper(f.col('`cards.red..percentage`')).isin('NAN','NONE'),
                None
            )
            .otherwise(f.col('`cards.red..percentage`'))
            .alias('cards__red__percentage')
        ),


        f.col('season')
    )

    .withColumn('rn',f.row_number().over(Window.partitionBy('league_id','team_id').orderBy(f.col('season'))))
    .withColumn('league_team', f.concat_ws('','rn','league','team'))
)


hudi_options = {
    "className": "org.apache.hudi",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.database": "football_silver",
    "hoodie.datasource.hive_sync.table": "silver_team_statistics",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.partition_fields": "season",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.table.name": "silver_team_statistics",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.recordkey.field": "league_team",
    "hoodie.datasource.write.partitionpath.field": "season",
    "hoodie.datasource.write.table.name": "silver_team_statistics",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.metadata.enable": False,
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.write.hive_style_partitioning": True,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.precombine.field": "league_team",
    "hoodie.upsert.shuffle.parallelism": 120,
    "hoodie.insert.shuffle.parallelism": 120,
    "hoodie.bulkinsert.shuffle.parallelism": 120,
    "hoodie.write.concurrency.mode": "single_writer",
    "hoodie.cleaner.policy.failed.writes": "EAGER"
}

path_s3 = 's3://football-hist-silver/team_statistics/'
teams.write.format("hudi").mode("append").options(**hudi_options).save(path_s3)