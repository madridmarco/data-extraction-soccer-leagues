from tasks import run_glue_job, run_crawler, delete_results
import boto3
from prefect import task, flow
from pydantic import BaseModel
from prefect.task_runners import ConcurrentTaskRunner
from prefect.states import Failed, Completed
from prefect_aws import S3Bucket
import time
from prefect_aws import AwsCredentials

s3_bucket_block =  S3Bucket.load("aws-prefect-football-data-space-results")


aws_credentials_block =  AwsCredentials.load("s3-credentials")

client_glue = aws_credentials_block.get_boto3_session().client("glue")

client_s3 = aws_credentials_block.get_boto3_session().client("s3")

@task(name = 'extractor-league-football', retries = 3, retry_delay_seconds = 10)
def extractor_league_football (job_name = 'extractor-league-football', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'extractor-team-statistics', retries = 3, retry_delay_seconds = 10)
def extractor_team_statistics (job_name = 'extractor-team-statistics', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'extractor-players', retries = 3, retry_delay_seconds = 10)
def extractor_players (job_name = 'extractor-players', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'extractor-matchday-statistics', retries = 3, retry_delay_seconds = 10)
def extractor_matchday_statistics (job_name = 'extractor-matchday-statistics', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'extractor-statistics-played-season', retries = 3, retry_delay_seconds = 10)
def extractor_statistics_played_season (job_name = 'extractor-statistics-played-season', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-clear-leagues', retries = 3, retry_delay_seconds = 10)
def silver_clear_leagues (job_name = 'silver-clear-leagues', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-matchday-statistics', retries = 3, retry_delay_seconds = 10)
def silver_matchday_statistics (job_name = 'silver-matchday-statistics', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-matchday-events', retries = 3, retry_delay_seconds = 10)
def silver_matchday_events (job_name = 'silver_matchday_events', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-matchday-player-stats', retries = 3, retry_delay_seconds = 10)
def silver_matchday_player_stats (job_name = 'silver_matchday_player_stats', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-matchday-lineups', retries = 3, retry_delay_seconds = 10)
def silver_matchday_lineups (job_name = 'silver-matchday_lineups', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'silver-statistics-played-season', retries = 3, retry_delay_seconds = 10)
def silver_statistics_played_season (job_name = 'silver-statistics-played-season', client_glue = client_glue):
    return run_glue_job(job_name, client_glue)

@task(name = 'run-crawler-countries-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_countries_bronze (crawler_name = 'countries-bronce', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@task(name = 'run-crawler-leagues-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_leagues_bronce (crawler_name = 'leagues-bronces client_glue', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@task(name = 'run-crawler-matchday-statistics-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_matchday_statistics_bronce (crawler_name = 'matchday-statistics-bronce', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@task(name = 'run-crawler-player-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_player_bronce (crawler_name = 'player-bronce', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@task(name = 'run-crawler-statistics-played-season-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_statistics_played_season_bronce (crawler_name = 'statistics-played-season-bronce', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@task(name = 'run-crawler-team-statistics-bronce', retries = 3, retry_delay_seconds = 10)
def crawler_team_statistics_bronce (crawler_name = 'team-statistics-bronce', client_glue = client_glue):
    return run_crawler(crawler_name, client_glue)

@flow(task_runner = ConcurrentTaskRunner(), 
      log_prints = True,
      retries = 3,
      retry_delay_seconds = 90)
def etl_flow():
    extract_league_football_status = extractor_league_football.submit(client_glue=client_glue)
    crawler_countries_bronze_status = crawler_countries_bronze.submit(client_glue=client_glue, wait_for = [extract_league_football_status])
    extractor_team_statistics_status = extractor_team_statistics.submit(client_glue=client_glue, wait_for = [crawler_countries_bronze_status])
    crawler_team_statistics_bronce_status = crawler_team_statistics_bronce.submit(client_glue=client_glue, wait_for = [extractor_team_statistics_status])
    extractor_players_status = extractor_players.submit(client_glue=client_glue, wait_for = [crawler_team_statistics_bronce_status])
    crawler_player_bronce_status = crawler_player_bronce.submit(client_glue = client_glue, wait_for = [extractor_players_status])
    extractor_matchday_statistics_status = extractor_matchday_statistics.submit(client_glue=client_glue, wait_for = [crawler_player_bronce_status])
    crawler_matchday_statistics_bronce_status = crawler_matchday_statistics_bronce.submit(client_glue = client_glue, wait_for = [extractor_matchday_statistics_status])
    silver_clear_leagues_status = silver_clear_leagues.submit(client_glue=client_glue, wait_for = [crawler_matchday_statistics_bronce_status])
    silver_matchday_statistics_status = silver_matchday_statistics.submit(client_glue=client_glue, wait_for = [silver_clear_leagues_status])
    silver_matchday_events_status = silver_matchday_events.submit(client_glue=client_glue, wait_for = [silver_matchday_statistics_status])
    silver_matchday_player_stats_status = silver_matchday_player_stats.submit(client_glue=client_glue, wait_for = [silver_matchday_events_status])
    silver_matchday_lineups_status = silver_matchday_lineups.submit(client_glue=client_glue, wait_for = [silver_matchday_player_stats_status])
    silver_statistics_played_season_status = silver_statistics_played_season.submit(client_glue=client_glue, wait_for = [silver_matchday_lineups_status])

    print(f"Final job run status: {silver_statistics_played_season_status}")