import requests
import pandas as pd
from pandas import json_normalize
import http.client
import ssl
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, [
    "season"
])

season = int(args['season'])

ssl_context = ssl._create_unverified_context()
http.client.HTTPSConnection("v3.football.api-sports.io", context=ssl_context)

headers = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": "a7651e81f75b7516319f70fd91663320"
}


def lista_info_equipos(liga,id_season)->pd.DataFrame:
 
    season = int(id_season)
    id_liga = int(liga)
 
    url = "https://v3.football.api-sports.io/teams?league="+str(id_liga)+"&season="+str(season)
 
    response = requests.request("GET", url, headers=headers)
    data_info_team_league = json_normalize(response.json()['response'])
    data_info_team_league.columns = [str(col).replace('.','_') for col in data_info_team_league.columns]

    schema = {
        'team_id' :        'int64',
        'team_name' :     'object',
        'team_code' :     'object',
        'team_country' :  'object',
        'team_founded' : 'float64',
        'team_national' :   'bool',
        'team_logo' :     'object',
        'venue_id' :       'int64',
        'venue_name' :    'object',
        'venue_address' : 'object',
        'venue_city' :    'object',
        'venue_capacity' : 'int64',
        'venue_surface' : 'object',
        'venue_image' :   'object'
    }


    for col, dtype in schema.items():
        data_info_team_league[col] = data_info_team_league[col].astype(dtype)

    data_info_team_league.to_parquet(f's3://football-hist-bronce/leagues/season={season}/league={id_liga}/league_{id_liga}.parquet')    
    

lista_info_equipos(140,season)
lista_info_equipos(135,season)
lista_info_equipos(78,season)
lista_info_equipos(39,season)
lista_info_equipos(61,season)
lista_info_equipos(94,season)
lista_info_equipos(88,season)