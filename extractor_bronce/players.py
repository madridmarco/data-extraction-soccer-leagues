import requests
import pandas as pd
from pandas import json_normalize
import http.client
import ssl
import awswrangler as wr 


ssl_context = ssl._create_unverified_context()
http.client.HTTPSConnection("v3.football.api-sports.io", context=ssl_context)

headers = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": "a7651e81f75b7516319f70fd91663320"
}

def get_squad_team(id_team)->pd.DataFrame:

    team = int(id_team)

    url_squad = "https://v3.football.api-sports.io/players/squads?team="+str(team)

    response = requests.request("GET", url_squad, headers=headers)
    data_squad = json_normalize(response.json()['response'])

    data_squad = json_normalize(data_squad['players'][0])

    return data_squad

query = f''' 
    select distinct
        team_id
    from bronce_leagues
    where team_id is not null
'''

df_info_equipos_ligas_paises = wr.athena.read_sql_query(
    sql = query,
    database= 'football_bronce'
)

teamns = df_info_equipos_ligas_paises['team_id'].to_list()

for team in teamns:
    team = int(team)
    data_stats_aux = get_squad_team(team)
    
    if not data_stats_aux.empty:
        schema = {
            'id':            'int64',
            'name':         'object',
            'age':         'float64',
            'number':       'object',
            'position':     'object',
            'photo':        'object'
        }

        for col, dtype in schema.items():
            data_stats_aux[col] = data_stats_aux[col].astype(dtype)
        data_stats_aux.to_parquet(f's3://football-hist-bronce/players/team={team}/data.parquet')