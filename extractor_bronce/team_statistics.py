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


def get_list_stats_equipos(id_season,id_team,id_league):

    season = int(id_season)
    team = int(id_team)
    league = int(id_league)

    url_stats_equipos = "https://v3.football.api-sports.io/teams/statistics?season="+str(season)+"&team="+str(team)+"&league="+str(league)

    response = requests.request("GET", url_stats_equipos, headers=headers)
    if response.status_code == 200:
        data = response.json()['response']

        # Normalizando la respuesta principal
        df_response = json_normalize(data)

        # Normalizando 'fixtures'
        df_fixtures = json_normalize(data['fixtures'], sep='_')
        df_fixtures.columns = ['fixtures_' + col for col in df_fixtures.columns]  # Prefijo para las columnas

        # Normalizando 'goals for'
        df_goals_for = json_normalize(data['goals']['for'], sep='_')
        df_goals_for.columns = ['goals_for_' + col for col in df_goals_for.columns]  # Prefijo para las columnas

        # Normalizando 'goals against'
        df_goals_against = json_normalize(data['goals']['against'], sep='_')
        df_goals_against.columns = ['goals_against_' + col for col in df_goals_against.columns]  # Prefijo para las columnas

        # Normalizando 'lineups' si está presente y es una lista
        if 'lineups' in data and isinstance(data['lineups'], list):
            df_lineups = pd.DataFrame(data['lineups'])
            df_lineups.columns = ['lineups_' + col for col in df_lineups.columns]  # Prefijo para las columnas

        # Combinando todos los DataFrames en uno solo
        dfs_combined = pd.concat([df_response, df_fixtures, df_goals_for, df_goals_against, df_lineups], axis=1)
        return dfs_combined
    else:
        print("Error en la solicitud:", response.status_code)
        return pd.DataFrame()
    
season = 2023

query = f''' 
    select 
        *
    from bronce_leagues
    where season = '{season}'
'''

df_info_equipos_ligas_paises = wr.athena.read_sql_query(
    sql = query,
    database= 'football_bronce'
)

for i in range(len(df_info_equipos_ligas_paises)):
    # Accediendo a los valores usando .iloc[] para garantizar valores escalares
    team_value = df_info_equipos_ligas_paises.iloc[i]['team_id']
    league_value = df_info_equipos_ligas_paises.iloc[i]['league']

    # Asegurarse de que ambos valores no son NaN antes de convertirlos a int
    if pd.notnull(team_value) and pd.notnull(league_value):
        # Convertir directamente a int ya que pd.notnull garantiza que no son NaN
        team = int(team_value)
        league = int(league_value)

        # Llamada a la función con valores de equipo y liga
        data_stats_aux = get_list_stats_equipos(season, team, league)
        if not data_stats_aux.empty:
            data_stats_aux = data_stats_aux.astype(str)
            data_stats_aux.to_parquet(f's3://football-hist-bronce/team_statistics/season={season}/team={team}/league={league}/data.parquet')