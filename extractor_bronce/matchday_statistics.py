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

def get_fixture_stats(id_team, id_season):
    team = int(id_team)
    season = int(id_season)

    # Asegúrate de que 'headers' esté definido correctamente con tu clave de API
    url = f"https://v3.football.api-sports.io/fixtures?season={season}&team={team}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        if data['response']:
            # Normalizar los datos, preservando la estructura anidada de 'venue' dentro de 'fixture'
            df_fixtures = json_normalize(data['response'], sep='_')

            # Asegurarse de que la columna 'fixture_venue' exista antes de intentar acceder a ella
            if 'fixture_venue' in df_fixtures.columns:
                # Extraer 'id', 'name' y 'city' de 'venue' y crear nuevas columnas para estos
                venue_keys = ['id', 'name', 'city']
                for key in venue_keys:
                    df_fixtures[f'venue_{key}'] = df_fixtures['fixture_venue'].apply(lambda x: x.get(key) if isinstance(x, dict) else None)

            # Continuar con la extracción de otras secciones anidadas si es necesario

            return df_fixtures
        else:
            print("No se encontraron datos en la respuesta.")
            return pd.DataFrame()
    else:
        print(f"Error al obtener datos: {response.status_code}")
        return pd.DataFrame()
    
season = 2023

query = f''' 
    select distinct
        team_id
    from bronce_leagues
    where team_id is not null
'''

df_info_equipos = wr.athena.read_sql_query(
    sql = query,
    database= 'football_bronce'
)

teamns = df_info_equipos['team_id'].to_list()

# VAMOS A HACER LA LLAMADA PARA OBTENER TODOS LOS PARTIDOS DE LA TEMPORADA DE LOS EQUIPOS QUE HEMOS OBTENIDO EN EL PUNTO 3
# EN EL DATAFRAME df_info_equipos_ligas_paises, recorremos el bucle para obtener los datos de los partidos de cada equipo

data_fixture_stats = []
for team in teamns:
    team = int(team)
    # Llamada a la función con valores de equipo y liga
    data_stats_aux = get_fixture_stats(team,season)
    if not data_stats_aux.empty:
        data_stats_aux = data_stats_aux.astype(str)
        data_fixture_stats.append(data_stats_aux)

(
    pd.concat(data_fixture_stats, ignore_index=True)
    .astype(str)
    .to_parquet(
        f's3://football-hist-bronce/matchday_statistics/season={season}/data.parquet'
    )
)     