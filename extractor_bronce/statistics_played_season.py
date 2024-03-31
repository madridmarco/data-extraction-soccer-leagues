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

def get_list_stats_player(id_jugador, id_season):
    jugador = int(id_jugador)
    season = int(id_season)

    # URL para obtener las estadísticas de los jugadores de un equipo en una temporada específica
    url_plantilla_stats = f"https://v3.football.api-sports.io/players?id="+str(jugador)+"&season="+str(season)

    # Realizar la solicitud a la API
    response = requests.get(url_plantilla_stats, headers=headers)

    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Convertir la respuesta a JSON
        data = response.json()

        # Verificar si hay datos en 'response'
        if data['response']:
            # Normalizamos la parte superior que contiene la información del jugador
            # Aquí asumimos que 'response' es una lista de jugadores
            df_players_stats = json_normalize(data['response'], sep='_')

            # Lista para almacenar los DataFrames de estadísticas
            df_statistics_list = []

            # Iterar sobre cada jugador y normalizar sus estadísticas
            for player in data['response']:
                # Comprobar si el jugador tiene estadísticas disponibles
                if 'statistics' in player and player['statistics']:
                    df_stats = json_normalize(player['statistics'], sep='_')
                    df_statistics_list.append(df_stats)

            # Concatenar todas las DataFrames de estadísticas en una sola DataFrame, si hay alguna
            if df_statistics_list:
                df_statistics = pd.concat(df_statistics_list, ignore_index=True)
                return df_statistics
            else:
                print("No hay estadísticas disponibles para los jugadores.")
                return pd.DataFrame()
        else:
            print("No se encontraron datos en la respuesta.")
            return pd.DataFrame()
    else:
        print("Error en la solicitud:", response.status_code)
        return pd.DataFrame()

season = 2023
query = f''' 
    select distinct
        id
    from bronce_players
    where id is not null
'''

data_players = wr.athena.read_sql_query(
    sql = query,
    database= 'football_bronce'
)

ids = data_players['id'].to_list()

data_players_stats = []
for id in ids:
    # Accediendo a los valores usando .iloc[] para garantizar valores escalares
    id_player = id

    # Convertir directamente a int ya que pd.notnull garantiza que no son NaN
    player = int(id_player)

    # Llamada a la función con valores de equipo y liga
    data_stats_aux = get_list_stats_player(player,season)
    if not data_stats_aux.empty:
        data_players_stats.append(data_stats_aux)


(
    pd.concat(data_players_stats, ignore_index=True)
    .astype(str)
    .to_parquet(
        f's3://football-hist-bronce/statistics_played_season/season={season}/data.parquet'
    )
)     