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


# LINEUPS

def get_fixture_lineups(fixture_id):
    # Asegúrate de que 'headers' esté definido correctamente con tu clave de API
    url = f"https://v3.football.api-sports.io/fixtures/lineups?fixture={fixture_id}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        if data['response']:
            lineups_list = []

            for team_lineup in data['response']:
                team_info = json_normalize(team_lineup['team'], sep='_')

                # Lista para contener la información de los jugadores
                players_list = []

                # Procesar los jugadores titulares (startXI)
                try:
                    for player_info in team_lineup['startXI']:
                        player = player_info['player']
                        player['role'] = 'Starter'
                        players_list.append(player)
                        
                except:
                        players_list.append({'role' : None})

                # Procesar los suplentes
                
                
                try:
                    for player_info in team_lineup['substitutes']:
                        player = player_info['player']
                        player['role'] = 'Substitute'
                        players_list.append(player)
                except:
                        players_list.append({'role' : None})

                
                # Convertir la lista de jugadores a DataFrame
                df_players = pd.DataFrame(players_list)

                # Añadir información del equipo a los jugadores
                for column in team_info.columns:
                    df_players[f'team_{column}'] = team_info[column].iloc[0]

                # Añadir DataFrame de jugadores a la lista
                lineups_list.append(df_players)

            # Combinar todos los DataFrames de jugadores en uno solo
            df_lineups = pd.concat(lineups_list, ignore_index=True)

            # Añadir el fixture_id como una columna en el DataFrame
            df_lineups['fixture_id'] = fixture_id

            return df_lineups
        else:
            print("No se encontraron datos en la respuesta.")
            return pd.DataFrame()
    else:
        print(f"Error al obtener datos: {response.status_code}")
        return pd.DataFrame()

season = 2023

query = f''' 
    select distinct
        fixture_id
    from bronce_matchday_statistics
    where fixture_id is not null
'''

df_fixtures_stats = wr.athena.read_sql_query(
    sql = query,
    database= 'football_bronce'
)

fixtures = df_fixtures_stats['fixture_id'].to_list()


data_fixture_lineups = []
for fixture in fixtures:
    fixture = int(fixture)
    
    data_lineups_aux = get_fixture_lineups(fixture)
    if not data_lineups_aux.empty:
        data_lineups_aux = data_lineups_aux.astype(str)
        data_fixture_lineups.append(data_lineups_aux)
        
(
    pd.concat(data_fixture_lineups, ignore_index=True)
    .astype(str)
    .to_parquet(
        f's3://football-hist-bronce/matchday_lineups/season={season}/data.parquet'
        )

)
