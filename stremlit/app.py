import streamlit as st
import awswrangler as wr
from PIL import Image
import requests
from io import BytesIO
import plotly.express as px
import boto3
import matplotlib.pyplot as plt
import webbrowser


st.set_page_config(layout="wide")

# Configura tus credenciales y región aquí
aws_access_key_id = 'XXXXXXXXXXX'
aws_secret_access_key = 'XXXXXXXXXXX'
aws_session_token="XXXXXXXXXXX"
region_name = 'XXXXXXXXX'

# Crea una sesión de boto3 con tus credenciales
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
)

def show_news_button(team_name):
    # Crear la URL de búsqueda de Google News
    search_url = f"https://news.google.com/search?q={team_name}+football"

    # Mostrar el botón en Streamlit
    if st.button('Ver Noticias en Google News'):
        # Esto abrirá una nueva pestaña en el navegador del usuario con la búsqueda del equipo
        webbrowser.open_new_tab(search_url)


# Funcion para obtener imagen de una url
def load_image_from_url(url):
    response = requests.get(url)
    image = Image.open(BytesIO(response.content))
    return image

def get_dynamic_team_content(team_name):
    # Este es un texto genérico donde se insertará el nombre del equipo seleccionado
    team_content = (
        f"Explora la trayectoria y los logros del {team_name}, un equipo con una rica "
        "historia y un futuro prometedor. Desde sus fundadores hasta los héroes actuales en el campo, "
        "cada partido es un nuevo capítulo en su legado. Únete al fervor y la pasión que solo los verdaderos "
        f"seguidores del {team_name} pueden entender."
    )
    return team_content

# Consulta para obtener todos los datos del equipo
@st.cache_data
def get_team_data():
    query = "SELECT * FROM football_silver.silver_leagues"
    df_teams = wr.athena.read_sql_query(sql=query, database="football_silver",
                                        boto3_session=session)
    return df_teams

@st.cache_data
def get_player_data():
    query = "SELECT * FROM football_silver.silver_players"
    df_players = wr.athena.read_sql_query(sql=query, database="football_silver",
                                        boto3_session=session)
    return df_players

@st.cache_data
def get_fixture_data():
    query = "SELECT * FROM football_silver.matchday_stats"
    jornadas_df = wr.athena.read_sql_query(sql=query, database="football_silver",
                                        boto3_session=session)
    return jornadas_df

@st.cache_data
def get_alineaciones_data():
    query = "SELECT * FROM football_silver.matchday_lineups"
    alineaciones_df = wr.athena.read_sql_query(sql=query, database="football_silver",
                                        boto3_session=session)
    return alineaciones_df

def create_results_visualization(form_string):
    # Aquí adaptamos el código para usar las iniciales de los resultados de los partidos
    # en tu DataFrame para crear una fila de círculos de colores correspondientes
    html_string = """
    <style>
        .match-result {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 6px;
            margin-right: 5px;
        }
        .win { background-color: #4CAF50; }
        .draw { background-color: #FFC107; }
        .loss { background-color: #F44336; }
    </style>
    <div>
    """
    
    # Suponemos que 'W' representa una victoria, 'D' un empate y 'L' una derrota
    for result in form_string:
        if result == 'W':
            html_string += '<span class="match-result win"></span>'
        elif result == 'D':
            html_string += '<span class="match-result draw"></span>'
        elif result == 'L':
            html_string += '<span class="match-result loss"></span>'
    
    html_string += "</div>"
    
    # Usamos el HTML para mostrar la visualización en Streamlit
    st.markdown(html_string, unsafe_allow_html=True)

# Cargar datos del equipo
df_teams = get_team_data()
df_players = get_player_data()
jornadas_df = get_fixture_data()
alineaciones_df = get_alineaciones_data()

def get_team_details(team_name):
    team_details = df_teams[df_teams['team_name'] == team_name].iloc[0]
    return team_details

# Usando el argumento 'width' para ajustar el ancho de las columnas
col1, col2, col3 = st.columns([1, 1, 1])

with col1:

    st.markdown("<h3 style='text-align: center;'>Detalles Generales del Equipo</h3>", unsafe_allow_html=True)

    # Filtrar los países únicos
    paises = df_teams['team_country'].unique()

    # Crear el selector de país
    selected_country = st.selectbox('Selecciona un país', paises)

    # Filtrar los equipos basados en el país seleccionado
    equipos_del_pais = df_teams[df_teams['team_country'] == selected_country]

    # Obtener los nombres de los equipos únicos en el país seleccionado
    equipos = equipos_del_pais['team_name'].unique()

    # Crear el selector de equipo
    selected_team = st.selectbox('Selecciona un equipo', equipos)

    # Obtener los detalles del equipo seleccionado
    team_details = equipos_del_pais[equipos_del_pais['team_name'] == selected_team].iloc[0]

    team_news = show_news_button(selected_team)

    team_content = get_dynamic_team_content(selected_team)

    # Para el diseño de la tarjeta del equipo con escudo y noticias
    st.markdown(f"""
    <div style="display:flex; align-items:center; justify-content:space-between; background-color: #f1f1f1; padding: 20px; border-radius: 10px;">
        <div style="flex:1;">
            <img src="{team_details['team_logo']}" style="max-width:150px;">
        </div>
        <div style="flex:2; margin-left:20px;">
            <p>{team_content}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Para el diseño de la tarjeta con la foto del estadio y los datos del equipo
    st.markdown(f"""
    <div style="display:flex; align-items:center; justify-content:space-between; background-color: #f1f1f1; padding: 20px; border-radius: 10px; margin-top:20px;">
        <div style="flex:1; margin-right:20px;">
            <h4>Año de Fundación: {int(team_details['team_founded'])}</h4>
            <p>Nombre estadio: {team_details['venue_name']}</p>
            <p>Dirección: {team_details['venue_address']}</p>
            <p>Ciudad: {team_details['venue_city']}</p>
            <p>Capacidad: {team_details['venue_capacity']}</p>
        </div>
        <div style="flex:1;">
            <img src="{team_details['venue_image']}" style="max-width:300px;">
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    
with col2:

    st.markdown("<h3 style='text-align: center;'>Información de las Jornadas</h3>", unsafe_allow_html=True)

    selected_team_id = int(team_details['team_id'])  # Asegúrate de que esto sea un entero

    jornadas_df['teams_home_id'] = jornadas_df['teams_home_id'].astype(int)
    jornadas_df['teams_away_id'] = jornadas_df['teams_away_id'].astype(int)
    jornadas_df['fixture_id'] = jornadas_df['fixture_id'].astype(int)
    alineaciones_df['fixture_id'] = alineaciones_df['fixture_id'].astype(int)

    # Crear una variable de estado para almacenar el partido seleccionado
    if 'selected_match_id' not in st.session_state:
        st.session_state['selected_match_id'] = None

    try:
        filtered_matches = jornadas_df[(jornadas_df['teams_home_id'] == selected_team_id) | (jornadas_df['teams_away_id'] == selected_team_id)].head(10)

        for index, match in filtered_matches.iterrows():
            match_container = st.container()
            fulltime_result = f"{match['score_fulltime_home']} - {match['score_fulltime_away']}"
            halftime_result = f"({match['score_halftime_home']} - {match['score_halftime_away']})"
            
            # Tarjeta del partido
            st.markdown(f"""
            <div style="background-color: #f1f1f1; padding: 15px; border-radius: 10px; margin: 5px 0; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
                <h5 style="text-align: center; margin-bottom: 10px;">{match['league_name']} - Árbitro: {match['fixture_referee']}</h5>
                <div style="display: flex; justify-content: space-around; align-items: center;">
                    <div style="flex: 1; text-align: center;">
                        <img src="{match['teams_home_logo']}" style="max-width: 50px; height: auto;">
                        <p style="font-size: 0.9em;">{match['teams_home_name']}</p>
                    </div>
                    <div style="flex: 1; text-align: center;">
                        <p style="font-size: 18px; margin: 0;">{fulltime_result}</p>
                        <p style="font-size: 0.8em; margin: 0;">{halftime_result}</p>
                        <p style="font-size: 0.9em;">{match['fixture_venue_name']}</p>
                    </div>
                    <div style="flex: 1; text-align: center;">
                        <img src="{match['teams_away_logo']}" style="max-width: 50px; height: auto;">
                        <p style="font-size: 0.9em;">{match['teams_away_name']}</p>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
            # Mostrar las alineaciones si el partido ha sido seleccionado
            if st.session_state['selected_match_id'] == match['fixture_id']:
                alineaciones_partido = alineaciones_df[alineaciones_df['fixture_id'] == match['fixture_id']]
                # Aquí puedes agregar el código para mostrar las alineaciones de forma estética
                
    except KeyError as e:
        st.error(f"Columna no encontrada: {e}")
    except Exception as e:
        st.error(f"Se produjo un error: {e}")

with col3:

    st.markdown("<h3 style='text-align: center;'>Información de los Jugadores</h3>", unsafe_allow_html=True)

    # Obten el ID del equipo seleccionado (como un entero)
    selected_team_id = int(team_details['team_id'])  # Asegúrate de que esto sea un entero

    # Antes de filtrar df_players, asegúrate de que la columna 'team' sea del mismo tipo
    df_players['team'] = df_players['team'].astype(int)  # Convierte la columna 'team' a enteros

    # Ahora filtra df_players para el equipo seleccionado
    players_df = df_players[df_players['team'] == selected_team_id]
    
    # Usaremos un índice para crear un grid más estético
    num_columns = 4
    rows = [players_df.iloc[i:i + num_columns] for i in range(0, len(players_df), num_columns)]
    for row in rows:
        cols = st.columns(num_columns)
        for idx, player in enumerate(row.itertuples()):
            with cols[idx % num_columns]:
                # Usamos un div para el contenido, incluyendo la foto en el cuadro
                st.markdown(f"""
                <div style='text-align: center; padding: 8px;'>
                    <div style="background-color: #f1f1f1; padding: 8px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                        <img src="{player.photo}" style='border-radius: 50%; width: 75px; height: 75px; object-fit: cover; margin-bottom: 5px;'>
                        <h5 style="margin: 5px 0;">{player.name}</h5>
                        <p style="margin: 2px 0; font-size: 0.8em;">Dorsal: {player.number}</p>
                        <p style="margin: 2px 0; font-size: 0.8em;">Edad: {player.age}</p>
                        <p style="margin: 2px 0; font-size: 0.8em;">Posición: {player.position}</p>
                    </div>
                </div>
                """, unsafe_allow_html=True)