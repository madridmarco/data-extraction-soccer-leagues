import requests
from bs4 import BeautifulSoup

url = 'https://www.transfermarkt.es/uefa-champions-league/gesamtspielplan/pokalwettbewerb/CL/saison_id/2019'

# Realizar la solicitud GET y obtener el contenido HTML
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Encontrar la tabla que contiene la información de los partidos
tabla_partidos = soup.find('div', {'class': 'responsive-table'})
tabla_partidos
# Encontrar todas las filas de la tabla de partidos
filas = tabla_partidos.find_all('tr', {'class': ['bg_blau_20', 'bg_gelb_20']})

# Iterar sobre las filas para obtener la información de cada partido
for fila in filas:
    celdas = fila.find_all('td')
    if len(celdas) >= 4:
        fecha = celdas[0].text.strip()
        equipo_local = celdas[2].text.strip()
        resultado = celdas[3].text.strip()
        equipo_visitante = celdas[4].text.strip()
        
        print(fecha, equipo_local, resultado, equipo_visitante)
