# %%
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import pandas as pd

# %%
def get_df_summary (driver) -> pd.DataFrame:
    '''
    Extract the summary table of a determine match and return a dataframe with the data

    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the summary table of a determine match
    '''
    data = []

    for i in range(len(driver.find_elements(By.CSS_SELECTOR, 'tbody tr td a span.iconize.iconize-icon-left'))):
        player_name = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td a span.iconize.iconize-icon-left')[i].text
        player_shots_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ShotsTotal')[i].text #Tiros
        player_shots_on_target = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ShotOnTarget')[i].text # TirosAP
        player_key_pass_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.KeyPassTotal')[i].text #PClase
        player_pass_success_in_match = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassSuccessInMatch')[i].text #AP%
        player_duel_aerial_won = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.DuelAerialWon')[i].text #AereosGanados
        player_touches = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.Touches')[i].text #Toques
        player_rating = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.rating')[i].text #Rating

        new_data = {'Player': player_name,
                    'Shots': player_shots_total,
                    'ShotsOnTarget': player_shots_on_target,
                    'KeyPass': player_key_pass_total,
                    'PassSuccessInMatch': player_pass_success_in_match,
                    'DuelAerialWon': player_duel_aerial_won,
                    'Touches': player_touches,
                    'Rating': player_rating}

        data.append(new_data)

    return pd.DataFrame(data)

# %%
def get_df_offensive(driver) -> pd.DataFrame:
    '''
    Extract the offensive table of a determine match and return a dataframe with the data

    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the offensive table of a determine match 
    '''
    data = []

    for i in range(len(driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TackleWonTotal '))):
        player_tackle_won_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TackleWonTotal ')[i].text #Entradas
        player_interception_all = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.InterceptionAll ')[i].text
        player_clearance_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ClearanceTotal ')[i].text
        player_shot_blocked = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ShotBlocked ')[i].text
        player_foul_committed = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.FoulCommitted ')[i].text

        new_data = {'TackleWonTotal': player_tackle_won_total,
                    'InterceptionAll': player_interception_all,
                    'ClearanceTotal': player_clearance_total,
                    'ShotBlocked': player_shot_blocked,
                    'FoulCommitted': player_foul_committed}

        data.append(new_data)

    return pd.DataFrame(data)

# %%
def get_df_defensive(driver) -> pd.DataFrame:
    ''' 
    Extract the defensive table of a determine match and return a dataframe with the data
    
    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the defensive table of a determine match
    '''
    data = []

    for i in range(len(driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TackleWonTotal '))):
        player_tackle_won_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TackleWonTotal ')[i].text #Entradas
        player_interception_all = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.InterceptionAll ')[i].text
        player_clearance_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ClearanceTotal ')[i].text
        player_shot_blocked = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.ShotBlocked ')[i].text
        player_foul_committed = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.FoulCommitted ')[i].text

        new_data = {'TackleWonTotal': player_tackle_won_total,
                    'InterceptionAll': player_interception_all,
                    'ClearanceTotal': player_clearance_total,
                    'ShotBlocked': player_shot_blocked,
                    'FoulCommitted': player_foul_committed}

        data.append(new_data)

    return pd.DataFrame(data)
        

# %%
def get_df_distribution(driver) -> pd.DataFrame:
    ''' 
    Extract the distribution table of a determine match and return a dataframe with the data

    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the distribution table of a determine match
    ''' 
    data = []

    for i in range(len(driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TotalPasses '))):
        player_total_passes = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.TotalPasses ')[i].text # Pases
        player_pass_cross_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassCrossTotal ')[i].text # Centros
        player_pass_cross_accurate = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassCrossAccurate ')[i].text # CentrPrec
        player_pass_long_ball_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassLongBallTotal')[i].text # BL
        player_pass_long_ball_accurate = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassLongBallAccurate ')[i].text # BLPrec
        player_pass_trough_ball_total = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassThroughBallTotal ')[i].text # PHueco
        player_pass_through_ball_accurate = driver.find_elements(By.CSS_SELECTOR, 'tbody tr td.PassThroughBallAccurate ')[0].text # PHPrec
        
        new_data = {'TotalPasses': player_total_passes,
                    'PassCrossTotal': player_pass_cross_total,
                    'PassCrossAccurate': player_pass_cross_accurate,
                    'PassLongBallTotal': player_pass_long_ball_total,
                    'PassLongBallAccurate': player_pass_long_ball_accurate,
                    'PassThroughBallTotal': player_pass_trough_ball_total,
                    'PassThroughBallAccurate': player_pass_through_ball_accurate}

        data.append(new_data)

    return pd.DataFrame(data)

# %%
# chromedriver = ChromeDriverManager().install()

# %%
options = webdriver.ChromeOptions()
options.add_argument('executable_path=chromedriver')
driver = webdriver.Chrome(options=options)


# %%
driver.get('https://es.whoscored.com/Matches/1734941/Live/Espa%C3%B1a-LaLiga-2023-2024-Sevilla-Real-Sociedad')
time.sleep(2)

# %%
# # Para productizarlo
# from selenium.webdriver.chrome.options import Options
# chrome_options = Options()
# chrome_options.add_argument("--headless")

# driver = webdriver.Chrome(chromedriver, options=chrome_options)

# %%
driver.find_elements(By.CSS_SELECTOR, 'button')[2].click()
time.sleep(2)

# %%
try: 
    driver.find_elements(By.CSS_SELECTOR, 'button')[0].click()
    time.sleep(2)
except:
    pass


# %%
link_estadisticas_de_jugador = driver.find_elements(By.CSS_SELECTOR, '#sub-sub-navigation a')[1].get_attribute('href')

# %%
driver.get(link_estadisticas_de_jugador)

# %%
tabs_summary_offensive_defensive_distrib = driver.find_elements(By.CSS_SELECTOR, 'div.option-group ul.tabs li')

# Definition of the elements of the tabs for the summary, offensive, defensive and distribution stats
home_summary = tabs_summary_offensive_defensive_distrib[3]
home_offensive = tabs_summary_offensive_defensive_distrib[4]
home_defensive = tabs_summary_offensive_defensive_distrib[5]
home_distrib = tabs_summary_offensive_defensive_distrib[6]
away_summary = tabs_summary_offensive_defensive_distrib[7]
away_offensive = tabs_summary_offensive_defensive_distrib[8]
away_defensive = tabs_summary_offensive_defensive_distrib[9]
away_distrib = tabs_summary_offensive_defensive_distrib[10]

# %%
df_players_summary = get_df_summary (driver)

# %%
home_offensive.click()
time.sleep(1)
away_offensive.click()
time.sleep(1)

# %%
df_players_offensive = get_df_offensive(driver)

# %%
home_defensive.click()
time.sleep(1)
away_defensive.click()
time.sleep(1)

# %%
df_players_defensive = get_df_defensive(driver)

# %%
home_distrib.click()
time.sleep(1)
away_distrib.click()
time.sleep(1)

# %%
df_players_distribution = get_df_distribution(driver)

# %%
df_players_stats_match = pd.concat([df_players_summary, df_players_offensive, df_players_defensive, df_players_distribution], axis=1)


