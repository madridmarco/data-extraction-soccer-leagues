from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd
import re 
def get_df_summary_backup (driver) -> pd.DataFrame:
    '''
    Extract the summary table of a determine match and return a dataframe with the data

    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the summary table of a determine match
    '''
    home_team = driver.find_element(By.CSS_SELECTOR, 'div div.teams-score-info span.home.team a.team-link').text

    away_team = driver.find_element(By.CSS_SELECTOR, 'div div.teams-score-info span.away.team a.team-link').text

    match_result = driver.find_element(By.CSS_SELECTOR, 'div.teams-score-info span.result').text

    home_goals = int(match_result.split(' : ')[0])
    away_goals = int(match_result.split(' : ')[1])

    match_date = driver.find_elements(By.CSS_SELECTOR, 'div.info-block.cleared dl dd')[4].text

    # Delete the day of the week from the date and split the date in day, month and year
    match_date = match_date.split(',')[1].strip()
    match_date = match_date.split('-')

    # Dictionary to convert the month from spanish to english
    month_conversion = {
        "ene": "01",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dic": "12"
    }

    # Change the format of the date from spanish to english
    match_date = '20' + match_date[2] + '-' + month_conversion[match_date[1]] + '-' + match_date[0]

    # Change the data type of match_date to DATE
    match_date = pd.to_datetime(match_date).date()

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

        new_data = {
            'MatchDate': match_date,
            'HomeTeam': home_team,
            'AwayTeam': away_team,
            'HomeGoals': home_goals,
            'AwayGoals': away_goals,
            'Player': player_name,
            'Shots': player_shots_total,
            'ShotsOnTarget': player_shots_on_target,
            'KeyPass': player_key_pass_total,
            'PassSuccessInMatch': player_pass_success_in_match,
            'DuelAerialWon': player_duel_aerial_won,
            'Touches': player_touches,
            'Rating': player_rating}

        data.append(new_data)

    return pd.DataFrame(data)

def get_df_summary (driver) -> pd.DataFrame:
    '''
    Extract the summary table of a determine match and return a dataframe with the data

    Args:
    driver: webdriver.Chrome

    Returns:
    pd.DataFrame: Dataframe with the summary table of a determine match
    '''
    home_team = driver.find_element(By.CSS_SELECTOR, 'div div.teams-score-info span.home.team a.team-link').text

    away_team = driver.find_element(By.CSS_SELECTOR, 'div div.teams-score-info span.away.team a.team-link').text

    match_result = driver.find_element(By.CSS_SELECTOR, 'div.teams-score-info span.result').text

    numbers = numbers = re.findall(r'\d+', match_result)
    home_goals = int(numbers[0])
    away_goals = int(numbers[1])
    match_date_error = 0
    try:
        match_date = driver.find_elements(By.CSS_SELECTOR, 'div.info-block.cleared dl dd')[6].text
    except:
        match_date_error = 1
    
    if match_date_error == 1:
        match_date = driver.find_elements(By.CSS_SELECTOR, 'div.info-block.cleared dl dd')[4].text

    # Delete the day of the week from the date and split the date in day, month and year
    match_date = match_date.split(',')[1].strip()
    match_date = match_date.split('-')

    # Dictionary to convert the month from spanish to english
    month_conversion = {
        "ene": "01",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",    
        "dic": "12"
    }

    # Change the format of the date from spanish to english
    match_date = '20' + match_date[2] + '-' + month_conversion[match_date[1]] + '-' + match_date[0]

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
        if player_rating == '-':
            player_rating = 0

        new_data = {
            'MatchDate': match_date,
            'HomeTeam': home_team,
            'AwayTeam': away_team,
            'HomeGoals': int(home_goals),
            'AwayGoals': int(away_goals),
            'Player': player_name,
            'Shots': int(player_shots_total),
            'ShotsOnTarget': int(player_shots_on_target),
            'KeyPass': int(player_key_pass_total),
            'PassSuccessInMatch': float(player_pass_success_in_match),
            'DuelAerialWon': int(player_duel_aerial_won),
            'Touches': int(player_touches),
            'Rating': float(player_rating)}

        data.append(new_data)

    return pd.DataFrame(data)

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

        new_data = {'TackleWonTotal': int(player_tackle_won_total),
                    'InterceptionAll': int(player_interception_all),
                    'ClearanceTotal': int(player_clearance_total),
                    'ShotBlocked': int(player_shot_blocked),
                    'FoulCommitted': int(player_foul_committed)}

        data.append(new_data)

    return pd.DataFrame(data)

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

        new_data = {'TackleWonTotal': int(player_tackle_won_total),
                    'InterceptionAll': int(player_interception_all),
                    'ClearanceTotal': int(player_clearance_total),
                    'ShotBlocked': int(player_shot_blocked),
                    'FoulCommitted': int(player_foul_committed)}

        data.append(new_data)

    return pd.DataFrame(data)
    
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
        
        new_data = {'TotalPasses': int(player_total_passes),
                    'PassCrossTotal': int(player_pass_cross_total),
                    'PassCrossAccurate': int(player_pass_cross_accurate),
                    'PassLongBallTotal': int(player_pass_long_ball_total),
                    'PassLongBallAccurate': int(player_pass_long_ball_accurate),
                    'PassThroughBallTotal': int(player_pass_trough_ball_total),
                    'PassThroughBallAccurate': int(player_pass_through_ball_accurate)}

        data.append(new_data)

    return pd.DataFrame(data)

def close_pop_up_cookies(driver) -> webdriver.Chrome:
    '''
    Close the pop-up window of cookies

    Args:
    driver: webdriver.Chrome
    '''

    # retry the below steps three times:
    i = 0
    while i<3:
        try:
            driver.find_element(By.CSS_SELECTOR, 'div.webpush-swal2-header button').click()
            time.sleep(2)
        except:
            pass

        try:
            driver.find_elements(By.CSS_SELECTOR, 'button')[2].click()
            time.sleep(2)
        except:
            pass

        try: 
            driver.find_elements(By.CSS_SELECTOR, 'button')[0].click()
            time.sleep(2)
        except:
            pass
        
        i += 1

    return driver
   
chrome_options = Options()
chrome_options.add_argument("--headless")

driver = webdriver.Chrome(options=chrome_options)

driver.get('https://es.whoscored.com/LiveScores')
time.sleep(2)

close_pop_up_cookies(driver)

driver.get('https://es.whoscored.com/LiveScores')
time.sleep(2)
close_pop_up_cookies(driver)

# Go to the previous day
try:
    previous_day = WebDriverWait(driver, 15).until(  
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'button#dayChangeBtn-prev.Calendar-module_dayChangeBtn__sEvC8'))
    )
except TimeoutException:
    print("The 'Previous day' element was not clickable within the timeout.")
    # Perform alternative actions if the element was not clickable

previous_day.click()

# List the links to each of the matches of the day
time.sleep(5)
links_to_matches_list = [driver.find_elements(By.CSS_SELECTOR, 'div.Match-module_right_oddsOn__o-ux- a')[i].get_attribute('href')
    for i in range(len(driver.find_elements(By.CSS_SELECTOR, 'div.Match-module_right_oddsOn__o-ux- a')))]

df_day_stats = pd.DataFrame() #Dataframe which will store the stats of all the matches of the day
for match_link in links_to_matches_list:
    driver.get(match_link)

    # Click in the link to the player stats
    close_pop_up_cookies(driver)
    try:
        player_stats_link = WebDriverWait(driver, 10).until(  # Adjust timeout as needed
            EC.presence_of_element_located((By.LINK_TEXT, "Estadísticas de Jugador"))
        )
        player_stats_link.click()  # Click the link if found
    except TimeoutException:
        print("The 'Estadísticas de Jugador' link was not found within the timeout.")
    i=0
    while len(driver.find_elements(By.CSS_SELECTOR, 'div.option-group ul.tabs li')) == 0:
        driver.back()
        close_pop_up_cookies(driver)
        try:
            player_stats_link = WebDriverWait(driver, 10).until(  # Adjust timeout as needed
                EC.presence_of_element_located((By.LINK_TEXT, "Estadísticas de Jugador"))
            )
            player_stats_link.click()  # Click the link if found
            close_pop_up_cookies(driver)
        except TimeoutException:
            print("The 'Estadísticas de Jugador' link was not found within the timeout.")
        i+=1
        if i==5:
            break

    tabs_summary_offensive_defensive_distrib = driver.find_elements(By.CSS_SELECTOR, 'div.option-group ul.tabs li')

    # Definition of the elements of the tabs for the summary, offensive, defensive and distribution stats
    home_summary = tabs_summary_offensive_defensive_distrib[0]
    home_offensive = tabs_summary_offensive_defensive_distrib[1]
    home_defensive = tabs_summary_offensive_defensive_distrib[2]
    home_distrib = tabs_summary_offensive_defensive_distrib[3]
    away_summary = tabs_summary_offensive_defensive_distrib[4]
    away_offensive = tabs_summary_offensive_defensive_distrib[5]
    away_defensive = tabs_summary_offensive_defensive_distrib[6]
    away_distrib = tabs_summary_offensive_defensive_distrib[7]

    df_summary = get_df_summary(driver)

    # Go to the offensive tab
    home_offensive.click()
    time.sleep(4)
    away_offensive.click()
    time.sleep(4)
    df_offensive = get_df_offensive(driver)

    # Go to the defensive tab
    home_defensive.click()
    time.sleep(4)
    away_defensive.click()
    time.sleep(4)
    df_defensive = get_df_defensive(driver)

    # Go to the distribution tab
    home_distrib.click()
    time.sleep(4)
    away_distrib.click()
    time.sleep(4)
    df_distribution = get_df_distribution(driver)

    df_match_stats = pd.concat([df_summary, df_offensive, df_defensive, df_distribution], axis=1)
    df_day_stats = pd.concat([df_day_stats, df_match_stats])