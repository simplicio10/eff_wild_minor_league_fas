'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists published from 2021 (reflecting free agents available for the following season).

The code returns a DataFrame with the following columns:
-Player Name
-Team (most recently played for)
-Free agent class
-Position
'''

import requests
from bs4 import BeautifulSoup
import regex
import polars as pl

# Define the regex patterns 
find_teams = r'\w+?\.?\s\w+\s?(Royals|Dodgers|Angel|Angels|Mets|Yankees|Padres|Giants|Cardinals|Cadinals|Jays|Sox|Rays)?\s?'
find_players_by_position = r'([A-Z0-9]{1,3}) ([A-Za-z\. ]+?)(?=([A-Z0-9]{1,3}) |$)'

webpage = 'https://www.baseballamerica.com/stories/2022-23-minor-league-free-agents-for-all-30-mlb-teams/'

#Create empty list
all_players = []

page = requests.get(webpage)
content = page.content
soup = BeautifulSoup(content, 'html.parser')
article_body = soup.find(class_="page-layout__main").contents

article_body_clean = [line.get_text() for line in article_body]
article_body_clean = [line.replace('\xa0', ' ') for line in article_body_clean]
article_body_clean = [x for x in article_body_clean if x not in ['\n']]

list_of_idxs = []

for i in article_body_clean:
    if regex.fullmatch(find_teams, i):
        idx = article_body_clean.index(i)
        list_of_idxs.append(idx)

list_of_comb_teams = []

for team_idx in list_of_idxs:
    team_idx_num = list_of_idxs.index(team_idx)
    if team_idx_num < (len(list_of_idxs) - 1):
        next_team = list_of_idxs[team_idx_num + 1]
        full_string = ' '.join(article_body_clean[team_idx:next_team])
        list_of_comb_teams.append(full_string)
    else:
        full_string = ' '.join(article_body_clean[team_idx:])
        list_of_comb_teams.append(full_string)

for team in list_of_comb_teams:
    player_matches = regex.findall(find_players_by_position, team)
    team_name = regex.search(find_teams, team)
    players = [{'player_name':player_name, 'position':position} for position, player_name, _ in player_matches]
    for player in players:
        all_players.append({'player_name': player['player_name'].strip(), 'team': team_name.group(0).strip(), 'fa_class': 2023, 
                            'position': player['position']})

df = pl.DataFrame(data=all_players)

df.write_csv('../files/free_agents/fas_23.csv')