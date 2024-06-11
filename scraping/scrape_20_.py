'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists published from 2019 (reflecting free agents available for the following season).

The code returns a DataFrame with the following columns:
-Player Name
-Team (most recently played for)
-Free agent class
-Position
-Minor League Level (as listed by BA)
'''

import requests
from bs4 import BeautifulSoup
import regex
import polars as pl

# Define the regex patterns 
find_teams = r'^\w+\.?\s\w+\s?(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays|Sox|Rays)?'
find_players_by_position = r'([A-Z0-9]+):\s*([^:]+?(?=\s*[A-Z0-9]+:|\s*$))'
find_player = r'(.+?)\s*\((.+?)\)'

webpage = 'https://www.baseballamerica.com/stories/minor-league-free-agents-2019/'

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
    full_string = ' '.join(article_body_clean[team_idx:team_idx + 2])
    list_of_comb_teams.append(full_string)

for team in list_of_comb_teams:
    matches = regex.findall(find_players_by_position, team)
    team_name = regex.search(find_teams, team)
    for position, players in matches:
        player_list = players.split(", ")
        for player in player_list:
            match = regex.match(find_player, player)
            if match:
                player_team = team_name.group(0).strip()
                player_name = match.group(1).strip()
                player_level = match.group(2).strip()
                all_players.append({'player_name': player_name, 'team': player_team, 'fa_class': 2020, 
                                    'position': position, 'minor_league_level': player_level})
                
df = pl.DataFrame(data=all_players)  

df.write_csv('../files/free_agents/fas_20_.csv')