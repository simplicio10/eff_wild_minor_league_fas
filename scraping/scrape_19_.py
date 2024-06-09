'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists from 2013 to 2015. These webpages are available at the Internet Archive.

The code returns a dictionary with the following format:

'[Player name]: [[Team], [Position], [Level]]'
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

new_list = [line.get_text() for line in article_body]
new_list = [line.replace('\xa0', ' ') for line in new_list]
new_list = [x for x in new_list if x not in ['\n']]


team_index = new_list.index('Baltimore Orioles')
comb_string = ''.join(new_list[team_index:team_index + 2])

#print(teams)

list_of_idxs = []

for i in new_list:
    if regex.fullmatch(find_teams, i):
        idx = new_list.index(i)
        list_of_idxs.append(idx)

list_of_comb_teams = []

for team_idx in list_of_idxs:
    full_string = ''.join(new_list[team_idx:team_idx + 2])
    list_of_comb_teams.append(full_string)

for team in list_of_comb_teams:
    matches = regex.findall(find_players_by_position, team)
    result = [f"{position}: {players}" for position, players in matches]
    team_name = regex.search(find_teams, team)
    for position, players in matches:
        player_list = players.split(", ")
        for player in player_list:
            match = regex.match(find_player, player)
            if match:
                player_team = team_name.group(0).strip()
                player_name = match.group(1).strip()
                player_level = match.group(2).strip()
                all_players.append({'player_name': player_name, 'team': player_team, 'fa_class': 2019, 
                                    'position': position, 'minor_league_level': player_level})
                
df = pl.DataFrame(data = all_players)  

print(df.head(10))