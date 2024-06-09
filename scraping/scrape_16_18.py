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
find_teams = r'^.+?\s.+?\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays|Sox|Rays)?'
find_players_by_position = r'([A-Z0-9]+):\s*([^:]+?(?=\s*[A-Z0-9]+:|\s*$))'
find_player = r'(.+?)\s*\((.+?)\)'

#Create empty list
all_players = []

webpages = [
    'https://www.baseballamerica.com/stories/minor-league-free-agents-2016/',
    'https://www.baseballamerica.com/stories/minor-league-free-agents-2017/',
    'https://www.baseballamerica.com/stories/minor-league-free-agents-2018/'
    ]

fa_class = 2017

#Scrape webpage to isolate a list of Minor League Free Agents for each team
for webpage in webpages:
    page = requests.get(webpage)
    content = page.content
    soup = BeautifulSoup(content, 'html.parser')
    article_body = soup.find(class_="page-layout__main").contents

    #Create clean text string for each team and add to list
    free_agents_by_team = []
    for s in article_body:
        free_agents_by_team.append(s.get_text().strip().replace('\n', ' '))
        free_agents_by_team = [x for x in free_agents_by_team if x != '']

    #Function creates dictionary of players from all 30 organizations
    for team in free_agents_by_team:
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
                    all_players.append({'player_name': player_name, 'team': player_team, 'fa_class': fa_class, 
                                        'position': position, 'minor_league_level': player_level})
    
    fa_class += 1


df = pl.DataFrame(data=all_players)

print(df.height)
print(df['team'].unique(maintain_order=True))