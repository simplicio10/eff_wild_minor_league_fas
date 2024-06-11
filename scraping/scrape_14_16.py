'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists published from 2013 to 2015 (reflecting free agents available for the following season). 
These webpages are available at the Internet Archive.

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
find_teams = r'^.+?\s.+?\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays|Sox|Rays)?'
find_players_by_position = r'([A-Z0-9]+):\s*([^:]+?(?=\s*[A-Z0-9]+:|\s*$))'
find_player = r'(.+?)\s*\((.+?)\)'

#Create empty list
all_players = []

#List of webpages
webpages = [
    'https://web.archive.org/web/20131108050307/https://www.baseballamerica.com/minors/minor-league-free-agents-2013/',
    'https://web.archive.org/web/20141110163331/https://www.baseballamerica.com/minors/minor-league-free-agents-2014/',
    'https://web.archive.org/web/20151114021021/https://www.baseballamerica.com/minors/minor-league-free-agents-2015/']

fa_class = 2014

#Scrape webpage to isolate a list of Minor League Free Agents for each team
for webpage in webpages:
    page = requests.get(webpage)
    content = page.content
    soup = BeautifulSoup(content, 'html.parser')
    article_body = soup.find(class_="article-content").contents[1].contents

    #Create clean text string for each team and add to list
    free_agents_by_team = []
    for s in article_body:
        free_agents_by_team.append(s.get_text().strip().replace('\n', ' '))
        free_agents_by_team = [x for x in free_agents_by_team if x != '']

    #Function creates dictionary of players from all 30 organizations
    for team in free_agents_by_team:
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
                    all_players.append({'player_name': player_name, 'team': player_team, 'fa_class': fa_class, 
                                        'position': position, 'minor_league_level': player_level})
    
    fa_class += 1


df = pl.DataFrame(data=all_players)
df.write_csv('../files/free_agents/fas_14_16.csv')
