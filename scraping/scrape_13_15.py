'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists from 2013 to 2015. These webpages are available at the Internet Archive.

The code returns a dictionary with the following format:

'[Player name]: [[Team], [Position], [Level]]'
'''

import requests
from bs4 import BeautifulSoup
import regex

#Scrape webpage to isolate a list of Minor League Free Agents for each team
webpage = requests.get('https://web.archive.org/web/20151114021021/https://www.baseballamerica.com/minors/minor-league-free-agents-2015/') #2015 URL
content = webpage.content
soup = BeautifulSoup(content, 'html.parser')
article_body = soup.find(class_="article-content").contents[1].contents

#Create clean text string for each team and add to list
free_agents_by_team = []
for s in article_body:
    free_agents_by_team.append(s.get_text().strip().replace('\n', ' '))
    free_agents_by_team = [x for x in free_agents_by_team if x != '']

#Remove non-team info from text block
free_agents_by_team = free_agents_by_team[6:]

# Define the regex patterns 
find_teams = r'^\w+\.?\s\w+\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays)?'
find_players_by_position = r'([A-Z0-9]+):\s*([^:]+?(?=\s*[A-Z0-9]+:|\s*$))'
find_player = r'(.+?)\s*\((.+?)\)'

#Create empty dictionary
player_dict = {}

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
                player_name = match.group(1).strip()
                player_level = match.group(2).strip()
                player_dict[player_name] = (team_name.group(0), position, player_level)


print(len(player_dict))

