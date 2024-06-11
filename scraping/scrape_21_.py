'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists published from 2020 (reflecting free agents available for the following season).

The code returns a DataFrame with the following columns:
-Player Name
-Team (most recently played for)
'''

import requests
from bs4 import BeautifulSoup
import regex
import polars as pl

webpage = 'https://www.baseballamerica.com/stories/full-list-of-2020-2021-milb-free-agents/'

#Create empty list
all_players = []

page = requests.get(webpage)
content = page.content
soup = BeautifulSoup(content, 'html.parser')
#Isolate body text from website
article_body = soup.find(class_="page-layout__main").contents
#Clean body text to isolate teams
article_body_clean = [line.get_text() for line in article_body]
article_body_clean = [line.replace('\xa0', ' ') for line in article_body_clean]
article_body_clean = [x for x in article_body_clean if x not in ['\n']]
article_body_clean = [x for x in article_body_clean if x not in [' ']]

teams = article_body_clean[2:32]

#Function creates list of dictionaries with team name for each player
for team in teams:
    team_split = team.split(': ')
    team_name = team_split[0]
    team_name = regex.sub(r'\([0-9]+\)', '', team_name) #Removes the number of players in parentheses next to the team name
    players = team_split[1].split(', ') #Create list of all players on team

    for player in players:
        all_players.append({'player_name': player, 'team_name': team_name.strip(), 'fa_class': 2021})

df = pl.DataFrame(data=all_players)

df.write_csv('../files/free_agents/fas_21.csv')