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
find_teams = r'^\w+\.?\s\w+\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays|Sox|Rays)?'
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
new_list = new_list.join()

print(new_list)