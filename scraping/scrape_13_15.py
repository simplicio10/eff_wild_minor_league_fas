'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists from 2013 to 2015. These webpages are available at the Internet Archive.
'''

import requests
from bs4 import BeautifulSoup
import re
from teams import teams_pre_23 as mlb_teams

#Scrape webpage to isolate a list of Minor League Free Agents for each team
webpage = requests.get('https://web.archive.org/web/20151114021021/https://www.baseballamerica.com/minors/minor-league-free-agents-2015/')
content = webpage.content
soup = BeautifulSoup(content, 'html.parser')
article_body = soup.find(class_="article-content").contents[1].contents
#Create clean text string for each team and add to list
free_agents_by_team = []
for s in article_body:
    free_agents_by_team.append(s.get_text().strip().replace('\n', ' '))
    free_agents_by_team = [x for x in free_agents_by_team if x != '']


print(re.findall(free_agents_by_team[8]))

'''
Goal is to return dictionary with the following format:

'[Player name]: [[Position], [Level], [Team]]'
'''