'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists from 2013 to 2015. These webpages are available at the Internet Archive.
'''

import requests
from bs4 import BeautifulSoup
import re
from teams import teams_pre_23 as mlb_teams

webpage = requests.get('https://web.archive.org/web/20151114021021/https://www.baseballamerica.com/minors/minor-league-free-agents-2015/')
content = webpage.content
soup = BeautifulSoup(content, 'html.parser')
paras = soup.find(class_="article-content")
paras_2 = paras.contents[1]

print(paras_2.contents[20:30])