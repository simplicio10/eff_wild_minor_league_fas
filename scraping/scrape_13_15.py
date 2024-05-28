'''
This code scrapes archived versions of Baseball America Minor League Free Agent
lists from 2013 to 2015. These webpages are available at the Internet Archive.
'''

import requests
from bs4 import BeautifulSoup
import regex
from teams import teams_pre_23 as mlb_teams

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

free_agents_by_team = free_agents_by_team[6:]
#print(re.findall('\w*:', free_agents_by_team[8]))
find_teams = regex.search(r'^\w+\.?\s\w+\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays)?', free_agents_by_team[20])

#print(len(players))
#print(type(players[1]))
#print(players)

'''for team in free_agents_by_team:
    found_team = regex.search(r'^\w+\.?\s\w+\s(Royals|Dodgers|Angels|Mets|Yankees|Padres|Giants|Cardinals|Jays|Rays|Sox)?', team)
    print(found_team.group(0))'''

# Define the regex pattern
player_by_position = r"([A-Z0-9]+):\s*([^:]+?(?=\s*[A-Z0-9]+:|\s*$))"
find_player = r'\w+\s\w+.\w+\s\(\w+\s?\w?\)'

# Find all matches
matches = regex.findall(player_by_position, free_agents_by_team[2])

# Convert matches to the desired list format
result = [f"{position}: {players}" for position, players in matches]

for group in result:
    position = r'([A-Z0-9]+):'
    level = r'\(\w+\s?\w?\)'


'''Baltimore Orioles RHP: Jesse Beal (Hi A), Pedro Beato (AAA), Bobby Bundy (AA), Matt Buschmann (AAA), Terry Doyle (AAA), Eddie Gamboa (AAA), 
Matt Hobgood (AA), Kenn Kasparek (AA), Williams Louico (Lo A), Jose Mateo (AA), Mikey O’Brien (Hi A), Marcel Prado (AA), Andrew Robinson (AA), 
Jason Stoffel (AA), Elih Villanueva (AAA) LHP: Mike Belfiore (AAA), Sean Bierman (Hi A), Pat McCoy (AAA), Andy Oliver (AAA), Ronan Pacheco (Hi A), 
Jhonathan Ramos (AA), Cody Wheeler (Hi A) C: Zach Booker (AAA), Shawn McGill (AA), Pedro Perez (Hi A), Rossmel Perez (AAA), Joel Polanco (AA) 
1B: Brandon Snyder (AA) 2B: Derrik Gibson (AAA), Sharlon Schoop (AAA) OF: Julio Borbon (AAA), Sean Halton (AAA), Anthony Hewitt (Hi A)'''

'''
Goal is to return dictionary with the following format:

'[Player name]: [[Position], [Level], [Team]]'
'''