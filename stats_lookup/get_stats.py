'''Use pybaseball to import MLB stats for each season and save to csv.
Includes player's FanGraphs ID, Season, Name, Team, Age, Games Played, and Plate Appearances/Total Batters Faced.
Saved as two separate csv files due to timeout issues in pybaseball when searching for stats across the full timeframe.'''

from pybaseball import batting_stats, pitching_stats
import polars as pl

batting_14_18 = pl.DataFrame(batting_stats(2014, 2018, qual=0, ind=1))
batting_14_18 = batting_14_18.select(['IDfg', 'Season', 'Name', 'Team', 'Age', 'G', 'PA'])

batting_19_23 = pl.DataFrame(batting_stats(2019, 2023, qual=0, ind=1))
batting_19_23 = batting_19_23.select(['IDfg', 'Season', 'Name', 'Team', 'Age', 'G', 'PA'])

pitching_14_18 = pl.DataFrame(pitching_stats(2014, 2018, qual=0, ind=1))
pitching_14_18 = pitching_14_18.select(['IDfg', 'Season', 'Name', 'Team', 'Age', 'G', 'TBF'])

pitching_19_23 = pl.DataFrame(pitching_stats(2019, 2023, qual=0, ind=1))
pitching_19_23 = pitching_19_23.select(['IDfg', 'Season', 'Name', 'Team', 'Age', 'G', 'TBF'])

batting_14_18.write_csv('../files/stats/batting_14_18.csv')
batting_19_23.write_csv('../files/stats/batting_19_23.csv')
pitching_14_18.write_csv('../files/stats/pitching_14_18.csv')
pitching_19_23.write_csv('../files/stats/pitching_19_23.csv')