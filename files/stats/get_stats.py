'''Use pybaseball to import MLB stats for each season and save to csv.
Includes player's FanGraphs ID, Season, Name, Team, Age, Games Played, and Plate Appearances/Total Batters Faced.
Separated by pitcher v. batter and year due to timeout issues in pybaseball when searching for stats across the full timeframe.'''

from pybaseball import batting_stats, pitching_stats
import polars as pl

batting_14_18 = pl.DataFrame(batting_stats(2014, 2018, qual=0, ind=1))
batting_14_18 = batting_14_18.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'PA'])

batting_19_23 = pl.DataFrame(batting_stats(2019, 2023, qual=0, ind=1))
batting_19_23 = batting_19_23.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'PA'])

pitching_14_18 = pl.DataFrame(pitching_stats(2014, 2018, qual=0, ind=1))
pitching_14_18 = pitching_14_18.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'TBF'])

pitching_19_23 = pl.DataFrame(pitching_stats(2019, 2023, qual=0, ind=1))
pitching_19_23 = pitching_19_23.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'TBF'])

all_stats = pl.concat([batting_14_18, batting_19_23, pitching_14_18, pitching_19_23], how='diagonal')
all_stats = all_stats.with_columns(
    pl.col('Season')
    .str.strptime(
        pl.Date,
        format='%Y',
        strict=False   
    ))

all_stats.write_csv('player_stats.csv')
