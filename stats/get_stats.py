'''Use pybaseball to import MLB stats for each season and save to csv.
Includes player's FanGraphs ID, Season, Name, Team, Age, Games Played, and Plate Appearances/Total Batters Faced.
Separated by pitcher v. batter and year due to timeout issues in pybaseball when searching for stats across the full timeframe.'''

from pybaseball import batting_stats, pitching_stats
import polars as pl

#years = list(range(1993, 2024, 5))
all_stats = pl.scan_csv('../files/stats/player_stats_1.csv', schema_overrides={'Season': pl.String})

'''for year in years:
    idx = years.index(year)
    if year in years[0:-1]:
        year_to = years[idx + 1] - 1
    else:
        year_to = years[-1]

    batting_df = batting_stats(year, year_to, qual=0, ind=1)
    batting_lazy = pl.from_pandas(batting_df).lazy()
    batting_lazy = batting_lazy.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'PA'])

    pitching_df = pitching_stats(year, year_to, qual=0, ind=1)
    pitching_lazy = pl.from_pandas(pitching_df).lazy()
    pitching_lazy = pitching_lazy.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'TBF'])

    all_stats = pl.concat([all_stats, batting_lazy.collect(), pitching_lazy.collect()], how='diagonal')'''


#2024
year_24 = 2024

def get_annual_stats(year):
    batting_df = batting_stats(year, year, qual=0, ind=1)
    batting_lazy = pl.from_pandas(batting_df).lazy()
    batting_lazy = batting_lazy.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'PA'])

    pitching_df = pitching_stats(year, year, qual=0, ind=1)
    pitching_lazy = pl.from_pandas(pitching_df).lazy()
    pitching_lazy = pitching_lazy.select(['IDfg', pl.col('Season').cast(pl.String), 'Age', 'G', 'TBF'])

    new_stats = pl.concat([all_stats, batting_lazy, pitching_lazy], how='diagonal_relaxed')

    return new_stats

stats_update = get_annual_stats(year_24)
stats_update.collect().write_csv('../files/stats/player_stats_2.csv')

