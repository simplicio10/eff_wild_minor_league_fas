import polars as pl
from datetime import datetime
#Read in CSV with scraped player data
df = pl.read_csv('../files/free_agents/fas_13_15.csv')
#Split off player names into first and last, the lowercase is necessary for pybaseball player id lookup
names = list(df['player_name'])
split_names = [name.lower().split(' ', maxsplit=1) for name in names]
#Create list of player first and last names
first_names = [name[0] for name in split_names]
last_names = [name[1] for name in split_names]
#Add columns
df = df.with_columns(
    pl.col('fa_class').cast(pl.String), # Cast free agent year to string value
    pl.col('player_name').rank(method='ordinal').alias('rank'), #Creates a unique ID value for each player
    pl.Series(first_names).alias('first_name_lower'),
    pl.Series(last_names).alias('last_name_lower'),
    pl.when((pl.col('position') == 'RHP') | (pl.col('position') == 'LHP')) #Creates general position category for batters and pitchers
    .then(pl.lit('pitcher'))
    .otherwise(pl.lit('batter'))
    .alias('normed_position')
)
#Convert free agent class to date format
df = df.with_columns(
    pl.col('fa_class').str.strptime(
        pl.Date,
        format='%Y', 
        strict=False
    ).dt.year()
)

print(df.head(10))

#df.write_csv('../files/free_agents/fas_13_15_positions.csv')
    