import polars as pl
#Read in CSVs with scraped player data
columns = ['player_name', 'team', 'fa_class', 'position', 'minor_league_level']

#Combine and concatenate dataframes
df = pl.scan_csv('filepath') #Use asterisk to glob CSVs

#Split off player names into first and last, the lowercase is necessary for pybaseball player id lookup
names = df.select('player_name').to_series().to_list()
split_names = [name.split(' ', maxsplit=1) for name in names]

#Create list of player first and last names
first_names = [name[0] for name in split_names]
last_names = [name[1].strip('*') for name in split_names]

#Add column with abbreviated team names
team_names = pl.scan_csv('team_abbreviations.csv')
df = df.join(team_names, on='team')

#Add columns
df = df.with_columns(
    pl.col('fa_class').cast(pl.String), # Cast free agent year to string value
    pl.col('player_name').rank(method='ordinal').cast(pl.String).alias('internal_id'), #Creates a unique ID value for each player
    pl.Series(first_names).alias('first_name'),
    pl.Series(last_names).alias('last_name'),
    pl.when((pl.col('position') == 'RHP') | (pl.col('position') == 'LHP')) #Creates general position category for batters and pitchers
    .then(pl.lit('pitcher'))
    .when((pl.col('position').is_null()))
    .then(pl.lit('N/A'))
    .otherwise(pl.lit('batter'))
    .alias('normed_position')
)

#Reorder columns
df = df.select(
    pl.col('internal_id'),
    pl.col('player_name'),
    pl.col('first_name'),
    pl.col('last_name'),
    pl.col('team_abbreviation').alias('team'),
    pl.col('fa_class'),
    pl.col('position'),
    pl.col('normed_position'),
    pl.col('minor_league_level'),
 
)

#df.collect().write_csv('filepath')

