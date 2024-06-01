import polars as pl

df = pl.read_csv('../files/fas_13_15.csv')

names = list(df['player_name'])
split_names = [name.lower().split(' ', maxsplit=1) for name in names]

first_names = [name[0] for name in split_names]
last_names = [name[1] for name in split_names]

df = df.with_columns(
    pl.Series(first_names).alias('first_name_lower'),
    pl.Series(last_names).alias('last_name_lower'),
    pl.when((pl.col('position') == 'RHP') | (pl.col('position') == 'LHP'))
    .then(pl.lit('pitcher'))
    .otherwise(pl.lit('batter'))
    .alias('normed_position')
)

df.write_csv('../files/fas_13_15_positions.csv')
    