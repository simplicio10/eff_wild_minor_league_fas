import polars as pl

players = pl.scan_csv('../files/free_agents/final/fas_final.csv')
players = players.rename({'fa_class': 'Season'})

all_stats = pl.scan_csv('../files/stats/player_stats_2.csv', schema_overrides={'PA': pl.Int64, 'TBF': pl.Int64})
all_stats = all_stats.rename({'IDfg': 'key_fangraphs'})

stats_plus_id = all_stats.join(players, on=['key_fangraphs', 'Season'])
stats_plus_id = stats_plus_id.group_by(['key_fangraphs', 'Season']).agg([
    pl.col('internal_id').first(),
    pl.col('Age').first(),
    pl.col('G').first(),
    pl.col('PA').sum(),
    pl.col('TBF').sum()
])
stats_plus_id = stats_plus_id.with_columns([
    pl.coalesce(pl.max_horizontal(['PA', 'TBF'])).alias('pa_tbf')
])
stats_plus_id = stats_plus_id.select('key_fangraphs', 'internal_id', 'Season', 'Age', 'G', 'pa_tbf', 'PA', 'TBF')

stats_plus_id.collect().write_csv('../files/stats/player_stats_3.csv')