import polars as pl

players = pl.scan_csv('../files/free_agents/final/fas_final.csv', schema_overrides={'internal_id': pl.String})
stats = pl.scan_csv('../files/stats/player_stats_filtered.csv', schema_overrides={'internal_id': pl.String})
#stats_original = pl.scan_csv('../files/stats/player_stats.csv', schema_overrides={'internal_id': pl.String})
draftees = pl.scan_csv('../files/draft_results/draft_results_w_both_ids.csv', schema_overrides={'internal_id': pl.String})


#print(players.filter(pl.col('key_bbref_minors') == 'lawren001cas').select('full_name', 'fa_class', 'internal_id', 'key_bbref_minors', 'key_fangraphs').collect())
#print(stats.filter(pl.col('key_fangraphs') == 11121).collect())

all_players = players.join(
    stats, 
    on='internal_id',
    how='left')
all_players = all_players.join(draftees, on='internal_id', how='left').with_columns(
    is_drafted=pl.col('contestant').is_not_null()
).select(
    'internal_id', 
    'key_bbref_minors', 
    'full_name', 
    'fa_class', 
    'team', 
    'position', 
    'position_cat', 
    'G', 
    'pa_tbf',
    'is_drafted'
)

'''print(all_players.group_by(
    'fa_class')
    .agg(pl.col(on=['internal_id',
                    ]
                how='left'))
    )'''

all_players.collect().write_csv('test.csv')