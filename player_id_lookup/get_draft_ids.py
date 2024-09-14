import polars as pl
import sys
sys.path.insert(0, '/home/salviati/projects/baseball/custom_player_lookup/script')
import id_lookup_tool as player_id_lookup

players = pl.scan_csv('../files/free_agents/final/fas_final.csv')
players = players.rename({'fa_class': 'draft_year'})
draft_results = pl.scan_csv('../files/draft_results/draft_results_w_ids.csv')
draft_results = draft_results.with_columns([
    pl.col('player').str.splitn(by=' ', n=2)
    .alias('name_struct')
]).with_columns([
    pl.col('name_struct').struct[0].alias('name_first'),
    pl.col('name_struct').struct[1].alias('name_last'),
]).drop('name_struct')

draft_combined = draft_results.join(players, on=['name_first', 'name_last', 'draft_year'], how='left')
draft_combined = draft_combined.select('player', 'pa_tbf', 'contestant', 'draft_year', 'key_bbref_minors')

draft_combined.collect().write_csv('../files/draft_results/test_join.csv')
