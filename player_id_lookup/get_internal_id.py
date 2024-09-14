import polars as pl

players = pl.scan_csv('../free_agents/final/fas_final.csv')
players = players.rename({'fa_class': 'draft_year'})
draft_results = pl.scan_csv('draft_results_w_ids.csv')

draft_results_w_id = draft_results.join(players, on=['key_bbref_minors', 'draft_year'])
draft_results_w_id = draft_results_w_id.select('player', 'pa_tbf', 'contestant', 'draft_year', 'key_bbref_minors', 'internal_id')

draft_results_w_id.collect().write_csv('draft_results_w_both_ids.csv')