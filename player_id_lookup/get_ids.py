import polars as pl
import sys
sys.path.insert(0, '/home/salviati/projects/baseball/custom_player_lookup/script')
import id_lookup_tool as player_id_lookup

fas_list = pl.scan_csv('../files/free_agents/final/fas_to_check_cleaned.csv', schema_overrides={'internal_id': pl.String})
stats = pl.scan_csv('../files/stats/player_stats.csv')
stats = stats.rename({'IDfg': 'key_fangraphs'})

def process_valid_id(valid_id, player) -> pl.LazyFrame:
    if valid_id.collect().height > 1:
        valid_id = valid_id.with_columns(
            full_name=pl.lit(player['player_name']),
            team=pl.lit(player['team']),
            internal_id=pl.lit(player['internal_id']),
            fa_class=pl.lit(player['fa_class']),
            position_cat=pl.lit(player['normed_position']),
            chadwick_returned=pl.lit('multiple_passed_bref_check')
        )
    elif valid_id.collect().height == 1:
        valid_id = valid_id.with_columns(
            full_name=pl.lit(player['player_name']),
            team=pl.lit(player['team']),
            internal_id=pl.lit(player['internal_id']),
            fa_class=pl.lit(player['fa_class']),
            position_cat=pl.lit(player['normed_position']),
            chadwick_returned=pl.lit('one')
        )
    else:
        valid_id = valid_id.with_columns(
            full_name=pl.lit(player['player_name']),
            team=pl.lit(player['team']),
            internal_id=pl.lit(player['internal_id']),
            fa_class=pl.lit(player['fa_class']),
            position_cat=pl.lit(player['normed_position']),
            chadwick_returned=pl.lit('multiple_failed_bref_check')
        )
    return valid_id

def get_id(df) -> pl.LazyFrame:
    final_players_list = []

    players = df.select('internal_id', 
                        'team', 
                        'player_name', 
                        'last_name', 
                        'first_name', 
                        'fa_class',
                        'position', 
                        'normed_position').collect().to_dicts()

    for player in players:
        if 'Jr' in player['last_name']:
            player['last_name'] = player['last_name'].removesuffix(' Jr')
        
        if 'Jr.' in player['last_name']:
            player['last_name'] = player['last_name'].removesuffix(' Jr.')

        no_entry = {
            'full_name': player['player_name'],
            'team': player['team'],
            'name_last': player['last_name'],
            'name_first': player['first_name'],
            'key_mlbam': None,
            'key_retro': None,
            'key_bbref': None,
            'key_fangraphs': None,
            'mlb_played_first': None,
            'mlb_played_last': None,
            'pro_played_first': None,
            'pro_played_last': None,
            'internal_id': player['internal_id'],
            'fa_class': player['fa_class'],
            'position': player['position'],
            'position_cat': player['normed_position'],
            'chadwick_returned': 'zero'
        }       

        active_year = 2019 if player['fa_class'] == 2021 else player['fa_class'] - 1
        
        id = pl.LazyFrame(player_id_lookup.player_info_lookup(
            player['last_name'], 
            player['first_name'], 
            year_active=active_year, 
            check_accents=True,
            phonetic_match=True))    

        if id.collect().height == 1:
            valid_id = process_valid_id(id, player)
            final_players_list.append(valid_id)
        elif id.collect().height > 1:
            try:
                valid_id = pl.LazyFrame(player_id_lookup.ml_affiliate_filter(
                    id.collect(),
                    active_year,
                    player['team']
                ))
            except:
                valid_id = id            
            valid_id = process_valid_id(valid_id, player)
            final_players_list.append(valid_id)
        else:
            years_back = 1
            while years_back < 9:
                active_year = active_year - years_back

                id = pl.LazyFrame(player_id_lookup.player_info_lookup(
                    player['last_name'], 
                    player['first_name'], 
                    year_active=active_year, 
                    check_accents=True,
                    phonetic_match=True))      
                         
                if id.collect().height == 1:
                    valid_id = process_valid_id(id, player)
                    final_players_list.append(valid_id)
                    break
                elif id.collect().height > 1:
                    try:
                        valid_id = pl.LazyFrame(player_id_lookup.ml_affiliate_filter(
                            id.collect(),
                            active_year,
                            player['team']
                        ))
                    except:
                        valid_id = id                 
                    valid_id = process_valid_id(valid_id, player)
                    final_players_list.append(valid_id)
                    break
                else:
                    years_back += 1

            if years_back == 9:
                invalid_id = pl.LazyFrame(no_entry)
                final_players_list.append(invalid_id)

    players = pl.concat(final_players_list, how='diagonal_relaxed')
    players = players.select(['internal_id', 'full_name', 'team', 'name_last', 'name_first', 'fa_class', 'key_fangraphs', 
                            'key_bbref_minors', 'mlb_played_first', 'mlb_played_last', 'pro_played_first', 'pro_played_last',
                            'position', 'position_cat', 'chadwick_returned'])

    return players

missing_pro_fields = pl.scan_csv('../files/free_agents/final/fa_id_matched_updated.csv', schema_overrides={
    'key_bbref_minors': pl.String,
    'key_fangraphs': pl.String})


fas_to_update = pl.scan_csv('../files/free_agents/final/fa_id_matched_updated.csv')
fas_to_update_filtered = fas_to_update.filter(pl.col('chadwick_returned') == 'zero')
ids_to_update = fas_to_update_filtered.collect().select('key_bbref_minors').to_series().to_list()
cols_to_update = ['pro_played_first', 
                'pro_played_last', 
                'mlb_played_first', 
                'mlb_played_last', 
                'key_fangraphs']

def update_row(minor_key_list: list):
    results = []
    for key in minor_key_list:
        result = pl.DataFrame(player_id_lookup.player_lookup_by_milb_id(key))
        result = result.with_columns([pl.struct(cols_to_update).alias('pro_info')
    ])
        result = result.with_columns([
            pl.col('pro_info').struct.field(cols_to_update[0]).alias(cols_to_update[0]),
            pl.col('pro_info').struct.field(cols_to_update[1]).alias(cols_to_update[1]),
            pl.col('pro_info').struct.field(cols_to_update[2]).alias(cols_to_update[2]),
            pl.col('pro_info').struct.field(cols_to_update[3]).alias(cols_to_update[3]),
            pl.col('pro_info').struct.field(cols_to_update[4]).alias(cols_to_update[4])
        ]).drop('pro_info')

        results.append(result)
    final_results = pl.concat(results)

    return final_results.lazy()

filtered_data = update_row(ids_to_update)
filtered_data = filtered_data.select('key_bbref_minors', 
                                     cols_to_update[0],
                                     cols_to_update[1],
                                     cols_to_update[2],
                                     cols_to_update[3],
                                     cols_to_update[4])

fas_to_update = (
    fas_to_update
    .join(
        filtered_data,
        left_on='key_bbref_minors',
        right_on='key_bbref_minors',
        how='left',
        suffix='_update'
    )
    .with_columns([
        pl.when(pl.col(f'{col}_update').is_not_null())
        .then(pl.col(f'{col}_update'))
        .otherwise(pl.col(col))
        .alias(col)
        for col in cols_to_update
    ])
    .drop([f'{col}_update' for col in cols_to_update])
    .unique(maintain_order=True)
)

fas_to_update.collect().write_csv('../files/free_agents/final/fa_ids_complete.csv')

'''players_and_stats = id.lazy().collect().join(stats.lazy().collect(), on='key_fangraphs', how='left')
players_and_stats = players_and_stats.with_columns(
    pl.when(pl.col('fa_class') == pl.col('Season'))
        .then(pl.lit(True, dtype=bool))
        .otherwise(pl.lit(False, dtype=bool))
        .alias('played_in_fa_season'),
    pl.when(pl.col('mlb_played_first').is_not_null())
        .then(pl.lit(True, dtype=bool))
        .otherwise(pl.lit(False, dtype=bool))
        .alias('played_in_mlb')
    )'''













