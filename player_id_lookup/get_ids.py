import polars as pl
import sys
sys.path.insert(0, '/home/salviati/projects/baseball/custom_player_lookup/script')
import id_lookup_tool as player_id_lookup

fas_list = pl.read_csv('../files/free_agents/final/fas_to_check_cleaned.csv', schema_overrides={'internal_id': pl.String})
stats = pl.read_csv('../files/stats/player_stats.csv')
stats = stats.rename({'IDfg': 'key_fangraphs'})

#Testing
partial = fas_list.sample(n=25)

def process_valid_id(valid_id, player) -> pl.DataFrame:
    if valid_id.height > 1:
        valid_id = valid_id.with_columns(
            full_name=pl.lit(player['player_name']),
            team=pl.lit(player['team']),
            internal_id=pl.lit(player['internal_id']),
            fa_class=pl.lit(player['fa_class']),
            position_cat=pl.lit(player['normed_position']),
            chadwick_returned=pl.lit('multiple_passed_bref_check')
        )
    elif valid_id.height == 1:
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

def get_id(df) -> pl.DataFrame:

    final_players_list = []

    players = df.select('internal_id', 
                        'team', 
                        'player_name', 
                        'last_name', 
                        'first_name', 
                        'fa_class', 
                        'normed_position').to_dicts()

    for player in players:

        if 'Jr' in player['last_name']:
            player['last_name'] = player['last_name'].removesuffix(' Jr')
        
        if 'Jr.' in player['last_name']:
            player['last_name'] = player['last_name'].removesuffix(' Jr.')

        #Default blank entry if player not found
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
            'position_cat': player['normed_position'],
            'chadwick_returned': 'zero'
            }       
        #Look up player id in custom script
        if player['fa_class'] == 2021: 
            active_year = 2019 #Correction because no minor league season in 2020
        else:
            active_year = player['fa_class'] - 1
        id = pl.DataFrame(player_id_lookup.player_info_lookup(
            player['last_name'], 
            player['first_name'], 
            year_active=active_year, 
            check_accents=True,
            phonetic_match=True))    
        #Function returns a dataframe for the individual player. If the player has MLB experience (len(df) > 0), convert the dataframe into Polars.
        if id.height == 1:
            valid_id = process_valid_id(id, player)
            final_players_list.append(valid_id)
        elif id.height > 1:
            try:
                valid_id = player_id_lookup.ml_affiliate_filter(
                    id,
                    active_year,
                    player['team']
                )
            except:
                valid_id = id            
            valid_id = process_valid_id(valid_id, player)
            final_players_list.append(valid_id)
        #Otherwise, create a dataframe with null values using the no_entry variable
        else:
            years_back = 1
            while years_back < 9:
                active_year = active_year - years_back

                id = pl.DataFrame(player_id_lookup.player_info_lookup(
                    player['last_name'], 
                    player['first_name'], 
                    year_active=active_year, 
                    check_accents=True,
                    phonetic_match=True))      
                         
                if id.height == 1:
                    valid_id = process_valid_id(id, player)
                    final_players_list.append(valid_id)
                    break
                elif id.height > 1:
                    try:
                        valid_id = player_id_lookup.ml_affiliate_filter(
                            id,
                            active_year,
                            player['team']
                        )
                    except:
                        valid_id = id                 
                    valid_id = process_valid_id(valid_id, player)
                    final_players_list.append(valid_id)
                    break
                else:
                    years_back += 1

            if years_back == 9:
                invalid_id = pl.from_dict(no_entry)
                final_players_list.append(invalid_id)

    #Concatenate the individual player dataframes into a single dataframe
    players = pl.concat(final_players_list, how='diagonal_relaxed')
    #Filter out unnecessary columns
    players = players.select(['internal_id', 'full_name', 'team', 'name_last', 'name_first', 'fa_class', 'key_fangraphs', 
                            'key_bbref_minors','mlb_played_first', 'mlb_played_last', 'pro_played_first', 'pro_played_last',
                            'position', 'position_cat', 'chadwick_returned'])

    return players

id = get_id(fas_list)
#id = get_id(partial)
id.write_csv('../files/free_agents/final/fa_id_matched.csv')


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

#players_and_stats.write_csv('check_100.csv')
#print(players_and_stats)

#diaz = player_id_lookup.player_info_lookup('Diaz', 'Edwin', year_active=2021, check_accents=True)
#smith = player_id_lookup.player_info_lookup('Smith', 'Josh')











