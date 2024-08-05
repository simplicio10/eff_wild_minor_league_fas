import baseball.custom_player_lookup as playerid_lookup
import polars as pl

fas_list = pl.read_csv('../files/free_agents/final/fas_to_check.csv', dtypes={'internal_id': pl.String})
stats = pl.read_csv('../files/stats/player_stats.csv')
stats = stats.rename({'IDfg': 'key_fangraphs'})

#Testing
partial = fas_list.sample(n=100)

def get_id(df):

    final_players_list = []

    players = df.select('internal_id', 
                        'team', 
                        'player_name', 
                        'last_name_lower', 
                        'first_name_lower', 
                        'fa_class', 
                        'normed_position').to_dicts()

    for player in players:
        #Default blank entry if player was not in MLB
        no_mlb_entry = {
            'full_name': player['player_name'],
            'team': player['team'],
            'name_last': player['last_name_lower'],
            'name_first': player['first_name_lower'],
            'key_mlbam': None,
            'key_retro': None,
            'key_bbref': None,
            'key_fangraphs': None,
            'mlb_played_first': None,
            'mlb_played_last': None,
            'internal_id': player['internal_id'],
            'fa_class': player['fa_class'],
            'position_cat': player['normed_position']
            }
        #Look up player id in pybaseball
        id = playerid_lookup(player['last_name_lower'], player['first_name_lower'])
        #Pybaseball returns a dataframe for the individual player. If the player has MLB experience (len(df) > 0), convert the dataframe into Polars.
        if len(id) > 0:
            valid_id = pl.from_pandas(id)
            valid_id = valid_id.with_columns(
                full_name=pl.lit(player['player_name']),
                team=pl.lit(player['team']),
                internal_id=pl.lit(player['internal_id']),
                fa_class=pl.lit(player['fa_class']),
                position_cat=pl.lit(player['normed_position'])
            )
            final_players_list.append(valid_id)
        #Otherwise, create a dataframe with null values using the no_mlb_entry variable
        else:
            invalid_id = pl.from_dict(no_mlb_entry)
            final_players_list.append(invalid_id)
    #Concatenate the individual player dataframes into a single dataframe
    players = pl.concat(final_players_list, how='diagonal_relaxed')
    #Filter out unnecessary columns
    players = players.select(['internal_id', 'full_name', 'team', 'name_last', 'name_first', 'fa_class', 'key_fangraphs', 
                            pl.col('mlb_played_first').cast(pl.String), pl.col('mlb_played_last').cast(pl.String), 
                            'position_cat'])

    return players

#players = get_id(fas_list)

id = get_id(partial)

players_and_stats = id.lazy().collect().join(stats.lazy().collect(), on='key_fangraphs', how='left')
players_and_stats = players_and_stats.with_columns(
    pl.when(pl.col('fa_class') == pl.col('Season'))
        .then(pl.lit(True, dtype=bool))
        .otherwise(pl.lit(False, dtype=bool))
        .alias('played_in_fa_season'),
    pl.when(pl.col('mlb_played_first').is_not_null())
        .then(pl.lit(True, dtype=bool))
        .otherwise(pl.lit(False, dtype=bool))
        .alias('played_in_mlb')
    )

players_and_stats.write_csv('check_100.csv')
#print(players_and_stats)








