import glob
from pybaseball import playerid_lookup
import polars as pl



fas_list = pl.read_csv('../files/free_agents/final/fas_to_check.csv', dtypes={'internal_id': pl.String})

partial = fas_list.sample(n=20)

stats = [
   pl.scan_csv(file, ignore_errors=True)
   for file in glob.glob('../files/stats/*.csv')
]
stats = pl.concat(stats, how='diagonal')
stats = stats.rename({'IDfg': 'key_fangraphs'})

def get_id(df):

    final_players_list = []

    players = df.select('internal_id', 'first_name_lower', 'last_name_lower').to_dicts()

    for player in players:
        #Default blank entry if player was not in MLB
        no_mlb_entry = {
        'name_last': player['last_name_lower'],
        'name_first': player['first_name_lower'],
        'key_mlbam': None,
        'key_retro': None,
        'key_bbref': None,
        'key_fangraphs': None,
        'mlb_played_first': None,
        'mlb_played_last': None,
        'internal_id': player['internal_id']
        }
        #Look up player id in pybaseball
        id = playerid_lookup(player['last_name_lower'], player['first_name_lower'])
        #Pybaseball returns a dataframe for the individual player. If the player has MLB experience (len(df) > 0), convert the dataframe into Polars.
        if len(id) > 0:
            valid_id = pl.from_dataframe(id)
            valid_id = valid_id.with_columns(
                internal_id=pl.lit(player['internal_id'])
            )
            final_players_list.append(valid_id)
        #Otherwise, create a dataframe with null values using the above dictionary
        else:
            invalid_id = pl.from_dict(no_mlb_entry)
            final_players_list.append(invalid_id)
    #Concatenate the individual player dataframes into a single dataframe
    players = pl.concat(final_players_list, how='diagonal_relaxed')
    #Filter out the unnecessary columns
    players = players.select(['internal_id', 'name_last', 'name_first', 'key_fangraphs', 
                            pl.col('mlb_played_first').cast(pl.String), pl.col('mlb_played_last').cast(pl.String)])
    #Convert years to Polars date format
    players = players.with_columns(
        start_date=pl.col('mlb_played_first')
            .str.slice(0, length=4)
            .str.strptime(
                pl.Date,
                format='%Y',
                strict=False
        ),
        end_date=pl.col('mlb_played_last')
            .str.slice(0, length=4)
            .str.strptime(
                pl.Date,
                format='%Y',
                strict=False
        ))

    return players

#players = get_id(fas_list)

id = get_id(partial)

players_and_stats = id.lazy().collect().join(stats.lazy().collect(), on='key_fangraphs', how='left')
players_and_stats.write_csv('test_join.csv')








