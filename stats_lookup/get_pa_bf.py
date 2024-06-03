from pybaseball import playerid_lookup
import polars as pl

ben_2015 = [('001', 'axford', 'john'), ('002', 'savery', 'joe'), ('003', 'romero', 'deibinson'), ('004', 'herrera', 'jonathan'), ('005', 'richard', 'clayton'),
            ('006', 'phelps', 'cord'), ('007', 'sizemore', 'scott'), ('008', 'baxter', 'mike'), ('009', 'britton', 'buck')]

players_to_id

def get_id(players_list):

    empty_list = []

    for player in players_list:
        #Default blank entry if player was not in MLB
        no_mlb_entry = {
        'name_last': player[2],
        'name_first': player[1],
        'key_mlbam': None,
        'key_retro': None,
        'key_bbref': None,
        'key_fangraphs': None,
        'mlb_played_first': None,
        'mlb_played_last': None
        }
        #Look up player id in pybaseball
        id = playerid_lookup(player[1], player[2])
        #Pybaseball returns a dataframe for the individual player. If the length is greater than 0 (i.e., the player has MLB experience), 
        #convert the dataframe into Polars
        if len(id) > 0:
            valid_id = pl.from_dataframe(id)
            valid_id = valid_id.with_columns(
                pl.Series(player[0]).alias('native_id')
            )
            empty_list.append(valid_id)
        #Otherwise, create a dataframe with null values using the above dictionary
        else:
            invalid_id = pl.from_dict(no_mlb_entry)
            invalid_id = invalid_id.with_columns(
                pl.Series(player[0]).alias('native_id')
            )
            empty_list.append(invalid_id)
        #Concatenate the individual player dataframes into a single dataframe
        players = pl.concat(empty_list)
        #Filter out the unnecessary columns
        players = players.select(['native_id', 'name_last', 'name_first', 'key_fangraphs', 
                                  'mlb_played_first', 'mlb_played_last'])
        players = players.with_columns([
            pl.col('mlb_played_first').cast(pl.String),
            pl.col('mlb_played_last').cast(pl.String)
        ])

    return players

#df = get_id(ben_2015)
#print(df)





