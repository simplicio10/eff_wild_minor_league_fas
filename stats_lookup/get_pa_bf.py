from pybaseball import playerid_lookup, batting_stats, pitching_stats
import polars as pl

ben_2015 = [('axford', 'john'), ('savery', 'joe'), ('romero', 'deibinson'), ('herrera', 'jonathan'), ('richard', 'clayton'),
            ('phelps', 'cord'), ('sizemore', 'scott'), ('baxter', 'mike'), ('britton', 'buck')]

def get_id(players_list):

    empty_list = []

    for player in players_list:
        #Default blank entry if player was not in MLB
        no_mlb_entry = {
        'name_last': player[1],
        'name_first': player[0],
        'key_mlbam': None,
        'key_retro': None,
        'key_bbref': None,
        'key_fangraphs': None,
        'mlb_played_first': None,
        'mlb_played_last': None
        }

        id = playerid_lookup(player[0], player[1])

        if len(id) > 0:
            valid_id = pl.from_dataframe(id)
            empty_list.append(valid_id)
        else:
            invalid_id = pl.from_dict(no_mlb_entry)
            empty_list.append(invalid_id)
        players = pl.concat(empty_list)

    return players

df = get_id(ben_2015)
print(df)


batting_15 = pl.DataFrame(pitching_stats(2015, qual=0))
axford_df = batting_15.filter(pl.col('IDfg') == 9059)
print(axford_df)

