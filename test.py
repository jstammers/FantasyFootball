from sqlite_utils import Database

from create_database import get_lineups
from utils import lineups_to_array


db = Database('openddata.db')

lineups = get_lineups(db)
home_players, away_players, home_scores, away_scores = lineups_to_array(lineups)
