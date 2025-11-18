"""
Creates a database of matches, events and lineups from data downloaded from
open-data
"""
import glob
from typing import List

from sqlite_utils import Database
import json
from schemas import Match, Lineup, MatchLineup, Season, Competition
from itertools import chain
def insert_match(db:Database, match: Match):
	db['matches'].insert(match.dict())
	pass

def insert_lineup(db:Database, lineup: Lineup, match_id:int ):
	l = lineup.dict()
	l['match_id'] = match_id
	db['lineups'].insert(l)


if __name__ == '__main__':
	db = Database('openddata.db')
	for lineup_file in glob.glob('data/open-data/data/lineups/*.json'):
		data = json.load(open(lineup_file))
		lineup = MatchLineup.parse_obj({'home_team':data[0], 'away_team':data[1]})
		id = int(lineup_file.split('/')[-1][:-5])
		try:
			insert_lineup(db, lineup, id)
		except:
			print(f"Failed to insert lineup for {lineup_file}")
	for match_file in glob.glob('data/open-data/data/matches/*/*.json'):
		matches = json.load(open(match_file))
		for match in matches:
			try:
				match = Match.parse_obj(match)
				insert_match(db, match)
			except:
				print(f"Failed to insert match from {matches}")



