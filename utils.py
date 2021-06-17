from datetime import datetime
from enum import Enum
from typing import Tuple, List

import pandas as pd
from pathlib import Path
import urllib.request
from jax import numpy as jnp

from schemas import MatchLineup


class League(Enum):
    premier = "E0"
    championship = "E1"
    league_1 = "E2"
    league_2 = "E3"
    conference = "EC"


def load_historical_data(season: str, league: League):
    local_path = Path(f"data/s_{season}_{league}.csv")
    url = f"http://www.football-data.co.uk/mmz4281/{season}/{league}.csv"
    try:
        if local_path.exists():
            df = pd.read_csv(local_path)
        else:
            data = urllib.request.urlretrieve(url, local_path)
            df = pd.read_csv(data)
    except:
        print(f"Failed to parse {season} - {league}")
        return None
    df["Season"] = season
    return df.dropna(axis=1, how='all')


def load_all_data():
    def short_year(x):
        return str(x)[-2:]
    years = pd.date_range("01-01-1993", end=datetime.today(), freq='Y').year
    seasons = [f"{short_year(y)}{short_year(y + 1)}" for y in years]
    for season in seasons:
        for league in League:
            try:
                yield load_historical_data(season, league.value)
            except Exception:
                print(f"Failed for {season} - {league}")

def lineups_to_array(match_lineups: List[MatchLineup]) -> Tuple[jnp.array, jnp.array, jnp.array, jnp.array]:
    home_players = []
    home_scores = []
    away_players = []
    away_scores = []
    for lineup in match_lineups:
        home_scores.append(lineup.home_score)
        away_scores.append(lineup.away_score)
        home_players.append([p.player_id for p in lineup.home_team.lineup][0:11])
        away_players.append([p.player_id for p in lineup.away_team.lineup][0:11])
    return jnp.array(home_players), jnp.array(away_players), jnp.array(home_scores), jnp.array(away_scores)





