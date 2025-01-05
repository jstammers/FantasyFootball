import pandas as pd
import polars as pl
from pathlib import Path
from bayesball.config import ADVANCED_MATCH_STATS, SEASON_PLAYER_STATS, SEASON_TEAM_STATS
schema_dict = {}

for team_player in ["team", "player"]:
    for stat_type in ADVANCED_MATCH_STATS:
        data_dir = Path(f"data/ingest/fbref/advanced_match_stats/")
        files = list(data_dir.glob(f"{team_player}/*{stat_type}*.csv"))

        if len(files) == 0:
            continue
        columns = [pl.read_csv(f).columns for f in files]
        # keep unique lists
        schema_dict[("match", team_player, stat_type)] = columns

for team_player in ["team", "player"]:
    for stat_type in SEASON_PLAYER_STATS:
        data_dir = Path(f"data/ingest/fbref/season_stats/")
        files = list(data_dir.glob(f"{team_player}/*{stat_type}*.csv"))

        if len(files) == 0:
            continue
        columns = [list(pd.read_csv(f).columns) for f in files]
        # keep unique lists
        schema_dict[("season", team_player, stat_type)] = columns
    for stat_type in SEASON_TEAM_STATS:
        data_dir = Path(f"data/ingest/fbref/season_stats/")
        files = list(data_dir.glob(f"{team_player}/*{stat_type}*.csv"))

        if len(files) == 0:
            continue
        columns = [list(pd.read_csv(f).columns) for f in files]
        # keep unique lists
        schema_dict[("season", team_player, stat_type)] = columns

d = {}
for key, c in schema_dict.items():
    sorted(c, key=lambda x: len(x))
    d[key] = c[0]

