from pydantic import BaseModel
from dataclasses import dataclass
import polars as pl
from typing import Optional


def _add_df(df1=None, df2=None):
    if df1 is None or df1.shape[0] == 0:
        return df2
    if df2 is None or df2.shape[0] == 0:
        return df1
    if (df1 is None or df1.shape[0] == 0) and (df2 is None or df2.shape[0] == 0):
        return None
    else:
        return pl.concat([df1, df2], how="diagonal_relaxed")


@dataclass
class AdvancedMatchStats:
    summary: pl.DataFrame
    keeper: Optional[pl.DataFrame]
    passing: Optional[pl.DataFrame]
    passing_types: Optional[pl.DataFrame]
    possession: Optional[pl.DataFrame]
    defense: Optional[pl.DataFrame]
    misc: Optional[pl.DataFrame]

    def __add__(self, other):
        return AdvancedMatchStats(
            summary=_add_df(self.summary, other.summary),
            keeper=_add_df(self.keeper, other.keeper),
            passing=_add_df(self.passing, other.passing),
            passing_types=_add_df(self.passing_types, other.passing_types),
            possession=_add_df(self.possession, other.possession),
            defense=_add_df(self.defense, other.defense),
            misc=_add_df(self.misc, other.misc),
        )


@dataclass
class MatchStats:
    match_summary: pl.DataFrame
    lineups: Optional[pl.DataFrame]
    shooting_data: Optional[pl.DataFrame]
    team_stats: AdvancedMatchStats
    player_stats: AdvancedMatchStats

    def __add__(self, other):
        return MatchStats(
            match_summary=_add_df(self.match_summary, other.match_summary),
            lineups=_add_df(self.lineups, other.lineups),
            shooting_data=_add_df(self.shooting_data, other.shooting_data),
            team_stats=self.team_stats + other.team_stats,
            player_stats=self.player_stats + other.player_stats,
        )
