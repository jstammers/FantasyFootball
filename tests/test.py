from itertools import chain
from typing import List, Tuple

from sqlite_utils import Database

from schemas import MatchLineup, Player, Lineup, Season, Competition
from jax import numpy as jnp
from model import player_score_model


class MatchScorer:
    def __init__(self, model=None):
        self.db = Database("openddata.db")
        self.player_id_map = self._get_player_map()
        if model is None:
            self.model = player_score_model

    def _get_player_map(self):
        lineups = self.db.execute("""SELECT DISTINCT
		home_team
		FROM lineups
		UNION SELECT DISTINCT
		away_team
		FROM lineups;""").fetchall()

        lineups = [Lineup.parse_raw(lineup[0]) for lineup in lineups]
        players = list(chain(*[p.lineup for p in lineups]))
        unique_players = {}
        i = 0
        for player in players:
            if player.player_id not in unique_players:
                unique_players[player.player_id] = i
                i += 1
        return unique_players

    def lineups_to_array(
        self, match_lineups: List[MatchLineup]
    ) -> Tuple[jnp.array, jnp.array, jnp.array, jnp.array]:
        home_players = []
        home_scores = []
        away_players = []
        away_scores = []
        for lineup in match_lineups:
            home_scores.append(lineup.home_score)
            away_scores.append(lineup.away_score)
            home_players.append(
                [self.player_id_map[p.player_id] for p in lineup.home_team.lineup][0:11]
            )
            away_players.append(
                [self.player_id_map[p.player_id] for p in lineup.away_team.lineup][0:11]
            )
        return (
            jnp.array(home_players),
            jnp.array(away_players),
            jnp.array(home_scores),
            jnp.array(away_scores),
        )

    def get_lineups(self) -> List[MatchLineup]:
        result = self.db.execute("""SELECT 
		   l.home_team, 
		   l.away_team,
		   home_score,
		   away_score,
		   season,
		   competition
	FROM matches
			 left join lineups l on matches.match_id = l.match_id;""").fetchall()
        return [
            MatchLineup(
                home_team=Lineup.parse_raw(match[0]),
                away_team=Lineup.parse_raw(match[1]),
                home_score=match[2],
                away_score=match[3],
                season=Season.parse_raw(match[4]),
                competition=Competition.parse_raw(match[5]),
            )
            for match in result
        ]


scorer = MatchScorer()
lineups = scorer.get_lineups()
home_players, away_players, home_scores, away_scores = scorer.lineups_to_array(lineups)

from numpyro.infer import MCMC, NUTS, Predictive
from jax import random

mcmc = MCMC(NUTS(player_score_model), 1000, 1000)
mcmc.run(
    random.PRNGKey(0),
    home_players,
    away_players,
    len(scorer.player_id_map),
    home_scores,
    away_scores,
)

predictor = Predictive(player_score_model, posterior_samples=mcmc.get_samples())

preds = predictor(
    random.PRNGKey(0),
    home_players=home_players,
    away_players=away_players,
    N_players=len(scorer.player_id_map),
)

pred_home_score = preds["l1"].mean(axis=0)
pred_away_score = preds["l2"].mean(axis=0)

home_win = home_scores > away_scores
pred_win = pred_home_score > pred_away_score
