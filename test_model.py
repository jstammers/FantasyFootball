import pytest
from model import player_score_model
from jax import numpy as jnp
from numpyro.handlers import seed
def test_player_score_model():
	home_players = jnp.array([[0,1,2],[3,4,5]])
	away_players = jnp.array([[3,4,5],[0,1,2]])
	home_scores = jnp.array([1,2])
	away_scores = jnp.array([2,1])

	with seed(rng_seed=1):
		player_score_model(home_players, away_players,
						   N_players=6, home_score=home_scores, away_score=away_scores)