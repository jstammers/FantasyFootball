import jax.numpy as jnp
import numpyro as npyr
import numpyro.distributions as dist


# class BivariatePoisson(dist.Distribution):
#     def __init__(self, a, b, c):
#         super().__init__()
#         self.a=dist.Poisson()
#         self.b=b
#         self.c=c
#
#     def sample(self, sample_shape=torch.Size()):
#         pass
#     def log_prob(self, value):
#         pass


def ScoreModel(
    home_attack,
    home_defence,
    away_attack,
    away_defence,
    home_advantage,
    score_mixing,
    scores=(None, None),
):
    home_intensity = jnp.exp(home_attack + away_defence + home_advantage)
    away_intensity = jnp.exp(away_attack + home_defence)
    home_score = npyr.sample(
        "l1", dist.Poisson(home_intensity + score_mixing), obs=scores[0]
    )
    away_score = npyr.sample(
        "l2", dist.Poisson(away_intensity + score_mixing), obs=scores[1]
    )
    return home_score, away_score


def PairwiseProbModel(score_matrix: jnp.ndarray, **options):
    N = score_matrix.shape[0]
    d = 2
    # num_matches = num_teams ** 2 - num_teams
    delta = npyr.sample("home_advantage", dist.Normal(0, 1))
    gamma = npyr.sample("score_mixing", dist.Normal(0, 1))
    theta = npyr.sample("theta", dist.HalfCauchy(jnp.ones(d, **options)))
    # Lower cholesky factor of a correlation matrix
    eta = jnp.ones(
        1, **options
    )  # Implies a uniform distribution over correlation matrices
    L_omega = npyr.sample("L_omega", dist.LKJCholesky(d, eta))
    # Lower cholesky factor of the covariance matrix
    L_Omega = jnp.mm(jnp.diag(theta.sqrt()), L_omega)
    mu = jnp.zeros(d, **options)
    with npyr.plate("observations", N):
        alphas = npyr.sample("alphas", dist.MultivariateNormal(mu, scale_tril=L_Omega))
    for i, h in enumerate(alphas):
        for j, aw in enumerate(alphas):
            if i == j:
                continue
            else:
                ScoreModel(
                    h[0],
                    h[1],
                    aw[0],
                    aw[1],
                    delta,
                    gamma,
                    [score_matrix[i, j], score_matrix[j, i]],
                )
    return alphas


def player_score_model(
    home_players, away_players, N_players: int, home_score=None, away_score=None
):
    d = 2  # attack and defense
    Rho = npyr.sample("Rho", dist.LKJ(d, 2))
    sigma = npyr.sample("sigma", dist.Exponential(1).expand([d]))

    cov = jnp.outer(sigma, sigma) * Rho
    stats = npyr.sample(
        "player_stats", dist.MultivariateNormal(0, cov).expand([N_players])
    )
    delta = npyr.sample("home_advantage", dist.Normal(0, 1))
    gamma = npyr.sample("score_mixing", dist.Normal(0, 1))
    with npyr.plate("matches", home_players.shape[0]):
        home_attack = stats[0, home_players].sum(axis=1)
        home_defense = stats[1, home_players].sum(axis=1)
        away_attack = stats[0, away_players].sum(axis=1)
        away_defense = stats[1, away_players].sum(axis=1)
        ScoreModel(
            home_attack,
            home_defense,
            away_attack,
            away_defense,
            home_advantage=delta,
            score_mixing=gamma,
            scores=(home_score, away_score),
        )
