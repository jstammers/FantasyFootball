import numpy as np
import pyro
import pyro.distributions as dist
import torch

class BivariatePoisson(dist.TorchDistribution):
    def __init__(self, a, b, c):
        super().__init__()
        self.a=a
        self.b=b
        self.c=c
    def sample(self, sample_shape=torch.Size()):
        pass
    def log_prob(self, value):
        pass

def PairwiseProbModel(score_matrix: np.ndarray):
    num_teams = score_matrix.shape[0]
    num_matches = num_teams ** 2 - num_teams
    for i in range(0,score_matrix.shape[0]):
        for j in range(0, score_matrix.shape[1]):
            if i==j:
                continue
            a_i = pyro.sample("a_i")
            b_i = pyro.sample("b_i")
            a_j = pyro.sample("a_j")
            b_j = pyro.sample("b_j")
            d = pyro.sample("d")
            home_intensity = np.exp(d + a_i + b_j)
            away_intensity = np.exp(a_j + b_i)
            dist.GammaPoisson
    home_a = pyro.sample("a")
    home
    with pyro.plate()
