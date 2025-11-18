import os

import ibis

# from scrape_fbref import match_shooting, match_summary
from pathlib import Path

os.chdir(Path(__file__).parent.parent.parent)
ibis.options.interactive = True
con = ibis.duckdb.connect()

results = con.read_csv("data/raw/*_match_results.csv")

match_shooting = con.read_csv("data/raw/*_match_shooting.csv", union_by_name=True)

match_summary = con.read_csv("data/raw/*_match_summary.csv", union_by_name=True)

match_defense_player = con.read_csv(
    "data/raw/*_defense_player_advanced_match_stats.csv", union_by_name=True
)
season_defense_player = con.read_csv(
    "data/raw/*_defense_player_season_stats.csv", union_by_name=True
)

match_defense_team = con.read_csv(
    "data/raw/*_defense_team_advanced_match_stats.csv", union_by_name=True
)
season_defense_team = con.read_csv(
    "data/raw/*_defense_team_season_stats.csv", union_by_name=True
)


season_gca_team = con.read_csv(
    "data/raw/*_gca_team_season_stats.csv", union_by_name=True
)
season_gca_player = con.read_csv(
    "data/raw/*_gca_player_season_stats.csv", union_by_name=True
)

match_keeper_team = con.read_csv(
    "data/raw/*_keeper_team_advanced_match_stats.csv", union_by_name=True
)
match_keeper_player = con.read_csv(
    "data/raw/*_keeper_player_advanced_match_stats.csv", union_by_name=True
)

season_keeper_adv_team = con.read_csv(
    "data/raw/*_keepers_adv_team_season_stats.csv", union_by_name=True
)
season_keeper_adv_player = con.read_csv(
    "data/raw/*_keepers_adv_player_season_stats.csv", union_by_name=True
)

match_misc_team = con.read_csv(
    "data/raw/*_misc_team_advanced_match_stats.csv", union_by_name=True
)
match_misc_player = con.read_csv(
    "data/raw/*_misc_player_advanced_match_stats.csv", union_by_name=True
)

season_misc_team = con.read_csv(
    "data/raw/*_misc_team_season_stats.csv", union_by_name=True
)
season_misc_player = con.read_csv(
    "data/raw/*_misc_player_season_stats.csv", union_by_name=True
)

match_passing_team = con.read_csv(
    "data/raw/*_passing_team_advanced_match_stats.csv", union_by_name=True
)
match_passing_player = con.read_csv(
    "data/raw/*_passing_player_advanced_match_stats.csv", union_by_name=True
)

season_passing_team = con.read_csv(
    "data/raw/*_passing_team_season_stats.csv", union_by_name=True
)
season_passing_player = con.read_csv(
    "data/raw/*_passing_player_season_stats.csv", union_by_name=True
)

match_passing_types_team = con.read_csv(
    "data/raw/*_passing_types_team_advanced_match_stats.csv", union_by_name=True
)
match_passing_types_player = con.read_csv(
    "data/raw/*_passing_types_player_advanced_match_stats.csv", union_by_name=True
)

season_passing_types_team = con.read_csv(
    "data/raw/*_passing_types_team_season_stats.csv", union_by_name=True
)
season_passing_types_player = con.read_csv(
    "data/raw/*_passing_types_player_season_stats.csv", union_by_name=True
)

season_playing_time_team = con.read_csv(
    "data/raw/*_playing_time_team_season_stats.csv", union_by_name=True
)
season_playing_time_player = con.read_csv(
    "data/raw/*_playing_time_player_season_stats.csv", union_by_name=True
)

match_possession_team = con.read_csv(
    "data/raw/*_possession_team_advanced_match_stats.csv", union_by_name=True
)
match_possession_player = con.read_csv(
    "data/raw/*_possession_player_advanced_match_stats.csv", union_by_name=True
)

season_possession_team = con.read_csv(
    "data/raw/*_possession_team_season_stats.csv", union_by_name=True
)
season_possession_player = con.read_csv(
    "data/raw/*_possession_player_season_stats.csv", union_by_name=True
)

# match_shooting_team = con.read_csv("data/raw/*_shooting_team_advanced_match_stats.csv")
# match_shooting_player = con.read_csv("data/raw/*_shooting_player_advanced_match_stats.csv")

season_shooting_team = con.read_csv(
    "data/raw/*_shooting_team_season_stats.csv", union_by_name=True
)
season_shooting_player = con.read_csv(
    "data/raw/*_shooting_player_season_stats.csv", union_by_name=True
)

# match_standard_team = con.read_csv("data/raw/*_standard_team_advanced_match_stats.csv")
# match_standard_player = con.read_csv("data/raw/*_standard_player_advanced_match_stats.csv")

season_standard_team = con.read_csv(
    "data/raw/*_standard_team_season_stats.csv", union_by_name=True
)
season_standard_player = con.read_csv(
    "data/raw/*_standard_player_season_stats.csv", union_by_name=True
)

match_summary_team = con.read_csv(
    "data/raw/*_summary_team_advanced_match_stats.csv", union_by_name=True
)
match_summary_player = con.read_csv(
    "data/raw/*_summary_player_advanced_match_stats.csv", union_by_name=True
)


# match_wages = con.read_csv("data/raw/*_wages.csv")


results.to_parquet("data/processed/results.parquet")

match_shooting.to_parquet("data/processed/match_shooting.parquet")

match_summary.to_parquet("data/processed/match_summary.parquet")

match_defense_player.to_parquet("data/processed/match_defense_player.parquet")
season_defense_player.to_parquet("data/processed/season_defense_player.parquet")

match_defense_team.to_parquet("data/processed/match_defense_team.parquet")
season_defense_team.to_parquet("data/processed/season_defense_team.parquet")

season_gca_team.to_parquet("data/processed/season_gca_team.parquet")
season_gca_player.to_parquet("data/processed/season_gca_player.parquet")

match_keeper_team.to_parquet("data/processed/match_keeper_team.parquet")
match_keeper_player.to_parquet("data/processed/match_keeper_player.parquet")

season_keeper_adv_team.to_parquet("data/processed/season_keeper_adv_team.parquet")
season_keeper_adv_player.to_parquet("data/processed/season_keeper_adv_player.parquet")

match_misc_team.to_parquet("data/processed/match_misc_team.parquet")
match_misc_player.to_parquet("data/processed/match_misc_player.parquet")

season_misc_team.to_parquet("data/processed/season_misc_team.parquet")
season_misc_player.to_parquet("data/processed/season_misc_player.parquet")

match_passing_team.to_parquet("data/processed/match_passing_team.parquet")
match_passing_player.to_parquet("data/processed/match_passing_player.parquet")

season_passing_team.to_parquet("data/processed/season_passing_team.parquet")
season_passing_player.to_parquet("data/processed/season_passing_player.parquet")

match_passing_types_team.to_parquet("data/processed/match_passing_types_team.parquet")
match_passing_types_player.to_parquet(
    "data/processed/match_passing_types_player.parquet"
)

season_passing_types_team.to_parquet("data/processed/season_passing_types_team.parquet")
season_passing_types_player.to_parquet(
    "data/processed/season_passing_types_player.parquet"
)

season_playing_time_team.to_parquet("data/processed/season_playing_time_team.parquet")
season_playing_time_player.to_parquet(
    "data/processed/season_playing_time_player.parquet"
)

match_possession_team.to_parquet("data/processed/match_possession_team.parquet")
match_possession_player.to_parquet("data/processed/match_possession_player.parquet")

season_possession_team.to_parquet("data/processed/season_possession_team.parquet")
season_possession_player.to_parquet("data/processed/season_possession_player.parquet")

season_shooting_team.to_parquet("data/processed/season_shooting_team.parquet")
season_shooting_player.to_parquet("data/processed/season_shooting_player.parquet")

season_standard_team.to_parquet("data/processed/season_standard_team.parquet")
season_standard_player.to_parquet("data/processed/season_standard_player.parquet")

match_summary_team.to_parquet("data/processed/match_summary_team.parquet")
match_summary_player.to_parquet("data/processed/match_summary_player.parquet")

match_wages.to_parquet("data/processed/match_wages.parquet")
