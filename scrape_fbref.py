from bayesball.worldfootballr import FootballDataLoader
from bayesball.utils import maybe_download_file, maybe_split_csv
from pathlib import Path
import pandas as pd

data_dir = Path("data/wordfootballr_data")
if not data_dir.exists():
    data_dir.mkdir(parents=True, exist_ok=True)

worldfootballr_data = "https://github.com/JaseZiv/worldfootballR_data/releases"
gender = "M"

for country in ["ENG", "GER", "ITA", "FRA", "ESP", "USA"]:
    match_results = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/match_results/{country}_match_results.rds"
    maybe_download_file(match_results, data_dir)
    for tier in ["1st", "2nd", "3rd", "4th", "5th"]:
        match_summary = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/fb_match_summary/{country}_{gender}_{tier}_match_summary.csv"
        maybe_download_file(match_summary, data_dir)
        match_shooting = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/fb_match_shooting/{country}_{gender}_{tier}_match_shooting.csv"
        maybe_download_file(match_shooting, data_dir)
        for stat in [
            "summary",
            "passing",
            "passing_types",
            "defense",
            "possession",
            "misc",
            "keeper",
        ]:
            for team_player in ["team", "player"]:
                advanced_match_stats = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/fb_advanced_match_stats/{country}_{gender}_{tier}_{stat}_{team_player}_advanced_match_stats.csv"
                maybe_download_file(advanced_match_stats, data_dir)


big5_season_stats = "https://github.com/JaseZiv/worldfootballR_data/releases/download/fb_big5_advanced_season_stats/big5_{team_player}_{stat}.rds"
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri

pandas2ri.activate()

readRDS = robjects.r["readRDS"]

rds_files = data_dir.glob("*.rds")

for rds_file in rds_files:
    rds_df = readRDS(str(rds_file))
    df = pandas2ri.rpy2py(rds_df)
    df["Date"] = pd.to_datetime(df["Date"], unit="D", origin="1970-01-01")
    df.sort_values(["Tier", "Gender", "Date"]).to_csv(
        data_dir / f"{rds_file.stem}.csv", index=False
    )

gender = "M"
for country in ["ENG", "GER", "ITA", "FRA", "ESP", "USA"]:
    match_results = Path(data_dir) / f"{country}_match_results.csv"
    maybe_split_csv(match_results, "results")
    for tier in ["1st", "2nd", "3rd", "4th", "5th"]:
        match_summary = Path(data_dir) / f"{country}_{gender}_{tier}_match_summary.csv"
        maybe_split_csv(match_summary, "summary")
        match_shooting = (
            Path(data_dir) / f"{country}_{gender}_{tier}_match_shooting.csv"
        )
        maybe_split_csv(match_shooting, "shooting")
        for stat in [
            "summary",
            "passing",
            "passing_types",
            "defense",
            "possession",
            "misc",
            "keeper",
        ]:
            for team_player in ["team", "player"]:
                advanced_match_stats = (
                    Path(data_dir)
                    / f"{country}_{gender}_{tier}_{stat}_{team_player}_advanced_match_stats.csv"
                )
                maybe_split_csv(advanced_match_stats, f"{team_player}/{stat}")
