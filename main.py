from bayesball.scraper import CompetitionStats, SeasonTeamStats, SeasonPlayerStats
import pandas as pd
from pathlib import Path
from collections import defaultdict

data_dir = Path("data/FBRef")

out_dir = Path("data/raw/fbref")
big5_files = list((data_dir / "big5").glob("*.csv"))

countries = ["ENG", "GER", "ITA", "FRA", "ESP"]

years = set([x.name[0:4] for x in big5_files if x.name[0].isdigit()])

tier = "1st"
gender = "M"


match_results = pd.read_csv(data_dir / "big5"/ "match_results.csv")

wages = pd.read_csv(data_dir / "big5"/ "wages.csv")

df_dict = {k.stem: pd.read_csv(k, index_col=0) for k in big5_files}

k = list(df_dict.keys())[0]

stat_dict = {}
group_mapper = {"Bundesliga": "GER", "La Liga": "ESP", "Ligue 1": "FRA", "Premier League": "ENG", "Serie A": "ITA", "Division 1": "FRA"}
for key in df_dict.keys():
    year = key.split("_")[0]
    tp = key.split("_")[-1]
    stat_type = "_".join(key.split("_")[2:-1])
    if stat_type == "":
        continue
    else:
        # split based on Comp
        df = df_dict[key]
        for g, gdf in df.groupby("Comp"):
            country = group_mapper[g]
            k = (country, year, tp, stat_type)
            if tp == "team":
                tp_label = "season_team_stats"
            else:
                tp_label = "season_player_stats"

            o = out_dir / country / "1st" / year / tp_label

            if not o.exists():
                o.mkdir(parents=True, exist_ok=True)
            gdf.to_csv(o / f"{stat_type}.csv", index=False)


match_results = pd.read_csv(data_dir / "big5"/ "match_results.csv", index_col=0)

for k, mdf in match_results.groupby(["Country", "Season_End_Year"]):
    country, year = k
    o = out_dir / country / "1st" / str(year) / "match_stats"
    if not o.exists():
        o.mkdir(parents=True, exist_ok=True)
    mdf.to_csv(o / "results.csv", index=False)


t1_dir = Path("data") / "FBRef" / "ENG" / "t1"

for f in t1_dir.iterdir():
    key = f.stem
    year = key.split("_")[0]
    if "match_results" in key:
        continue

    stat_type = "_".join(key.split("_")[2:-1])
    if "match_log" in key:
        o = out_dir / "ENG" / "1st" / year / "match_stats" / "match_log"
    elif "player" in key:
        o = out_dir / "ENG" / "1st" / year / "match_stats" / "player"
    elif "team" in key:
        o = out_dir / "ENG" / "1st" / year / "match_stats" / "team"
    elif "wages" in key:
        stat_type = "wages"
        o = out_dir / "ENG" / "1st" / year / "match_stats"
    else:
        raise ValueError(f"Unknown stat type {key}")
    if not o.exists():
        o.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(f)
    df.to_csv(o / f"{stat_type}.csv", index=False)



data_dir = Path("data/raw/wordfootballr_data")


