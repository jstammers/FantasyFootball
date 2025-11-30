import shutil

import pandas as pd
from pathlib import Path

from bayesball.config import ADVANCED_MATCH_STATS, SEASON_PLAYER_STATS

data_dir = Path("data/FBRef")

out_dir = Path("data/raw/fbref")
big5_files = list((data_dir / "big5").glob("*.csv"))

countries = ["ENG", "GER", "ITA", "FRA", "ESP"]

years = set([x.name[0:4] for x in big5_files if x.name[0].isdigit()])

tier = "1st"
gender = "M"


match_results = pd.read_csv(data_dir / "big5" / "match_results.csv")

wages = pd.read_csv(data_dir / "big5" / "wages.csv")

df_dict = {k.stem: pd.read_csv(k, index_col=0) for k in big5_files}

k = list(df_dict.keys())[0]

stat_dict = {}
group_mapper = {
    "Bundesliga": "GER",
    "La Liga": "ESP",
    "Ligue 1": "FRA",
    "Premier League": "ENG",
    "Serie A": "ITA",
    "Division 1": "FRA",
}
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


match_results = pd.read_csv(data_dir / "big5" / "match_results.csv", index_col=0)

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


input_dir = Path("data/raw")
output_dir = Path("data/ingest/fbref")

base = "advanced_match_stats"


for stat in ADVANCED_MATCH_STATS:
    base = "advanced_match_stats"
    input_files = list(input_dir.glob(f"*{stat}*{base}.csv"))
    for f in input_files:
        if "player" in f.name:
            output_file = (
                output_dir
                / base
                / "player"
                / stat
                / f.name.replace(f"_{base}", "")
                .replace("_player", "")
                .replace(f"_{stat}", "")
            )
        elif "team" in f.name:
            output_file = (
                output_dir
                / base
                / "team"
                / stat
                / f.name.replace(f"_{base}", "")
                .replace("_team", "")
                .replace(f"_{stat}", "")
            )
        else:
            output_file = output_dir / base / f.name
        # output_file = output_dir / base / f.name.replace(f"_{base}", "")
        output_file = output_file.with_name(output_file.name[:-4] + "_fb_0001.csv")
        print(output_file)
        shutil.copy(f, output_file)

for stat in SEASON_PLAYER_STATS:
    base = "season_player_stats"
    input_files = list(input_dir.glob(f"*{stat}*{base}.csv"))
    for f in input_files:
        output_file = (
            output_dir
            / base
            / stat
            / f.name.replace(f"_{base}", "")
            .replace("_player", "")
            .replace(f"_{stat}", "")
        )
        output_file = output_file.with_name(output_file.name[:-4] + "_fb_0001.csv")
        print(output_file)
        # shutil.copy(f, output_file)

input_dir = Path("data/ingest/fbref")

files = list(input_dir.glob("**/*_fbref.csv"))

for team_player in ["team", "player"]:
    for stat in SEASON_PLAYER_STATS + SEASON_TEAM_STATS:
        files = list(input_dir.glob(f"season_stats/{team_player}/*{stat}.csv"))
        for f in files:
            new_file = f.parent / stat / f.name.replace(stat, "fbref_0001")
            print(new_file)
            if not new_file.parent.exists():
                new_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(f, new_file)


for f in input_dir.glob("**/*fb_*.csv"):
    new_name = f.name.replace("fb_", "fbref_")
    new_file = f.with_name(new_name)
    shutil.move(f, new_file)
