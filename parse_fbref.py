import pandas as pd
from pathlib import Path

comp_mapper = {
    "Bundesliga": "GER",
    "La Liga": "ESP",
    "Ligue 1": "FRA",
    "Premier League": "ENG",
    "Serie A": "ITA",
}

season_stats = [
    "defense",
    "gca",
    "keepers_adv",
    "keepers",
    "misc",
    "passing",
    "passing_types",
    "playing_time",
    "possession",
    "shooting",
    "standard",
]


def split_dataframe(df: pd.DataFrame, keys: list[str]):
    for group, gdf in df.groupby(keys):
        yield group, gdf


def process_df(file_name):
    df = pd.read_csv(file_name)
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)
    year = file_name.stem.split("_")[0]
    df["Gender"] = "M"
    df["Tier"] = "1st"
    df["Season_End_Year"] = year
    df["Country"] = df["Comp"].map(comp_mapper)
    return df


def process_files(data_dir, out_dir):
    sort_keys = ["Season_End_Year", "Squad"]
    for t in ["team", "player"]:
        if t == "player":
            sort_keys.append("Player")
        for stat in season_stats:
            files = data_dir.glob(f"*_{stat}_{t}.csv")
            df = pd.concat([process_df(f) for f in files])
            for k, gdf in split_dataframe(df, ["Country", "Gender", "Tier"]):
                out_file = out_dir / f"{k[0]}_{k[1]}_{k[2]}_{stat}_{t}_season_stats.csv"
                gdf.sort_values(sort_keys).to_csv(out_file, index=False)


if __name__ == "__main__":
    data_dir = Path("data/FBRef/big5")
    out_dir = Path("data/raw")
    process_files(data_dir, out_dir)
