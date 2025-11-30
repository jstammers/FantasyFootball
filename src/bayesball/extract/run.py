import logging as log
from pathlib import Path

import duckdb
import polars as pl
from rich.progress import track
from functools import reduce
from typing import Dict, List, Optional, Union
from bayesball.config import ADVANCED_MATCH_STATS
from bayesball.schema import stat_schema
from bayesball.utils import setup_logging

INGEST_DIR = Path("data/ingest/fbref")
EXTRACT_DIR = Path("data/extract")


def get_season_end_year(date_col="Match_Date"):
    d = pl.col(date_col).cast(pl.Date)
    return pl.when(d.dt.month() <= 7).then(d.dt.year()).otherwise(d.dt.year() + 1)


def _load_dataframe(f: Union[str, Path], schema: Optional[Dict] = None) -> pl.DataFrame:
    """
    Load a CSV file into a Polars DataFrame with optional schema overrides.
    """
    schema_overrides = {k: pl.Float64 for k in schema} if schema else None
    df = pl.read_csv(f, schema_overrides=schema_overrides)

    if schema:
        to_cast = {k: v for k, v in schema.items() if k in df.columns}
        df = df.cast(to_cast)
        if to_cast.keys() != schema.keys():
            log.warning(f"Schema mismatch for {f}. Expected {schema.keys()}, got {to_cast.keys()}.")
        schema_keys = list(to_cast.keys())
        all_missing = pl.all_horizontal(pl.col(schema_keys).is_null())
        df_filtered = df.filter(all_missing)
        if df_filtered.shape[0] != 0:
            log.warning(f"{f} contains missing values in schema columns. Filtered {df_filtered.shape[0]} rows.")
            df = df.filter(~all_missing)
        # drop columns that are all null
        all_nulls = [s.name for s in df if not (s.null_count() != df.height)]
        if len(all_nulls) > 0:
            log.warning(f"{f} contains columns that are all null. Dropping {all_nulls}.")
            df = df.drop_nulls(subset=all_nulls)

    date_col = "Date" if "Date" in df.columns else "Match_Date"

    if ("Country" not in df.columns or df["Country"].null_count() > 0) and date_col in df.columns:
        country, gender, tier, *_ = f.stem.split("_")
        df = df.with_columns(
            pl.lit(country).alias("Country"),
            pl.lit(gender).alias("Gender"),
            pl.lit(tier).alias("Tier"),
            get_season_end_year(date_col).alias("Season_End_Year"),
        )
    elif "Season_End_Year" in df.columns:
        df = df.with_columns(Season_End_Year=get_season_end_year(date_col))

    return df


def _process_team_player_data(
    df: pl.DataFrame, team_player: str, stat: str, join_keys: List[str], match_keys: List[str]
) -> pl.DataFrame:
    """
    Process team or player data by casting columns and dropping unnecessary ones.
    """
    df = df.with_columns(
        Home_xG=pl.col("Home_xG").cast(pl.Float64),
        Away_xG=pl.col("Away_xG").cast(pl.Float64),
        Season_End_Year=pl.col("Season_End_Year").cast(pl.Float64).cast(pl.Int64),
        Min=pl.col("Min").cast(pl.Float64),
        Home_Score=pl.col("Home_Score").cast(pl.Float64).cast(pl.Int64),
        Away_Score=pl.col("Away_Score").cast(pl.Float64).cast(pl.Int64),
    ).unique()

    if team_player == "team":
        for col in ["Player_Href", "Min"]:
            if col in df.columns:
                df = df.drop(col)
        if stat == "keeper":
            df = df.drop("Player", "Nation", "Age")

    select_cols = [
        *join_keys,
        *[c for c in match_keys if c not in join_keys],
        *[c for c in df.columns if c in stat_schema[stat]],
    ]
    return df.select(select_cols)


def _join_stat_dfs(stat_dfs: List[pl.DataFrame], join_keys: List[str], match_keys: List[str]) -> pl.DataFrame:
    """
    Join multiple DataFrames on common keys.
    """
    def join(x: pl.DataFrame, y: pl.DataFrame) -> pl.DataFrame:
        common_stats = [c for c in x.columns if c in y.columns and c not in set(join_keys + match_keys)]
        return x.join(
            y.drop(common_stats).unique(),
            set(join_keys + match_keys),
            how="full",
            coalesce=True,
        )

    return reduce(join, stat_dfs)


def extract_data(
    data_type: str,
    sort_columns: List[str],
    output_file: Optional[str] = None,
    team_player: Optional[str] = None,
    stats: Optional[List[str]] = None,
    join_keys: Optional[List[str]] = None,
    match_keys: Optional[List[str]] = None,
) -> None:
    """
    Extract and process data based on the provided parameters.
    """
    setup_logging()
    log.info(f"Extracting {data_type.replace('_', ' ')}")

    if team_player and stats:
        if not join_keys:
            raise ValueError("join_keys must be provided when extracting team and player data")

        stat_dfs = []
        for stat in track(stats, description=f"Extracting {data_type.replace('_', ' ')} - {team_player}"):
            dfs = [
                _load_dataframe(f, schema=stat_schema[stat])
                for f in (INGEST_DIR / data_type / team_player / stat).glob("*.csv")
            ]
            df = pl.concat(dfs, how="diagonal_relaxed")
            df = _process_team_player_data(df, team_player, stat, join_keys, match_keys)

            out_dir = EXTRACT_DIR / data_type / team_player
            out_dir.mkdir(parents=True, exist_ok=True)
            if stat == "defense" and "Def.3rd_Tackles" in df.columns:
                aliases = {
                    "Def.3rd_Tackles": "Def 3rd_Tackles",
                    "Mid.3rd_Tackles": "Mid 3rd_Tackles",
                    "Att.3rd_Tackles": "Att 3rd_Tackles",
                    "Tkl.Int": "Tkl+Int",
                }
                alias_map = {v: pl.coalesce(pl.col(v), pl.col(k)) for k, v in aliases.items()}
                df = df.with_columns(alias_map).drop(list(aliases.keys()))
            stat_dfs.append(df)

        joined_df = _join_stat_dfs(stat_dfs, join_keys, match_keys)
        output_path = EXTRACT_DIR / (output_file if output_file else f"{data_type}_{team_player}.parquet")

        group_keys = [c for c in join_keys if c not in ["Player_Num", "Pos", "Player_Href"]] if team_player == "player" else join_keys
        joined_df.drop(match_keys[6:]).group_by(group_keys).max().sort(sort_columns).write_parquet(output_path)

        if team_player == "team":
            match_df = joined_df.select(match_keys).unique().sort(sort_columns)
            match_df.write_parquet(output_path.with_name("advanced_match_summary.parquet"))
    else:
        dfs = [_load_dataframe(f) for f in (INGEST_DIR / data_type).glob("*.csv")]
        df = pl.concat(dfs, how="diagonal_relaxed").sort(sort_columns)
        output_path = EXTRACT_DIR / (output_file if output_file else f"{data_type}.parquet")
        df.write_parquet(output_path)

def extract_advanced_match_stats():
    match_keys = [
        "Country",
        "Gender",
        "Tier",
        "Season_End_Year",
        "MatchURL",
        "Match_Date",
        "League",
        "Matchweek",
        "Home_Team",
        "Home_Formation",
        "Home_Score",
        "Home_xG",
        "Home_Goals",
        "Home_Red_Cards",
        "Home_Yellow_Cards",
        "Away_Team",
        "Away_Formation",
        "Away_Score",
        "Away_xG",
        "Away_Goals",
        "Away_Red_Cards",
        "Away_Yellow_Cards",
        "Game_URL",
    ]
    player_join_keys = [
        "MatchURL",
        "Team",
        "Home_Away",
        "Player",
        "Player_Href",
        "Player_Num",
        "Pos",
        "Nation",
        "Age",
        "Min",
        # "Competition_Name",
        "Gender",
        "Country",
        "Tier",
        "Season_End_Year",
    ]
    team_join_keys = [
        c
        for c in player_join_keys
        if c
        not in ["Player", "Player_Num", "Pos", "Nation", "Age", "Player_Href", "Min",]
    ]

    extract_data(
        "advanced_match_stats",
        ["Country", "Gender", "Tier", "Season_End_Year", "Match_Date"],
        team_player="team",
        stats=ADVANCED_MATCH_STATS,
        join_keys=team_join_keys,
        match_keys=match_keys,
    )
    extract_data(
        "advanced_match_stats",
        ["Country", "Gender", "Tier", "Season_End_Year", "Match_Date"],
        team_player="player",
        stats=ADVANCED_MATCH_STATS,
        join_keys=player_join_keys,
        match_keys=match_keys,
    )


def extract_match_results():
    extract_data(
        "match_results", ["Country", "Gender", "Tier", "Season_End_Year", "Date"]
    )


def extract_match_shooting():
    extract_data(
        "match_shooting", ["Country", "Gender", "Tier", "Season_End_Year", "Date"]
    )


def extract_match_summary():
    extract_data(
        "match_summary", ["Country", "Gender", "Tier", "Season_End_Year", "Match_Date"]
    )


def extract_season_stats():
    pass


def extract_wages():
    extract_data(
        "wages",
        ["Comp", "Season", "Team", "WeeklyWageGBP"],
        output_file="wages.parquet",
    )


def load_data():
    """Load into a duckdb database"""
    db_conn = f"duckdb:///{str(EXTRACT_DIR / 'bayesball.db')}"
    db = duckdb.connect(str(EXTRACT_DIR / 'bayesball.db'))
    for f in EXTRACT_DIR.glob("*.parquet"):
        # use duckdb to write the parquet files to a database
        table_name = f.stem
        db.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{f}')")

        # db.read_parquet(f.stem, pl.read_parquet(f))
    # db.create_table("advanced_match_stats_team", pl.read_parquet(EXTRACT_DIR / "advanced_match_stats_team.parquet"))
    # db.create_table("advanced_match_stats_player", pl.read_parquet(EXTRACT_DIR / "advanced_match_stats_player.parquet"))
    # db.create_table("advanced_match_summary", pl.read_parquet(EXTRACT_DIR / "advanced_match_summary.parquet"))
    # db.create_table("match_results", pl.read_parquet(EXTRACT_DIR / "match_results.parquet"))
    # db.create_table("match_shooting", pl.read_parquet(EXTRACT_DIR / "match_shooting.parquet"))
    # db.create_table("match_summary", pl.read_parquet(EXTRACT_DIR / "match_summary.parquet"))
    # db.create_table("wages", pl.read_parquet(EXTRACT_DIR / "wages.parquet"))
    return db


def main():
    # load_data()
    extract_advanced_match_stats()
    extract_match_results()
    extract_match_shooting()
    extract_match_summary()
    extract_season_stats()
    extract_wages()


if __name__ == "__main__":
    setup_logging()
    main()
