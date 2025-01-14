"""A module to ingest data from the FBRef website"""

import dataclasses
import logging as log
import os
import shutil

import polars as pl
from pathlib import Path
from rich.progress import track
from concurrent.futures import ProcessPoolExecutor, as_completed

from bayesball.schema import MatchSummarySchema
from bayesball.worldfootballr import call_wf_function, fb_parse_match_data
from bayesball.utils import (
    get_current_season,
    maybe_download_file,
    setup_logging,
    r_to_python,
)
from bayesball.config import (
    ADVANCED_MATCH_STATS,
    COUNTRIES,
    TIERS,
    MIN_SEASON_END_YEAR,
    LEAGUE_STATS,
)
from bayesball.models import AdvancedMatchStats, MatchStats

STAGE_DIR = "data/ingest/stage"

BASE_DIR = "data/ingest/fbref"

SOURCE_SUFFIX = "fbref"
GENDER = "M"


def get_missing_matches(gender=GENDER) -> pl.DataFrame:
    match_summaries = pl.read_csv(Path(BASE_DIR) / "match_summary" / "*.csv")
    match_shooting = pl.concat(
        [
            pl.read_csv(x)
            .select("Country", "Gender", "Tier", "Season_End_Year", "MatchURL")
            .unique()
            for x in (Path(BASE_DIR) / "match_shooting").glob("*.csv")
        ],
        how="diagonal_relaxed",
    )
    team_summary_stats = pl.concat(
        [
            pl.read_csv(x).select("MatchURL").unique()
            for x in (
                Path(BASE_DIR) / "advanced_match_stats" / "team" / "summary"
            ).glob("*.csv")
        ],
        how="diagonal_relaxed",
    )
    team_advanced_stats = pl.concat(
        [
            pl.read_csv(x).select("MatchURL").unique()
            for x in (
                Path(BASE_DIR) / "advanced_match_stats" / "team" / "possession"
            ).glob("*.csv")
        ],
        how="diagonal_relaxed",
    )
    tier_df = pl.DataFrame(LEAGUE_STATS)
    match_results = (
        pl.read_csv(Path(BASE_DIR) / "match_results" / "*.csv")
        .join(tier_df, on=["Country", "Tier"])
        .filter(
            pl.col("Season_End_Year") >= MIN_SEASON_END_YEAR, pl.col("Gender") == gender
        )
    )
    match_shooting.filter(pl.col("Season_End_Year").is_null())
    match_results_filtered = match_results.filter(
        ~pl.col("MatchURL").str.contains("History"),
        ~pl.col("Notes").str.contains("Cancelled"),
        ~pl.col("MatchURL").str.contains("RelegationPromotion-Play-offs"),
    )
    in_match_summary = (
        match_summaries.select("MatchURL").unique().with_columns(InMatchSummary=True)
    )
    in_team_match_summary = (
        team_summary_stats.select("MatchURL").unique().with_columns(InTeamSummary=True)
    )
    in_team_advanced_stats = (
        team_advanced_stats.select("MatchURL")
        .unique()
        .with_columns(InTeamAdvanced=True)
    )
    match_results_filtered = (
        match_results_filtered.join(in_match_summary, on="MatchURL", how="left")
        .join(in_team_match_summary, on="MatchURL", how="left")
        .join(in_team_advanced_stats, on="MatchURL", how="left")
        .fill_null(False)
    )
    missing_cond = (
        ~pl.col("InMatchSummary")
        | ~pl.col("InTeamSummary")
        | (
            ~pl.col("InTeamAdvanced")
            & (pl.col("Season_End_Year") >= pl.col("Min_Advanced_Season"))
        )
    )
    missing_matches = match_results_filtered.filter(missing_cond)
    missing_matches = missing_matches.with_columns(
        filename=pl.lit(f"{STAGE_DIR}/html/")
        + pl.col("Country")
        + pl.lit("/")
        + pl.col("MatchURL").str.split("/").list.last()
        + pl.lit(".html")
    )
    return missing_matches


def _fix_match_url(df: pl.DataFrame, match_mapping: pl.DataFrame):
    if "MatchURL" in df.columns:
        if "Game_URL" in df.columns:
            df_remapped = df.with_columns(MatchURL=pl.col("Game_URL")).select(
                df.columns
            )
        else:
            df_remapped = df.join(match_mapping, on="MatchURL")
            df_remapped = df_remapped.with_columns(MatchURL=pl.col("Game_URL")).select(
                df.columns
            )
        return df_remapped
    return df


def _merge_data_dicts(stat_dicts: list[dict]) -> dict:
    merged_dict = {}
    for d in stat_dicts:
        for k, v in d.items():
            if k not in merged_dict:
                merged_dict[k] = v
            else:
                merged_dict[k] = pl.concat(merged_dict[k], v)
    return merged_dict


def _merge_stats(stats: list[MatchStats]) -> MatchStats:
    stats = [s for s in stats if s is not None]
    if len(stats) == 0:
        return stats
    s = stats[0]
    for s1 in stats[1:]:
        s = s + s1
    return s


def get_match_stats(missing_matches: pl.DataFrame) -> MatchStats:
    setup_logging()
    advanced_paths = (
        missing_matches.filter(
            pl.col("Season_End_Year") >= pl.col("Min_Advanced_Season")
        )["filename"]
        .unique()
        .to_list()
    )
    basic_paths = (
        missing_matches.filter(
            (pl.col("Season_End_Year") < pl.col("Min_Advanced_Season"))
            | pl.col("Min_Advanced_Season").is_null()
        )["filename"]
        .unique()
        .to_list()
    )

    basic_stats = [
        extract_match_data(p, advanced_stats=False)
        for p in track(basic_paths, description="Extracting basic match data")
    ]
    advanced_stats = [
        extract_match_data(p, advanced_stats=True)
        for p in track(advanced_paths, description="Extracting advanced match data")
    ]

    basic_stats = _merge_stats(basic_stats)
    advanced_stats = _merge_stats(advanced_stats)
    # basic_stats = extract_match_data(basic_paths, advanced_stats=False)
    # advanced_stats = extract_match_data(advanced_paths, advanced_stats=True)
    if advanced_stats and basic_stats:
        advanced_stats += basic_stats
        return advanced_stats
    return basic_stats if basic_stats else advanced_stats


def extract_match_data(download_paths, advanced_stats=True) -> MatchStats:
    # Define team match stats
    competitions = _get_competitions()
    if advanced_stats:
        team_match_stats = ADVANCED_MATCH_STATS
        shooting = True
    else:
        team_match_stats = ["summary"]
        shooting = False
    try:
        match_data = fb_parse_match_data(
            download_paths, stat_types=team_match_stats, shooting=shooting
        )
        match_summaries = r_to_python(match_data[3])
        match_mapping = match_summaries[["Game_URL", "MatchURL"]].unique()
        match_summaries = match_summaries.with_columns(MatchURL=pl.col("Game_URL"))
    except Exception as e:
        log.error(f"Error parsing {download_paths}")
        return None
    if "Competition_Name" not in match_summaries.columns:
        match_summaries = match_summaries.join(
            competitions.select("Country", "Gender", "Tier", "Competition_Name"),
            on=["Country", "Gender", "Tier"], how="left")
    MatchSummarySchema.validate(match_summaries.to_pandas())
    match_summaries = match_summaries.select(MatchSummarySchema.columns.keys())
    player_stats = {}
    team_stats = {}
    for i, stat_type in enumerate(team_match_stats):
        try:
            team_stats[stat_type] = _fix_match_url(
                r_to_python(match_data[0][2 * i]), match_mapping
            )
            player_stats[stat_type] = _fix_match_url(
                r_to_python(match_data[0][2 * i + 1]), match_mapping
            )
        except IndexError:
            log.error(f"Error loading {stat_type}")
            continue
    for stat in ADVANCED_MATCH_STATS:
        if stat not in team_stats:
            team_stats[stat] = None
        if stat not in player_stats:
            player_stats[stat] = None
    team_stats = AdvancedMatchStats(**team_stats)
    player_stats = AdvancedMatchStats(**player_stats)
    shooting_data = _fix_match_url(r_to_python(match_data[1]), match_mapping)
    lineups = _fix_match_url(r_to_python(match_data[2]), match_mapping)
    return MatchStats(
        match_summary=match_summaries,
        lineups=lineups,
        player_stats=player_stats,
        team_stats=team_stats,
        shooting_data=shooting_data,
    )


def scrape_matches(missing_matches: pl.DataFrame, time_pause=4):
    """Scrape match data from the FBRef website"""
    setup_logging()
    missing_files = missing_matches.filter(
        [not Path(f).exists() for f in missing_matches["filename"].to_list()]
    )
    for country in COUNTRIES:
        match_urls = missing_files.filter(pl.col("Country") == country)[
            "MatchURL"
        ].to_list()
        log.info(f"Scraping matches for {country}")
        for url in track(match_urls, description="Scraping matches"):
            filename = Path("html") / country / (Path(url).name + ".html")
            maybe_download_file(url, STAGE_DIR, filename, time_pause=time_pause)


def ingest_wages(gender=GENDER, update_current_season=False):
    competitions = _get_competitions()
    tier_df = pl.DataFrame(LEAGUE_STATS)
    filtered_competitions = competitions.join(tier_df, on=["Country", "Tier"]).filter(
        pl.col("Season_End_Year") >= MIN_SEASON_END_YEAR, pl.col("Gender") == gender
    )
    league_parts = filtered_competitions.partition_by(
        ["Country", "Gender", "Tier", "Season_End_Year"], as_dict=True
    )
    for (country, gender, tier, season), part_df in track(league_parts.items()):
        csv_file = f"{country}_{gender}_{tier}_{SOURCE_SUFFIX}_{season}.csv"
        filename = (
            Path(BASE_DIR)
            / "wages"
            / csv_file
        )
        if not filename.parent.exists():
            filename.parent.mkdir(parents=True, exist_ok=True)
        if filename.exists() and ((season < get_current_season()) or not update_current_season):
            continue
        team_urls = call_wf_function(
            "fb_teams_urls", league_url=part_df["seasons_urls"].to_list()[0]
        )
        wages = call_wf_function("fb_squad_wages", team_urls=team_urls, time_pause=4)
        if wages is not None:
            stage_name = Path(STAGE_DIR) / "wages" / csv_file
            wages.write_csv(stage_name)


def _get_competitions():
    competitions = pl.read_csv(Path(BASE_DIR) / "competitions.csv").rename(
        {
            "country": "Country",
            "tier": "Tier",
            "season_end_year": "Season_End_Year",
            "gender": "Gender",
            "competition_name": "Competition_Name",
        }
    )
    return competitions


def ingest_season_stats(update_current_season=False):
    pass


def ingest_match_summary_fb(
    missing_matches: pl.DataFrame, match_summaries: pl.DataFrame
):
    setup_logging()
    match_df = missing_matches[
        ["MatchURL", "Country", "Tier", "Season_End_Year", "Gender"]
    ]
    summary_joined = match_summaries.join(match_df, on="MatchURL", how="left")
    shooting_parts = summary_joined.partition_by(
        ["Country", "Gender", "Tier"], as_dict=True
    )
    for (country, gender, tier), part_df in shooting_parts.items():
        filename = (
            Path(STAGE_DIR)
            / "match_summary"
            / f"{country}_{gender}_{tier}_match_summary_{SOURCE_SUFFIX}.csv"
        )
        if not filename.parent.exists():
            filename.parent.mkdir(parents=True, exist_ok=True)
        part_df.write_csv(filename)


def ingest_advanced_match_stats_fb(
    missing_matches: pl.DataFrame,
    player_stats: AdvancedMatchStats,
    team_stats: AdvancedMatchStats,
):
    setup_logging()
    match_df = missing_matches[
        ["MatchURL", "Country", "Tier", "Season_End_Year", "Gender"]
    ].with_columns(match_id=pl.col("MatchURL").str.split("/").list[-2])
    for team_player, stats in zip(
        ["team", "player"],
        [dataclasses.asdict(team_stats), dataclasses.asdict(player_stats)],
    ):
        for stat_type, df in track(
            stats.items(), description=f"Ingesting {team_player} stats"
        ):
            if df is None:
                continue
            df_joined = df.with_columns(match_id=pl.col("MatchURL").str.split("/").list[-2]).drop("MatchURL").join(match_df, on="match_id", how="left").drop("match_id")
            df_parts = df_joined.partition_by(
                ["Country", "Gender", "Tier"], as_dict=True
            )
            for (country, gender, tier), part_df in df_parts.items():
                filename = (
                    Path(STAGE_DIR)
                    / "advanced_match_stats"
                    / team_player
                    / stat_type
                    / f"{country}_{gender}_{tier}_{SOURCE_SUFFIX}.csv"
                )
                if not filename.parent.exists():
                    filename.parent.mkdir(parents=True, exist_ok=True)
                part_df.write_csv(filename)


def ingest_match_shooting_fb(
    missing_matches: pl.DataFrame, shooting_data: pl.DataFrame
):
    setup_logging()
    if shooting_data.shape[0] == 0:
        log.warning("No shooting data to ingest")
        return
    match_df = missing_matches[
        ["MatchURL", "Country", "Tier", "Season_End_Year", "Gender"]
    ]
    shooting_joined = shooting_data.join(match_df, on="MatchURL", how="left")
    shooting_parts = shooting_joined.partition_by(
        ["Country", "Gender", "Tier"], as_dict=True
    )
    for (country, gender, tier), part_df in shooting_parts.items():
        filename = (
            Path(STAGE_DIR)
            / "match_shooting"
            / f"{country}_{gender}_{tier}_match_shooting_{SOURCE_SUFFIX}.csv"
        )
        if not filename.parent.exists():
            filename.parent.mkdir(parents=True, exist_ok=True)
        part_df.write_csv(filename)


def stage_new_results():
    new_files = list(Path(STAGE_DIR).glob("**/*.csv"))
    for f in new_files:
        next_path = str(f).replace(STAGE_DIR, BASE_DIR)
        existing_pattern = Path(next_path).with_name(
            f.name.replace("_fbref", "_fbref_*"))
        # get the maximum 4 digit number in the existing files
        existing_files = len([x for x in Path().glob(str(existing_pattern))]) + 1
        new_name = Path(next_path).with_name(
            f.name.replace("_fbref", f"_fbref_{existing_files:>04}"))
        shutil.move(f, new_name)


if __name__ == "__main__":
    scrape_matches()
    # ingest_advanced_match_stats_fb()
    # ingest_season_stats()
    # ingest_match_summary_fb()
    # ingest_match_shooting_fb()
    # ingest_wages()
    # stage_new_results()
