"""A module to ingest data from the worldfootballR_data repository"""

import logging as log
import os
import tempfile

import pandas as pd
import pandera as pa

from bayesball.utils import (
    create_output_dir,
    maybe_download_file,
    read_rds,
    setup_logging,
)

from bayesball.schema import MatchSummarySchema, MatchResultsSchema, MatchShootingSchema

from bayesball.config import ADVANCED_MATCH_STATS, COUNTRIES, TIERS

BASE_DIR = "data/ingest/fbref"
SOURCE_SUFFIX = "wf"


def download_and_save_file(
    base_url, country, tier, gender, file_suffix, output_dir, reload=False, schema=None
):
    url = f"{base_url}/{country}_{gender}_{tier}_{file_suffix}.csv"
    maybe_download_file(
        url, output_dir, reload=reload, filename=f"{country}_{tier}_{file_suffix}.csv"
    )
    if schema is not None:
        fpath = os.path.join(output_dir, f"{country}_{tier}_{file_suffix}.csv")
        df = pd.read_csv(fpath)
        df = df[schema.columns.keys()]
        df.to_csv(fpath, index=False)


def ingest_match_data(data_type, file_suffix, output_dir=None):
    setup_logging()
    if output_dir is None:
        output_dir = data_type
    output_dir = create_output_dir(BASE_DIR, output_dir)
    gender = "M"

    for country in COUNTRIES:
        tiers = TIERS.get(country, ["1st"])
        for tier in tiers:
            download_and_save_file(
                f"https://github.com/JaseZiv/worldfootballR_data/releases/download/{data_type}",
                country,
                tier,
                gender,
                file_suffix,
                output_dir,
                reload=True,
            )


@pa.check_types
def ingest_match_summary_wf():
    ingest_match_data(
        "fb_match_summary",
        f"match_summary_{SOURCE_SUFFIX}",
        output_dir="match_summary",
        schema=MatchSummarySchema,
    )


def ingest_match_shooting_wf():
    ingest_match_data(
        "fb_match_shooting",
        f"match_shooting_{SOURCE_SUFFIX}",
        output_dir="match_shooting",
        schema=MatchShootingSchema,
    )


def ingest_competitions():
    setup_logging()
    output_dir = create_output_dir(BASE_DIR, "")

    url = "https://raw.githubusercontent.com/JaseZiv/worldfootballR_data/master/raw-data/all_leages_and_cups/all_competitions.csv"
    competitions = pd.read_csv(url)
    competitions.to_csv(os.path.join(output_dir, "competitions.csv"), index=False)
    log.info("Saved competitions.csv")


def read_match_results(filepath) -> pd.DataFrame:
    df = read_rds(filepath)
    df["Date"] = pd.to_datetime(df["Date"], origin="1970-01-01", unit="D")
    df = df[(MatchResultsSchema.columns.keys())]
    if "USA" in filepath:
        df["MatchURL"] = df["MatchURL"].str.replace("Sporting-KC", "Sporting-Kansas-City")
    return df


def ingest_match_results():
    setup_logging()
    output_dir = create_output_dir(BASE_DIR, "match_results")

    with tempfile.TemporaryDirectory() as td:
        for country in COUNTRIES:
            url = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/match_results/{country}_match_results.rds"
            log.info(f"Downloading {url}")
            maybe_download_file(url, td)
            df = read_match_results(os.path.join(td, f"{country}_match_results.rds"))
            df.to_csv(
                os.path.join(
                    output_dir, f"{country}_match_results_{SOURCE_SUFFIX}.csv"
                ),
                index=False,
            )
            log.info(f"Saved {country}_match_results.csv")


def ingest_advanced_match_stats_wf():
    setup_logging()
    gender = "M"

    for country in COUNTRIES:
        tiers = TIERS.get(country, ["1st"])
        for tier in tiers:
            for stat in ADVANCED_MATCH_STATS:
                for team_player in ["team", "player"]:
                    url = f"https://github.com/JaseZiv/worldfootballR_data/releases/download/fb_advanced_match_stats/{country}_{gender}_{tier}_{stat}_{team_player}_advanced_match_stats.csv"
                    output_dir = create_output_dir(
                        BASE_DIR, f"advanced_match_stats/{team_player}/{stat}"
                    )
                    out_path = f"{country}_{gender}_{tier}_{SOURCE_SUFFIX}.csv"
                    maybe_download_file(
                        url, data_dir=output_dir, filename=out_path, reload=True
                    )
