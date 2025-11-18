"""A module to ingest data from the FBRef website"""

import logging as log
import os
import polars as pl
from pathlib import Path
from rich.progress import track

from bayesball.utils import maybe_download_file, setup_logging
from bayesball.config import COUNTRIES, TIERS, MIN_SEASON_END_YEAR
STAGE_DIR = "data/ingest/stage"

BASE_DIR = "data/ingest/fbref"

SOURCE_SUFFIX = "fbref"
# WF_DIR = "data/ingest/wf"
GENDER = "M"

def _get_match_urls(country, gender=GENDER):
    # get all results
    # match_urls_filename = f"{country}_match_results.csv"
    # get all summaries in wf and fbref
    match_results = pl.scan_csv(Path(BASE_DIR) / "match_results" / "*.csv").select("MatchURL")
    match_summaries = pl.read_csv(Path(BASE_DIR) / "match_summary" / "*.csv")
    tier_df = pl.DataFrame(TIERS, columns=["Country", "Tier"])
    match_urls = pl.read_csv(BASE_DIR / "match_results" / "*.csv").filter(
        pl.col("Season_End_Year") >= MIN_SEASON_END_YEAR,
        pl.col("Tier") == tier,
        pl.col("Gender") == gender,
    )["MatchURL"]
    match_urls = list(
        [c for c in match_urls.unique() if c is not None and "History" not in c]
    )
    return match_urls

def scrape_matches(time_pause=4):
    """Scrape match data from the FBRef website"""
    setup_logging()
    match_urls = _get_match_urls()
    for url in track(match_urls, description="Scraping matches"):
        filename = Path(STAGE_DIR) / "html" / Path(url).name + ".html"
        maybe_download_file(url, STAGE_DIR, filename, time_pause=time_pause)


def ingest_wages():
    pass

def ingest_season_stats():
    pass

def ingest_match_summary_fb():
    pass

def ingest_advanced_match_stats_fb():
    pass

def ingest_match_shooting_fb():
    pass

def stage_new_results():
    pass
