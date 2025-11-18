import logging as log
import os
import time
from urllib import request
from pathlib import Path

import polars as pl
import toml

import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri

pandas2ri.activate()

readRDS = robjects.r["readRDS"]


def get_current_season():
    """Return the current season based on the current date"""
    import datetime

    now = datetime.datetime.now()
    if now.month < 7:
        return now.year
    else:
        return now.year + 1


def maybe_download_file(url, data_dir, filename=None, time_pause=3, reload=False):
    """Download a file from a URL if it does not exist"""
    setup_logging()
    if filename is None:
        filename = Path(data_dir) / Path(url).name
    elif data_dir not in Path(filename).parents:
        filename = Path(data_dir) / filename
    if not filename.exists() or reload:
        filename.parent.mkdir(parents=True, exist_ok=True)
        try:
            log.info(f"Downloading {url}")
            request.urlretrieve(url, filename)
            time.sleep(time_pause)
        except Exception as e:
            log.error(f"Failed to download {url} with error {e}")
    else:
        log.debug(f"{filename} already exists, skipping download")


def maybe_split_csv(data_path, data_dir, file_name):
    """Split a CSV file into multiple files based on the country, tier, and season"""
    if not data_path.exists():
        log.warning(f"{data_path} does not exist, skipping split")
        return
    df = pl.read_csv(data_path)
    for key, gdf in df.groupby(["Country", "Tier", "Season_End_Year"]):
        country, tier, year = key
        o = data_dir / country / tier / str(year) / "match_stats"
        full_path = o / f"{file_name}.csv"
        if not full_path.exists():
            full_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f"Writing {full_path}")
        gdf.to_csv(full_path, index=False)


def read_rds(rds_file):
    """Read an RDS file and return a pandas DataFrame"""
    rds_df = readRDS(str(rds_file))
    return pandas2ri.rpy2py(rds_df)


def setup_logging():
    """Setup logging for the ingest module"""
    log.basicConfig(level=log.INFO)


def get_config():
    """Read the ingest configuration from the config file"""
    return toml.load("config.toml").get("ingest")


def create_output_dir(base_dir, sub_dir):
    """Create an output directory"""
    output_dir = os.path.join(base_dir, sub_dir)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir
