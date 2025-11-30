"""An interface to the worldfootballr R package"""

from typing import Callable

from rpy2.robjects.packages import importr
import rpy2.robjects as ro
import os

# import pandas as pd
import polars as pl
from pathlib import Path

import logging
from rich.logging import RichHandler

from bayesball.utils import r_to_python, maybe_download_file

LOGFORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOGFORMAT_RICH = "%(message)s"

rh = RichHandler()
rh.setFormatter(logging.Formatter(LOGFORMAT_RICH))
logging.basicConfig(
    level=logging.ERROR,
    format=LOGFORMAT,
    handlers=[
        rh,
    ],
)
LOGGER = logging.getLogger(__name__)
wf = importr("worldfootballR")
r_source = ro.r["source"]
match_wf = r_source(str(Path(__file__).parent.parent / "parse_match_pages_2.R"))

fb_parse_match_data = ro.globalenv["fb_parse_match_data"]
# Define stat types

season_player_stats = [
    "standard",
    "shooting",
    "passing",
    "passing_types",
    "gca",
    "defense",
    "possession",
    "playing_time",
    "misc",
    "keepers",
    "keepers_adv",
]
season_team_stats = [
    "league_table",
    "league_table_home_away",
    "standard",
    "keeper",
    "keeper_adv",
    "shooting",
    "passing",
    "passing_types",
    "goal_shot_creation",
    "defense",
    "possession",
    "misc",
    "playing_time",
]
team_match_stats = [
    "shooting",
    "keeper",
    "passing",
    "passing_types",
    "gca",
    "defense",
    "misc",
]


def replace_none_with_na(value, expected_type="logical"):
    """Replace None with NA in R"""
    if value is None:
        if expected_type == "character":
            return ro.NA_Character
        elif expected_type == "integer":
            return ro.NA_Integer
        elif expected_type == "real":
            return ro.NA_Real
        else:
            return ro.NA_Logical
    else:
        return value


def _call_r_func(r_func, *args, **kwargs) -> pl.DataFrame:
    try:
        # Replace None with NA in args and kwargs
        args = tuple(replace_none_with_na(arg) for arg in args)
        kwargs = {k: replace_none_with_na(v) for k, v in kwargs.items()}
        args_no_url = tuple(arg for arg in args if not isinstance(arg, list))
        kwargs_no_url = {k: v for k, v in kwargs.items() if not isinstance(v, list)}
        result = r_func(*args, **kwargs)
        result = r_to_python(result)
        return result
    except Exception as e:
        print(f"An error occurred while calling '{r_func}': {e}")
        LOGGER.error(f"An error occurred while calling '{r_func}': {e}")
        return None


def call_match_wf_function(func_name, *args, **kwargs):
    """Call a function from the match_wf R source"""
    try:
        r_func = ro.r[func_name]
    except AttributeError:
        raise ValueError(f"Function '{func_name}' not found in source package.")
    return _call_r_func(r_func, *args, **kwargs)


def call_wf_function(func_name, *args, **kwargs):
    """Call a function from the worldfootballr package"""
    try:
        r_func = getattr(wf, func_name)
    except AttributeError:
        raise ValueError(f"Function '{func_name}' not found in worldfootballr package.")
    return _call_r_func(r_func, *args, **kwargs)


class FootballDataLoader:
    """A class to load football data from worldfootballr"""

    def __init__(self, country, season, tier, gender, data_dir, reload=False):
        self.data_dir = Path(data_dir)
        self.country = country
        self.season = season
        self.tier = tier
        self.gender = gender
        self._reload = reload
        self.match_mapping = {}
        # Ensure the data directory exists
        os.makedirs(self.data_dir, exist_ok=True)

    # Helper method to construct file paths
    def _get_file_path(self, filename):
        return self.data_dir / filename

    # Helper method to construct filenames
    def _construct_filename(self, prefix, stat_type):
        if prefix != "":
            prefix = f"{prefix}_"
        return Path(f"{self.country}_{self.gender}_{self.tier}_{prefix}{stat_type}.csv")

    # General method to load or fetch data
    def _load_or_fetch_data(
        self, filename: Path, fetch_func: Callable, *args, **kwargs
    ):
        file_path = self._get_file_path(filename)
        assert self.tier in filename.name, f"Tier {self.tier} not in {filename}"
        assert self.gender in filename.name, f"Gender {self.gender}" not in filename
        assert self.country in filename.name, (
            f"Country {self.country} not in {filename}"
        )
        if file_path.exists():
            df = pl.read_csv(file_path, infer_schema_length=10000)
            if "Season_End_Year" in df.columns:
                df = df.filter(pl.col("Season_End_Year") == self.season)
            elif "url" in df.columns:
                season_url = f"{self.season - 1}-{self.season}"
                df = df.filter(pl.col("url").str.contains(season_url))
            else:
                raise ValueError(f"Season_End_Year or url not in {file_path}")
            num_rows = df.shape[0]
            if num_rows > 0:
                LOGGER.info(f"Loading {num_rows} rows from {file_path}")
                return df
        df_new = fetch_func(*args, **kwargs)
        if "Season_End_Year" not in df_new.columns:
            s = pl.Series(
                "Season_End_Year", [self.season for _ in range(df_new.shape[0])]
            )
            df_new = df_new.insert_column(0, s)
            # df_new = df_new.withColumns(Season_End_Year=pl.lit(self.season))
        self._update_data(filename, df_new)
        return df_new

    def _add_meta_data(self, df: pl.DataFrame):
        df = df.with_columns(
            Match_Date=pl.col("Match_Date").str.to_date()
        ).with_columns(
            **{
                "Gender": pl.lit(self.gender),
                "Season_End_Year": pl.when(pl.col("Match_Date").dt.month() > 6)
                .then(pl.col("Match_Date").dt.year() + 1)
                .otherwise(pl.col("Match_Date").dt.year()),
                "Country": pl.lit(self.country),
                "Tier": pl.lit(self.tier),
            }
        )
        return df

    def _update_data(self, filename, df: pl.DataFrame):
        file_path = self._get_file_path(filename)
        if "MatchURL" in df:
            df = df.with_columns(
                MatchURL=pl.col("MatchURL").replace(self.match_mapping)
            )
        if "Date" in df and df["Date"].dtype.is_numeric():
            df = df.with_columns(Date=pl.from_epoch(pl.col("Date"), time_unit="d"))
        if file_path.exists():
            LOGGER.info(f"Updating data in {file_path}")
            known_df = pl.read_csv(file_path)
            updated_df = pl.concat([known_df, df], how="diagonal_relaxed")
            if "Match_Date" in updated_df.columns:
                updated_df = self._add_meta_data(updated_df)
            if "Competition_Name" in updated_df.columns:
                updated_df = updated_df.with_columns(
                    Competition_Name=pl.col("Competition_Name").fill_null(
                        strategy="forward"
                    )
                )
            if "Game_URL" in updated_df.columns:
                updated_df = updated_df.with_columns(MatchURL=pl.col("Game_URL"))
            updated_df = updated_df.with_columns(
                pl.col(pl.String).replace("NA_character_", "")
            )
            ix_cols = [
                c
                for c in updated_df.columns
                if c
                in ["Season_End_Year", "Rk", "Match_Date", "Squad", "Team", "Player"]
            ]
            updated_df = updated_df.unique().sort(ix_cols)
            updated_df.write_csv(file_path)
        else:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.info(f"Saving data to {file_path}")
            df.write_csv(file_path)

    # Method to get league season stats
    def get_league_season_stats(self):
        # Define stat types
        season_stats = [
            "standard",
            "shooting",
            "passing",
            "passing_types",
            "gca",
            "defense",
            "possession",
            "playing_time",
            "misc",
            "keepers",
            "keepers_adv",
        ]
        # Initialize dictionaries to store data
        player_stats = {}
        team_stats = {}

        # Load or fetch player stats
        for stat_type in season_stats:
            filename = self._construct_filename("player", stat_type + "_season_stats")

            df = self._load_or_fetch_data(
                filename,
                call_wf_function,
                "fb_league_stats",
                country=self.country,
                gender=self.gender,
                season_end_year=self.season,
                stat_type=stat_type,
                team_or_player="player",
                tier=self.tier,
            )
            player_stats[stat_type] = df

        # Load or fetch team stats
        for stat_type in season_stats:
            filename = self._construct_filename("team", stat_type + "_season_stats")
            df = self._load_or_fetch_data(
                filename,
                call_wf_function,
                "fb_league_stats",
                country=self.country,
                gender=self.gender,
                season_end_year=self.season,
                stat_type=stat_type,
                team_or_player="team",
                tier=self.tier,
            )
            team_stats[stat_type] = df

        return player_stats, team_stats

    def get_match_urls(self):
        LOGGER.info(
            f"Getting match URLs for {self.country} {self.gender} {self.season} {self.tier}"
        )
        prefix = "match"
        match_urls_filename = self._construct_filename(prefix, "urls")
        match_urls_filename = f"{self.country}_match_results.csv"
        match_urls = pl.read_csv(self.data_dir / match_urls_filename).filter(
            pl.col("Season_End_Year") == self.season,
            pl.col("Tier") == self.tier,
            pl.col("Gender") == self.gender,
        )["MatchURL"]
        match_urls = list(
            [c for c in match_urls.unique() if c is not None and "History" not in c]
        )
        return match_urls

    def scrape_matches(self, time_pause=4):
        match_urls = self.get_match_urls()
        for match_url in match_urls:
            maybe_download_file(
                match_url,
                data_dir=self.data_dir,
                filename=Path(self.data_dir)
                / "html"
                / self.country
                / self.tier
                / str(self.season)
                / (Path(match_url).name + ".html"),
                time_pause=time_pause,
            )

    # Method to get match stats
    def get_match_stats(self):
        # File names
        LOGGER.info(
            f"Getting match stats for {self.country} {self.gender} {self.tier} {self.season}"
        )
        prefix = "match"
        match_summaries_filename = self._construct_filename(
            prefix,
            "summary",
        )
        match_urls = self.get_match_urls()
        # TODO: replace this with current season
        if (not self._reload) and (self.data_dir / match_summaries_filename).exists():
            matches = pl.read_csv(self.data_dir / match_summaries_filename)
            if "MatchURL" in matches.columns:
                num_matches = (
                    matches.filter(pl.col("Season_End_Year") == self.season)
                    .select("MatchURL")
                    .n_unique()
                )
                expected_matches = len(match_urls)
                match_urls = [
                    c for c in match_urls if c not in matches["MatchURL"].unique()
                ]
                if len(match_urls) == 0:
                    LOGGER.info(
                        f"Skipping as {match_summaries_filename} contains {num_matches} matches"
                    )
                    return

        self.match_mapping = {}
        # TODO: append new match_urls
        download_paths = []
        for match_url in match_urls:
            download_path = (
                Path(self.data_dir)
                / "html"
                / self.country
                / self.tier
                / str(self.season)
                / (Path(match_url).name + ".html")
            )
            download_paths.append(str(download_path))
            self.match_mapping[str(download_path)] = match_url
            maybe_download_file(
                match_url, data_dir=self.data_dir, filename=download_path, time_pause=4
            )

        # Define team match stats
        if (self.season > 2017 and self.tier == "1st") or (
            self.season > 2018 and self.tier == "2nd"
        ):
            team_match_stats = [
                "summary",
                "passing",
                "passing_types",
                "defense",
                "possession",
                "misc",
                "keeper",
            ]
            shooting = True
        else:
            team_match_stats = ["summary"]
            shooting = False

        match_data = fb_parse_match_data(
            download_paths, stat_types=team_match_stats, shooting=shooting
        )

        player_stats = {}
        team_stats = {}
        for i, stat_type in enumerate(team_match_stats):
            try:
                team_stats[stat_type] = r_to_python(match_data[0][2 * i])
                player_stats[stat_type] = r_to_python(match_data[0][2 * i + 1])
            except IndexError:
                LOGGER.error(f"Error loading {stat_type}")
                continue

        shooting_data = r_to_python(match_data[1])
        lineups = r_to_python(match_data[2])
        match_summaries = r_to_python(match_data[3])

        self._update_data(
            match_summaries_filename,
            match_summaries,
        )

        if shooting:
            shooting_filename = self._construct_filename(prefix, "shooting")
            self._update_data(
                shooting_filename,
                shooting_data,
            )
        for stat_type in team_match_stats:
            if stat_type in player_stats:
                self._update_data(
                    self._construct_filename(
                        "", f"{stat_type}_player_advanced_match_stats"
                    ),
                    player_stats[stat_type],
                )
            if stat_type in team_stats:
                self._update_data(
                    self._construct_filename(
                        "", f"{stat_type}_team_advanced_match_stats"
                    ),
                    team_stats[stat_type],
                )

        return {
            # 'match_results': match_results,
            # 'match_reports': match_reports,
            "match_summaries": match_summaries,
            "lineups": lineups,
            "player_stats": player_stats,
            "team_stats": team_stats,
        }

    # Method to get player stats
    def get_player_stats(self):
        # File names
        team_urls_filename = self._construct_filename("season_team_stats", "urls")
        # player_urls_filename = self._construct_filename('season_player_stats', 'urls')
        scouting_report_filename = self._construct_filename(
            "player_stats", "scouting_report"
        )
        wages_filename = self._construct_filename("player_stats", "wages")
        league_urls = call_wf_function(
            "fb_league_urls",
            country=self.country,
            gender=self.gender,
            season_end_year=self.season,
            tier=self.tier,
        )
        # Load or fetch team URLs
        team_urls = call_wf_function("fb_teams_urls", league_url=str(league_urls[0]))
        team_url_df = pl.DataFrame(
            {
                "Country": pl.lit(self.country),
                "Tier": pl.lit(self.tier),
                "Gender": pl.lit(self.gender),
                "Season_End_Year": pl.lit(self.season),
                "url": team_urls,
            }
        )

        team_filename = self._construct_filename("", "_season_team_urls")
        self._update_data(team_filename, team_url_df)

        # Load or fetch wages
        wages = self._load_or_fetch_data(
            wages_filename, call_wf_function, "fb_squad_wages", team_urls=team_urls
        )

        return {
            # 'scouting_report': scouting_report,
            "wages": wages
        }


if __name__ == "__main__":
    data_dir = Path("data/raw/fbref")
    loader = FootballDataLoader(
        country="END", tier="1st", season=2018, gender="M", data_dir=data_dir
    )
    loader.get_match_stats()
