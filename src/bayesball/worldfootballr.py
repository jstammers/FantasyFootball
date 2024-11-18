"""An interface to the worldfootballr R package"""
import time
import urllib

from rpy2.robjects.packages import importr
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
import numpy as np
import os
import pandas as pd
from pathlib import Path

#Use rich for consfrom rich.logging import RichHandler
import logging
from rich.logging import RichHandler
from logging.handlers import  RotatingFileHandler

LOGFORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOGFORMAT_RICH = '%(message)s'

rh = RichHandler()
rh.setFormatter(logging.Formatter(LOGFORMAT_RICH))
rh.setLevel(logging.DEBUG)
logging.basicConfig(
    level=logging.INFO,
    format=LOGFORMAT,
    handlers=[
        rh,
        RotatingFileHandler("worldscraper.log", maxBytes=1000000, backupCount=3)
    ]
)
LOGGER = logging.getLogger(__name__)
wf = importr("worldfootballR")
r_source = ro.r['source']
match_wf = r_source(str(Path(__file__).parent.parent / "parse_match_pages.R"))
# Define stat types
season_player_stats = ["standard", "shooting", "passing", "passing_types", "gca",
                       "defense", "possession", "playing_time", "misc", "keepers",
                       "keepers_adv"]
season_team_stats = ["league_table", "league_table_home_away", "standard", "keeper",
                     "keeper_adv", "shooting", "passing", "passing_types",
                     "goal_shot_creation", "defense", "possession", "misc",
                     "playing_time"]
team_match_stats = ["shooting", "keeper", "passing", "passing_types", "gca", "defense",
                    "misc"]


def replace_none_with_na(value, expected_type='logical'):
    if value is None:
        if expected_type == 'character':
            return ro.NA_Character
        elif expected_type == 'integer':
            return ro.NA_Integer
        elif expected_type == 'real':
            return ro.NA_Real
        else:
            return ro.NA_Logical
    else:
        return value

def _call_r_func(r_func, *args, **kwargs):
    try:
        # Replace None with NA in args and kwargs
        args = tuple(replace_none_with_na(arg) for arg in args)
        kwargs = {k: replace_none_with_na(v) for k, v in kwargs.items()}
        args_no_url = tuple(arg for arg in args if not isinstance(arg, list))
        kwargs_no_url = {k: v for k, v in kwargs.items() if not isinstance(v, list)}
        with (ro.default_converter + pandas2ri.converter).context():
            LOGGER.debug(f"Calling '{r_func}' with args: {args_no_url} and kwargs: {kwargs_no_url}")
            result = r_func(*args, **kwargs)
            # Convert R DataFrame to pandas DataFrame if applicable
            if isinstance(result, (ro.vectors.DataFrame, ro.vectors.StrVector)):
                result = pandas2ri.rpy2py(result)
                if isinstance(result, np.ndarray):
                    result = list(result)
            LOGGER.debug(
                f"Successfully called '{r_func}'")
        return result
    except Exception as e:
        print(f"An error occurred while calling '{r_func}': {e}")
        LOGGER.error(f"An error occurred while calling '{r_func}': {e}")
        return None

def call_match_wf_function(func_name, *args, **kwargs):
    try:
        r_func = ro.r[func_name]
    except AttributeError:
        raise ValueError(f"Function '{func_name}' not found in source package.")
    return _call_r_func(r_func, *args, **kwargs)

def call_wf_function(func_name, *args, **kwargs):
    try:
        r_func = getattr(wf, func_name)
    except AttributeError:
        raise ValueError(f"Function '{func_name}' not found in worldfootballr package.")
    return _call_r_func(r_func, *args, **kwargs)

def fb_advanced_match_stats(match_url, stat_type, team_or_player, **kwargs):
    """

    Parameters
    ----------
    match_url
    stat_type
    team_or_player
    kwargs

    Returns
    -------

    Examples
    --------
    >>> urls = fb_match_urls("AUS", "F", 2021, "1st")
    >>> df = fb_advanced_match_stats(urls, "possession", "player")  # doctest: +ELLIPSIS
    >>> len(df) > 0
    True
    """
    params = {
        'match_url': match_url,
        'stat_type': stat_type,
        'team_or_player': team_or_player,
        **kwargs
    }
    return call_wf_function('fb_advanced_match_stats', **params)


def fb_big5_advanced_season_stats(season_end_year, stat_type, team_or_player, **kwargs):
    """

    Parameters
    ----------
    season_end_year
    stat_type
    team_or_player
    kwargs

    Returns
    -------
    Examples
    --------
    >>> df = fb_big5_advanced_season_stats(season_end_year=2021,stat_type="possession",team_or_player="player")
    """
    params = {
        'season_end_year': season_end_year,
        'stat_type': stat_type,
        'team_or_player': team_or_player,
        **kwargs
    }
    return call_wf_function('fb_big5_advanced_season_stats', **params)


def fb_league_stats(country, gender, season_end_year, stat_type='standard',
                    team_or_player='team', tier="1st", **kwargs):
    """

    Parameters
    ----------
    country
    gender
    season_end_year
    stat_type
    team_or_player
    kwargs

    Returns
    -------

    Examples
    --------
    >>> df = fb_league_stats(country = "ENG",gender = "M",season_end_year = 2022,tier = "1st",stat_type = "shooting",team_or_player = "player")  # doctest: +ELLIPSIS
    ...
    >>> len(df) > 0
    True
    Non-domestic league stats are more likely to fail due to the volume of players

    >>> df = fb_league_stats(country=None, gender="M", season_end_year=2023, tier=None, non_dom_league_url="https://fbref.com/en/comps/8/history/Champions-League-Seasons", stat_type="standard", team_or_player="player") # doctest: +ELLIPSIS
    ...
    >>> len(df) > 0
    True
    """
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'stat_type': stat_type,
        'team_or_player': team_or_player,
        'tier': tier,
        **kwargs
    }
    return call_wf_function('fb_league_stats', **params)


def fb_league_urls(country, gender, season_end_year, tier='1st'):
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'tier': tier
    }
    return call_wf_function('fb_league_urls', **params)


def fb_match_lineups(match_url):
    params = {'match_url': match_url}
    return call_wf_function('fb_match_lineups', **params)


def fb_match_report(match_url, time_pause=3):
    params = {'match_url': match_url, 'time_pause': time_pause}
    return call_wf_function('fb_match_report', **params)


def fb_match_results(country, gender, season_end_year, tier='1st',
                     non_dom_league_url=None, time_pause=3, alternates=False):
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'tier': tier,
        'non_dom_league_url': non_dom_league_url,
        'time_pause': time_pause,
        'alternates': alternates
    }
    return call_wf_function('fb_match_results', **params)


def fb_match_shooting(match_url):
    params = {'match_url': match_url}
    return call_wf_function('fb_match_shooting', **params)


def fb_match_summary(match_url):
    params = {'match_url': match_url}
    return call_wf_function('fb_match_summary', **params)


def fb_match_urls(country, gender, season_end_year, tier='1st', non_dom_league_url=None,
                  time_pause=3, alternates=False):
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'tier': tier,
        'non_dom_league_url': non_dom_league_url,
        'time_pause': time_pause,
    }
    return call_wf_function('fb_match_urls', **params)


def fb_player_goal_logs(player_url, time_pause=3):
    params = {'player_url': player_url, 'time_pause': time_pause}
    return call_wf_function('fb_player_goal_logs', **params)


def fb_player_match_logs(player_url, season_end_year=None, stat_type='summary',
                         time_pause=3):
    params = {
        'player_url': player_url,
        'season_end_year': season_end_year,
        'stat_type': stat_type,
        'time_pause': time_pause
    }
    return call_wf_function('fb_player_match_logs', **params)


def fb_player_scouting_report(player_url, pos_versus='primary', season_end_year=None,
                              league_comp_name=None, time_pause=3):
    params = {
        'player_url': player_url,
        'pos_versus': pos_versus,
        'season_end_year': season_end_year,
        'league_comp_name': league_comp_name,
        'time_pause': time_pause
    }
    return call_wf_function('fb_player_scouting_report', **params)


def fb_player_season_stats(player_url, stat_type='standard', national=False,
                           time_pause=3):
    params = {
        'player_url': player_url,
        'stat_type': stat_type,
        'national': national,
        'time_pause': time_pause
    }
    return call_wf_function('fb_player_season_stats', **params)


def fb_player_urls(team_urls, time_pause=3):
    params = {'team_urls': team_urls, 'time_pause': time_pause}
    return call_wf_function('fb_player_urls', **params)


def fb_season_team_stats(team_url, stat_type='standard', season_end_year=None,
                         time_pause=3):
    params = {
        'team_url': team_url,
        'stat_type': stat_type,
        'season_end_year': season_end_year,
        'time_pause': time_pause
    }
    return call_wf_function('fb_season_team_stats', **params)


def fb_squad_wages(team_url):
    params = {'team_url': team_url}
    return call_wf_function('fb_squad_wages', **params)


def fb_team_goal_logs(team_urls, time_pause=3):
    params = {'team_urls': team_urls, 'time_pause': time_pause}
    return call_wf_function('fb_team_goal_logs', **params)


def fb_team_match_log_stats(team_url, stat_type='standard', season_end_year=None,
                            time_pause=3):
    params = {
        'team_url': team_url,
        'stat_type': stat_type,
        'season_end_year': season_end_year,
        'time_pause': time_pause
    }
    return call_wf_function('fb_team_match_log_stats', **params)


def fb_team_player_stats(team_url, stat_type='standard'):
    params = {'team_url': team_url, 'stat_type': stat_type}
    return call_wf_function('fb_team_player_stats', **params)


def fb_team_match_stats(match_url):
    params = {'match_url': match_url}
    return call_wf_function('fb_team_match_stats', **params)


def fb_team_urls(country, gender, season_end_year, tier='1st', time_pause=3,
                 non_dom_league_url=None):
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'tier': tier,
        'time_pause': time_pause,
        'non_dom_league_url': non_dom_league_url
    }
    return call_wf_function('fb_team_urls', **params)


def load_fb_advanced_match_stats(match_urls, stat_type='summary', team_or_player='team',
                                 time_pause=3):
    params = {
        'match_urls': match_urls,
        'stat_type': stat_type,
        'team_or_player': team_or_player,
        'time_pause': time_pause
    }
    return call_wf_function('load_fb_advanced_match_stats', **params)


def load_fb_big5_advanced_season_stats(season_end_year, stat_type='standard',
                                       team_or_player='team', time_pause=3):
    params = {
        'season_end_year': season_end_year,
        'stat_type': stat_type,
        'team_or_player': team_or_player,
        'time_pause': time_pause
    }
    return call_wf_function('load_fb_big5_advanced_season_stats', **params)


def load_fb_match_shooting(match_urls, time_pause=3):
    params = {'match_urls': match_urls, 'time_pause': time_pause}
    return call_wf_function('load_fb_match_shooting', **params)


def load_fb_match_summary(match_urls, time_pause=3):
    params = {'match_urls': match_urls, 'time_pause': time_pause}
    return call_wf_function('load_fb_match_summary', **params)


def load_match_comp_results(country, gender, season_end_year, tier='1st', time_pause=3):
    params = {
        'country': country,
        'gender': gender,
        'season_end_year': season_end_year,
        'tier': tier,
        'time_pause': time_pause
    }
    return call_wf_function('load_match_comp_results', **params)


def load_match_results(match_urls, time_pause=3):
    params = {'match_urls': match_urls, 'time_pause': time_pause}
    return call_wf_function('load_match_results', **params)


def load_understat_league_shots(league, season):
    params = {'league': league, 'season': season}
    return call_wf_function('load_understat_league_shots', **params)


class FootballDataLoader:
    def __init__(self, country, season, tier, gender, data_dir, reload=False):
        self.data_dir = Path(data_dir)
        self.country = country
        self.season = season
        self.tier = tier
        self.gender = gender
        self._reload = reload
        # Ensure the data directory exists
        os.makedirs(self.data_dir, exist_ok=True)

    # Helper method to construct file paths
    def _get_file_path(self, filename):
        return self.data_dir / filename

    # Helper method to construct filenames
    def _construct_filename(self, prefix, stat_type):
        return Path(f"{self.country}/{self.tier}/{self.season}/{prefix}/{stat_type}.csv")

    # General method to load or fetch data
    def _load_or_fetch_data(self, filename, fetch_func, *args, **kwargs):
        file_path = self._get_file_path(filename)
        if file_path.exists() and not self._reload:
            # Load from CSV
            LOGGER.debug(f"Loading data from {file_path}")
            df = pd.read_csv(file_path)
            if "match_url" in kwargs:
                cols = df.columns
                if "MatchURL" in cols:
                    known_urls = df["MatchURL"].unique().tolist()
                elif "url" in cols:
                    known_urls = df["url"].unique().tolist()
                elif "Game_URL" in cols:
                    known_urls = df["Game_URL"].unique().tolist()
                else:
                    raise ValueError(f"Unknown URL column, found: {cols}")
                # Modify this to filter based on match_id
                ku2 = []
                for x in known_urls:
                    if isinstance(x, str):
                        k = x.split("/")[-1]
                        ku2.append(k)
                known_urls = ku2
                new_urls = []
                for url in kwargs["match_url"]:
                    match_id = url.split("/")[-1].replace(".html", "")
                    if match_id not in known_urls:
                        new_urls.append(url)
                if len(new_urls) > 0:
                    LOGGER.info(f"Fetching data for {len(new_urls)} new matches")
                    kwargs["match_url"] = new_urls
                    df_new = fetch_func(*args, **kwargs)
                    if len(df_new) > 0:
                        df = pd.concat([df, df_new], axis=0, ignore_index=True)
                        df.to_csv(file_path, index=False)
                else:
                    LOGGER.debug("All match data already loaded")
        else:
            # Fetch data using the provided function
            df = fetch_func(*args, **kwargs)
            if isinstance(df, list):
                df = pd.DataFrame({"url": df})
            # Save to CSV
            file_path.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.debug(f"Saving data to {file_path}")
            df.to_csv(file_path, index=False)
        return df

    # Method to get league season stats
    def get_league_season_stats(self):
        # Define stat types
        season_stats = ["standard", "shooting", "passing", "passing_types",
                        "gca", "defense",
                        "possession", "playing_time", "misc", "keepers",
                        "keepers_adv"]
        # Initialize dictionaries to store data
        player_stats = {}
        team_stats = {}

        # Load or fetch player stats
        for stat_type in season_stats:
            filename = self._construct_filename('season_player_stats',
                                                stat_type)
            df = self._load_or_fetch_data(
                filename,
                call_wf_function,
                'fb_league_stats',
                country=self.country,
                gender=self.gender,
                season_end_year=self.season,
                stat_type=stat_type,
                team_or_player='player',
                tier=self.tier
            )
            player_stats[stat_type] = df

        # Load or fetch team stats
        for stat_type in season_stats:
            filename = self._construct_filename('season_team_stats',
                                                stat_type)
            df = self._load_or_fetch_data(
                filename,
                call_wf_function,
                'fb_league_stats',
                country=self.country,
                gender=self.gender,
                season_end_year=self.season,
                stat_type=stat_type,
                team_or_player='team',
                tier=self.tier
            )
            team_stats[stat_type] = df

        return player_stats, team_stats

    def get_match_urls(self):
        prefix = 'match_stats'
        match_urls_filename = self._construct_filename(prefix, 'urls')
        match_urls_df = self._load_or_fetch_data(
            match_urls_filename,
            call_wf_function,
            'fb_match_urls',
            country=self.country,
            gender=self.gender,
            season_end_year=self.season,
            tier=self.tier
        )
        match_urls = match_urls_df[
            'Match_URL'].tolist() if 'Match_URL' in match_urls_df.columns else \
            match_urls_df['url'].tolist()
        match_urls = [str(x) for x in match_urls]
        return match_urls

    def scrape_matches(self, time_pause=3):
        match_urls = self.get_match_urls()
        for match_url in match_urls:
            _maybe_download_file(match_url, data_dir=self.data_dir, filename=Path(self.data_dir)/"html"/self.country/self.tier/str(self.season)/(Path(match_url).name + ".html"), time_pause=time_pause)

    # Method to get match stats
    def get_match_stats(self):
        # File names
        LOGGER.info(f"Getting match stats for {self.country} {self.tier} {self.season}")
        prefix = 'match_stats'
        match_urls = self.get_match_urls()

        #TODO: append new match_urls
        download_paths = []
        for match_url in match_urls:
            download_path = Path(self.data_dir) / "html" / self.country / self.tier / str(self.season) / (Path(match_url).name + ".html")
            download_paths.append(str(download_path))
            _maybe_download_file(match_url, data_dir=self.data_dir, filename=download_path, time_pause=4)


        # match_results_filename = self._construct_filename(prefix, 'results',
        #                                                   )
        match_reports_filename = self._construct_filename(prefix, 'report', )
        #
        match_summaries_filename = self._construct_filename(
            prefix, 'summary',
        )

        lineups_filename = self._construct_filename(prefix, 'lineup')

        # Load or fetch match URLs

        # # Load or fetch match results
        # match_results = self._load_or_fetch_data(
        #     match_results_filename,
        #     call_wf_function,
        #     'fb_match_results',
        #     country=self.country,
        #     gender=self.gender,
        #     season_end_year=self.season,
        #     tier=self.tier
        # )

        # # Load or fetch match reports
        match_reports = self._load_or_fetch_data(
            match_reports_filename,
            call_match_wf_function,
            'fb_match_report',
            match_url=download_paths
        )
        #
        # # Load or fetch match summaries
        if self.season > 2017:
            match_summaries = self._load_or_fetch_data(
                match_summaries_filename,
                call_match_wf_function,
                'fb_match_summary',
                match_url=download_paths
            )
        else:
            match_summaries = None

        # Load or fetch lineups
        lineups = self._load_or_fetch_data(
            lineups_filename,
            call_match_wf_function,
            'fb_match_lineups',
            match_url=download_paths,
        )

        # Define team match stats
        if self.season > 2017:
            team_match_stats = ["summary",
                                "passing",
                                "passing_types",
                                "defense",
                                "possession",
                                "misc",
                                "keeper"]
        else:
            team_match_stats = ["summary"]
        player_stats = {}
        team_stats = {}

        # Fetch or load player stats
        for stat_type in team_match_stats:
            filename = self._construct_filename(prefix + "/player", stat_type)
            df = self._load_or_fetch_data(
                filename,
                call_match_wf_function,
                'fb_advanced_match_stats',
                match_url=download_paths,
                stat_type=stat_type,
                team_or_player='player',
            )
            player_stats[stat_type] = df

        # Fetch or load team stats
        for stat_type in team_match_stats:
            filename = self._construct_filename(prefix + "/team", stat_type)
            df = self._load_or_fetch_data(
                filename,
                call_match_wf_function,
                'fb_advanced_match_stats',
                match_url=download_paths,
                stat_type=stat_type,
                team_or_player='team',
            )
            team_stats[stat_type] = df

        return {
            # 'match_results': match_results,
            'match_reports': match_reports,
            'match_summaries': match_summaries,
            'lineups': lineups,
            'player_stats': player_stats,
            'team_stats': team_stats
        }

    # Method to get player stats
    def get_player_stats(self):
        # File names
        team_urls_filename = self._construct_filename('season_team_stats', 'urls')
        player_urls_filename = self._construct_filename('season_player_stats', 'urls')
        scouting_report_filename = self._construct_filename('player_stats',
                                                            'scouting_report')
        wages_filename = self._construct_filename('player_stats',
                                                  'wages')
        league_urls = call_wf_function('fb_league_urls', country=self.country, gender=self.gender, season_end_year=self.season, tier=self.tier)
        # Load or fetch team URLs
        team_urls = call_wf_function('fb_teams_urls', league_url=str(league_urls[0]))

        if not player_urls_filename.exists():
            player_urls = []
            for team_url in team_urls:
                player_urls.extend(call_wf_function('fb_player_urls', team_url=str(team_url)))
            pd.DataFrame({"url": player_urls}).to_csv(self.data_dir / player_urls_filename, index=False)
        else:
            player_urls_df = pd.read_csv(self.data_dir / player_urls_filename)
            player_urls = player_urls_df[
                'Player_URL'].tolist() if 'Player_URL' in player_urls_df.columns else \
                player_urls_df['url'].tolist()

        # Load or fetch scouting reports
        scouting_report = self._load_or_fetch_data(
            scouting_report_filename,
            call_wf_function,
            'fb_player_scouting_report',
            player_url=[str(x) for x in player_urls],
            pos_versus='primary',
            time_pause=3
        )

        # Load or fetch wages
        wages = self._load_or_fetch_data(
            wages_filename,
            call_wf_function,
            'fb_squad_wages',
            team_urls=team_urls
        )

        return {
            'scouting_report': scouting_report,
            'wages': wages
        }


def _maybe_download_file(url,data_dir, filename=None, time_pause=3):
    if filename is None:
        filename = Path(data_dir) / Path(url).name
    if not filename.exists():
        filename.parent.mkdir(parents=True, exist_ok=True)
        try:
            LOGGER.info(f"Downloading {url}")
            urllib.request.urlretrieve(url, filename)
            time.sleep(time_pause)
        except Exception as e:
            LOGGER.error(f"Failed to download {url} with error {e}")
    else:
        LOGGER.debug(f"{filename} already exists, skipping download")


def _maybe_split_csv(data_path, data_dir, file_name):
    if not data_path.exists():
        LOGGER.warning(f"{data_path} does not exist, skipping split")
        return
    df = pd.read_csv(data_path)
    for key, gdf in df.groupby(["Country", "Tier", "Season_End_Year"]):
        country, tier, year = key
        o = data_dir / country / tier / str(year) / "match_stats"
        full_path = o / f"{file_name}.csv"
        if not full_path.exists():
            full_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.debug(f"Writing {full_path}")
        gdf.to_csv(full_path, index=False)

if __name__ == "__main__":
    data_dir = Path("data/raw/fbref")
    country = "GER"
    season = 2018
    gender = "M"
    tier = "1st"
    loader = FootballDataLoader(country=country, tier=tier,season=season, gender=gender, data_dir=data_dir)
    loader.get_match_stats()
