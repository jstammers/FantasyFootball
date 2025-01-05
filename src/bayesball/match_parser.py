import os.path

import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import re
from tqdm import tqdm
from datetime import datetime


def load_page(url):
    if os.path.exists(url):
        with open(url, "r") as f:
            return f.read()
    else:
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"Error loading {url}: {e}")
            return None


def fb_parse_match(match_url, stat_types=None, shooting=True):
    if stat_types is None:
        stat_types = [
            "summary",
            "passing",
            "passing_types",
            "defense",
            "possession",
            "misc",
            "keeper",
        ]

    # Load the match page
    content = load_page(match_url)
    if content is None:
        print(f"Match page not available for {match_url}")
        return None

    soup = BeautifulSoup(content, "html.parser")

    # Initialize data dictionaries
    advanced_stats = fb_get_all_advanced_match_stats(soup, stat_types)
    shooting_data = fb_get_match_shooting_data(soup) if shooting else None
    lineups = fb_get_match_lineups(soup)
    match_summary = fb_get_match_summary(soup)

    return {
        "advanced_stats": advanced_stats,
        "shooting_data": shooting_data,
        "lineups": lineups,
        "match_summary": match_summary,
    }


def fb_get_all_advanced_match_stats(soup, stat_types):
    team_or_player_options = [
        "summary",
        "keeper",
        "keeper_adv",
        "passing",
        "passing_types",
        "defense",
        "possession",
        "misc",
        "gca",
        "shooting",
        "playing_time",
    ]
    all_stats = {}

    for stat_type in stat_types:
        for team_or_player in ["home", "away"]:
            stat_df_output = fb_get_advanced_match_stats(
                soup, stat_type, team_or_player
            )
            df_name = f"{stat_type}_{team_or_player}"
            if stat_df_output is not None and not stat_df_output.empty:
                all_stats[df_name] = stat_df_output

    return all_stats


def fb_get_advanced_match_stats(soup, stat_type, team_or_player):
    # Map stat types to table IDs
    table_id = f"stats_{team_or_player}_{stat_type}"
    table = soup.find("table", {"id": table_id})

    if table is None:
        # Sometimes tables are in comments
        comment_soup = parse_html_comment(soup, table_id)
        if comment_soup:
            table = comment_soup.find("table", {"id": table_id})
        if table is None:
            print(f"NOTE: Stat Type '{stat_type}' is not found for this match.")
            return None

    # Parse the table into a DataFrame
    df = pd.read_html(str(table))[0]
    df = fb_clean_stats_df(df)

    return df


def parse_html_comment(soup, comment_id):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if comment_id in comment:
            comment_soup = BeautifulSoup(comment, "html.parser")
            return comment_soup
    return None


def fb_clean_stats_df(df):
    # Remove multi-level column headers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join(col).strip() for col in df.columns.values]
    else:
        df.columns = [col.strip() for col in df.columns]

    # Drop unnecessary columns
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]

    return df.reset_index(drop=True)


def fb_get_match_shooting_data(soup):
    shots_div = soup.find("div", {"id": "all_shots"})
    if shots_div is None:
        print("Detailed shot data unavailable for this match.")
        return pd.DataFrame()

    shots_soup = parse_html_comment(soup, "shots")
    if shots_soup:
        shots_tables = shots_soup.find_all("table")
    else:
        print("No shots tables found.")
        return pd.DataFrame()

    if len(shots_tables) < 2:
        print("Insufficient shots data.")
        return pd.DataFrame()

    # Parse home and away shots tables
    home_shots_df = pd.read_html(str(shots_tables[0]))[0]
    away_shots_df = pd.read_html(str(shots_tables[1]))[0]

    # Get team names
    teams = soup.find_all("div", {"itemprop": "performer"})
    if len(teams) < 2:
        print("Cannot find team names.")
        return pd.DataFrame()
    home_team = teams[0].text.strip()
    away_team = teams[1].text.strip()

    home_shots_df["Squad"] = home_team
    away_shots_df["Squad"] = away_team
    home_shots_df["Home_Away"] = "Home"
    away_shots_df["Home_Away"] = "Away"

    # Combine dataframes
    all_shots_df = pd.concat([home_shots_df, away_shots_df], ignore_index=True)

    return all_shots_df


def fb_get_match_lineups(soup):
    lineup_divs = soup.find_all("div", {"class": "lineup"})

    if not lineup_divs:
        print("Lineups not available for this match.")
        return pd.DataFrame()

    all_lineups = []

    for lineup_div in lineup_divs:
        table = lineup_div.find("table")
        if table:
            df = pd.read_html(str(table))[0]
            df = fb_clean_lineup_df(df)
            all_lineups.append(df)

    if not all_lineups:
        print("No lineups data found.")
        return pd.DataFrame()

    all_lineups_df = pd.concat(all_lineups, ignore_index=True)
    return all_lineups_df


def fb_clean_lineup_df(df):
    # Standardize column names
    df.columns = ["Player_Num", "Player_Name"]
    df["Player_Num"] = df["Player_Num"].astype(str).str.strip()
    df["Player_Name"] = df["Player_Name"].str.strip()
    return df


def fb_get_match_summary(soup):
    events_wrap = soup.find("div", {"id": "events_wrap"})

    if events_wrap is None:
        print("Match Summary not available for this match.")
        return pd.DataFrame()

    event_rows = events_wrap.find_all("div", {"class": re.compile("event.*")})

    events_data = []
    for event in event_rows:
        minute_div = event.find("div", {"class": "minute"})
        if not minute_div:
            continue
        event_time = minute_div.text.strip()

        event_icon = event.find("div", {"class": "event-icon"})
        event_type = " ".join(event_icon.get("class", [])) if event_icon else ""

        player_div = event.find("div", {"class": "player"})
        event_player = player_div.text.strip() if player_div else ""

        team_class = event.get("class", [])
        home_or_away = "Home" if "a" in team_class else "Away"

        events_data.append(
            {
                "Event_Time": event_time,
                "Event_Type": event_type,
                "Event_Player": event_player,
                "Home_Away": home_or_away,
            }
        )

    events_df = pd.DataFrame(events_data)

    return events_df


def fb_parse_match_data(match_urls, stat_types=None, shooting=True):
    if stat_types is None:
        stat_types = [
            "summary",
            "passing",
            "passing_types",
            "defense",
            "possession",
            "misc",
            "keeper",
        ]

    combined_shooting_data = []
    combined_lineups = []
    combined_match_summary = []
    combined_advanced_stats = {}

    for match_url in tqdm(match_urls, desc="Processing matches"):
        match_data = fb_parse_match(match_url, stat_types=stat_types, shooting=shooting)
        if match_data is None:
            print(f"Skipping match {match_url} due to errors.")
            continue

        # Shooting data
        if (
            match_data["shooting_data"] is not None
            and not match_data["shooting_data"].empty
        ):
            match_data["shooting_data"]["MatchURL"] = match_url
            combined_shooting_data.append(match_data["shooting_data"])

        # Lineups
        if match_data["lineups"] is not None and not match_data["lineups"].empty:
            match_data["lineups"]["MatchURL"] = match_url
            combined_lineups.append(match_data["lineups"])

        # Match summary
        if (
            match_data["match_summary"] is not None
            and not match_data["match_summary"].empty
        ):
            match_data["match_summary"]["MatchURL"] = match_url
            combined_match_summary.append(match_data["match_summary"])

        # Advanced stats
        for stat_name, stat_df in match_data["advanced_stats"].items():
            if stat_name not in combined_advanced_stats:
                combined_advanced_stats[stat_name] = []
            if stat_df is not None and not stat_df.empty:
                stat_df["MatchURL"] = match_url
                combined_advanced_stats[stat_name].append(stat_df)

    # Combine dataframes
    shooting_data_df = (
        pd.concat(combined_shooting_data, ignore_index=True)
        if combined_shooting_data
        else pd.DataFrame()
    )
    lineups_df = (
        pd.concat(combined_lineups, ignore_index=True)
        if combined_lineups
        else pd.DataFrame()
    )
    match_summary_df = (
        pd.concat(combined_match_summary, ignore_index=True)
        if combined_match_summary
        else pd.DataFrame()
    )

    # Combine advanced stats
    advanced_stats_combined = {}
    for stat_name, df_list in combined_advanced_stats.items():
        advanced_stats_combined[stat_name] = pd.concat(df_list, ignore_index=True)

    return {
        "advanced_stats": advanced_stats_combined,
        "shooting_data": shooting_data_df,
        "lineups": lineups_df,
        "match_summary": match_summary_df,
    }


if __name__ == "__main__":
    # Example usage
    match_urls = [
        "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2018/Arsenal-Bournemouth-September-9-2017-Premier-League.html",
        "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2023/Arsenal-Crystal-Palace-March-19-2023-Premier-League.html",
        "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2023/Arsenal-Newcastle-United-January-3-2023-Premier-League.html",
    ]
    # Call the function
    all_match_data = fb_parse_match_data(
        match_urls, shooting=False, stat_types=["summary"]
    )
    print(all_match_data["match_summary"])
