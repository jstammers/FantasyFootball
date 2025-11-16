"""Python-based scraper for FBRef using BeautifulSoup to replace worldfootballR"""

import time
import logging
from typing import Optional, List, Dict
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import polars as pl

LOGGER = logging.getLogger(__name__)

# Base URL for FBRef
FBREF_BASE_URL = "https://fbref.com"

# Country codes mapping
COUNTRY_CODES = {
    "ENG": {"name": "Premier-League", "comp_id": "9"},
    "ESP": {"name": "La-Liga", "comp_id": "12"},
    "ITA": {"name": "Serie-A", "comp_id": "11"},
    "GER": {"name": "Bundesliga", "comp_id": "20"},
    "FRA": {"name": "Ligue-1", "comp_id": "13"},
}

# Tier codes
TIER_CODES = {
    "1st": "1",
    "2nd": "2",
}


def get_league_url(country: str, gender: str, season_end_year: int, tier: str) -> str:
    """Construct the FBRef league URL"""
    if country not in COUNTRY_CODES:
        raise ValueError(f"Country {country} not supported")
    
    comp_info = COUNTRY_CODES[country]
    comp_id = comp_info["comp_id"]
    comp_name = comp_info["name"]
    
    # FBRef uses season format like 2023-2024
    season_str = f"{season_end_year - 1}-{season_end_year}"
    
    # Basic URL structure for stats
    url = f"{FBREF_BASE_URL}/en/comps/{comp_id}/{season_str}/{season_str}-{comp_name}-Stats"
    
    return url


def fetch_page(url: str, time_pause: float = 3.0) -> BeautifulSoup:
    """Fetch and parse a webpage with rate limiting"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    LOGGER.info(f"Fetching {url}")
    time.sleep(time_pause)  # Rate limiting
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        return soup
    except Exception as e:
        LOGGER.error(f"Error fetching {url}: {e}")
        raise


def parse_table_to_polars(table) -> pl.DataFrame:
    """Parse an HTML table to a Polars DataFrame"""
    if table is None:
        return pl.DataFrame()
    
    # Get headers
    headers = []
    header_row = table.find('thead')
    if header_row:
        # Handle multi-level headers by taking the last row
        header_rows = header_row.find_all('tr')
        if header_rows:
            last_header_row = header_rows[-1]
            for th in last_header_row.find_all(['th', 'td']):
                col_name = th.get('data-stat', th.text.strip())
                if col_name:
                    headers.append(col_name)
    
    # Get data rows
    rows = []
    tbody = table.find('tbody')
    if tbody:
        for tr in tbody.find_all('tr'):
            # Skip header rows within tbody (rows with class 'thead' or containing th with class 'thead')
            if 'thead' in tr.get('class', []) or tr.find('th', class_='thead'):
                continue
            
            row_data = []
            for td in tr.find_all(['th', 'td']):
                # Use data-stat attribute value if available
                value = td.get('data-stat')
                if value:
                    row_data.append(td.text.strip())
                else:
                    row_data.append(td.text.strip())
            
            if row_data:
                rows.append(row_data)
    
    # Create DataFrame
    if not rows or not headers:
        return pl.DataFrame()
    
    # Ensure all rows have same length as headers
    cleaned_rows = []
    for row in rows:
        if len(row) == len(headers):
            cleaned_rows.append(row)
        elif len(row) < len(headers):
            # Pad with empty strings
            cleaned_rows.append(row + [''] * (len(headers) - len(row)))
        else:
            # Truncate
            cleaned_rows.append(row[:len(headers)])
    
    if not cleaned_rows:
        return pl.DataFrame()
    
    # Create dictionary for DataFrame
    data_dict = {header: [row[i] for row in cleaned_rows] for i, header in enumerate(headers)}
    
    return pl.DataFrame(data_dict)


def fb_league_stats(
    country: str,
    gender: str,
    season_end_year: int,
    tier: str,
    stat_type: str,
    team_or_player: str
) -> pl.DataFrame:
    """
    Scrape league season stats from FBRef
    
    Args:
        country: Country code (e.g., "ENG", "ESP")
        gender: Gender ("M" or "F")
        season_end_year: Ending year of season (e.g., 2024 for 2023-2024)
        tier: League tier ("1st", "2nd", etc.)
        stat_type: Type of stats (e.g., "standard", "shooting", "passing")
        team_or_player: "team" or "player"
    
    Returns:
        Polars DataFrame with the requested stats
    """
    # Get base league URL
    base_url = get_league_url(country, gender, season_end_year, tier)
    
    # Modify URL based on stat type and team_or_player
    stat_type_map = {
        "standard": "stats",
        "shooting": "shooting",
        "passing": "passing",
        "passing_types": "passing_types",
        "gca": "gca",
        "defense": "defense",
        "possession": "possession",
        "playing_time": "playingtime",
        "misc": "misc",
        "keepers": "keepers",
        "keepers_adv": "keepersadv",
    }
    
    url_stat = stat_type_map.get(stat_type, stat_type)
    season_str = f"{season_end_year - 1}-{season_end_year}"
    comp_info = COUNTRY_CODES[country]
    comp_name = comp_info["name"]
    comp_id = comp_info["comp_id"]
    
    url = f"{FBREF_BASE_URL}/en/comps/{comp_id}/{season_str}/{url_stat}/{season_str}-{comp_name}-Stats"
    
    # Fetch the page
    soup = fetch_page(url)
    
    # Find the appropriate table
    # For player stats, look for tables with id containing 'stats_standard' or similar
    # For team stats, look for tables with id containing 'stats_squads'
    
    if team_or_player == "player":
        table_id_patterns = [f"stats_standard_{stat_type}", f"stats_{stat_type}", "stats_standard"]
    else:
        table_id_patterns = [f"stats_squads_{stat_type}", f"stats_{stat_type}_squads", "stats_squads_standard"]
    
    table = None
    for pattern in table_id_patterns:
        table = soup.find('table', {'id': pattern})
        if table:
            break
    
    # If not found by id, try to find by looking at all tables
    if not table:
        tables = soup.find_all('table')
        if tables:
            # Use first table as fallback
            table = tables[0]
    
    if not table:
        LOGGER.warning(f"No table found for {country} {season_end_year} {stat_type} {team_or_player}")
        return pl.DataFrame()
    
    # Parse table to DataFrame
    df = parse_table_to_polars(table)
    
    # Add metadata columns
    if not df.is_empty():
        df = df.with_columns([
            pl.lit(season_end_year).alias("Season_End_Year"),
            pl.lit(country).alias("Country"),
            pl.lit(tier).alias("Tier"),
            pl.lit(gender).alias("Gender"),
        ])
    
    return df


def fb_league_urls(
    country: str,
    gender: str,
    season_end_year: int,
    tier: str
) -> List[str]:
    """Get league URLs for a given country, gender, season, and tier"""
    url = get_league_url(country, gender, season_end_year, tier)
    return [url]


def fb_teams_urls(league_url: str) -> List[str]:
    """Get team URLs from a league page"""
    soup = fetch_page(league_url)
    
    team_urls = []
    
    # Find the league table or squad stats table
    tables = soup.find_all('table')
    
    for table in tables:
        # Look for links to team pages in the table
        for row in table.find_all('tr'):
            for cell in row.find_all(['th', 'td']):
                link = cell.find('a', href=lambda x: x and '/squads/' in x)
                if link:
                    team_url = urljoin(FBREF_BASE_URL, link['href'])
                    if team_url not in team_urls:
                        team_urls.append(team_url)
    
    return team_urls


def fb_squad_wages(team_urls: List[str]) -> pl.DataFrame:
    """
    Scrape squad wages from team pages
    
    Args:
        team_urls: List of team URLs
    
    Returns:
        Polars DataFrame with wages data
    """
    all_wages = []
    
    for team_url in team_urls:
        try:
            soup = fetch_page(team_url)
            
            # Find wages table - usually has id 'wages' or similar
            wages_table = soup.find('table', {'id': lambda x: x and 'wages' in x.lower()})
            
            if wages_table:
                df = parse_table_to_polars(wages_table)
                if not df.is_empty():
                    # Add team URL as metadata
                    df = df.with_columns([
                        pl.lit(team_url).alias("Team_URL")
                    ])
                    all_wages.append(df)
        except Exception as e:
            LOGGER.error(f"Error scraping wages from {team_url}: {e}")
            continue
    
    if all_wages:
        return pl.concat(all_wages, how="diagonal")
    
    return pl.DataFrame()


def parse_match_html(html_path: str, stat_types: List[str], shooting: bool = True) -> Dict:
    """
    Parse match HTML file to extract match statistics
    
    Args:
        html_path: Path to HTML file
        stat_types: List of stat types to extract
        shooting: Whether to extract shooting data
    
    Returns:
        Dictionary containing team_stats, player_stats, lineups, match_summaries, shooting_data
    """
    with open(html_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'lxml')
    
    result = {
        'team_stats': {},
        'player_stats': {},
        'lineups': pl.DataFrame(),
        'match_summary': pl.DataFrame(),
        'shooting_data': pl.DataFrame()
    }
    
    # Extract match summary from scorebox
    scorebox = soup.find('div', class_='scorebox')
    if scorebox:
        # Extract basic match info
        teams = scorebox.find_all('div', class_='team')
        if len(teams) >= 2:
            home_team = teams[0].find('a').text if teams[0].find('a') else ''
            away_team = teams[1].find('a').text if teams[1].find('a') else ''
            
            scores = scorebox.find_all('div', class_='score')
            home_score = scores[0].text if len(scores) > 0 else '0'
            away_score = scores[1].text if len(scores) > 1 else '0'
            
            # Get date
            date_elem = scorebox.find('span', class_='venuetime')
            match_date = date_elem.get('data-venue-date', '') if date_elem else ''
            
            result['match_summary'] = pl.DataFrame({
                'Home': [home_team],
                'Away': [away_team],
                'Home_Score': [home_score],
                'Away_Score': [away_score],
                'Match_Date': [match_date],
                'MatchURL': [html_path]
            })
    
    # Extract stats tables
    tables = soup.find_all('table')
    
    for stat_type in stat_types:
        # Look for player and team stats tables
        for table in tables:
            table_id = table.get('id', '')
            
            # Check if this is a stats table for the requested type
            if stat_type in table_id.lower():
                df = parse_table_to_polars(table)
                
                if 'player' in table_id.lower() or 'summary' in table_id.lower():
                    result['player_stats'][stat_type] = df
                elif 'team' in table_id.lower():
                    result['team_stats'][stat_type] = df
    
    # Extract shooting data if requested
    if shooting:
        for table in tables:
            table_id = table.get('id', '')
            if 'shots' in table_id.lower() or 'shooting' in table_id.lower():
                result['shooting_data'] = parse_table_to_polars(table)
                break
    
    # Extract lineups
    lineup_tables = soup.find_all('table', class_=lambda x: x and 'lineup' in x.lower())
    if not lineup_tables:
        # Alternative: look for div with lineups
        lineup_divs = soup.find_all('div', class_='lineup')
        if lineup_divs and len(lineup_divs) >= 2:
            # Parse lineup information
            lineup_data = []
            for lineup_div in lineup_divs:
                team_name = lineup_div.find('th')
                if team_name:
                    team_name = team_name.text.strip()
                
                players = lineup_div.find_all('a', href=lambda x: x and '/players/' in x)
                for player in players:
                    lineup_data.append({
                        'Team': team_name,
                        'Player': player.text.strip(),
                        'Player_URL': urljoin(FBREF_BASE_URL, player['href'])
                    })
            
            if lineup_data:
                result['lineups'] = pl.DataFrame(lineup_data)
    
    return result


def fb_parse_match_data(
    html_paths: List[str],
    stat_types: List[str],
    shooting: bool = True
) -> tuple:
    """
    Parse multiple match HTML files
    
    Args:
        html_paths: List of paths to HTML files
        stat_types: List of stat types to extract
        shooting: Whether to extract shooting data
    
    Returns:
        Tuple containing (match_data, shooting_data, lineups, match_summaries)
        where match_data is a list of [team_stats, player_stats] pairs
    """
    all_team_stats = {st: [] for st in stat_types}
    all_player_stats = {st: [] for st in stat_types}
    all_shooting = []
    all_lineups = []
    all_summaries = []
    
    for html_path in html_paths:
        try:
            result = parse_match_html(html_path, stat_types, shooting)
            
            # Collect team stats
            for stat_type in stat_types:
                if stat_type in result['team_stats'] and not result['team_stats'][stat_type].is_empty():
                    all_team_stats[stat_type].append(result['team_stats'][stat_type])
            
            # Collect player stats
            for stat_type in stat_types:
                if stat_type in result['player_stats'] and not result['player_stats'][stat_type].is_empty():
                    all_player_stats[stat_type].append(result['player_stats'][stat_type])
            
            # Collect shooting data
            if not result['shooting_data'].is_empty():
                all_shooting.append(result['shooting_data'])
            
            # Collect lineups
            if not result['lineups'].is_empty():
                all_lineups.append(result['lineups'])
            
            # Collect summaries
            if not result['match_summary'].is_empty():
                all_summaries.append(result['match_summary'])
                
        except Exception as e:
            LOGGER.error(f"Error parsing {html_path}: {e}")
            continue
    
    # Concatenate all data
    match_data = []
    for stat_type in stat_types:
        team_df = pl.concat(all_team_stats[stat_type], how="diagonal") if all_team_stats[stat_type] else pl.DataFrame()
        player_df = pl.concat(all_player_stats[stat_type], how="diagonal") if all_player_stats[stat_type] else pl.DataFrame()
        match_data.append([team_df, player_df])
    
    shooting_data = pl.concat(all_shooting, how="diagonal") if all_shooting else pl.DataFrame()
    lineups = pl.concat(all_lineups, how="diagonal") if all_lineups else pl.DataFrame()
    match_summaries = pl.concat(all_summaries, how="diagonal") if all_summaries else pl.DataFrame()
    
    # Return in format compatible with original R function
    # Original returns: [[team_stats, player_stats] for each stat_type], shooting_data, lineups, match_summaries
    return (match_data, shooting_data, lineups, match_summaries)
