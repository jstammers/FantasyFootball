# Install required package (if not installed)
devtools::install_github("JaseZiv/worldfootballR")

# Load required libraries
library(worldfootballR)
library(tidyverse)

# Define data directories
data_dir <- "data/FBRef"

# Define stat types
season_player_stats <- c("standard", "shooting", "passing", "passing_types", "gca", "defense", "possession", "playing_time", "misc", "keepers", "keepers_adv")
season_team_stats <- c("league_table", "league_table_home_away", "standard", "keeper", "keeper_adv", "shooting", "passing", "passing_types", "goal_shot_creation", "defense", "possession", "misc", "playing_time")
team_match_stats <- c("shooting", "keeper", "passing", "passing_types", "gca", "defense", "misc")

# Utility function to create directories if they don't exist
create_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }
}

# Function to download and save league season data
download_league_season_data <- function(country, gender, season_end_year, tier, quick_scrape = FALSE) {
  root <- file.path(data_dir, "raw", "fbref")
  create_dir(root)

  scrape_fn <- if (quick_scrape) load_fb_big5_advanced_season_stats else fb_big5_advanced_season_stats

  for (stat_type in season_player_stats) {
    team_file <- file.path(root, 'team_stats', country, season_end_year, paste0(stat_type,'.csv'))
    player_file <- file.path(root, 'player_stats', country, season_end_year,  paste0(stat_type,'.csv'))

    create_dir(dirname(team_file))
    create_dir(dirname(player_file))

    if (!file.exists(team_file)) {
      team_stats <- scrape_fn(season_end_year = season_end_year, stat_type = stat_type, team_or_player = "team")
      write.csv(team_stats, team_file, row.names = FALSE)
    }

    if (!file.exists(player_file)) {
      player_stats <- scrape_fn(season_end_year = season_end_year, stat_type = stat_type, team_or_player = "player")
      write.csv(player_stats, player_file, row.names = FALSE)
    }
  }
}

# Function to download and save match-level data
download_match_level_data <- function(country, gender, season_end_year, tier, load_match_stats = TRUE) {
  root <- file.path(data_dir, "raw", "fbref")
  create_dir(root)

  match_results_file <- file.path(root, 'match_results', country, paste0(season_end_year,'.csv'))
  create_dir(dirname(match_results_file))

  if (!file.exists(match_results_file)) {
    match_results <- fb_match_results(season_end_year = season_end_year, country = country, tier = tier, gender = gender)
    write.csv(match_results, match_results_file, row.names = FALSE)
  }

  if (load_match_stats) {
    for (stat in team_match_stats) {
      team_file <- file.path(root, 'team_match_stats', country, season_end_year, paste0(stat_type,'.csv'))
      player_file <- file.path(root, 'player_match_stats', country, season_end_year,  paste0(stat_type,'.csv'))

      create_dir(dirname(team_file))
      create_dir(dirname(player_file))

      if (!file.exists(team_file)) {
        match_team_stats <- fb_advanced_match_stats(country = country, gender = gender, tier = tier, stat_type = stat, team_or_player = "team")
        write.csv(match_team_stats, team_file, row.names = FALSE)
      }

      if (!file.exists(player_file)) {
        match_player_stats <- fb_advanced_match_stats(country = country, gender = gender, tier = tier, stat_type = stat, team_or_player = "player")
        write.csv(match_player_stats, player_file, row.names = FALSE)
      }
    }
  }
}

# Function to download and save team match log data
download_team_match_log_data <- function(country, gender, season_end_year, tier) {
  league_url <- fb_league_urls(country = country, gender = gender, season_end_year = season_end_year, tier = tier)
  team_urls <- fb_teams_urls(league_url = league_url)

  root <- file.path(data_dir, "raw", "fbref")
  create_dir(root)

  for (stat in team_match_stats) {
    match_log_file <- file.path(root, 'team_match_logs', country, season_end_year, paste0(stat_type,'.csv'))
    create_dir(dirname(match_log_file))

    if (!file.exists(match_log_file)) {
      match_log_stats <- fb_team_match_log_stats(team_urls = team_urls, stat_type = stat)
      write.csv(match_log_stats, match_log_file, row.names = FALSE)
    }
  }
}

# Function to download data for all leagues, tiers, and competitions
download_all_leagues_data <- function(gender, season_years, tiers) {
  countries <- fb_league_countries()
  for (country in countries) {
    for (tier in tiers) {
      for (year in season_years) {
        download_league_season_data(country, gender, year, tier, quick_scrape = FALSE)
        download_match_level_data(country, gender, year, tier, quick_scrape = FALSE, load_match_stats = TRUE)
        download_team_match_log_data(country, gender, year, tier)
      }
    }
  }
}

# Download data for all leagues, tiers, and competitions from 2010 to 2024
season_years <- 2010:2024
tiers <- c("1st", "2nd", "3rd", "4th", "5th")
download_all_leagues_data("M", season_years, tiers)

# Save all squad wages
countries <- fb_league_countries()
league_urls <- fb_league_urls(countries, "M", 2013:2024, tiers)
all_urls <- purrr::map(league_urls, fb_teams_urls)

# Combine the URLs into one vector
big5_urls_joined <- tibble(url = do.call(c, all_urls)) %>% mutate(team_name = stringr::str_extract(url, "[^/]+$"))
all_urls <- all_urls_joined %>% pull(url)
all_wages <- purrr::map(all_urls, possibly(.f = fb_squad_wages, otherwise = NULL), .progress = TRUE)

# Save all wages as RDS
saveRDS(all_wages, file.path(data_dir, "raw", "fbref", "all_wages.rds"))

# Combine non-null wage data frames and save
combined_wage_df <- bind_rows(purrr::compact(all_wages))
write_csv(combined_wage_df, file.path(data_dir, "raw", "fbref", "wages.csv"))
