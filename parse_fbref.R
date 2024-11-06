# devtools::install_github("JaseZiv/worldfootballR")

library(worldfootballR)
library(tidyverse)
season_player_stats <- c("standard",
                         "shooting",
                         "passing",
                         "passing_types",
                         "gca",
                         "defense",
                         "possession",
                         "playing_time", "misc", "keepers", "keepers_adv")
season_team_stats <- c("league_table", "league_table_home_away", "standard", "keeper", "keeper_adv", "shooting",
                       "passing", "passing_types", "goal_shot_creation", "defense", "possession", "misc", "misc_adv",
                       "playing_time", "possession", "misc")
team_match_stats <- c("shooting", "keeper", "passing", "passing_types", "gca", "defense", "misc")
player_match_stats <- c("summary", "keepers", "passing", "passing_types", "gca", "defense", "possession", "misc")

try_load <- function(load_fun, scrape_fun, ...) {
  tryCatch({
    load_fun(...)
  }, error = function(e) {
    message("Error: ", e)
    scrape_fun(...)
  })
}

# Get Big 5 league team stats
data_dir = "data/FBRef"
# League Season-Level Data
get_league_season_data <- function(country, gender, season_end_year, tier, quick_scrape = FALSE) {
  if (all(country == c("ENG", "FRA", "ITA", "GER", "ESP")) & (tier == "1st")) {
    if (quick_scrape) {
      scrape_fn <- load_fb_big5_advanced_season_stats
    } else {
        scrape_fn <- fb_big5_advanced_season_stats
        }
    root <- paste0("big5", "/", season_end_year, "_", gender)
    if (!dir.exists(file.path(data_dir, root))) {
      dir.create(file.path(data_dir, root))
    }
    for (stat_type in season_player_stats) {
      if (!file.exists(file.path(data_dir, paste0(root, "_", stat_type, "_team.csv")))) {
        team_stats <- scrape_fn(season_end_year = season_end_year, stat_type = stat_type,
                               team_or_player = "team")
        write.csv(team_stats, file.path(data_dir, paste0(root, "_", stat_type, "_team.csv"))) }
      if (!file.exists(file.path(data_dir, paste0(root, "_", stat_type, "_player.csv")))) {
        player_stats <- scrape_fn(season_end_year = season_end_year, stat_type = stat_type, team_or_player = "player")
        write.csv(player_stats, file.path(data_dir, paste0(root, "_", stat_type, "_player.csv")))
      }
    }
  } else {
    tmap <- c("1st" = "t1", "2nd" = "t2", "3rd" = "t3", "4th" = "t4", "5th" = "t5")
    root <- paste0(country, "/", tmap[[tier]], "_", season_end_year, "_", gender)
    if (!dir.exists(file.path(data_dir, root))) {
      dir.create(file.path(data_dir, root))
    }
    for (s in season_player_stats) {
      if (!file.exists(file.path(data_dir, paste0(root, "_", s, "_team.csv")))) {
        team_stats <- fb_league_stats(season_end_year = season_end_year, gender = gender, stat_type = s, team_or_player =
          "team", tier = tier, country = country)
        write.csv(team_stats, file.path(data_dir, paste0(root, "_", s, "_team.csv")))
        # wait 1 second
        Sys.sleep(3)
      }
      if (!file.exists(file.path(data_dir, paste0(root, "_", s, "_player.csv")))) {
        player_stats <- fb_league_stats(season_end_year = season_end_year, gender = gender, stat_type = s,
                                        team_or_player = "player", tier =
                                          tier, country = country)
        write.csv(player_stats, file.path(data_dir, paste0(root, "_", s, "_player.csv")))
        Sys.sleep(3)
      }
    }
  }
  closeAllConnections()
}

get_match_level_data <- function(country, gender, season_end_year, tier, quick_scrape = TRUE, load_match_stats = TRUE) {
  # league_url <-fb_league_urls(country = country,
  #                              gender = gender,
  #                              season_end_year = season_end_year,
  #                              tier = tier)
  tmap <- c("1st" = "t1", "2nd" = "t2", "3rd" = "t3", "4th" = "t4", "5th" = "t5")
  root <- paste0(country, "/", tmap[[tier]], "_", season_end_year, "_", gender)

  if (!(file.exists(file.path(data_dir, paste0(root, "_match_results.csv"))))) {
    if (quick_scrape) {
    match_results <- load_match_results(season_end_year = season_end_year, country = country, tier = tier, gender = gender)
    } else {
      match_results <- fb_match_results(season_end_year = season_end_year, country = country, tier = tier, gender = gender)
    }
    write.csv(match_results, file.path(data_dir, paste0(root, "_match_results.csv")))
    Sys.sleep(3)
  }
  if (load_match_stats){
  Sys.sleep(3)
  for (stat in team_match_stats) {
    if (!(file.exists(file.path(data_dir, paste0(root, "_", stat, "_match_stats_team.csv"))))){
    match_team_stats <- load_fb_advanced_match_stats(country = country, gender = gender, tier = tier, stat_type = stat, team_or_player = "team")
    write.csv(match_team_stats, file.path(data_dir, paste0(root, "_", stat, "_match_stats_team.csv")))
    Sys.sleep(3)}
    if (!(file.exists(file.path(data_dir, paste0(root, "_", stat, "_match_stats_player.csv"))))){
    match_player_stats <- load_fb_advanced_match_stats(country = country, gender = gender, tier = tier, stat_type = stat, team_or_player = "player")
    write.csv(match_player_stats, file.path(data_dir, paste0(root, "_", stat, "_match_stats_player.csv")))
    Sys.sleep(3)
    }
  }
  }
  closeAllConnections()
}

get_player_level_data <- function() { }

get_team_level_data <- function(country, gender, season_end_year, tier) {
  league_url <- fb_league_urls(country = country,
                               gender = gender,
                               season_end_year = season_end_year,
                               tier = tier)
  team_urls <- fb_teams_urls(league_url = league_url)

  tmap <- c("1st" = "t1", "2nd" = "t2", "3rd" = "t3", "4th" = "t4", "5th" = "t5")
  root <- paste0(country, "/", tmap[[tier]], "_", season_end_year, "_", gender)
  for (stat in team_match_stats) {
    if (!file.exists(file.path(data_dir, paste0(root, "_", stat, "_match_log.csv")))){
    match_log_stats <- fb_team_match_log_stats(team_urls = team_urls, stat_type = stat)
    write.csv(match_log_stats, file.path(data_dir, paste0(root, "_", stat, "_match_log.csv")))
    Sys.sleep(3)}
  }
  closeAllConnections()
}


big5 <- c("ENG", "FRA", "ITA", "GER", "ESP")
year <- 2007

for (year in  2010:2024) {
  get_league_season_data(big5, "M", year, "1st",quick_scrape = FALSE)
}


for (year in  1996:2024) {
  get_match_level_data("ENG", "M", year, "1st", quick_scrape = FALSE, load_match_stats = FALSE)
  get_team_level_data("ENG", "M", year, "1st")
}

league_urls <- fb_league_urls(big5, "M", 2013:2024, "1st")
big5_urls <- purrr::map(league_urls,fb_teams_urls)

# Combine the urls into one vector
big5_urls_joined <- tibble(url=do.call(c, big5_urls)) %>% mutate(team_name = stringr::str_extract(url, "[^/]+$"))


all_urls <- big5_urls_joined %>% pull(url)
all_wages <- purrr::map(all_urls, possibly(.f=fb_squad_wages, otherwise = NULL, ),.progress = TRUE)

# save all_wages as Rds
saveRDS(all_wages, "data/FBRef/all_wages.rds")

# find which ones are NULL
null_wages <- purrr::map_lgl(all_wages, is.data.frame)

last_if_numeric <- function(x) {
  if (is.numeric(unlist(x[1]))) {   # Checks if the first element of the list is numeric
    sapply(x, tail, n = 1)          # Extracts the last element of each list element
  } else {
    x                               # Returns the original if not numeric
  }
}
last_numeric_element <- function(x) {
  if (all(sapply(x, function(e) is.vector(e)))) {
    sapply(x, function(lst) last(lst))  # Extract the last element from each list
  } else {
    x  # Return the column unchanged if it's not a list of numeric vectors
  }
}
a <- function(x) { map_dbl(x %>% replace_na(list(0)), last)}
# Mutate columns that are lists of numbers to just contain the last element of each list
df_modified <- t %>%
  mutate(across(where(is.list),a))
# iterate through list and if row is a list of doubles, replace with the max
new_dfs <- c()
for (df in all_wages[null_wages]){
  df_modified <- df %>%
    mutate(across(where(is.list), a))
    new_dfs <- append(new_dfs, list(df_modified))
}
# combine to a single data frame
combined_wage_df <- bind_rows(new_dfs)
# replace zeros with a NULL
write_csv(combined_wage_df, "data/FBRef/big5/wages.csv")
all_wages[null_wages]
countries <- c("ENG", "FRA", "ITA", "GER", "ESP")
years <- 1996:2024
tiers <- c("1st", "2nd", "3rd", "4th", "5th")
stat_level <- c("season", "match", "team", "player")
stat_agg <- c("team", "player")
stats <- list(list(season_team_stats, season_player_stats), list(team_match_stats, player_match_stats), list(team_match_stats), list())
# create a dataframe that is a cartesian product of the above metrics
all_combos <- expand.grid(countries, years, tiers, stat_level, stat_agg) %>% mutate(stat = map2(.x = Var1, .y = Var2, ~stats[[.x]][[.y]]))

for (tier in c("2nd", "3rd", "4th", "5th")) {
  for (year in 2010:2024) {
    # get_league_season_data("ENG", "M", year, tier, quick_scrape = FALSE)
    get_match_level_data("ENG", "M", year, tier, quick_scrape = FALSE, load_match_stats = FALSE)
    get_team_level_data("ENG", "M", year, tier)
  }
}


match_results <- fb_match_results(season_end_year = 1995:2024, country = big5, tier = "1st", gender = "M")

write.csv(match_results, "data/FBRef/big5/match_results.csv")

for (tier in c("2nd","3rd", "4th", "5th")){
  match_results <- fb_match_results(season_end_year = 1995:2024, country = "ENG", tier = tier, gender = "M")
  write.csv(match_results, file.path(data_dir, paste0("ENG/", tier, "_match_results.csv")))
}

match_results <- fb_match_results(season_end_year = 1995:2024, country = "ENG", tier = "2nd", gender = "M")


league_tables <- fb_