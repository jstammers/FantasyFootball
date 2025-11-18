library(worldfootballR)

data_dir = "data/raw/fbref"
country <- "ENG"
tier <- "1st"
season_end_year <- 2017
gender <- "M"

create_path <- function(data_dir, country, tier, season) {
    path <- file.path(data_dir, country, tier, as.character(season))
    if (!dir.exists(path)) {
        dir.create(path, recursive = TRUE)
    }
    return(path)
}

lineup_path <- file.path(create_path(data_dir, country, tier, season_end_year), "match_stats/lineups.csv")
if (!file.exists(lineup_path)) {
    print(paste("Backfilling lineups for", country, tier, season_end_year))
    match_urls <- worldfootballR::fb_match_urls(country, gender,  season_end_year, tier, time_pause = 4)
    print(paste("Found", length(match_urls), "matches"))
    lineups <- worldfootballR::fb_match_lineups(match_urls, time_pause = 4)
    if (!dir.exists(dirname(lineup_path))) {
        dir.create(dirname(lineup_path), recursive = TRUE)
    }
    write.csv(lineups, lineup_path)
}
