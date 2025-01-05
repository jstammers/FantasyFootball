library(magrittr)
library(rvest)
library(dplyr)
library(glue)
library(progress)
library(purrr)

# Function to load the page once
load_page <- function(path) {
  tryCatch(
    xml2::read_html(path),
    error = function(e) {
      message(glue::glue("Error loading {path}"))
      return(NA)
    }
  )
}

# Main function to parse all data
fb_parse_match <- function(match_url, stat_types = c("summary", "passing", "passing_types", "defense", "possession", "misc", "keeper"), shooting = TRUE) {
  # Load the page once
  match_page <- load_page(match_url)

  if (is.na(match_page)) {
    message(glue::glue("Match page not available for {match_url}"))
    return(NULL)
  }

  # Extract match report (used in multiple functions)
  match_report <- worldfootballR:::.get_match_report_page(match_page)

  # Initialize progress bar
  pb <- progress::progress_bar$new(
    format = "  Parsing data [:bar] :percent eta: :eta",
    total = 4, clear = FALSE, width = 60
  )

  # Extract advanced match stats for all stat types and both team and player
  pb$tick()
  advanced_stats <- fb_get_all_advanced_match_stats(match_page, match_report, stat_types)

  # Extract shooting data
  pb$tick()
  if (shooting){
    shooting_data <- fb_get_match_shooting_data(match_page)
  } else {
    shooting_data <- NULL
  }

  # Extract lineups
  pb$tick()
  lineups <- fb_get_match_lineups(match_page)

  # Extract match summary
  pb$tick()
  match_summary <- fb_get_match_summary(match_page, match_report)

  return(list(
    advanced_stats = advanced_stats,
    shooting_data = shooting_data,
    lineups = lineups,
    match_summary = match_summary
  ))
}

# Function to extract all advanced match stats for both team and player
fb_get_all_advanced_match_stats <- function(match_page, match_report, stat_types = c("summary", "passing", "passing_types", "defense", "possession", "misc", "keeper")) {
  # Define all stat types
  # stat_types <- c("summary", "passing", "passing_types", "defense", "possession", "misc", "keeper")
  team_or_player_options <- c("team", "player")

  # Initialize a list to store all stats
  all_stats <- list()

  # Extract all tables from the page
  all_tables <- match_page %>%
    rvest::html_nodes(".table_container")

  # Loop over each stat type and team/player option
  for (stat_type in stat_types) {
    for (team_or_player in team_or_player_options) {
      # Extract the stats
      stat_df_output <- fb_get_advanced_match_stats(
        match_page = match_page,
        match_report = match_report,
        all_tables = all_tables,
        stat_type = stat_type,
        team_or_player = team_or_player
      )

      # Create a name for the data frame
      df_name <- paste0(stat_type, "_", team_or_player)

      # Store in the list if data is available
      if (!is.null(stat_df_output) && nrow(stat_df_output) > 0) {
        all_stats[[df_name]] <- stat_df_output
      }
    }
  }

  return(all_stats)
}

# Modified function to extract advanced match stats for a given stat type and team/player
fb_get_advanced_match_stats <- function(match_page, match_report, all_tables, stat_type, team_or_player) {
  # Select the correct table based on stat_type
  stat_df <- switch(
    stat_type,
    "summary" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "summary$"))] %>% rvest::html_nodes("table"),
    "passing" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "passing$"))] %>% rvest::html_nodes("table"),
    "passing_types" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "passing_types"))] %>% rvest::html_nodes("table"),
    "defense" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "defense$"))] %>% rvest::html_nodes("table"),
    "possession" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "possession$"))] %>% rvest::html_nodes("table"),
    "misc" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "misc$"))] %>% rvest::html_nodes("table"),
    "keeper" = all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "keeper_stats"))] %>% rvest::html_nodes("table"),
    stop(glue::glue("Stat type '{stat_type}' not recognized"))
  )

  if(length(stat_df) == 0) {
    message(glue::glue("NOTE: Stat Type '{stat_type}' is not found for this match."))
    return(NULL)
  }

  # Extract home and away stats
  home_stat <- worldfootballR:::.extract_team_players(
    match_page = match_page,
    xml_elements = stat_df,
    team_idx = 1,
    home_away = "Home"
  )
  away_stat <- worldfootballR:::.extract_team_players(
    match_page = match_page,
    xml_elements = stat_df,
    team_idx = 2,
    home_away = "Away"
  )
  stat_df_output <- dplyr::bind_rows(home_stat, away_stat)

  # Filter based on team_or_player
  if(any(grepl("Nation", colnames(stat_df_output)))) {
    if(!stat_type %in% c("keeper", "shots")) {
      if(team_or_player == "team") {
        stat_df_output <- stat_df_output %>%
          dplyr::filter(stringr::str_detect(.data[["Player"]], " Players")) %>%
          dplyr::select(-.data[["Player"]], -.data[["Player_Num"]], -.data[["Nation"]], -.data[["Pos"]], -.data[["Age"]])
      } else {
        stat_df_output <- stat_df_output %>%
          dplyr::filter(!stringr::str_detect(.data[["Player"]], " Players"))
      }
    }
  } else {
    if(!stat_type %in% c("keeper", "shots")) {
      if(team_or_player == "team") {
        stat_df_output <- stat_df_output %>%
          dplyr::filter(stringr::str_detect(.data[["Player"]], " Players")) %>%
          dplyr::select(-.data[["Player"]], -.data[["Player_Num"]], -.data[["Pos"]], -.data[["Age"]])
      } else {
        stat_df_output <- stat_df_output %>%
          dplyr::filter(!stringr::str_detect(.data[["Player"]], " Players"))
      }
    }
  }

  # Bind match report data
  stat_df_output <- dplyr::bind_cols(match_report, stat_df_output)

  return(stat_df_output)
}

# Modified function to extract shooting data
fb_get_match_shooting_data <- function(match_page) {
  match_date <- match_page %>% rvest::html_nodes(".venuetime") %>% rvest::html_attr("data-venue-date")

  all_shots <- match_page %>% rvest::html_nodes("#switcher_shots") %>% rvest::html_nodes("div")

  if(length(all_shots) == 0) {
    message("Detailed shot data unavailable for this match.")
    return(data.frame())
  }

  home_shot_df <- worldfootballR:::.extract_team_shot_df(
    all_shots[2],
    home_away = "Home",
    match_date = match_date
  )

  away_shot_df <- worldfootballR:::.extract_team_shot_df(
    all_shots[3],
    home_away = "Away",
    match_date = match_date
  )

  all_shot_df <- rbind(
    home_shot_df,
    away_shot_df
  )

  all_shot_df <- all_shot_df %>%
    dplyr::select(
      dplyr::all_of(c("Date", "Squad", "Home_Away", "Match_Half")),
      dplyr::everything()
    )

  return(all_shot_df)
}

# Modified function to extract lineups
fb_get_match_lineups <- function(match_page) {
  main_url <- "https://fbref.com"

  match_date <- match_page %>% rvest::html_nodes(".venuetime") %>% rvest::html_attr("data-venue-date")

  lineups <- match_page %>% rvest::html_nodes(".lineup") %>% rvest::html_nodes("table")

  if(length(lineups) == 0) {
    message("Lineups not available for this match.")
    return(data.frame())
  }

  home <- 1
  away <- 2

  get_each_lineup <- function(home_away) {
    lineup <- lineups[home_away] %>% rvest::html_table() %>% data.frame()

    player_urls <- lineups[home_away] %>% rvest::html_nodes("a") %>% rvest::html_attr("href") %>% paste0(main_url, .)
    formation <- names(lineup)[1]
    is_diamond <- grepl("\\..$", formation)
    if(grepl("u", formation, ignore.case = T)) {
      formation  <- formation %>% gsub("u\\..*", "", ., ignore.case = T) %>%stringr::str_extract_all(., "[[:digit:]]") %>% unlist() %>% paste(collapse = "-")
    } else {
      formation  <- formation %>% stringr::str_extract_all(., "[[:digit:]]") %>% unlist() %>% paste(collapse = "-")
    }
    if(is_diamond) {
      formation <- paste0(formation, "-diamond")
    }
    team <- tryCatch(match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[home_away], error = function(e) NA)

    bench_index <- which(lineup[,1] == "Bench")

    suppressMessages(
      lineup <- lineup[1:(bench_index-1),] %>% dplyr::mutate(Starting = "Pitch") %>%
        dplyr::bind_rows(
          lineup[(bench_index+1):nrow(lineup),] %>% dplyr::mutate(Starting = "Bench")
        )
    )

    lineup <- lineup %>%
      dplyr::mutate(Matchday = match_date,
                    Team = team,
                    Formation = formation,
                    PlayerURL = player_urls)

    names(lineup) <- c("Player_Num", "Player_Name", "Starting", "Matchday", "Team", "Formation", "PlayerURL")

    all_tables <- match_page %>%
      rvest::html_nodes(".table_container")

    stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "summary$"))] %>%
      rvest::html_nodes("table")

    if(home_away == 1) {
      home_or_away <- "Home"
    } else {
      home_or_away <- "Away"
    }

    additional_info <- stat_df[home_away]%>% rvest::html_table() %>% data.frame()

    additional_info <- additional_info %>%
      worldfootballR:::.clean_match_advanced_stats_data() %>%
      dplyr::filter(!is.na(.data[["Player_Num"]])) %>%
      dplyr::bind_cols(Team=team, Home_Away=home_or_away, .) %>%
      dplyr::mutate(Player_Num = as.character(.data[["Player_Num"]]))

    if(any(grepl("Nation", colnames(additional_info)))) {
      additional_info <- additional_info %>%
        dplyr::select(.data[["Team"]], .data[["Home_Away"]], .data[["Player"]], .data[["Player_Num"]], .data[["Nation"]], .data[["Pos"]], .data[["Age"]], .data[["Min"]], .data[["Gls"]], .data[["Ast"]], .data[["CrdY"]], .data[["CrdR"]])
    } else {
      additional_info <- additional_info %>%
        dplyr::select(.data[["Team"]], .data[["Home_Away"]], .data[["Player"]], .data[["Player_Num"]], .data[["Pos"]], .data[["Age"]], .data[["Min"]], .data[["Gls"]], .data[["Ast"]], .data[["CrdY"]], .data[["CrdR"]])
    }

    lineup <- lineup %>%
      dplyr::mutate(Player_Num = as.character(.data[["Player_Num"]])) %>%
      dplyr::left_join(additional_info, by = c("Team", "Player_Name" = "Player", "Player_Num")) %>%
      dplyr::mutate(Home_Away = ifelse(is.na(.data[["Home_Away"]]), home_or_away, .data[["Home_Away"]])) %>%
      dplyr::select(.data[["Matchday"]], .data[["Team"]], .data[["Home_Away"]], .data[["Formation"]], .data[["Player_Num"]], .data[["Player_Name"]], .data[["Starting"]], dplyr::everything()) %>%
      dplyr::mutate(Matchday = lubridate::ymd(.data[["Matchday"]]))

    return(lineup)
  }

  all_lineup <- tryCatch(c(home, away) %>%
                           purrr::map_df(get_each_lineup), error = function(e) data.frame())

  if(nrow(all_lineup) == 0) {
    message("Lineups not available for this match.")
  }

  return(all_lineup)
}

# Modified function to extract match summary
fb_get_match_summary <- function(match_page, match_report) {
  events <- match_page %>% rvest::html_nodes("#events_wrap")

  if(length(events) == 0) {
    message("Match Summary not available for this match.")
    return(data.frame())
  }

  Home_Team <- tryCatch(match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[1], error = function(e) NA)
  Away_Team <- tryCatch(match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[2], error = function(e) NA)

  events_home <- events %>% rvest::html_nodes(".a") %>% rvest::html_text() %>% stringr::str_squish()
  events_away <- events %>% rvest::html_nodes(".b") %>% rvest::html_text() %>% stringr::str_squish()

  home_events <- tryCatch(data.frame(Team=Home_Team, Home_Away="Home", events_string=events_home), error = function(e) data.frame())
  away_events <- tryCatch(data.frame(Team=Away_Team, Home_Away="Away", events_string=events_away), error = function(e) data.frame())

  events_df <- dplyr::bind_rows(home_events, away_events)

  if(nrow(events_df) == 0) {
    message("No events found for this match.")
    return(data.frame())
  }

  events_df <- events_df %>%
    dplyr::mutate(Event_Time = gsub("&rsquor.*", "", .data[["events_string"]])) %>%
    dplyr::mutate(Is_Pens = stringr::str_detect(.data[["Event_Time"]], "[A-Z]"))

  suppressWarnings(
    events_df <- events_df %>%
      dplyr::mutate(Event_Half = dplyr::case_when(
        !.data[["Is_Pens"]] & as.numeric(gsub("\\+.*", "", .data[["Event_Time"]])) <= 45 ~ 1,
        !.data[["Is_Pens"]] & dplyr::between(as.numeric(gsub("\\+.*", "", .data[["Event_Time"]])), 46, 90) ~ 2,
        !.data[["Is_Pens"]] & dplyr::between(as.numeric(gsub("\\+.*", "", .data[["Event_Time"]])), 91, 105) ~ 3,
        !.data[["Is_Pens"]] & dplyr::between(as.numeric(gsub("\\+.*", "", .data[["Event_Time"]])), 106, 120) ~ 4,
        TRUE ~ 5
      ))
  )

  events_df <- events_df %>%
    dplyr::mutate(
      Event_Time = gsub("&rsquor.*", "", .data[["events_string"]]) %>%
        ifelse(stringr::str_detect(., "\\+"), (as.numeric(gsub("\\+.*", "", .)) + as.numeric(gsub(".*\\+", "", .))), .),
      Event_Time = ifelse(.data[["Is_Pens"]], 121, .data[["Event_Time"]]),
      Event_Time = as.numeric(.data[["Event_Time"]]),
      Event_Type = ifelse(stringr::str_detect(tolower(.data[["events_string"]]), "penalty"), "Penalty",
                          ifelse(stringr::str_detect(tolower(.data[["events_string"]]), "own goal"), "Own Goal",
                                 gsub(".* [^\x20-\x7E] ", "", .data[["events_string"]]) %>% gsub("[[:digit:]]:[[:digit:]]", "", .))) %>% stringr::str_squish(),
      Event_Players = gsub(".*\\;", "", .data[["events_string"]]) %>% gsub(" [^\x20-\x7E] .*", "", .),
      Score_Progression = stringr::str_extract(.data[["Event_Players"]], "[[:digit:]]:[[:digit:]]"),
      Event_Players = gsub("[[:digit:]]:[[:digit:]]", "", .data[["Event_Players"]]) %>% stringr::str_squish(),
      Penalty_Number = dplyr::case_when(
        .data[["Is_Pens"]] ~ gsub("([0-9]+).*$", "\\1", .data[["Event_Players"]]),
        TRUE ~ NA_character_
      ),
      Penalty_Number = as.numeric(.data[["Penalty_Number"]]),
      Event_Players = gsub("[[:digit:]]+\\s", "", .data[["Event_Players"]]),
      Event_Type = ifelse(.data[["Is_Pens"]], "Penalty Shootout", .data[["Event_Type"]])
    ) %>%
    dplyr::select(-.data[["events_string"]]) %>%
    dplyr::arrange(.data[["Event_Half"]], .data[["Event_Time"]])

  events_df <- cbind(match_report, events_df)

  return(events_df)
}

fb_parse_match_data <- function(match_urls, stat_types = c("summary", "passing", "passing_types", "defense", "possession", "misc", "keeper"), shooting=True) {
  # Initialize empty lists to store data frames
  combined_shooting_data <- list()
  combined_lineups <- list()
  combined_match_summary <- list()

  # For advanced stats, we'll need a nested list
  combined_advanced_stats <- list()

  # Initialize progress bar
  pb <- progress::progress_bar$new(
    format = "  Processing matches [:bar] :percent eta: :eta",
    total = length(match_urls), clear = FALSE, width = 60
  )

  # Loop over each match URL
  for (match_url in match_urls) {
    pb$tick()

    # Parse the match data
    match_data <- fb_parse_match(match_url, stat_types = stat_types, shooting = shooting)  # Modified to include shooting

    if (is.null(match_data)) {
      message(glue::glue("Skipping match {match_url} due to errors."))
      next
    }

    # Combine shooting data
    if (!is.null(match_data$shooting_data) && nrow(match_data$shooting_data) > 0) {
      combined_shooting_data[[length(combined_shooting_data) + 1]] <- match_data$shooting_data %>%
        mutate(MatchURL = match_url)
    }

    # Combine lineups
    if (!is.null(match_data$lineups) && nrow(match_data$lineups) > 0) {
      combined_lineups[[length(combined_lineups) + 1]] <- match_data$lineups %>%
        mutate(MatchURL = match_url)
    }

    # Combine match summary
    if (!is.null(match_data$match_summary) && nrow(match_data$match_summary) > 0) {
      combined_match_summary[[length(combined_match_summary) + 1]] <- match_data$match_summary %>%
        mutate(MatchURL = match_url)
    }

    # Combine advanced stats
    if (!is.null(match_data$advanced_stats) && length(match_data$advanced_stats) > 0) {
      # Loop over each advanced stat data frame
      for (stat_name in names(match_data$advanced_stats)) {
        stat_df <- match_data$advanced_stats[[stat_name]]
        if (!is.null(stat_df) && nrow(stat_df) > 0) {
          stat_df <- stat_df %>%
            mutate(MatchURL = match_url)

          # Initialize list for this stat type if not already
          if (is.null(combined_advanced_stats[[stat_name]])) {
            combined_advanced_stats[[stat_name]] <- list()
          }

          # Add the data frame to the list
          combined_advanced_stats[[stat_name]][[length(combined_advanced_stats[[stat_name]]) + 1]] <- stat_df
        }
      }
    }
  }

  # Bind the lists into data frames
  shooting_data_df <- bind_rows(combined_shooting_data)
  lineups_df <- bind_rows(combined_lineups)
  match_summary_df <- bind_rows(combined_match_summary)

  # For advanced stats, bind each stat type separately
  advanced_stats_combined <- list()
  for (stat_name in names(combined_advanced_stats)) {
    advanced_stats_combined[[stat_name]] <- bind_rows(combined_advanced_stats[[stat_name]])
  }

  # Return a list containing all combined data frames
  return(list(
    advanced_stats = advanced_stats_combined,
    shooting_data = shooting_data_df,
    lineups = lineups_df,
    match_summary = match_summary_df
  ))
}

# # Example usage
# match_url <-
# # Example usage
# # # Suppose you have a vector of match URLs or file paths
# match_urls <- c(
#    "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2018/Arsenal-Bournemouth-September-9-2017-Premier-League.html",
#   "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2023/Arsenal-Crystal-Palace-March-19-2023-Premier-League.html",
#   "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2023/Arsenal-Newcastle-United-January-3-2023-Premier-League.html"
# )
#
# # Call the function
# all_match_data <- fb_parse_match_data(match_urls, stat_types = c("summary"))
#
# # Access combined data frames
# advanced_stats_combined <- all_match_data$advanced_stats
# shooting_data_combined <- all_match_data$shooting_data
# lineups_combined <- all_match_data$lineups
# match_summary_combined <- all_match_data$match_summary
#
# # Example of accessing a specific advanced stat
# summary_team_combined <- advanced_stats_combined$summary_team