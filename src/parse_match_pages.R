library(magrittr)

load_page <- function(path) {
  tryCatch(
    xml2::read_html(path),
    error = function(e) {
      message(glue::glue("Error loading {url}"))
      return(NA)
    }
  )
}

fb_advanced_match_stats <- function(match_url, stat_type, team_or_player) {
  get_each_match_statistic <- function(match_url) {
    pb$tick()
    match_page <- tryCatch(load_page(match_url), error = function(e) NA)

    # match_page <- tryCatch(.load_page(match_url), error = function(e) NA)

    if(!is.na(match_page)) {
      match_report <- worldfootballR:::.get_match_report_page(match_page = match_page)

      league <- match_page %>%
        rvest::html_nodes("#content") %>%
        rvest::html_node("a") %>% rvest::html_text()

      all_tables <- match_page %>%
        rvest::html_nodes(".table_container")


      if(stat_type == "summary") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "summary$"))] %>%
          rvest::html_nodes("table")

      } else if(stat_type == "passing") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "passing$"))] %>%
          rvest::html_nodes("table")

      } else if(stat_type == "passing_types") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "passing_types"))] %>%
          rvest::html_nodes("table")

      } else if(stat_type == "defense") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "defense$"))] %>%
          rvest::html_nodes("table")

      } else if(stat_type == "possession") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "possession$"))] %>%
          rvest::html_nodes("table")


      } else if(stat_type == "misc") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "misc$"))] %>%
          rvest::html_nodes("table")

      } else if(stat_type == "keeper") {
        stat_df <- all_tables[which(stringr::str_detect(all_tables %>% rvest::html_attr("id"), "keeper_stats"))] %>%
          rvest::html_nodes("table")

      }

      if(length(stat_df) != 0) {

        if(!stat_type %in% c("shots")) {
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


          stat_df_output <- dplyr::bind_cols(match_report, stat_df_output)

          if(nrow(stat_df_output) == 0) {
            print(glue::glue("NOTE: Stat Type '{stat_type}' is not found for this match. Check {match_url} to see if it exists."))
          }

        } else if(stat_type == "shots") {

        }
      } else {
        print(glue::glue("NOTE: Stat Type '{stat_type}' is not found for this match. Check {match_url} to see if it exists."))
        stat_df_output <- data.frame()
      }

    } else {
      print(glue::glue("Stats data not available for {match_url}"))
      stat_df_output <- data.frame()
    }

    return(stat_df_output)
  }

  # create the progress bar with a progress function.
  pb <- progress::progress_bar$new(total = length(match_url))

  suppressWarnings(
    final_df <- match_url %>%
      purrr::map_df(get_each_match_statistic)
  )

  return(final_df)
}

fb_match_shooting <- function(match_url, time_pause=3) {
  # .pkg_message("Scraping detailed shot and shot creation data...")

  time_wait <- time_pause

  get_each_match_shooting_data <- function(match_url, time_pause=time_wait) {

    pb$tick()

    match_page <- tryCatch(load_page(match_url), error = function(e) NA)
    tryCatch( {home_team <- match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[1]}, error = function(e) {home_team <- NA})
    tryCatch( {away_team <- match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[2]}, error = function(e) {away_team <- NA})

    # home_away_df <- data.frame(Team=home_team, Home_Away="Home") %>%
    #   rbind(data.frame(Team=away_team, Home_Away = "Away"))

    match_date <- match_page %>% rvest::html_nodes(".venuetime") %>% rvest::html_attr("data-venue-date")

    all_shots <- match_page %>% rvest::html_nodes("#switcher_shots") %>% rvest::html_nodes("div")

    if(length(all_shots) == 0) {
      rlang::inform(glue::glue("Detailed shot data unavailable for {match_url}"))
      all_shot_df <- data.frame()
    } else {

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
    }

    return(all_shot_df)
  }

  # create the progress bar with a progress function.
  pb <- progress::progress_bar$new(total = length(match_url))

  all_shooting <- match_url %>%
    purrr::map_df(get_each_match_shooting_data)

  return(all_shooting)
}

fb_match_lineups <- function(match_url, time_pause=3) {
  # .pkg_message("Scraping lineups")

  main_url <- "https://fbref.com"

  time_wait <- time_pause

  get_each_match_lineup <- function(match_url, time_pause=time_wait) {
    pb$tick()

    match_page <- tryCatch(load_page(match_url), error = function(e) NA)
    if(!is.na(match_page)) {
      match_date <- match_page %>% rvest::html_nodes(".venuetime") %>% rvest::html_attr("data-venue-date")

      lineups <- match_page %>% rvest::html_nodes(".lineup") %>% rvest::html_nodes("table")

      home <- 1
      away <- 2

      get_each_lineup <- function(home_away) {
        lineup <- lineups[home_away] %>% rvest::html_table() %>% data.frame()

        player_urls <- lineups[home_away] %>% rvest::html_nodes("a") %>% rvest::html_attr("href") %>% paste0(main_url, .)
        formation <- names(lineup)[1]
        is_diamond <- grepl("\\..$", formation)
        # on Windows, the diamond is coming through as utf-8, while on MacOS coming through as ".."
        if(grepl("u", formation, ignore.case = T)) {
          formation  <- formation %>% gsub("u\\..*", "", ., ignore.case = T) %>%stringr::str_extract_all(., "[[:digit:]]") %>% unlist() %>% paste(collapse = "-")
        } else {
          formation  <- formation %>% stringr::str_extract_all(., "[[:digit:]]") %>% unlist() %>% paste(collapse = "-")
        }
        if(is_diamond) {
          formation <- paste0(formation, "-diamond")
        }
        tryCatch( {team <- match_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[home_away]}, error = function(e) {team <- NA})

        bench_index <- which(lineup[,1] == "Bench")

        suppressMessages(lineup <- lineup[1:(bench_index-1),] %>% dplyr::mutate(Starting = "Pitch") %>%
                           dplyr::bind_rows(
                             lineup[(bench_index+1):nrow(lineup),] %>% dplyr::mutate(Starting = "Bench")
                           ) )

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
          dplyr::mutate(Matchday = lubridate::ymd(.data[["Matchday"]])) %>%
          dplyr::mutate(MatchURL = match_url)

        return(lineup)
      }

      all_lineup <- tryCatch(c(home, away) %>%
                               purrr::map_df(get_each_lineup), error = function(e) data.frame())

      if(nrow(all_lineup) == 0) {
        print(glue::glue("Lineups not available for {match_url}"))
      }

    } else {
      print(glue::glue("Lineups not available for {match_url}"))
      all_lineup <- data.frame()
    }

    return(all_lineup)
  }

  # create the progress bar with a progress function.
  pb <- progress::progress_bar$new(total = length(match_url))

  all_lineups <- match_url %>%
    purrr::map_df(get_each_match_lineup)

  return(all_lineups)
}

fb_match_report <- function(match_url, time_pause=3) {

  time_wait <- time_pause
  each_match_report <- function(match_url, time_pause=time_wait) {
    pb$tick()

    match_page <- tryCatch(load_page(match_url), error = function(e) NA)

    if(!is.na(match_page)) {
      each_game <- worldfootballR:::.get_match_report_page(match_page)
    } else {
      print(glue::glue("{match_url} is not available"))
      each_game <- data.frame()
    }
    return(each_game)
  }

  # create the progress bar with a progress function.
  pb <- progress::progress_bar$new(total = length(match_url))

  all_games <- match_url %>%
    purrr::map_df(each_match_report)

  return(all_games)
}

fb_match_summary <- function(match_url, time_pause=3) {

  time_wait <- time_pause

  get_each_match_summary <- function(match_url, time_pause=time_wait) {
    pb$tick()

    each_game_page <- tryCatch(load_page(match_url), error = function(e) NA)

    if(!is.na(each_game_page)) {
      match_report <- worldfootballR:::.get_match_report_page(match_page = each_game_page)
      Home_Team <- tryCatch(each_game_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[1], error = function(e) NA)
      Away_Team <- tryCatch(each_game_page %>% rvest::html_nodes("div+ strong a") %>% rvest::html_text() %>% .[2], error = function(e) NA)

      events <- each_game_page %>% rvest::html_nodes("#events_wrap")

      events_home <- events %>% rvest::html_nodes(".a") %>% rvest::html_text() %>% stringr::str_squish()
      events_away <- events %>% rvest::html_nodes(".b") %>% rvest::html_text() %>% stringr::str_squish()

      home_events <- tryCatch(data.frame(Team=Home_Team, Home_Away="Home", events_string=events_home), error = function(e) data.frame())
      away_events <- tryCatch(data.frame(Team=Away_Team, Home_Away="Away", events_string=events_away), error = function(e) data.frame())

      events_df <- dplyr::bind_rows(home_events, away_events)

      if(nrow(events_df) > 0) {
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
          dplyr::mutate(Event_Time = gsub("&rsquor.*", "", .data[["events_string"]]) %>% ifelse(stringr::str_detect(., "\\+"), (as.numeric(gsub("\\+.*", "", .)) + as.numeric(gsub(".*\\+", "", .))), .),
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
                        Event_Type = ifelse(.data[["Is_Pens"]], "Penalty Shootout", .data[["Event_Type"]])) %>%
          dplyr::select(-.data[["events_string"]]) %>%
          dplyr::arrange(.data[["Event_Half"]], .data[["Event_Time"]])


        events_df <- cbind(match_report, events_df)
      } else {
        events_df <- data.frame()
      }

    } else {
      print(glue::glue("Match Summary not available for {match_url}"))
      events_df <- data.frame()
    }

    return(events_df)
  }

  # create the progress bar with a progress function.
  pb <- progress::progress_bar$new(total = length(match_url))

  all_events_df <- match_url %>%
    purrr::map_df(get_each_match_summary)


  return(all_events_df)
}

#
# test <- "/home/jimmy/Code/FantasyFootball/data/raw/fbref/html/ENG/1st/2018/Arsenal-Bournemouth-September-9-2017-Premier-League.html"
#
# fb_match_summary(test)