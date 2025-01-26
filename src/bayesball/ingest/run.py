from bayesball.ingest.wf import (
    ingest_advanced_match_stats_wf,
    ingest_competitions,
    ingest_match_results,
    ingest_match_shooting_wf,
    ingest_match_summary_wf,
)
from bayesball.ingest.fbref import (
    get_missing_matches,
    ingest_wages,
    ingest_season_stats,
    ingest_advanced_match_stats_fb,
    ingest_match_summary_fb,
    scrape_matches,
    ingest_match_shooting_fb,
    stage_new_results,
    get_match_stats,
)


def main(
    fb: bool = True,
    wf: bool = False,
    backfill_wf: bool = False,
    update_current_season: bool = False,
    backfill_season_stats: bool = False,
):
    if wf:
        ingest_competitions()
        ingest_advanced_match_stats_wf()
        ingest_match_results()

    if backfill_wf:
        ingest_match_shooting_wf()
        ingest_match_summary_wf()
        ingest_advanced_match_stats_wf()
    if backfill_season_stats or update_current_season:
        ingest_season_stats(update_current_season)
    if fb:
        missing_matches = get_missing_matches()
        scrape_matches(missing_matches)
        match_stats = get_match_stats(missing_matches)
        ingest_advanced_match_stats_fb(
            missing_matches, match_stats.player_stats, match_stats.player_stats
        )
        ingest_match_summary_fb(missing_matches, match_stats.match_summary)
        ingest_match_shooting_fb(missing_matches, match_stats.shooting_data)
        ingest_wages(update_current_season)

    if update_current_season or fb:
        stage_new_results()



if __name__ == "__main__":
    main()
