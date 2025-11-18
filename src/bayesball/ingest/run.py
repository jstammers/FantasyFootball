from bayesball.ingest.wf import (
    ingest_advanced_match_stats_wf,
    ingest_competitions,
    ingest_match_results,
    ingest_match_shooting_wf,
    ingest_match_summary_wf,
)
from bayesball.ingest.fbref import ingest_wages, ingest_season_stats, \
    ingest_advanced_match_stats_fb, ingest_match_summary_fb, scrape_matches, \
    ingest_match_shooting_fb, stage_new_results


def main(fb: bool = True, wf: bool = True, backfill_wf: bool = False):
    if fb:
        scrape_matches()
        ingest_advanced_match_stats_fb()
        ingest_season_stats()
        ingest_match_summary_fb()
        ingest_match_shooting_fb()
        ingest_wages()
        stage_new_results()
    if wf:
        ingest_advanced_match_stats_wf()
        ingest_match_results()
        ingest_competitions()
    if backfill_wf:
        ingest_match_shooting_wf()
        ingest_match_summary_wf()
        ingest_advanced_match_stats_wf()
