import typer
import logging
from rich.logging import RichHandler

from bayesball.ingest.run import main as ingest_main
from bayesball.extract.run import main as extract_main

# Configure Rich logging
logging.basicConfig(
    level="INFO",  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(message)s",  # Rich handles formatting, so keep it simple
    handlers=[RichHandler(rich_tracebacks=True)],  # Enable Rich's tracebacks
)

# Create a logger instance
logger = logging.getLogger(__name__)
app = typer.Typer()


@app.command()
def ingest(
    fb: bool = typer.Option(False, help="Ingest data from fbref"),
    wf: bool = typer.Option(True, help="Ingest data from worldfootballr_data"),
    backfill_wf: bool = typer.Option(
        False, help="Backfill data from worldfootballr_data"
    ),
    update_current_season: bool = typer.Option(
        False, help="Update the current season data"
    ),
    backfill_season_stats: bool = typer.Option(False, help="Backfill season stats"),
):
    """Ingest the source data"""
    try:
        ingest_main(
            fb=fb,
            wf=wf,
            backfill_wf=backfill_wf,
            update_current_season=update_current_season,
            backfill_season_stats=backfill_season_stats,
        )

    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")


@app.command()
def extract():
    """Extract the data"""
    try:
        extract_main()
    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")
