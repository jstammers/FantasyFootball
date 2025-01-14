import typer

from bayesball.ingest.run import main as ingest_main

app = typer.Typer()


@app.command()
def ingest(
    fb: bool = typer.Option(False, help="Ingest data from fbref"),
    wf: bool = typer.Option(True, help="Ingest data from worldfootballr_data"),
    backfill_wf: bool = typer.Option(
        False, help="Backfill data from worldfootballr_data"
    ),
):
    """Ingest the source data"""
    try:
        ingest_main(fb=fb, wf=wf, backfill_wf=backfill_wf)
    except KeyboardInterrupt:
        typer.echo("stopping...")

    except Exception as e:
        typer.echo(f"error: {e}")
