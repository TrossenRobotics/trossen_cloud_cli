"""Main CLI app definition."""

from typing import Annotated

import typer
import typer.rich_utils

from .commands import auth, config, datasets, models, training_jobs
from .output import console

# Override Typer's default styling for better readability on light and dark terminals.
typer.rich_utils.STYLE_ERRORS_PANEL_BORDER = "red1"
typer.rich_utils.STYLE_ERRORS_SUGGESTION = "bold"
typer.rich_utils.RICH_HELP = "Try [bold dodger_blue2]'{command_path} {help_option}'[/] for help."


# Create main app
app = typer.Typer(
    name="trc",
    help="CLI for interacting with Trossen Cloud datasets and models APIs.",
    no_args_is_help=True,
)


@app.callback()
def main_callback(
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Suppress all output"),
    ] = False,
) -> None:
    """
    Trossen Cloud CLI.
    """
    if quiet:
        console.quiet = True


# Add subcommands
app.add_typer(auth.app, name="auth")
app.add_typer(config.app, name="config")
app.add_typer(datasets.app, name="dataset")
app.add_typer(models.app, name="model")
app.add_typer(training_jobs.app, name="training-job")


USAGE_TEXT = """\
[bold]Getting started:[/bold]

  trc auth login --token <your-api-token>
  trc auth status

[bold]Datasets:[/bold]

  trc dataset upload ./my-data --name my-dataset --type lerobot
  trc dataset import-hf org/dataset-name --name my-dataset
  trc dataset download <dataset-id> ./output
  trc dataset list --mine
  trc dataset info <dataset-id>
  trc dataset view <user>/<name>
  trc dataset update <dataset-id> --name new-name --privacy public
  trc dataset delete <dataset-id>

[bold]Models:[/bold]

  trc model upload ./my-model --name my-model
  trc model download <model-id> ./output
  trc model list --mine
  trc model info <model-id>

[bold]Training jobs:[/bold]

  trc training-job create --name my-job --base-model-id <id> --dataset-id <id>
  trc training-job list
  trc training-job info <job-id>
  trc training-job cancel <job-id>
  trc training-job models <job-id>

[bold]Configuration:[/bold]

  trc config show
  trc config set upload.chunk_size_mb 100
  trc config reset

[bold]Options:[/bold]

  -q, --quiet       Suppress all output
  TROSSEN_API_URL   Override the API endpoint
  TROSSEN_TOKEN     Override the stored auth token
"""


@app.command()
def usage() -> None:
    """
    Show usage examples.
    """
    console.print(USAGE_TEXT)


def main() -> None:
    """
    Main entry point.
    """
    app()


if __name__ == "__main__":
    main()
