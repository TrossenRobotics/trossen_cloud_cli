"""Training job commands."""

import asyncio
import json
from typing import Annotated

import typer
from rich.table import Table

from ..api_client import ApiClient, ApiError
from ..auth import require_auth
from ..output import console, print_error, print_info, print_success
from ..types import TrainingJobStatus

app = typer.Typer(help="Manage training jobs")


@app.command("create")
def create(
    name: Annotated[
        str,
        typer.Option("--name", "-n", help="Training job name"),
    ],
    base_model_id: Annotated[
        str,
        typer.Option("--base-model-id", "-b", help="Base model ID (UUID) to fine-tune"),
    ],
    num_steps: Annotated[
        int,
        typer.Option("--num-steps", help="Total training steps"),
    ],
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", help="Training batch size"),
    ],
    learning_rate: Annotated[
        float,
        typer.Option("--learning-rate", help="Learning rate"),
    ],
    checkpoint_frequency: Annotated[
        int,
        typer.Option("--checkpoint-frequency", help="Steps between checkpoints"),
    ],
    dataset_id: Annotated[
        str | None,
        typer.Option("--dataset-id", "-d", help="Training dataset ID (UUID)"),
    ] = None,
    hf_dataset: Annotated[
        str | None,
        typer.Option("--hf-dataset", help="HuggingFace dataset repo ID (e.g., org/dataset-name)"),
    ] = None,
    instance_type: Annotated[
        str,
        typer.Option(
            "--instance-type",
            "-i",
            help="Instance type (e.g., gpu-small, gpu-medium, gpu-large, a100, h200, b200, rtx5090)",
        ),
    ] = "gpu-medium",
    extra_hyperparams: Annotated[
        str | None,
        typer.Option("--extra-hyperparams", help="Additional hyperparameters as JSON string"),
    ] = None,
) -> None:
    """
    Create a new training job.
    """
    require_auth()

    if not dataset_id and not hf_dataset:
        print_error("Provide either --dataset-id or --hf-dataset")
        raise typer.Exit(1)
    if dataset_id and hf_dataset:
        print_error("Provide either --dataset-id or --hf-dataset, not both")
        raise typer.Exit(1)

    hyperparameters: dict = {
        "num_steps": num_steps,
        "batch_size": batch_size,
        "learning_rate": learning_rate,
        "checkpoint_frequency": checkpoint_frequency,
    }

    if extra_hyperparams:
        try:
            extra = json.loads(extra_hyperparams)
            hyperparameters.update(extra)
        except json.JSONDecodeError:
            print_error("Invalid JSON for --extra-hyperparams")
            raise typer.Exit(1)

    payload: dict = {
        "name": name,
        "base_model_id": base_model_id,
        "instance_type": instance_type,
        "hyperparameters": hyperparameters,
    }
    if dataset_id:
        payload["dataset_id"] = dataset_id
    if hf_dataset:
        payload["hf_dataset_repo_id"] = hf_dataset

    async def do_create():
        async with ApiClient() as client:
            return await client.post("/training-jobs", json=payload)

    try:
        result = asyncio.run(do_create())
        print_success("Training job created!")
        console.print(f"[label]Job ID:[/label] {result['job_id']}")
        console.print(f"[label]Status:[/label] {result['status']}")
        console.print(f"[label]Created:[/label] {result['created_at']}")

    except ApiError as e:
        print_error(f"Failed to create training job: {e.message}")
        raise typer.Exit(1)


@app.command("list")
def list_jobs(
    status: Annotated[
        str | None,
        typer.Option(
            "--status", "-s", help="Filter by job status (comma-separated, e.g., running,queued)"
        ),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Maximum number of jobs to show"),
    ] = 10,
    offset: Annotated[
        int,
        typer.Option("--offset", help="Number of results to skip"),
    ] = 0,
) -> None:
    """
    List your training jobs.
    """
    require_auth()

    # Parse comma-separated status filters
    status_filters: set[str] | None = None
    if status:
        status_filters = {s.strip().lower() for s in status.split(",")}
        valid = {s.value for s in TrainingJobStatus}
        invalid = status_filters - valid
        if invalid:
            print_error(f"Invalid status: {', '.join(invalid)}. Valid: {', '.join(sorted(valid))}")
            raise typer.Exit(1)

    async def fetch():
        async with ApiClient() as client:
            params: dict[str, str | int] = {"limit": limit, "offset": offset}
            if status_filters and len(status_filters) == 1:
                params["status"] = next(iter(status_filters))
            return await client.get("/training-jobs/me", params=params)

    try:
        response = asyncio.run(fetch())
        jobs = response if isinstance(response, list) else response.get("items", [])

        # Client-side filter when multiple statuses requested
        if status_filters and len(status_filters) > 1:
            jobs = [j for j in jobs if j.get("status") in status_filters]

        if not jobs:
            print_info("No training jobs found")
            return

        table = Table(title="Training Jobs", show_edge=False)
        table.add_column("ID", style="table.id", no_wrap=True)
        table.add_column("Name", style="bold")
        table.add_column("Status")
        table.add_column("Base Model")
        table.add_column("Dataset")
        table.add_column("Progress")

        for job in jobs:
            progress = job.get("progress")
            progress_str = f"{progress * 100:.0f}%" if progress is not None else "-"
            table.add_row(
                job["id"],
                job["name"],
                job.get("status", "-"),
                job.get("base_model_id") or "-",
                job.get("dataset_id") or "-",
                progress_str,
            )

        console.print(table)

    except ApiError as e:
        print_error(f"Failed to list training jobs: {e.message}")
        raise typer.Exit(1)


@app.command("info")
def info(
    job_id: Annotated[
        str,
        typer.Argument(help="Training job ID (UUID)"),
    ],
) -> None:
    """
    Get detailed information about a training job.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            return await client.get(f"/training-jobs/{job_id}")

    try:
        job = asyncio.run(fetch())
        _display_job_info(job)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Training job '{job_id}' not found")
        else:
            print_error(f"Failed to get training job: {e.message}")
        raise typer.Exit(1)


@app.command("cancel")
def cancel(
    job_id: Annotated[
        str,
        typer.Argument(help="Training job ID (UUID) to cancel"),
    ],
    reason: Annotated[
        str | None,
        typer.Option("--reason", "-r", help="Reason for cancellation"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Skip confirmation"),
    ] = False,
) -> None:
    """
    Cancel a training job.
    """
    require_auth()

    if not force:
        confirm = typer.confirm(f"Are you sure you want to cancel training job {job_id}?")
        if not confirm:
            print_info("Cancelled")
            return

    async def do_cancel():
        async with ApiClient() as client:
            payload = {"reason": reason} if reason else None
            return await client.post(f"/training-jobs/{job_id}/cancel", json=payload)

    try:
        result = asyncio.run(do_cancel())
        print_success(f"Training job cancelled: {result.get('message', '')}")

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Training job '{job_id}' not found")
        else:
            print_error(f"Failed to cancel training job: {e.message}")
        raise typer.Exit(1)


@app.command("models")
def list_checkpoint_models(
    job_id: Annotated[
        str,
        typer.Argument(help="Training job ID (UUID)"),
    ],
) -> None:
    """
    List checkpoint models created by a training job.
    """
    require_auth()

    async def fetch():
        async with ApiClient() as client:
            return await client.get(f"/training-jobs/{job_id}/models")

    try:
        response = asyncio.run(fetch())
        models = response if isinstance(response, list) else response.get("items", [])

        if not models:
            print_info("No checkpoint models found")
            return

        table = Table(title="Checkpoint Models", show_edge=False)
        table.add_column("ID", style="table.id", no_wrap=True)
        table.add_column("Name", style="bold")
        table.add_column("Privacy")
        table.add_column("Created")

        for model in models:
            table.add_row(
                model["id"],
                model["name"],
                model.get("privacy", "-"),
                model.get("created_at", "-"),
            )

        console.print(table)

    except ApiError as e:
        if e.status_code == 404:
            print_error(f"Training job '{job_id}' not found")
        else:
            print_error(f"Failed to list checkpoint models: {e.message}")
        raise typer.Exit(1)


def _display_job_info(job: dict) -> None:
    """
    Display training job information.
    """
    console.print(f"\n[heading]Training Job: {job['name']}[/heading]\n")
    console.print(f"[label]ID:[/label] {job['id']}")
    console.print(f"[label]Status:[/label] {job.get('status', '-')}")
    console.print(f"[label]Instance:[/label] {job.get('instance_type', '-')}")
    console.print(f"[label]Base Model:[/label] {job.get('base_model_id', '-')}")
    console.print(f"[label]Dataset:[/label] {job.get('dataset_id', '-')}")
    console.print(f"[label]Created:[/label] {job.get('created_at', '-')}")

    if job.get("started_at"):
        console.print(f"[label]Started:[/label] {job['started_at']}")

    if job.get("completed_at"):
        console.print(f"[label]Completed:[/label] {job['completed_at']}")

    progress = job.get("progress")
    if progress is not None:
        console.print(
            f"[label]Progress:[/label] {progress * 100:.1f}% "
            f"(step {job.get('current_step', '?')}/{job.get('total_steps', '?')})"
        )

    if job.get("loss_metric") is not None:
        console.print(f"[label]Loss:[/label] {job['loss_metric']}")

    if job.get("error_message"):
        console.print(f"[error]Error:[/error] {job['error_message']}")

    if job.get("hyperparameters"):
        console.print("\n[label]Hyperparameters:[/label]")
        console.print_json(data=job["hyperparameters"])
