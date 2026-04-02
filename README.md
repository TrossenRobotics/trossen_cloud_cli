# trossen_cloud_cli

CLI for interacting with Trossen Cloud datasets, models, and training jobs.

## Installation

Requires Python 3.11+.

```bash
uv tool install trossen-cloud-cli
```

Or with pipx:

```bash
pipx install trossen-cloud-cli
```

Or with pip:

```bash
pip install trossen-cloud-cli
```

## Authentication

Create an API token in the Trossen Cloud web UI, then:

```bash
# Directly provide your token
trc auth login --token <your-api-token>

# Or receive a password prompt
trc auth login

# Check authentication status
trc auth status
```

The token is stored securely in your OS keyring.

## Usage

### Datasets

```bash
# Upload a local dataset
trc dataset upload ./my-data --name my-dataset --type lerobot

# Download a dataset
trc dataset download <dataset-id> ./output

# Browse and manage
trc dataset list --mine
trc dataset info <dataset-id>
trc dataset view <user>/<name>
trc dataset update <dataset-id> --name new-name --privacy public
trc dataset delete <dataset-id>
```

### Models

```bash
# Upload a model
trc model upload ./my-model --name my-model

# Download a model
trc model download <model-id> ./output

# Browse and manage
trc model list --mine
trc model info <model-id>
trc model view <user>/<name>
trc model update <model-id> --name new-name
trc model delete <model-id>
```

### Training Jobs

```bash
# Create a training job
trc training-job create --name my-job --base-model-id <id> --dataset-id <id>

# Monitor and manage
trc training-job list
trc training-job info <job-id>
trc training-job cancel <job-id>
trc training-job models <job-id>
```

Run `trc usage` to see a quick-reference of all commands, or `trc <command> --help` for detailed help on any command.

## Configuration

Transfer settings (chunk sizes, concurrency) can be tuned via `trc config`. See [docs/configuration.md](docs/configuration.md) for details.
