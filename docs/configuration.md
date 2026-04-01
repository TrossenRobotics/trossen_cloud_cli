# Configuration

The `trc` CLI stores configuration in `~/.trossen/trossen_cloud_cli/config.toml`. Manage it with the `trc config` commands.

## View current settings

```bash
trc config show
```

## Set a value

```bash
trc config set <key> <value>
```

### Available keys

| Key | Default | Description |
|-----|---------|-------------|
| `upload.chunk_size_mb` | 50 | Size of each multipart upload chunk in MB |
| `upload.parallel_parts` | 6 | Max concurrent parts per file upload |
| `upload.parallel_files` | 32 | Max concurrent file uploads |
| `download.parallel_files` | 16 | Max concurrent file downloads |
| `download.stream_chunk_size` | 65536 | Download stream buffer size in bytes |

### Examples

```bash
# Larger chunks for high-bandwidth connections
trc config set upload.chunk_size_mb 100

# More parallel uploads
trc config set upload.parallel_files 64

# Increase download concurrency
trc config set download.parallel_files 32
```

## Reset to defaults

```bash
trc config reset         # prompts for confirmation
trc config reset --force # no prompt
```

## Environment variables

These override stored configuration and cannot be set via `trc config`:

| Variable | Description |
|----------|-------------|
| `TROSSEN_API_URL` | Override the API endpoint |
| `TROSSEN_TOKEN` | Override the stored auth token |
