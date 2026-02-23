# LakeXpress MCP Server

<!-- mcp-name: io.github.arpe-io/lakexpress-mcp -->

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server for [LakeXpress](https://aetperf.github.io/LakeXpress-Documentation/) — a database to Parquet export tool with sync management and data lake publishing.

## Features

- **14 subcommands** supported: logdb management, config management, sync execution, status, and cleanup
- **5 source databases**: SQL Server, PostgreSQL, Oracle, MySQL, MariaDB
- **6 log databases**: SQL Server, PostgreSQL, MySQL, MariaDB, SQLite, DuckDB
- **6 storage backends**: Local, S3, S3-compatible, GCS, Azure ADLS Gen2, OneLake
- **7 publish targets**: Snowflake, Databricks, Fabric, BigQuery, MotherDuck, Glue, DuckLake
- Command preview before execution with safety confirmation
- Auth file validation
- Workflow suggestions based on use case

## Installation

```bash
pip install -e ".[dev]"
```

## Claude Code Configuration

Add to your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "lakexpress": {
      "command": "python",
      "args": ["-m", "src.server"],
      "cwd": "/path/to/lakexpress-mcp",
      "env": {
        "LAKEXPRESS_PATH": "/path/to/LakeXpress",
        "LAKEXPRESS_TIMEOUT": "3600",
        "LAKEXPRESS_LOG_DIR": "./logs"
      }
    }
  }
}
```

Or using the installed entry point:

```json
{
  "mcpServers": {
    "lakexpress": {
      "command": "lakexpress-mcp",
      "env": {
        "LAKEXPRESS_PATH": "/path/to/LakeXpress"
      }
    }
  }
}
```

## Tools

### `preview_command`
Build and preview any LakeXpress CLI command without executing it. Supports all 14 subcommands with full parameter validation.

### `execute_command`
Execute a previously previewed command. Requires `confirmation: true` as a safety mechanism.

### `validate_auth_file`
Validate that an authentication file exists, is valid JSON, and optionally check for specific `auth_id` entries.

### `list_capabilities`
List all supported source databases, log databases, storage backends, publishing targets, compression types, and available commands.

### `suggest_workflow`
Given a use case (source DB type, storage destination, optional publish target), suggest the full sequence of LakeXpress commands with example parameters.

### `get_version`
Report the detected LakeXpress binary version and capabilities.

## Workflow Example

```
# 1. Initialize the log database (first-time setup)
LakeXpress logdb init -a auth.json --log_db_auth_id export_db

# 2. Create a sync configuration
LakeXpress config create -a auth.json --log_db_auth_id export_db \
  --source_db_auth_id prod_db --source_schema_name sales \
  --output_dir ./exports --compression_type Zstd

# 3. Execute the sync
LakeXpress sync --sync_id <sync_id>

# 4. Check status
LakeXpress status -a auth.json --log_db_auth_id export_db --sync_id <sync_id>
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LAKEXPRESS_PATH` | `./LakeXpress` | Path to the LakeXpress binary |
| `LAKEXPRESS_TIMEOUT` | `3600` | Command execution timeout in seconds |
| `LAKEXPRESS_LOG_DIR` | `./logs` | Directory for execution logs |
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ -v --cov=src --cov-report=term-missing
```

## License

MIT
