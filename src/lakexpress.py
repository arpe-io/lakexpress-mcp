"""
LakeXpress command builder and executor.

This module provides functionality to build, validate, and execute
LakeXpress commands with proper security measures.
"""

import os
import subprocess
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .validators import (
    LakeXpressRequest,
    CommandType,
    LogdbInitParams,
    LogdbDropParams,
    LogdbTruncateParams,
    LogdbLocksParams,
    LogdbReleaseLocksParams,
    ConfigCreateParams,
    ConfigDeleteParams,
    ConfigListParams,
    SyncParams,
    SyncExportParams,
    SyncPublishParams,
    RunParams,
    StatusParams,
    CleanupParams,
)
from .version import VersionDetector


logger = logging.getLogger(__name__)


class LakeXpressError(Exception):
    """Base exception for LakeXpress operations."""

    pass


class CommandBuilder:
    """Builds LakeXpress commands from validated requests."""

    def __init__(self, binary_path: str):
        """
        Initialize the command builder.

        Args:
            binary_path: Path to the LakeXpress binary

        Raises:
            LakeXpressError: If binary doesn't exist or isn't executable
        """
        self.binary_path = Path(binary_path)
        self._validate_binary()
        self._version_detector = VersionDetector(str(self.binary_path))
        detected = self._version_detector.detect()
        if detected:
            logger.info(f"LakeXpress version {detected} detected")
        else:
            logger.warning("Could not detect LakeXpress version")

    @property
    def version_detector(self) -> VersionDetector:
        """Access the version detector instance."""
        return self._version_detector

    def get_version(self) -> Dict[str, Any]:
        """Get version information and capabilities.

        Returns:
            Dict with version string, detection status, binary path, and capabilities.
        """
        detected = self._version_detector.detect()
        caps = self._version_detector.capabilities

        return {
            "version": str(detected) if detected else None,
            "detected": detected is not None,
            "binary_path": str(self.binary_path),
            "capabilities": {
                "source_databases": sorted(caps.source_databases),
                "log_databases": sorted(caps.log_databases),
                "storage_backends": sorted(caps.storage_backends),
                "publish_targets": sorted(caps.publish_targets),
                "compression_types": sorted(caps.compression_types),
                "commands": sorted(caps.commands),
                "supports_no_banner": caps.supports_no_banner,
                "supports_version_flag": caps.supports_version_flag,
                "supports_incremental": caps.supports_incremental,
                "supports_cleanup": caps.supports_cleanup,
            },
        }

    def _validate_binary(self) -> None:
        """Validate that LakeXpress binary exists and is executable."""
        if not self.binary_path.exists():
            raise LakeXpressError(f"LakeXpress binary not found at: {self.binary_path}")

        if not self.binary_path.is_file():
            raise LakeXpressError(f"LakeXpress path is not a file: {self.binary_path}")

        if not os.access(self.binary_path, os.X_OK):
            raise LakeXpressError(
                f"LakeXpress binary is not executable: {self.binary_path}"
            )

    def build_command(self, request: LakeXpressRequest) -> List[str]:
        """
        Build a LakeXpress command from a validated request.

        Args:
            request: Validated LakeXpress request

        Returns:
            Command as list of strings (suitable for subprocess)
        """
        cmd = request.command

        if cmd == CommandType.LOGDB_INIT:
            assert request.logdb_init is not None
            return self._build_logdb_init(request.logdb_init)
        elif cmd == CommandType.LOGDB_DROP:
            assert request.logdb_drop is not None
            return self._build_logdb_drop(request.logdb_drop)
        elif cmd == CommandType.LOGDB_TRUNCATE:
            assert request.logdb_truncate is not None
            return self._build_logdb_truncate(request.logdb_truncate)
        elif cmd == CommandType.LOGDB_LOCKS:
            assert request.logdb_locks is not None
            return self._build_logdb_locks(request.logdb_locks)
        elif cmd == CommandType.LOGDB_RELEASE_LOCKS:
            assert request.logdb_release_locks is not None
            return self._build_logdb_release_locks(request.logdb_release_locks)
        elif cmd == CommandType.CONFIG_CREATE:
            assert request.config_create is not None
            return self._build_config_create(request.config_create)
        elif cmd == CommandType.CONFIG_DELETE:
            assert request.config_delete is not None
            return self._build_config_delete(request.config_delete)
        elif cmd == CommandType.CONFIG_LIST:
            assert request.config_list is not None
            return self._build_config_list(request.config_list)
        elif cmd == CommandType.SYNC:
            assert request.sync is not None
            return self._build_sync(request.sync)
        elif cmd == CommandType.SYNC_EXPORT:
            assert request.sync_export is not None
            return self._build_sync_export(request.sync_export)
        elif cmd == CommandType.SYNC_PUBLISH:
            assert request.sync_publish is not None
            return self._build_sync_publish(request.sync_publish)
        elif cmd == CommandType.RUN:
            assert request.run is not None
            return self._build_run(request.run)
        elif cmd == CommandType.STATUS:
            assert request.status is not None
            return self._build_status(request.status)
        elif cmd == CommandType.CLEANUP:
            assert request.cleanup is not None
            return self._build_cleanup(request.cleanup)
        else:
            raise LakeXpressError(f"Unknown command type: {cmd}")

    def _build_global_options(self, params) -> List[str]:
        """Build global option flags shared across most commands."""
        args = []
        args.extend(["-a", params.auth_file])
        args.extend(["--log_db_auth_id", params.log_db_auth_id])
        if params.log_level:
            args.extend(["--log_level", params.log_level.value])
        if params.log_dir:
            args.extend(["--log_dir", params.log_dir])
        if params.no_progress:
            args.append("--no_progress")
        if params.no_banner:
            args.append("--no_banner")
        return args

    def _build_common_options(self, params) -> List[str]:
        """Build common option flags for sync/run commands (no auth/log_db)."""
        args = []
        if params.log_level:
            args.extend(["--log_level", params.log_level.value])
        if params.log_dir:
            args.extend(["--log_dir", params.log_dir])
        if params.no_progress:
            args.append("--no_progress")
        if params.no_banner:
            args.append("--no_banner")
        return args

    def _build_logdb_init(self, params: LogdbInitParams) -> List[str]:
        """Build logdb init command."""
        cmd = [str(self.binary_path), "logdb", "init"]
        cmd.extend(self._build_global_options(params))
        return cmd

    def _build_logdb_drop(self, params: LogdbDropParams) -> List[str]:
        """Build logdb drop command."""
        cmd = [str(self.binary_path), "logdb", "drop"]
        cmd.extend(self._build_global_options(params))
        if params.confirm:
            cmd.append("--confirm")
        return cmd

    def _build_logdb_truncate(self, params: LogdbTruncateParams) -> List[str]:
        """Build logdb truncate command."""
        cmd = [str(self.binary_path), "logdb", "truncate"]
        cmd.extend(self._build_global_options(params))
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.confirm:
            cmd.append("--confirm")
        return cmd

    def _build_logdb_locks(self, params: LogdbLocksParams) -> List[str]:
        """Build logdb locks command."""
        cmd = [str(self.binary_path), "logdb", "locks"]
        cmd.extend(self._build_global_options(params))
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        return cmd

    def _build_logdb_release_locks(self, params: LogdbReleaseLocksParams) -> List[str]:
        """Build logdb release-locks command."""
        cmd = [str(self.binary_path), "logdb", "release-locks"]
        cmd.extend(self._build_global_options(params))
        if params.max_age_hours is not None:
            cmd.extend(["--max_age_hours", str(params.max_age_hours)])
        if params.table_id:
            cmd.extend(["--table_id", params.table_id])
        if params.confirm:
            cmd.append("--confirm")
        return cmd

    def _build_config_create(self, params: ConfigCreateParams) -> List[str]:
        """Build config create command."""
        cmd = [str(self.binary_path), "config", "create"]
        cmd.extend(self._build_global_options(params))

        # Source
        cmd.extend(["--source_db_auth_id", params.source_db_auth_id])
        if params.source_db_name:
            cmd.extend(["--source_db_name", params.source_db_name])
        if params.source_schema_name:
            cmd.extend(["--source_schema_name", params.source_schema_name])

        # Filtering
        if params.include:
            cmd.extend(["-i", params.include])
        if params.exclude:
            cmd.extend(["-e", params.exclude])
        if params.min_rows is not None:
            cmd.extend(["--min_rows", str(params.min_rows)])
        if params.max_rows is not None:
            cmd.extend(["--max_rows", str(params.max_rows)])

        # Incremental
        if params.incremental_table:
            for inc in params.incremental_table:
                cmd.extend(["--incremental_table", inc])
        if params.incremental_safety_lag is not None:
            cmd.extend(["--incremental_safety_lag", str(params.incremental_safety_lag)])

        # Storage
        if params.output_dir:
            cmd.extend(["--output_dir", params.output_dir])
        if params.target_storage_id:
            cmd.extend(["--target_storage_id", params.target_storage_id])
        if params.sub_path:
            cmd.extend(["--sub_path", params.sub_path])

        # FastBCP
        if params.fastbcp_dir_path:
            cmd.extend(["--fastbcp_dir_path", params.fastbcp_dir_path])
        if params.fastbcp_p is not None:
            cmd.extend(["-p", str(params.fastbcp_p)])
        if params.n_jobs is not None:
            cmd.extend(["--n_jobs", str(params.n_jobs)])
        if params.compression_type:
            cmd.extend(["--compression_type", params.compression_type.value])
        if params.large_table_threshold is not None:
            cmd.extend(["--large_table_threshold", str(params.large_table_threshold)])
        if params.fastbcp_table_config:
            cmd.extend(["--fastbcp_table_config", params.fastbcp_table_config])

        # Publishing
        if params.publish_target:
            cmd.extend(["--publish_target", params.publish_target])
        if params.publish_method:
            cmd.extend(["--publish_method", params.publish_method.value])
        if params.publish_database_name:
            cmd.extend(["--publish_database_name", params.publish_database_name])
        if params.publish_schema_pattern:
            cmd.extend(["--publish_schema_pattern", params.publish_schema_pattern])
        if params.publish_table_pattern:
            cmd.extend(["--publish_table_pattern", params.publish_table_pattern])

        # Features
        if params.no_views:
            cmd.append("--no_views")
        if params.pk_constraints:
            cmd.append("--pk_constraints")
        if params.generate_metadata:
            cmd.append("--generate_metadata")
        if params.manifest_name:
            cmd.extend(["--manifest_name", params.manifest_name])

        # Other
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.error_action:
            cmd.extend(["--error_action", params.error_action.value])
        if params.env_name:
            cmd.extend(["--env_name", params.env_name])

        return cmd

    def _build_config_delete(self, params: ConfigDeleteParams) -> List[str]:
        """Build config delete command."""
        cmd = [str(self.binary_path), "config", "delete"]
        cmd.extend(self._build_global_options(params))
        cmd.extend(["--sync_id", params.sync_id])
        if params.confirm:
            cmd.append("--confirm")
        return cmd

    def _build_config_list(self, params: ConfigListParams) -> List[str]:
        """Build config list command."""
        cmd = [str(self.binary_path), "config", "list"]
        cmd.extend(self._build_global_options(params))
        if params.env_name:
            cmd.extend(["--env_name", params.env_name])
        return cmd

    def _build_sync(self, params: SyncParams) -> List[str]:
        """Build sync command."""
        cmd = [str(self.binary_path), "sync"]
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.resume:
            cmd.append("--resume")
        if params.run_id:
            cmd.extend(["--run_id", params.run_id])
        if params.auth_file:
            cmd.extend(["-a", params.auth_file])
        if params.fastbcp_dir_path:
            cmd.extend(["--fastbcp_dir_path", params.fastbcp_dir_path])
        cmd.extend(self._build_common_options(params))
        return cmd

    def _build_sync_export(self, params: SyncExportParams) -> List[str]:
        """Build sync[export] command."""
        cmd = [str(self.binary_path), "sync[export]"]
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.auth_file:
            cmd.extend(["-a", params.auth_file])
        if params.fastbcp_dir_path:
            cmd.extend(["--fastbcp_dir_path", params.fastbcp_dir_path])
        cmd.extend(self._build_common_options(params))
        return cmd

    def _build_sync_publish(self, params: SyncPublishParams) -> List[str]:
        """Build sync[publish] command."""
        cmd = [str(self.binary_path), "sync[publish]"]
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.run_id:
            cmd.extend(["--run_id", params.run_id])
        if params.auth_file:
            cmd.extend(["-a", params.auth_file])
        cmd.extend(self._build_common_options(params))
        return cmd

    def _build_run(self, params: RunParams) -> List[str]:
        """Build run command."""
        cmd = [str(self.binary_path), "run"]
        cmd.extend(["-c", params.config])
        if params.auth_file:
            cmd.extend(["-a", params.auth_file])
        if params.log_db_auth_id:
            cmd.extend(["--log_db_auth_id", params.log_db_auth_id])
        cmd.extend(self._build_common_options(params))
        return cmd

    def _build_status(self, params: StatusParams) -> List[str]:
        """Build status command."""
        cmd = [str(self.binary_path), "status"]
        cmd.extend(self._build_global_options(params))
        if params.sync_id:
            cmd.extend(["--sync_id", params.sync_id])
        if params.run_id:
            cmd.extend(["--run_id", params.run_id])
        if params.verbose:
            cmd.append("--verbose")
        return cmd

    def _build_cleanup(self, params: CleanupParams) -> List[str]:
        """Build cleanup command."""
        cmd = [str(self.binary_path), "cleanup"]
        cmd.extend(self._build_global_options(params))
        cmd.extend(["--sync_id", params.sync_id])
        if params.older_than:
            cmd.extend(["--older-than", params.older_than])
        if params.status:
            cmd.extend(["--status", params.status.value])
        if params.dry_run:
            cmd.append("--dry-run")
        return cmd

    def format_command_display(self, command: List[str]) -> str:
        """
        Format command for display.

        Args:
            command: Command list

        Returns:
            Formatted command string
        """
        # Format for readability
        formatted_parts = [command[0]]  # Binary path

        i = 1
        while i < len(command):
            if i < len(command) - 1 and command[i].startswith("-"):
                # Check if next item is a value (not another flag)
                next_item = command[i + 1]
                if not next_item.startswith("-"):
                    param = command[i]
                    value = next_item
                    # Quote values that might contain spaces
                    if " " in value:
                        formatted_parts.append(f'{param} "{value}"')
                    else:
                        formatted_parts.append(f"{param} {value}")
                    i += 2
                else:
                    formatted_parts.append(command[i])
                    i += 1
            else:
                formatted_parts.append(command[i])
                i += 1

        return " \\\n  ".join(formatted_parts)

    def execute_command(
        self, command: List[str], timeout: int = 3600, log_dir: Optional[Path] = None
    ) -> Tuple[int, str, str]:
        """
        Execute a LakeXpress command.

        Args:
            command: Command to execute
            timeout: Timeout in seconds (default: 3600 = 1 hour)
            log_dir: Directory for execution logs

        Returns:
            Tuple of (return_code, stdout, stderr)

        Raises:
            subprocess.TimeoutExpired: If execution exceeds timeout
            LakeXpressError: If execution fails
        """
        start_time = datetime.now()

        logger.info(f"Executing LakeXpress command: {' '.join(command)}")

        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=timeout, check=False
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(
                f"LakeXpress completed in {duration:.2f}s with return code {result.returncode}"
            )

            # Save logs if directory provided
            if log_dir:
                self._save_execution_log(
                    log_dir,
                    command,
                    result.returncode,
                    result.stdout,
                    result.stderr,
                    duration,
                )

            return result.returncode, result.stdout, result.stderr

        except subprocess.TimeoutExpired as e:
            logger.error(f"LakeXpress execution timed out after {timeout}s")
            raise LakeXpressError(f"Execution timed out after {timeout} seconds") from e

        except Exception as e:
            logger.error(f"LakeXpress execution failed: {e}")
            raise LakeXpressError(f"Execution failed: {e}") from e

    def _save_execution_log(
        self,
        log_dir: Path,
        command: List[str],
        return_code: int,
        stdout: str,
        stderr: str,
        duration: float,
    ) -> None:
        """Save execution log to file."""
        try:
            log_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"lakexpress_{timestamp}.log"

            with open(log_file, "w") as f:
                f.write("LakeXpress Execution Log\n")
                f.write(f"{'=' * 80}\n\n")
                f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                f.write(f"Duration: {duration:.2f} seconds\n")
                f.write(f"Return Code: {return_code}\n\n")
                f.write(f"Command:\n{' '.join(command)}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDOUT:\n{stdout}\n\n")
                f.write(f"{'=' * 80}\n")
                f.write(f"STDERR:\n{stderr}\n")

            logger.info(f"Execution log saved to: {log_file}")

        except Exception as e:
            logger.warning(f"Failed to save execution log: {e}")


def get_supported_capabilities() -> Dict[str, Any]:
    """
    Get supported source databases, log databases, storage backends,
    publishing targets, compression types, and available commands.

    Returns:
        Dictionary with all supported capabilities
    """
    return {
        "Source Databases": [
            "SQL Server (sqlserver)",
            "PostgreSQL (postgresql)",
            "Oracle (oracle)",
            "MySQL (mysql)",
            "MariaDB (mariadb)",
        ],
        "Log Databases": [
            "SQL Server (sqlserver)",
            "PostgreSQL (postgresql)",
            "MySQL (mysql)",
            "MariaDB (mariadb)",
            "SQLite (sqlite)",
            "DuckDB (duckdb)",
        ],
        "Storage Backends": [
            "Local filesystem (local)",
            "AWS S3 (s3)",
            "S3-compatible (s3compatible)",
            "Google Cloud Storage (gcs)",
            "Azure ADLS Gen2 (azure_adls)",
            "OneLake (onelake)",
        ],
        "Publishing Targets": [
            "Snowflake (snowflake)",
            "Databricks (databricks)",
            "Microsoft Fabric (fabric)",
            "BigQuery (bigquery)",
            "MotherDuck (motherduck)",
            "AWS Glue (glue)",
            "DuckLake (ducklake)",
        ],
        "Compression Types": ["Zstd", "Snappy", "Gzip", "Lz4", "None"],
        "Commands": {
            "logdb init": "Create the log database schema",
            "logdb drop": "Drop the log database schema",
            "logdb truncate": "Clear all data, keep schema",
            "logdb locks": "Show currently locked tables",
            "logdb release-locks": "Release stale or stuck locks",
            "config create": "Create a new sync configuration",
            "config delete": "Delete an existing sync configuration",
            "config list": "List all sync configurations",
            "sync": "Execute sync (export + publish)",
            "sync[export]": "Execute export only",
            "sync[publish]": "Execute publish only",
            "run": "Run export from YAML config file",
            "status": "Query sync/run status",
            "cleanup": "Remove orphaned or stale runs",
        },
    }


def suggest_workflow(
    source_type: str,
    destination: str,
    publish_target: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Suggest an ordered workflow of LakeXpress commands based on use case.

    Args:
        source_type: Source database type (e.g., 'sqlserver', 'postgresql')
        destination: Storage destination (e.g., 'local', 's3', 'azure_adls')
        publish_target: Optional publishing target (e.g., 'snowflake', 'databricks')

    Returns:
        Dictionary with ordered workflow steps and example parameters
    """
    steps = []

    # Step 1: Initialize log database (if first time)
    steps.append(
        {
            "step": 1,
            "command": "logdb init",
            "description": "Initialize the log database schema (first-time setup only)",
            "example": (
                "LakeXpress logdb init -a auth.json --log_db_auth_id export_db"
            ),
        }
    )

    # Step 2: Create sync configuration
    config_desc = f"Create sync configuration for {source_type} source"
    config_example = (
        "LakeXpress config create -a auth.json --log_db_auth_id export_db "
        "--source_db_auth_id source_db"
    )
    if destination == "local":
        config_example += " --output_dir ./exports"
        config_desc += " with local storage"
    else:
        config_example += f" --target_storage_id {destination}_storage"
        config_desc += f" with {destination} storage"

    if publish_target:
        config_example += f" --publish_target {publish_target}_target"
        config_desc += f" and {publish_target} publishing"

    steps.append(
        {
            "step": 2,
            "command": "config create",
            "description": config_desc,
            "example": config_example,
        }
    )

    # Step 3: Execute sync
    if publish_target:
        steps.append(
            {
                "step": 3,
                "command": "sync",
                "description": "Execute full sync (export + publish)",
                "example": "LakeXpress sync --sync_id <sync_id>",
            }
        )
        steps.append(
            {
                "step": "3a",
                "command": "sync[export] + sync[publish]",
                "description": "Alternative: run export and publish separately",
                "example": (
                    "LakeXpress 'sync[export]' --sync_id <sync_id>\n"
                    "LakeXpress 'sync[publish]' --sync_id <sync_id>"
                ),
            }
        )
    else:
        steps.append(
            {
                "step": 3,
                "command": "sync[export]",
                "description": "Execute export only (no publishing target configured)",
                "example": "LakeXpress 'sync[export]' --sync_id <sync_id>",
            }
        )

    # Step 4: Check status
    steps.append(
        {
            "step": 4,
            "command": "status",
            "description": "Check the status of the sync run",
            "example": "LakeXpress status -a auth.json --log_db_auth_id export_db --sync_id <sync_id>",
        }
    )

    return {
        "source_type": source_type,
        "destination": destination,
        "publish_target": publish_target,
        "steps": steps,
    }
