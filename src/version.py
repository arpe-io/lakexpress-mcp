"""
Version detection and capabilities registry for LakeXpress.

This module detects the installed LakeXpress binary version and maps it
to known capabilities (supported source databases, log databases, storage backends,
publish targets, compression types, and feature flags).
"""

import logging
import re
import subprocess
from dataclasses import dataclass
from functools import total_ordering
from typing import Dict, FrozenSet, Optional


logger = logging.getLogger(__name__)


@total_ordering
@dataclass(frozen=True)
class LakeXpressVersion:
    """Represents a LakeXpress version number (X.Y.Z)."""

    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, version_string: str) -> "LakeXpressVersion":
        """Parse a version string like 'LakeXpress 0.2.8' or '0.2.8'.

        Args:
            version_string: Version string to parse

        Returns:
            LakeXpressVersion instance

        Raises:
            ValueError: If the string cannot be parsed
        """
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_string.strip())
        if not match:
            raise ValueError(f"Cannot parse version from: {version_string!r}")
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LakeXpressVersion):
            return NotImplemented
        return self._tuple == other._tuple

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, LakeXpressVersion):
            return NotImplemented
        return self._tuple < other._tuple

    @property
    def _tuple(self) -> tuple:
        return (self.major, self.minor, self.patch)


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific LakeXpress version."""

    source_databases: FrozenSet[str]
    log_databases: FrozenSet[str]
    storage_backends: FrozenSet[str]
    publish_targets: FrozenSet[str]
    compression_types: FrozenSet[str]
    commands: FrozenSet[str]
    supports_no_banner: bool = False
    supports_version_flag: bool = False
    supports_incremental: bool = False
    supports_cleanup: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.2.8": VersionCapabilities(
        source_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "oracle",
                "mysql",
                "mariadb",
            ]
        ),
        log_databases=frozenset(
            [
                "sqlserver",
                "postgresql",
                "mysql",
                "mariadb",
                "sqlite",
                "duckdb",
            ]
        ),
        storage_backends=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "gcs",
                "azure_adls",
                "onelake",
            ]
        ),
        publish_targets=frozenset(
            [
                "snowflake",
                "databricks",
                "fabric",
                "bigquery",
                "motherduck",
                "glue",
                "ducklake",
            ]
        ),
        compression_types=frozenset(
            [
                "Zstd",
                "Snappy",
                "Gzip",
                "Lz4",
                "None",
            ]
        ),
        commands=frozenset(
            [
                "logdb_init",
                "logdb_drop",
                "logdb_truncate",
                "logdb_locks",
                "logdb_release_locks",
                "config_create",
                "config_delete",
                "config_list",
                "sync",
                "sync_export",
                "sync_publish",
                "run",
                "status",
                "cleanup",
            ]
        ),
        supports_no_banner=True,
        supports_version_flag=True,
        supports_incremental=True,
        supports_cleanup=True,
    ),
}

# Pre-sorted list of known versions for lookup
_SORTED_VERSIONS = sorted(
    [(LakeXpressVersion.parse(k), v) for k, v in VERSION_REGISTRY.items()],
    key=lambda x: x[0],
)


class VersionDetector:
    """Detects LakeXpress binary version and resolves capabilities."""

    def __init__(self, binary_path: str):
        self._binary_path = binary_path
        self._detected_version: Optional[LakeXpressVersion] = None
        self._detection_done = False

    def detect(self, timeout: int = 10) -> Optional[LakeXpressVersion]:
        """Detect the LakeXpress version by running the binary.

        Runs ``[binary_path, "--version", "--no_banner"]`` and parses the output.
        Results are cached after the first call.

        Args:
            timeout: Subprocess timeout in seconds

        Returns:
            LakeXpressVersion if detected, None otherwise
        """
        if self._detection_done:
            return self._detected_version

        self._detection_done = True

        try:
            result = subprocess.run(
                [self._binary_path, "--version", "--no_banner"],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            output = (result.stdout + result.stderr).strip()
            match = re.search(r"LakeXpress\s+(\d+)\.(\d+)\.(\d+)", output)
            if match:
                self._detected_version = LakeXpressVersion(
                    major=int(match.group(1)),
                    minor=int(match.group(2)),
                    patch=int(match.group(3)),
                )
                logger.info(f"Detected LakeXpress version: {self._detected_version}")
            else:
                logger.warning(f"Could not parse version from output: {output!r}")
        except subprocess.TimeoutExpired:
            logger.warning("Version detection timed out")
        except FileNotFoundError:
            logger.warning(f"Binary not found at: {self._binary_path}")
        except Exception as e:
            logger.warning(f"Version detection failed: {e}")

        return self._detected_version

    @property
    def capabilities(self) -> VersionCapabilities:
        """Resolve capabilities for the detected version.

        If the detected version matches a registry entry exactly, return that.
        If the version is newer than all known entries, return the latest known.
        If detection failed, return the latest known entry as a fallback.
        """
        if not self._detection_done:
            self.detect()

        if not _SORTED_VERSIONS:
            # No registry entries at all — return empty capabilities
            return VersionCapabilities(
                source_databases=frozenset(),
                log_databases=frozenset(),
                storage_backends=frozenset(),
                publish_targets=frozenset(),
                compression_types=frozenset(),
                commands=frozenset(),
            )

        if self._detected_version is None:
            # Detection failed — fall back to latest known
            return _SORTED_VERSIONS[-1][1]

        # Find the highest registry entry <= detected version
        best: Optional[VersionCapabilities] = None
        for ver, caps in _SORTED_VERSIONS:
            if ver <= self._detected_version:
                best = caps
            else:
                break

        # If detected version is older than all known, fall back to latest
        return best if best is not None else _SORTED_VERSIONS[-1][1]
