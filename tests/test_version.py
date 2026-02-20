"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.version import (
    LakeXpressVersion,
    VersionDetector,
    VERSION_REGISTRY,
)


class TestLakeXpressVersion:
    """Tests for LakeXpressVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'LakeXpress X.Y.Z' string."""
        v = LakeXpressVersion.parse("LakeXpress 0.2.8")
        assert v.major == 0
        assert v.minor == 2
        assert v.patch == 8

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = LakeXpressVersion.parse("0.2.8")
        assert v == LakeXpressVersion(0, 2, 8)

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = LakeXpressVersion.parse("  LakeXpress 1.2.3  ")
        assert v == LakeXpressVersion(1, 2, 3)

    def test_parse_invalid_string(self):
        """Test that an unparseable string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            LakeXpressVersion.parse("no version here")

    def test_parse_incomplete_version(self):
        """Test that an incomplete version string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            LakeXpressVersion.parse("0.2")

    def test_str_representation(self):
        """Test string representation."""
        v = LakeXpressVersion(0, 2, 8)
        assert str(v) == "0.2.8"

    def test_equality(self):
        """Test equality comparison."""
        a = LakeXpressVersion(0, 2, 8)
        b = LakeXpressVersion(0, 2, 8)
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = LakeXpressVersion(0, 2, 8)
        b = LakeXpressVersion(0, 3, 0)
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = LakeXpressVersion(0, 2, 7)
        b = LakeXpressVersion(0, 2, 8)
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = LakeXpressVersion(0, 2, 8)
        b = LakeXpressVersion(0, 2, 7)
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch."""
        versions = [
            LakeXpressVersion(0, 1, 0),
            LakeXpressVersion(0, 2, 0),
            LakeXpressVersion(0, 2, 8),
            LakeXpressVersion(0, 3, 0),
            LakeXpressVersion(1, 0, 0),
        ]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]


class TestVersionDetector:
    """Tests for VersionDetector class."""

    @patch("src.version.subprocess.run")
    def test_detect_success(self, mock_run):
        """Test successful version detection."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version == LakeXpressVersion(0, 2, 8)
        mock_run.assert_called_once_with(
            ["/fake/binary", "--version", "--no_banner"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    @patch("src.version.subprocess.run")
    def test_detect_failure_no_match(self, mock_run):
        """Test detection when output doesn't match version pattern."""
        mock_result = Mock()
        mock_result.stdout = "Unknown output"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_timeout(self, mock_run):
        """Test detection handles timeout gracefully."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_binary_not_found(self, mock_run):
        """Test detection handles missing binary gracefully."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.version.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 0.2.8\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        assert "postgresql" in caps.source_databases
        assert "sqlite" in caps.log_databases
        assert "s3" in caps.storage_backends
        assert "snowflake" in caps.publish_targets
        assert "Zstd" in caps.compression_types
        assert caps.supports_no_banner is True
        assert caps.supports_version_flag is True
        assert caps.supports_incremental is True
        assert caps.supports_cleanup is True

    @patch("src.version.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "LakeXpress 1.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities (0.2.8)
        assert caps == VERSION_REGISTRY["0.2.8"]

    @patch("src.version.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps == VERSION_REGISTRY["0.2.8"]

    def test_registry_028_source_completeness(self):
        """Test that 0.2.8 registry has all expected source databases."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "sqlserver",
            "postgresql",
            "oracle",
            "mysql",
            "mariadb",
        }
        assert caps.source_databases == expected

    def test_registry_028_log_completeness(self):
        """Test that 0.2.8 registry has all expected log databases."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "sqlserver",
            "postgresql",
            "mysql",
            "mariadb",
            "sqlite",
            "duckdb",
        }
        assert caps.log_databases == expected

    def test_registry_028_storage_completeness(self):
        """Test that 0.2.8 registry has all expected storage backends."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "local",
            "s3",
            "s3compatible",
            "gcs",
            "azure_adls",
            "onelake",
        }
        assert caps.storage_backends == expected

    def test_registry_028_publish_completeness(self):
        """Test that 0.2.8 registry has all expected publish targets."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {
            "snowflake",
            "databricks",
            "fabric",
            "bigquery",
            "motherduck",
            "glue",
            "ducklake",
        }
        assert caps.publish_targets == expected

    def test_registry_028_compression_completeness(self):
        """Test that 0.2.8 registry has all expected compression types."""
        caps = VERSION_REGISTRY["0.2.8"]
        expected = {"Zstd", "Snappy", "Gzip", "Lz4", "None"}
        assert caps.compression_types == expected

    def test_registry_028_command_completeness(self):
        """Test that 0.2.8 registry has all 14 commands."""
        caps = VERSION_REGISTRY["0.2.8"]
        assert len(caps.commands) == 14
        expected = {
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
        }
        assert caps.commands == expected
