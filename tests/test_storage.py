# tests/test_storage.py

import pytest
from unittest.mock import patch, MagicMock
import pyarrow.fs as pafs

from app.storage import open_uri


# ---------------------------------------------------------------------------
# file://
# ---------------------------------------------------------------------------

class TestFileScheme:
    def test_returns_local_filesystem(self):
        fs, _ = open_uri("file:///data/monthly.parquet")
        assert isinstance(fs, pafs.LocalFileSystem)

    def test_returns_correct_path(self):
        _, path = open_uri("file:///data/monthly.parquet")
        assert path == "/data/monthly.parquet"

    def test_nested_path(self):
        _, path = open_uri("file:///home/user/project/data/file.parquet")
        assert path == "/home/user/project/data/file.parquet"


# ---------------------------------------------------------------------------
# gs://
# ---------------------------------------------------------------------------

class TestGcsScheme:
    def test_returns_gcs_filesystem(self):
        with patch("app.storage.pafs.GcsFileSystem", return_value=MagicMock(spec=pafs.GcsFileSystem)):
            fs, _ = open_uri("gs://my-bucket/path/to/file.parquet")
        assert fs is not None

    def test_returns_correct_path(self):
        with patch("app.storage.pafs.GcsFileSystem", return_value=MagicMock(spec=pafs.GcsFileSystem)):
            _, path = open_uri("gs://my-bucket/path/to/file.parquet")
        assert path == "my-bucket/path/to/file.parquet"

    def test_bucket_only_no_leading_slash(self):
        with patch("app.storage.pafs.GcsFileSystem", return_value=MagicMock(spec=pafs.GcsFileSystem)):
            _, path = open_uri("gs://my-bucket/file.parquet")
        assert path == "my-bucket/file.parquet"

    def test_no_double_slash_in_path(self):
        with patch("app.storage.pafs.GcsFileSystem", return_value=MagicMock(spec=pafs.GcsFileSystem)):
            _, path = open_uri("gs://my-bucket/nested/path/file.parquet")
        assert "//" not in path


# ---------------------------------------------------------------------------
# s3://
# ---------------------------------------------------------------------------

class TestS3Scheme:
    def test_returns_s3_filesystem(self):
        with patch("app.storage.pafs.S3FileSystem", return_value=MagicMock(spec=pafs.S3FileSystem)):
            fs, _ = open_uri("s3://my-bucket/path/to/file.parquet")
        assert fs is not None

    def test_returns_correct_path(self):
        with patch("app.storage.pafs.S3FileSystem", return_value=MagicMock(spec=pafs.S3FileSystem)):
            _, path = open_uri("s3://my-bucket/path/to/file.parquet")
        assert path == "my-bucket/path/to/file.parquet"

    def test_bucket_only_no_leading_slash(self):
        with patch("app.storage.pafs.S3FileSystem", return_value=MagicMock(spec=pafs.S3FileSystem)):
            _, path = open_uri("s3://my-bucket/file.parquet")
        assert path == "my-bucket/file.parquet"

    def test_no_double_slash_in_path(self):
        with patch("app.storage.pafs.S3FileSystem", return_value=MagicMock(spec=pafs.S3FileSystem)):
            _, path = open_uri("s3://my-bucket/nested/path/file.parquet")
        assert "//" not in path


# ---------------------------------------------------------------------------
# unsupported schemes
# ---------------------------------------------------------------------------

class TestUnsupportedScheme:
    def test_raises_for_http(self):
        with pytest.raises(ValueError, match="Unsupported URI scheme"):
            open_uri("http://example.com/file.parquet")

    def test_raises_for_az(self):
        with pytest.raises(ValueError, match="Unsupported URI scheme"):
            open_uri("az://container/file.parquet")

    def test_raises_for_empty_scheme(self):
        with pytest.raises(ValueError):
            open_uri("/local/path/file.parquet")

    def test_error_message_includes_scheme(self):
        with pytest.raises(ValueError, match="ftp"):
            open_uri("ftp://host/file.parquet")