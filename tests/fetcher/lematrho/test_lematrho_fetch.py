# Copyright 2025 Entalpic
import gzip
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from lematerial_fetcher.database.postgres import DatasetVersions, StructuresDatabase
from lematerial_fetcher.fetcher.lematrho.fetch import (
    RELAX_CALC_TYPE,
    STATIC_CALC_TYPE,
    STATIC_FILES,
    VALID_PREFIXES,
    LeMatRhoFetcher,
)
from lematerial_fetcher.fetcher.lematrho.utils import (
    build_raw_structure,
    download_gz_file_from_s3,
)
from lematerial_fetcher.utils.config import FetcherConfig


@pytest.fixture
def mock_aws_client():
    return MagicMock()


@pytest.fixture
def mock_db():
    return MagicMock(spec=StructuresDatabase)


@pytest.fixture
def mock_version_db():
    return MagicMock(spec=DatasetVersions)


@pytest.fixture
def mock_config():
    return FetcherConfig(
        base_url="https://api.test.com",
        db_conn_str="postgresql://test:test@localhost:5432/test",
        table_name="test_lematrho_raw",
        page_limit=10,
        page_offset=0,
        mp_bucket_name="",
        mp_bucket_prefix="",
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
        log_every=100,
        lematrho_bucket_name="lemat-rho",
        lematrho_grid_shape=(15, 15, 15),
    )


# ---------------------------------------------------------------------------
# get_items_to_process tests
# ---------------------------------------------------------------------------


class TestGetItemsToProcess:
    def test_filters_by_valid_prefix(self, mock_aws_client, mock_config, mock_version_db):
        """Only folders with oqmd-, mp-, or agm prefixes should be returned."""
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "agm000001/"},
                    {"Prefix": "mp-12345/"},
                    {"Prefix": "oqmd-678/"},
                    {"Prefix": "test-invalid/"},
                    {"Prefix": "random-folder/"},
                ]
            }
        ]

        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = LeMatRhoFetcher(config=mock_config, debug=True)
            fetcher.aws_client = mock_aws_client

            items = fetcher.get_items_to_process()

        assert items.total_count == 3
        assert set(items.items) == {"agm000001", "mp-12345", "oqmd-678"}

    def test_ignores_unknown_prefixes(self, mock_aws_client, mock_config, mock_version_db):
        """Folders like 'test-123/' or 'data/' should be excluded."""
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "test-123/"},
                    {"Prefix": "data/"},
                    {"Prefix": "backup-20240101/"},
                ]
            }
        ]

        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = LeMatRhoFetcher(config=mock_config, debug=True)
            fetcher.aws_client = mock_aws_client

            items = fetcher.get_items_to_process()

        assert items.total_count == 0
        assert items.items == []

    def test_handles_empty_bucket(self, mock_aws_client, mock_config, mock_version_db):
        """Empty bucket should return zero items."""
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]

        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = LeMatRhoFetcher(config=mock_config, debug=True)
            fetcher.aws_client = mock_aws_client

            items = fetcher.get_items_to_process()

        assert items.total_count == 0
        assert items.items == []

    def test_handles_pagination(self, mock_aws_client, mock_config, mock_version_db):
        """Should handle multiple pages of results."""
        mock_paginator = MagicMock()
        mock_aws_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "agm000001/"}]},
            {"CommonPrefixes": [{"Prefix": "agm000002/"}]},
        ]

        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = LeMatRhoFetcher(config=mock_config, debug=True)
            fetcher.aws_client = mock_aws_client

            items = fetcher.get_items_to_process()

        assert items.total_count == 2
        assert items.items == ["agm000001", "agm000002"]

    def test_raises_without_bucket_name(self, mock_aws_client, mock_version_db):
        """Should raise ValueError if bucket name is not configured."""
        config = FetcherConfig(
            base_url="https://api.test.com",
            db_conn_str="postgresql://test:test@localhost:5432/test",
            table_name="test_table",
            page_limit=10,
            page_offset=0,
            mp_bucket_name="",
            mp_bucket_prefix="",
            log_dir="./logs",
            max_retries=3,
            num_workers=2,
            retry_delay=2,
            log_every=100,
            lematrho_bucket_name=None,
        )

        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db
            mock_version_db.get_last_synced_version.return_value = None

            fetcher = LeMatRhoFetcher(config=config, debug=True)
            fetcher.aws_client = mock_aws_client

            with pytest.raises(ValueError, match="lematrho_bucket_name"):
                fetcher.get_items_to_process()


# ---------------------------------------------------------------------------
# _process_batch tests
# ---------------------------------------------------------------------------


class TestProcessBatch:
    def test_happy_path(self, mock_config):
        """Successful processing: downloads vasprun + all 4 charge files, inserts to DB."""
        mock_client = MagicMock()
        mock_db_instance = MagicMock(spec=StructuresDatabase)
        mock_structure = MagicMock()
        mock_structure.as_dict.return_value = {"lattice": {}, "sites": []}

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ) as mock_db_cls,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.parse_vasprun_structure"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.compress_chgcar"
            ) as mock_compress,
        ):
            mock_auth.return_value = mock_client
            mock_db_cls.return_value = mock_db_instance
            mock_download.return_value = b"fake data"
            mock_parse.return_value = mock_structure
            mock_compress.return_value = [[[1.0, 2.0]]]

            result = LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        assert result is True
        mock_db_instance.insert_data.assert_called_once()
        # vasprun + 4 charge files = 5 downloads
        assert mock_download.call_count == 5
        # 4 CHGCAR/AECCAR compressions
        assert mock_compress.call_count == 4

    def test_missing_vasprun_returns_false(self, mock_config):
        """If vasprun.xml.gz is missing, should return False."""
        mock_client = MagicMock()

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ),
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
        ):
            mock_auth.return_value = mock_client
            mock_download.side_effect = Exception("NoSuchKey: vasprun.xml.gz")

            result = LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        assert result is False

    def test_missing_aeccar1_still_processes_others(self, mock_config):
        """If AECCAR1.gz is missing, other files should still be processed."""
        mock_client = MagicMock()
        mock_db_instance = MagicMock(spec=StructuresDatabase)
        mock_structure = MagicMock()
        mock_structure.as_dict.return_value = {"lattice": {}, "sites": []}

        def download_side_effect(client, bucket, key):
            if "AECCAR1.gz" in key:
                raise Exception("NoSuchKey")
            return b"fake data"

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ) as mock_db_cls,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.parse_vasprun_structure"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.compress_chgcar"
            ) as mock_compress,
        ):
            mock_auth.return_value = mock_client
            mock_db_cls.return_value = mock_db_instance
            mock_download.side_effect = download_side_effect
            mock_parse.return_value = mock_structure
            mock_compress.return_value = [[[1.0]]]

            result = LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        assert result is True
        mock_db_instance.insert_data.assert_called_once()
        # vasprun + 3 successful charge files (AECCAR1 failed) = 4 downloads succeed
        # But download is called 5 times (1 vasprun + 4 charge files, 1 raises)
        assert mock_download.call_count == 5
        # Only 3 compressions (AECCAR1 failed before compression)
        assert mock_compress.call_count == 3

    def test_all_charge_files_missing_still_inserts(self, mock_config):
        """If all charge files fail but vasprun succeeds, still insert with None grids."""
        mock_client = MagicMock()
        mock_db_instance = MagicMock(spec=StructuresDatabase)
        mock_structure = MagicMock()
        mock_structure.as_dict.return_value = {"lattice": {}, "sites": []}

        call_count = {"n": 0}

        def download_side_effect(client, bucket, key):
            call_count["n"] += 1
            if "vasprun" in key:
                return b"fake vasprun"
            raise Exception("NoSuchKey")

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ) as mock_db_cls,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.parse_vasprun_structure"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.compress_chgcar"
            ) as mock_compress,
        ):
            mock_auth.return_value = mock_client
            mock_db_cls.return_value = mock_db_instance
            mock_download.side_effect = download_side_effect
            mock_parse.return_value = mock_structure

            result = LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        assert result is True
        mock_db_instance.insert_data.assert_called_once()
        # No compressions since all charge file downloads failed
        mock_compress.assert_not_called()

        # Verify the inserted structure has None grids
        inserted = mock_db_instance.insert_data.call_args[0][0]
        assert inserted.attributes["compressed_charge_density"] is None
        assert inserted.attributes["compressed_aeccar0"] is None
        assert inserted.attributes["compressed_aeccar1"] is None
        assert inserted.attributes["compressed_aeccar2"] is None

    def test_critical_error_sets_manager_flag(self, mock_config):
        """A connection error should flag the manager_dict for shutdown."""
        manager_dict = {"occurred": False}

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ),
        ):
            mock_auth.side_effect = Exception("Connection refused")

            result = LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, manager_dict
            )

        assert result is False
        assert manager_dict["occurred"] is True

    def test_correct_s3_keys(self, mock_config):
        """Verify the exact S3 keys constructed for downloads."""
        mock_client = MagicMock()
        mock_db_instance = MagicMock(spec=StructuresDatabase)
        mock_structure = MagicMock()
        mock_structure.as_dict.return_value = {}

        download_calls = []

        def capture_downloads(client, bucket, key):
            download_calls.append(key)
            return b"data"

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ) as mock_db_cls,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.parse_vasprun_structure"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.compress_chgcar"
            ) as mock_compress,
        ):
            mock_auth.return_value = mock_client
            mock_db_cls.return_value = mock_db_instance
            mock_download.side_effect = capture_downloads
            mock_parse.return_value = mock_structure
            mock_compress.return_value = []

            LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        expected_keys = [
            f"agm000001/{RELAX_CALC_TYPE}/vasprun.xml.gz",
            f"agm000001/{STATIC_CALC_TYPE}/CHGCAR.gz",
            f"agm000001/{STATIC_CALC_TYPE}/AECCAR0.gz",
            f"agm000001/{STATIC_CALC_TYPE}/AECCAR1.gz",
            f"agm000001/{STATIC_CALC_TYPE}/AECCAR2.gz",
        ]
        assert download_calls == expected_keys

    def test_uses_config_grid_shape(self, mock_config):
        """Verify that the configured grid shape is passed to compress_chgcar."""
        mock_client = MagicMock()
        mock_db_instance = MagicMock(spec=StructuresDatabase)
        mock_structure = MagicMock()
        mock_structure.as_dict.return_value = {}

        mock_config.lematrho_grid_shape = (20, 20, 20)

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.StructuresDatabase"
            ) as mock_db_cls,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.download_gz_file_from_s3"
            ) as mock_download,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.parse_vasprun_structure"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.compress_chgcar"
            ) as mock_compress,
        ):
            mock_auth.return_value = mock_client
            mock_db_cls.return_value = mock_db_instance
            mock_download.return_value = b"data"
            mock_parse.return_value = mock_structure
            mock_compress.return_value = []

            LeMatRhoFetcher._process_batch(
                "agm000001", mock_config, {"occurred": False}
            )

        # All 4 compress calls should use (20, 20, 20)
        for call in mock_compress.call_args_list:
            assert call[0][1] == (20, 20, 20)


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------


class TestDownloadGzFile:
    def test_decompresses_gzipped_content(self):
        """Verify gzip decompression works correctly."""

        original = b"hello world test content"
        compressed = gzip.compress(original)

        mock_client = MagicMock()
        mock_client.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=compressed))
        }

        result = download_gz_file_from_s3(mock_client, "bucket", "key.gz")
        assert result == original

    def test_propagates_s3_errors(self):
        """S3 download errors should propagate."""
        mock_client = MagicMock()
        mock_client.get_object.side_effect = Exception("NoSuchKey")

        with pytest.raises(Exception, match="NoSuchKey"):
            download_gz_file_from_s3(mock_client, "bucket", "key.gz")


class TestBuildRawStructure:
    def test_builds_correct_structure(self):
        """Verify the RawStructure has correct fields and type."""
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0),
            ["Si", "Si"],
            [[0, 0, 0], [0.5, 0.5, 0.5]],
        )
        compressed_grids = {
            "charge_density": [[[1.0]]],
            "aeccar0": [[[2.0]]],
            "aeccar1": None,
            "aeccar2": [[[4.0]]],
        }

        raw = build_raw_structure(
            material_id="agm000001",
            structure=structure,
            compressed_grids=compressed_grids,
            grid_shape=(15, 15, 15),
            s3_prefix="agm000001",
        )

        assert raw.id == "agm000001"
        assert raw.type == "lematrho"
        assert raw.attributes["compressed_charge_density"] == [[[1.0]]]
        assert raw.attributes["compressed_aeccar0"] == [[[2.0]]]
        assert raw.attributes["compressed_aeccar1"] is None
        assert raw.attributes["compressed_aeccar2"] == [[[4.0]]]
        assert raw.attributes["grid_shape"] == [15, 15, 15]
        assert raw.attributes["s3_prefix"] == "agm000001"
        assert raw.attributes["structure"] is not None
        assert raw.last_modified is not None


# ---------------------------------------------------------------------------
# Constants tests
# ---------------------------------------------------------------------------


def test_valid_prefixes():
    """Verify VALID_PREFIXES covers expected material ID patterns."""
    assert "oqmd-123".startswith(VALID_PREFIXES)
    assert "mp-456".startswith(VALID_PREFIXES)
    assert "agm000001".startswith(VALID_PREFIXES)
    assert not "test-789".startswith(VALID_PREFIXES)
    assert not "random".startswith(VALID_PREFIXES)


def test_static_files_list():
    """Verify STATIC_FILES contains all expected charge density files."""
    assert "CHGCAR.gz" in STATIC_FILES
    assert "AECCAR0.gz" in STATIC_FILES
    assert "AECCAR1.gz" in STATIC_FILES
    assert "AECCAR2.gz" in STATIC_FILES
    assert len(STATIC_FILES) == 4


def test_calc_type_constants():
    """Verify S3 subfolder constants."""
    assert STATIC_CALC_TYPE == "LeMatRhoStaticMaker"
    assert RELAX_CALC_TYPE == "LeMatRhoRelaxMaker_1"


# ---------------------------------------------------------------------------
# Fetcher lifecycle tests
# ---------------------------------------------------------------------------


class TestLeMatRhoFetcher:
    def test_setup_resources(self, mock_config, mock_aws_client, mock_version_db):
        """Test that setup_resources initializes the authenticated AWS client."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.fetch.get_authenticated_aws_client"
            ) as mock_auth,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
        ):
            mock_auth.return_value = mock_aws_client
            mock_ver_cls.return_value = mock_version_db

            fetcher = LeMatRhoFetcher(config=mock_config)
            fetcher.setup_resources()

            mock_auth.assert_called_once()
            assert fetcher.aws_client is mock_aws_client

    def test_get_new_version_returns_today(self, mock_config, mock_version_db):
        """Version should be today's date."""
        with (
            patch("lematerial_fetcher.fetch.DatasetVersions") as mock_ver_cls,
            patch("lematerial_fetcher.fetch.StructuresDatabase"),
        ):
            mock_ver_cls.return_value = mock_version_db

            fetcher = LeMatRhoFetcher(config=mock_config, debug=True)
            version = fetcher.get_new_version()

        assert version == datetime.now().strftime("%Y-%m-%d")
