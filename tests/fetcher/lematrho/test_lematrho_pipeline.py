# Copyright 2025 Entalpic
import json
import os
import subprocess
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from lematerial_fetcher.fetcher.lematrho.pipeline import (
    PARQUET_COLUMNS,
    PARQUET_SCHEMA,
    LeMatRhoDirectPipeline,
    _run_bader_from_bytes,
    _run_ddec6_from_bytes,
    _structure_to_row,
)
from lematerial_fetcher.models.optimade import Functional, OptimadeStructure
from lematerial_fetcher.utils.config import DirectPipelineConfig

# Minimal pymatgen Structure dict for testing
_MOCK_STRUCTURE_DICT = {
    "lattice": {
        "matrix": [[3.0, 0.0, 0.0], [0.0, 3.0, 0.0], [0.0, 0.0, 3.0]],
        "a": 3.0,
        "b": 3.0,
        "c": 3.0,
        "alpha": 90.0,
        "beta": 90.0,
        "gamma": 90.0,
    },
    "sites": [
        {
            "species": [{"element": "Si", "occu": 1}],
            "abc": [0.0, 0.0, 0.0],
            "xyz": [0.0, 0.0, 0.0],
            "label": "Si",
        },
        {
            "species": [{"element": "O", "occu": 1}],
            "abc": [0.5, 0.5, 0.5],
            "xyz": [1.5, 1.5, 1.5],
            "label": "O",
        },
    ],
}


def _make_mock_optimade_dict():
    """Create a mock OPTIMADE dict like get_optimade_from_pymatgen would return."""
    return {
        "elements": ["O", "Si"],
        "nelements": 2,
        "elements_ratios": [0.5, 0.5],
        "nsites": 2,
        "cartesian_site_positions": [[0.0, 0.0, 0.0], [1.5, 1.5, 1.5]],
        "species_at_sites": ["Si", "O"],
        "species": [
            {
                "mass": None,
                "name": "O",
                "attached": None,
                "nattached": None,
                "concentration": [1],
                "original_name": None,
                "chemical_symbols": ["O"],
            },
            {
                "mass": None,
                "name": "Si",
                "attached": None,
                "nattached": None,
                "concentration": [1],
                "original_name": None,
                "chemical_symbols": ["Si"],
            },
        ],
        "chemical_formula_anonymous": "AB",
        "chemical_formula_descriptive": "Si1 O1",
        "chemical_formula_reduced": "OSi",
        "dimension_types": [1, 1, 1],
        "nperiodic_dimensions": 3,
        "lattice_vectors": [[3.0, 0.0, 0.0], [0.0, 3.0, 0.0], [0.0, 0.0, 3.0]],
    }


@pytest.fixture
def tmp_output_dir():
    """Create a temp directory for pipeline output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_config(tmp_output_dir):
    return DirectPipelineConfig(
        lematrho_bucket_name="test-bucket",
        lematrho_grid_shape=(10, 10, 10),
        output_dir=tmp_output_dir,
        parquet_chunk_size=3,
        num_workers=2,
        log_every=10,
    )


@pytest.fixture
def no_tools():
    """Tool paths dict where no tools are available."""
    return {
        "bader_path": None,
        "chargemol_path": None,
        "chgsum_script_path": None,
        "perl_path": None,
        "atomic_densities_path": None,
        "can_generate_potcar": False,
        "can_run_bader": False,
        "can_run_ddec6": False,
    }


# ---------------------------------------------------------------------------
# TestListMaterials
# ---------------------------------------------------------------------------


class TestListMaterials:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_filters_by_valid_prefix(self, mock_get_client, mock_config):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "mp-123/"},
                    {"Prefix": "agm000001/"},
                    {"Prefix": "oqmd-456/"},
                    {"Prefix": "unknown-789/"},
                    {"Prefix": "test-data/"},
                ]
            }
        ]

        with patch.object(LeMatRhoDirectPipeline, "_validate_tools", return_value={}):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            result = pipeline._list_materials()

        assert result == ["mp-123", "agm000001", "oqmd-456"]

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_excludes_processed_ids(self, mock_get_client, mock_config):
        """Checkpoint filtering removes already-processed materials."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "mp-1/"},
                    {"Prefix": "mp-2/"},
                    {"Prefix": "mp-3/"},
                ]
            }
        ]

        # Write checkpoint with mp-1 already processed
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "w") as f:
            f.write("mp-1\n")

        with patch.object(LeMatRhoDirectPipeline, "_validate_tools", return_value={}):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            all_materials = pipeline._list_materials()
            pipeline._processed_ids = pipeline._load_checkpoint()
            remaining = [m for m in all_materials if m not in pipeline._processed_ids]

        assert remaining == ["mp-2", "mp-3"]


# ---------------------------------------------------------------------------
# TestProcessMaterial
# ---------------------------------------------------------------------------


class TestProcessMaterial:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_happy_path_no_tools(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_config,
        no_tools,
    ):
        """Full processing without external tools — charge analysis fields are None."""
        from pymatgen.core import Lattice, Structure

        mock_get_client.return_value = MagicMock()
        mock_download.return_value = b"mock_bytes"

        structure = Structure(
            Lattice.cubic(3.0),
            ["Si", "O"],
            [[0, 0, 0], [0.5, 0.5, 0.5]],
        )
        mock_parse_vasprun.return_value = structure
        mock_compress.return_value = [[[1.0] * 10] * 10] * 10
        mock_get_optimade.return_value = _make_mock_optimade_dict()

        result = LeMatRhoDirectPipeline._process_material(
            "mp-123", mock_config, no_tools
        )

        assert result is not None
        assert isinstance(result, dict)

        # Check key fields
        assert result["immutable_id"] == "mp-123"
        assert result["functional"] == "pbe"
        assert result["cross_compatibility"] is True
        assert result["nsites"] == 2
        assert result["charge_density_grid_shape"] == [10, 10, 10]

        # Compressed grids should be JSON strings
        assert isinstance(result["compressed_charge_density"], str)
        parsed = json.loads(result["compressed_charge_density"])
        assert isinstance(parsed, list)

        # No tools — charge analysis fields are None
        assert result["bader_charges"] is None
        assert result["bader_atomic_volume"] is None
        assert result["ddec6_charges"] is None

        # Species should be JSON string
        assert isinstance(result["species"], str)

        # All Parquet columns present
        for col in PARQUET_COLUMNS:
            assert col in result

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_missing_vasprun_returns_none(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_config,
        no_tools,
    ):
        mock_get_client.return_value = MagicMock()
        mock_download.side_effect = Exception("NoSuchKey: vasprun.xml.gz")

        result = LeMatRhoDirectPipeline._process_material(
            "mp-999", mock_config, no_tools
        )
        assert result is None

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_partial_charge_files(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_config,
        no_tools,
    ):
        """Missing AECCAR1 — other files still processed."""
        from pymatgen.core import Lattice, Structure

        mock_get_client.return_value = MagicMock()

        def download_side_effect(client, bucket, key):
            if "AECCAR1.gz" in key:
                raise Exception("NoSuchKey")
            return b"mock_bytes"

        mock_download.side_effect = download_side_effect

        structure = Structure(
            Lattice.cubic(3.0), ["Si", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        mock_parse_vasprun.return_value = structure
        mock_compress.return_value = [[[1.0]]]
        mock_get_optimade.return_value = _make_mock_optimade_dict()

        result = LeMatRhoDirectPipeline._process_material(
            "mp-123", mock_config, no_tools
        )

        assert result is not None
        # CHGCAR, AECCAR0, AECCAR2 should be present
        assert result["compressed_charge_density"] is not None
        assert result["compressed_aeccar0"] is not None
        assert result["compressed_aeccar2"] is not None
        # AECCAR1 should be None (failed to download)
        assert result["compressed_aeccar1"] is None

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_cross_compatibility_excludes_yb(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_config,
        no_tools,
    ):
        from pymatgen.core import Lattice, Structure

        mock_get_client.return_value = MagicMock()
        mock_download.return_value = b"mock_bytes"

        structure = Structure(
            Lattice.cubic(3.0), ["Yb", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        mock_parse_vasprun.return_value = structure
        mock_compress.return_value = [[[1.0]]]

        optimade_dict = _make_mock_optimade_dict()
        optimade_dict["elements"] = ["O", "Yb"]
        optimade_dict["species_at_sites"] = ["Yb", "O"]
        optimade_dict["species"] = [
            {
                "mass": None,
                "name": "O",
                "attached": None,
                "nattached": None,
                "concentration": [1],
                "original_name": None,
                "chemical_symbols": ["O"],
            },
            {
                "mass": None,
                "name": "Yb",
                "attached": None,
                "nattached": None,
                "concentration": [1],
                "original_name": None,
                "chemical_symbols": ["Yb"],
            },
        ]
        mock_get_optimade.return_value = optimade_dict

        result = LeMatRhoDirectPipeline._process_material(
            "mp-yb", mock_config, no_tools
        )

        assert result is not None
        assert result["cross_compatibility"] is False

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline._run_bader_from_bytes")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_bader_failure_still_returns_result(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_bader,
        mock_config,
    ):
        """When Bader fails, bader fields are None but row is still returned."""
        from pymatgen.core import Lattice, Structure

        mock_get_client.return_value = MagicMock()
        mock_download.return_value = b"mock_bytes"

        structure = Structure(
            Lattice.cubic(3.0), ["Si", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        mock_parse_vasprun.return_value = structure
        mock_compress.return_value = [[[1.0]]]
        mock_get_optimade.return_value = _make_mock_optimade_dict()
        mock_bader.return_value = (None, None)

        tools = {
            "bader_path": "/usr/bin/bader",
            "chargemol_path": None,
            "chgsum_script_path": "/opt/chgsum.pl",
            "perl_path": "/usr/bin/perl",
            "atomic_densities_path": None,
            "can_generate_potcar": True,
            "can_run_bader": True,
            "can_run_ddec6": False,
        }

        result = LeMatRhoDirectPipeline._process_material(
            "mp-123", mock_config, tools
        )

        assert result is not None
        assert result["bader_charges"] is None
        assert result["bader_atomic_volume"] is None
        mock_bader.assert_called_once()


# ---------------------------------------------------------------------------
# TestCheckpointing
# ---------------------------------------------------------------------------


class TestCheckpointing:
    def test_load_empty_checkpoint(self, mock_config):
        """No checkpoint file -> empty set."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            ids = pipeline._load_checkpoint()

        assert ids == set()

    def test_load_existing_checkpoint(self, mock_config):
        """Checkpoint file with IDs -> returns set of those IDs."""
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "w") as f:
            f.write("mp-1\nmp-2\nagm000001\n")

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            ids = pipeline._load_checkpoint()

        assert ids == {"mp-1", "mp-2", "agm000001"}

    def test_append_checkpoint(self, mock_config):
        """Appending to checkpoint writes ID and persists."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            pipeline._append_checkpoint("mp-100")
            pipeline._append_checkpoint("mp-200")

        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        assert lines == ["mp-100", "mp-200"]

    def test_checkpoint_skips_blank_lines(self, mock_config):
        """Blank lines in checkpoint file are ignored."""
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "w") as f:
            f.write("mp-1\n\n\nmp-2\n\n")

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            ids = pipeline._load_checkpoint()

        assert ids == {"mp-1", "mp-2"}

    def test_batch_checkpoint(self, mock_config):
        """Batch checkpoint writes multiple IDs atomically."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            pipeline._batch_checkpoint(["mp-1", "mp-2", "mp-3"])

        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        assert lines == ["mp-1", "mp-2", "mp-3"]


# ---------------------------------------------------------------------------
# TestFailureTracking
# ---------------------------------------------------------------------------


class TestFailureTracking:
    def test_load_empty_failures(self, mock_config):
        """No failures file -> empty set."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            ids = pipeline._load_failures()

        assert ids == set()

    def test_load_existing_failures(self, mock_config):
        """Failures file with IDs -> returns set."""
        failures_path = os.path.join(mock_config.output_dir, ".failures.txt")
        with open(failures_path, "w") as f:
            f.write("mp-bad1\nmp-bad2\n")

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            ids = pipeline._load_failures()

        assert ids == {"mp-bad1", "mp-bad2"}

    def test_append_failure(self, mock_config):
        """Appending failure records ID on disk."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            pipeline._append_failure("mp-fail1")
            pipeline._append_failure("mp-fail2")

        failures_path = os.path.join(mock_config.output_dir, ".failures.txt")
        with open(failures_path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        assert lines == ["mp-fail1", "mp-fail2"]

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_resume_skips_failures(self, mock_get_client, mock_config, no_tools):
        """Pipeline skips previously failed materials on resume."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "mp-0/"},
                    {"Prefix": "mp-1/"},
                    {"Prefix": "mp-2/"},
                ]
            }
        ]

        # mp-0 already processed, mp-1 previously failed
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "w") as f:
            f.write("mp-0\n")
        failures_path = os.path.join(mock_config.output_dir, ".failures.txt")
        with open(failures_path, "w") as f:
            f.write("mp-1\n")

        row = {col: None for col in PARQUET_COLUMNS}
        row.update(
            {
                "elements": ["Si"],
                "nsites": 1,
                "chemical_formula_anonymous": "A",
                "chemical_formula_reduced": "Si",
                "chemical_formula_descriptive": "Si1",
                "nelements": 1,
                "dimension_types": [1, 1, 1],
                "nperiodic_dimensions": 3,
                "lattice_vectors": [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
                "immutable_id": "mp-2",
                "cartesian_site_positions": [[0, 0, 0]],
                "species": json.dumps([{"name": "Si"}]),
                "species_at_sites": ["Si"],
                "last_modified": datetime.now().isoformat(),
                "elements_ratios": [1.0],
                "functional": "pbe",
                "cross_compatibility": True,
            }
        )

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config, debug=True)

        processed_ids = []

        def mock_process(material_id, config, tool_paths):
            processed_ids.append(material_id)
            r = dict(row)
            r["immutable_id"] = material_id
            return r

        with patch.object(
            LeMatRhoDirectPipeline, "_process_material", side_effect=mock_process
        ):
            pipeline.run()

        # Only mp-2 should be processed (mp-0 checkpointed, mp-1 failed)
        assert processed_ids == ["mp-2"]


# ---------------------------------------------------------------------------
# TestParquetWriting
# ---------------------------------------------------------------------------


class TestParquetWriting:
    def _make_row(self, material_id="mp-1"):
        """Create a minimal valid row dict matching PARQUET_COLUMNS."""
        row = {col: None for col in PARQUET_COLUMNS}
        row.update(
            {
                "elements": ["O", "Si"],
                "nsites": 2,
                "chemical_formula_anonymous": "AB",
                "chemical_formula_reduced": "OSi",
                "chemical_formula_descriptive": "Si1 O1",
                "nelements": 2,
                "dimension_types": [1, 1, 1],
                "nperiodic_dimensions": 3,
                "lattice_vectors": [[3.0, 0, 0], [0, 3.0, 0], [0, 0, 3.0]],
                "immutable_id": material_id,
                "cartesian_site_positions": [[0, 0, 0], [1.5, 1.5, 1.5]],
                "species": json.dumps([{"name": "O"}, {"name": "Si"}]),
                "species_at_sites": ["Si", "O"],
                "last_modified": datetime.now().isoformat(),
                "elements_ratios": [0.5, 0.5],
                "functional": "pbe",
                "cross_compatibility": True,
                "charge_density_grid_shape": [10, 10, 10],
                "compressed_charge_density": json.dumps([[[1.0]]]),
            }
        )
        return row

    def test_write_chunk(self, mock_config):
        """Verify Parquet file is created with correct schema."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            rows = [self._make_row(f"mp-{i}") for i in range(3)]
            pipeline._write_parquet_chunk(rows, 0)

        path = os.path.join(mock_config.output_dir, "chunk_000000.parquet")
        assert os.path.exists(path)

        table = pq.read_table(path)
        assert table.num_rows == 3
        assert set(table.column_names) == set(PARQUET_COLUMNS)

    def test_atomic_write_no_tmp_file_remains(self, mock_config):
        """After writing, no .tmp file should remain."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            rows = [self._make_row()]
            pipeline._write_parquet_chunk(rows, 0)

        tmp_files = [
            f
            for f in os.listdir(mock_config.output_dir)
            if f.endswith(".tmp")
        ]
        assert len(tmp_files) == 0

    def test_chunk_index_resume(self, mock_config):
        """Next chunk index should be max existing + 1."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)

            # Write chunks 0, 1, 2
            for i in range(3):
                rows = [self._make_row(f"mp-{i}")]
                pipeline._write_parquet_chunk(rows, i)

            assert pipeline._get_next_chunk_index() == 3

    def test_tmp_files_ignored_on_resume(self, mock_config):
        """Stale .tmp files don't affect chunk indexing."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)

            # Write one real chunk
            rows = [self._make_row()]
            pipeline._write_parquet_chunk(rows, 0)

            # Create a stale .tmp file
            tmp_path = os.path.join(
                mock_config.output_dir, "chunk_000001.parquet.tmp"
            )
            with open(tmp_path, "w") as f:
                f.write("stale")

            assert pipeline._get_next_chunk_index() == 1

    def test_chunk_index_empty_dir(self, mock_config):
        """Empty output dir -> chunk index 0."""
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value={}
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config)
            assert pipeline._get_next_chunk_index() == 0


# ---------------------------------------------------------------------------
# TestStructureToRow
# ---------------------------------------------------------------------------


class TestStructureToRow:
    def test_all_columns_present(self):
        """Row dict should have exactly the PARQUET_COLUMNS keys."""
        optimade_dict = _make_mock_optimade_dict()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert set(row.keys()) == set(PARQUET_COLUMNS)

    def test_species_json_serialized(self):
        """Species field should be a JSON string."""
        optimade_dict = _make_mock_optimade_dict()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert isinstance(row["species"], str)
        parsed = json.loads(row["species"])
        assert isinstance(parsed, list)

    def test_functional_is_string(self):
        """Functional enum should be converted to string value."""
        optimade_dict = _make_mock_optimade_dict()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert row["functional"] == "pbe"

    def test_last_modified_is_iso_string(self):
        """last_modified should be an ISO format string."""
        optimade_dict = _make_mock_optimade_dict()
        now = datetime.now()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=now,
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        # Model validator strips time to date-only (YYYY-MM-DD -> YYYY-MM-DDT00:00:00)
        assert row["last_modified"] == structure.last_modified.isoformat()

    def test_compressed_fields_json_serialized(self):
        """Compressed charge density fields should be JSON strings when present."""
        optimade_dict = _make_mock_optimade_dict()
        grid = [[[1.0, 2.0], [3.0, 4.0]]]
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compressed_charge_density=grid,
            charge_density_grid_shape=[1, 2, 2],
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert isinstance(row["compressed_charge_density"], str)
        assert json.loads(row["compressed_charge_density"]) == grid


# ---------------------------------------------------------------------------
# TestRunIntegration
# ---------------------------------------------------------------------------


class TestRunIntegration:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_full_pipeline_debug_mode(
        self, mock_get_client, mock_config, no_tools
    ):
        """Integration test: process 5 materials in debug mode, verify chunks + checkpoint."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock S3 listing
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": f"mp-{i}/"}
                    for i in range(5)
                ]
            }
        ]

        # Create mock results
        mock_results = []
        for i in range(5):
            row = {col: None for col in PARQUET_COLUMNS}
            row.update(
                {
                    "elements": ["Si"],
                    "nsites": 1,
                    "chemical_formula_anonymous": "A",
                    "chemical_formula_reduced": "Si",
                    "chemical_formula_descriptive": "Si1",
                    "nelements": 1,
                    "dimension_types": [1, 1, 1],
                    "nperiodic_dimensions": 3,
                    "lattice_vectors": [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
                    "immutable_id": f"mp-{i}",
                    "cartesian_site_positions": [[0, 0, 0]],
                    "species": json.dumps([{"name": "Si"}]),
                    "species_at_sites": ["Si"],
                    "last_modified": datetime.now().isoformat(),
                    "elements_ratios": [1.0],
                    "functional": "pbe",
                    "cross_compatibility": True,
                    "charge_density_grid_shape": [10, 10, 10],
                }
            )
            mock_results.append(row)

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config, debug=True)

        # Mock _process_material to return our pre-built rows
        call_count = [0]

        def mock_process(material_id, config, tool_paths):
            idx = call_count[0]
            call_count[0] += 1
            return mock_results[idx]

        with patch.object(
            LeMatRhoDirectPipeline, "_process_material", side_effect=mock_process
        ):
            pipeline.run()

        # With chunk_size=3 and 5 materials: should write 2 chunks (3 + 2)
        parquet_files = sorted(
            f
            for f in os.listdir(mock_config.output_dir)
            if f.endswith(".parquet")
        )
        assert len(parquet_files) == 2

        # Check row counts
        total_rows = 0
        for f in parquet_files:
            table = pq.read_table(os.path.join(mock_config.output_dir, f))
            total_rows += table.num_rows
        assert total_rows == 5

        # Check checkpoint
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path) as f:
            checkpoint_ids = {line.strip() for line in f if line.strip()}
        assert checkpoint_ids == {f"mp-{i}" for i in range(5)}

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_resume_skips_processed(self, mock_get_client, mock_config, no_tools):
        """Pipeline resumes from checkpoint, skipping already-processed materials."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "mp-0/"},
                    {"Prefix": "mp-1/"},
                    {"Prefix": "mp-2/"},
                ]
            }
        ]

        # Pre-existing checkpoint (mp-0 already done)
        checkpoint_path = os.path.join(mock_config.output_dir, ".checkpoint.txt")
        with open(checkpoint_path, "w") as f:
            f.write("mp-0\n")

        # Pre-existing Parquet chunk
        row = {col: None for col in PARQUET_COLUMNS}
        row.update(
            {
                "elements": ["Si"],
                "nsites": 1,
                "chemical_formula_anonymous": "A",
                "chemical_formula_reduced": "Si",
                "chemical_formula_descriptive": "Si1",
                "nelements": 1,
                "dimension_types": [1, 1, 1],
                "nperiodic_dimensions": 3,
                "lattice_vectors": [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
                "immutable_id": "mp-0",
                "cartesian_site_positions": [[0, 0, 0]],
                "species": json.dumps([{"name": "Si"}]),
                "species_at_sites": ["Si"],
                "last_modified": datetime.now().isoformat(),
                "elements_ratios": [1.0],
                "functional": "pbe",
                "cross_compatibility": True,
            }
        )

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config, debug=True)

        processed_ids = []

        def mock_process(material_id, config, tool_paths):
            processed_ids.append(material_id)
            r = dict(row)
            r["immutable_id"] = material_id
            return r

        with patch.object(
            LeMatRhoDirectPipeline, "_process_material", side_effect=mock_process
        ):
            pipeline.run()

        # Only mp-1 and mp-2 should be processed (mp-0 skipped)
        assert "mp-0" not in processed_ids
        assert set(processed_ids) == {"mp-1", "mp-2"}


# ---------------------------------------------------------------------------
# TestBaderFromBytes
# ---------------------------------------------------------------------------


class TestBaderFromBytes:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.write_potcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.subprocess.run")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.read_potcar_zval")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_acf_dat")
    def test_happy_path(
        self, mock_parse_acf, mock_read_zval, mock_subprocess, mock_potcar
    ):
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0), ["Si", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        raw_files = {
            "CHGCAR": b"chgcar_data",
            "AECCAR0": b"aeccar0_data",
            "AECCAR2": b"aeccar2_data",
        }
        tools = {
            "bader_path": "/usr/bin/bader",
            "perl_path": "/usr/bin/perl",
            "chgsum_script_path": "/opt/chgsum.pl",
        }

        mock_parse_acf.return_value = ([4.0, 6.0], [10.0, 12.0])
        mock_read_zval.return_value = {"Si": 4.0, "O": 6.0}

        charges, volumes = _run_bader_from_bytes(
            structure, raw_files, tools, "mp-test"
        )

        assert charges == [0.0, 0.0]  # valence - electron_count
        assert volumes == [10.0, 12.0]
        assert mock_subprocess.call_count == 2  # chgsum + bader

    def test_subprocess_timeout(self):
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0), ["Si"], [[0, 0, 0]]
        )
        raw_files = {"CHGCAR": b"x", "AECCAR0": b"x", "AECCAR2": b"x"}
        tools = {
            "bader_path": "/usr/bin/bader",
            "perl_path": "/usr/bin/perl",
            "chgsum_script_path": "/opt/chgsum.pl",
        }

        with patch(
            "lematerial_fetcher.fetcher.lematrho.pipeline.write_potcar"
        ):
            with patch(
                "lematerial_fetcher.fetcher.lematrho.pipeline.subprocess.run",
                side_effect=subprocess.TimeoutExpired("bader", 600),
            ):
                charges, volumes = _run_bader_from_bytes(
                    structure, raw_files, tools, "mp-test"
                )

        assert charges is None
        assert volumes is None


# ---------------------------------------------------------------------------
# TestDdec6FromBytes
# ---------------------------------------------------------------------------


class TestDdec6FromBytes:
    def test_subprocess_failure(self):
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0), ["Si"], [[0, 0, 0]]
        )
        raw_files = {"CHGCAR": b"x"}
        tools = {
            "chargemol_path": "/usr/bin/chargemol",
            "atomic_densities_path": "/opt/densities",
        }

        with patch(
            "lematerial_fetcher.fetcher.lematrho.pipeline.write_potcar"
        ):
            with patch(
                "lematerial_fetcher.fetcher.lematrho.pipeline.subprocess.run",
                side_effect=subprocess.CalledProcessError(1, "chargemol"),
            ):
                result = _run_ddec6_from_bytes(
                    structure, raw_files, tools, "mp-test"
                )

        assert result is None

    def test_happy_path(self):
        """DDEC6 returns charges when subprocess succeeds."""
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0), ["Si", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        raw_files = {"CHGCAR": b"chgcar_data"}
        tools = {
            "chargemol_path": "/usr/bin/chargemol",
            "atomic_densities_path": "/opt/densities",
        }

        with patch(
            "lematerial_fetcher.fetcher.lematrho.pipeline.write_potcar"
        ):
            with patch(
                "lematerial_fetcher.fetcher.lematrho.pipeline.subprocess.run"
            ):
                with patch(
                    "lematerial_fetcher.fetcher.lematrho.pipeline.parse_ddec6_charges",
                    return_value=[0.5, -0.5],
                ):
                    result = _run_ddec6_from_bytes(
                        structure, raw_files, tools, "mp-test"
                    )

        assert result == [0.5, -0.5]

    def test_timeout(self):
        """DDEC6 returns None on timeout."""
        from pymatgen.core import Lattice, Structure

        structure = Structure(
            Lattice.cubic(3.0), ["Si"], [[0, 0, 0]]
        )
        raw_files = {"CHGCAR": b"x"}
        tools = {
            "chargemol_path": "/usr/bin/chargemol",
            "atomic_densities_path": "/opt/densities",
        }

        with patch(
            "lematerial_fetcher.fetcher.lematrho.pipeline.write_potcar"
        ):
            with patch(
                "lematerial_fetcher.fetcher.lematrho.pipeline.subprocess.run",
                side_effect=subprocess.TimeoutExpired("chargemol", 600),
            ):
                result = _run_ddec6_from_bytes(
                    structure, raw_files, tools, "mp-test"
                )

        assert result is None


# ---------------------------------------------------------------------------
# TestValidateTools
# ---------------------------------------------------------------------------


class TestValidateTools:
    def test_all_tools_available(self, mock_config):
        """All tools present -> can_run_bader and can_run_ddec6 are True."""
        with patch("shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.dict(os.environ, {"PMG_VASP_PSP_DIR": "/opt/psp"}):
                config = DirectPipelineConfig(
                    lematrho_bucket_name="test-bucket",
                    output_dir=mock_config.output_dir,
                    bader_path="/usr/bin/bader",
                    chargemol_path="/usr/bin/chargemol",
                    chgsum_script_path=__file__,  # use this test file as a file that exists
                    atomic_densities_path=os.path.dirname(__file__),  # dir that exists
                )
                pipeline = LeMatRhoDirectPipeline(config=config)

        assert pipeline._tool_paths["can_run_bader"] is True
        assert pipeline._tool_paths["can_run_ddec6"] is True
        assert pipeline._tool_paths["can_generate_potcar"] is True

    def test_no_tools_available(self, mock_config):
        """No tools on PATH -> can_run_bader and can_run_ddec6 are False."""
        with patch("shutil.which", return_value=None):
            with patch.dict(os.environ, {}, clear=True):
                config = DirectPipelineConfig(
                    lematrho_bucket_name="test-bucket",
                    output_dir=mock_config.output_dir,
                )
                pipeline = LeMatRhoDirectPipeline(config=config)

        assert pipeline._tool_paths["can_run_bader"] is False
        assert pipeline._tool_paths["can_run_ddec6"] is False
        assert pipeline._tool_paths["can_generate_potcar"] is False

    def test_bader_but_no_chgsum(self, mock_config):
        """Bader on PATH but chgsum not set -> can_run_bader False."""
        with patch("shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            with patch.dict(os.environ, {"PMG_VASP_PSP_DIR": "/opt/psp"}):
                config = DirectPipelineConfig(
                    lematrho_bucket_name="test-bucket",
                    output_dir=mock_config.output_dir,
                    bader_path="/usr/bin/bader",
                    # no chgsum_script_path
                )
                pipeline = LeMatRhoDirectPipeline(config=config)

        assert pipeline._tool_paths["can_run_bader"] is False
        assert pipeline._tool_paths["bader_path"] == "/usr/bin/bader"

    def test_missing_pmg_vasp_psp_dir(self, mock_config):
        """No PMG_VASP_PSP_DIR -> can_generate_potcar False, both analyses disabled."""
        with patch("shutil.which", side_effect=lambda x: f"/usr/bin/{x}"):
            env = os.environ.copy()
            env.pop("PMG_VASP_PSP_DIR", None)
            with patch.dict(os.environ, env, clear=True):
                config = DirectPipelineConfig(
                    lematrho_bucket_name="test-bucket",
                    output_dir=mock_config.output_dir,
                    bader_path="/usr/bin/bader",
                    chargemol_path="/usr/bin/chargemol",
                    chgsum_script_path=__file__,
                    atomic_densities_path=os.path.dirname(__file__),
                )
                pipeline = LeMatRhoDirectPipeline(config=config)

        assert pipeline._tool_paths["can_generate_potcar"] is False
        assert pipeline._tool_paths["can_run_bader"] is False
        assert pipeline._tool_paths["can_run_ddec6"] is False


# ---------------------------------------------------------------------------
# TestStructureToRowNoneFields
# ---------------------------------------------------------------------------


class TestStructureToRowNoneFields:
    def test_all_charge_fields_none(self):
        """Structure with no charge density fields -> all charge columns None."""
        optimade_dict = _make_mock_optimade_dict()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert row["compressed_charge_density"] is None
        assert row["compressed_aeccar0"] is None
        assert row["compressed_aeccar1"] is None
        assert row["compressed_aeccar2"] is None
        assert row["charge_density_grid_shape"] is None
        assert row["bader_charges"] is None
        assert row["bader_atomic_volume"] is None
        assert row["ddec6_charges"] is None

    def test_partial_charge_fields(self):
        """Structure with only some charge fields -> only those are populated."""
        optimade_dict = _make_mock_optimade_dict()
        structure = OptimadeStructure(
            id="mp-1",
            source="lematrho",
            immutable_id="mp-1",
            last_modified=datetime.now(),
            **optimade_dict,
            functional=Functional.PBE,
            cross_compatibility=True,
            compressed_charge_density=[[[1.0]]],
            charge_density_grid_shape=[1, 1, 1],
            bader_charges=[0.1, -0.1],
            compute_space_group=True,
            compute_bawl_hash=True,
        )

        row = _structure_to_row(structure)
        assert row["compressed_charge_density"] is not None
        assert row["compressed_aeccar0"] is None
        assert row["bader_charges"] == [0.1, -0.1]
        assert row["ddec6_charges"] is None


# ---------------------------------------------------------------------------
# TestPushToHuggingface
# ---------------------------------------------------------------------------


class TestPushToHuggingface:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_push_called_when_configured(self, mock_get_client, mock_config, no_tools):
        """Pipeline calls push_to_hub when hf_repo_id is configured."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "mp-1/"}]}
        ]

        config = DirectPipelineConfig(
            lematrho_bucket_name="test-bucket",
            output_dir=mock_config.output_dir,
            hf_repo_id="test-org/test-dataset",
            hf_token="hf_test_token",
        )

        mock_row = {col: None for col in PARQUET_COLUMNS}
        mock_row.update({
            "elements": ["Si"],
            "nsites": 1,
            "chemical_formula_anonymous": "A",
            "chemical_formula_reduced": "Si",
            "chemical_formula_descriptive": "Si1",
            "nelements": 1,
            "dimension_types": [1, 1, 1],
            "nperiodic_dimensions": 3,
            "lattice_vectors": [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
            "immutable_id": "mp-1",
            "cartesian_site_positions": [[0, 0, 0]],
            "species": json.dumps([{"name": "Si"}]),
            "species_at_sites": ["Si"],
            "last_modified": datetime.now().isoformat(),
            "elements_ratios": [1.0],
            "functional": "pbe",
            "cross_compatibility": True,
        })

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=config, debug=True)

        mock_dataset = MagicMock()
        with patch.object(
            LeMatRhoDirectPipeline, "_process_material", return_value=mock_row
        ):
            with patch(
                "datasets.load_dataset",
                return_value={"train": mock_dataset},
            ):
                pipeline.run()

        mock_dataset.push_to_hub.assert_called_once_with(
            "test-org/test-dataset",
            token="hf_test_token",
            private=True,
        )

    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_push_not_called_without_repo_id(
        self, mock_get_client, mock_config, no_tools
    ):
        """Pipeline skips push when hf_repo_id is None."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_paginator = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "mp-1/"}]}
        ]

        mock_row = {col: None for col in PARQUET_COLUMNS}
        mock_row.update({
            "elements": ["Si"],
            "nsites": 1,
            "chemical_formula_anonymous": "A",
            "chemical_formula_reduced": "Si",
            "chemical_formula_descriptive": "Si1",
            "nelements": 1,
            "dimension_types": [1, 1, 1],
            "nperiodic_dimensions": 3,
            "lattice_vectors": [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
            "immutable_id": "mp-1",
            "cartesian_site_positions": [[0, 0, 0]],
            "species": json.dumps([{"name": "Si"}]),
            "species_at_sites": ["Si"],
            "last_modified": datetime.now().isoformat(),
            "elements_ratios": [1.0],
            "functional": "pbe",
            "cross_compatibility": True,
        })

        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=mock_config, debug=True)

        with patch.object(
            LeMatRhoDirectPipeline, "_process_material", return_value=mock_row
        ):
            with patch.object(
                LeMatRhoDirectPipeline, "_push_to_huggingface"
            ) as mock_push:
                pipeline.run()

        mock_push.assert_not_called()


# ---------------------------------------------------------------------------
# TestProcessMaterialWithDdec6
# ---------------------------------------------------------------------------


class TestProcessMaterialWithDdec6:
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline._run_ddec6_from_bytes")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_optimade_from_pymatgen")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.compress_chgcar")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.parse_vasprun_structure")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.download_gz_file_from_s3")
    @patch("lematerial_fetcher.fetcher.lematrho.pipeline.get_authenticated_aws_client")
    def test_ddec6_populates_charges(
        self,
        mock_get_client,
        mock_download,
        mock_parse_vasprun,
        mock_compress,
        mock_get_optimade,
        mock_ddec6,
        mock_config,
    ):
        """When DDEC6 tools available and succeed, ddec6_charges is populated."""
        from pymatgen.core import Lattice, Structure

        mock_get_client.return_value = MagicMock()
        mock_download.return_value = b"mock_bytes"

        structure = Structure(
            Lattice.cubic(3.0), ["Si", "O"], [[0, 0, 0], [0.5, 0.5, 0.5]]
        )
        mock_parse_vasprun.return_value = structure
        mock_compress.return_value = [[[1.0] * 10] * 10] * 10
        mock_get_optimade.return_value = _make_mock_optimade_dict()
        mock_ddec6.return_value = [0.3, -0.3]

        tools = {
            "bader_path": None,
            "chargemol_path": "/usr/bin/chargemol",
            "chgsum_script_path": None,
            "perl_path": None,
            "atomic_densities_path": "/opt/densities",
            "can_generate_potcar": True,
            "can_run_bader": False,
            "can_run_ddec6": True,
        }

        result = LeMatRhoDirectPipeline._process_material(
            "mp-123", mock_config, tools
        )

        assert result is not None
        assert result["ddec6_charges"] == [0.3, -0.3]
        assert result["bader_charges"] is None
        mock_ddec6.assert_called_once()


# ---------------------------------------------------------------------------
# TestIntegrationS3 (requires credentials, skipped in normal runs)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIntegrationS3:
    """Integration tests that pull real data from the LeMatRho S3 bucket.

    To run these tests:
        1. Create a .env.integration file with AWS credentials:
           AWS_ACCESS_KEY_ID=...
           AWS_SECRET_ACCESS_KEY=...
           AWS_DEFAULT_REGION=us-east-1
        2. Run: pytest -m integration tests/fetcher/lematrho/test_lematrho_pipeline.py
    """

    @pytest.fixture(autouse=True)
    def _load_integration_env(self):
        """Load .env.integration if available, skip otherwise."""
        env_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "..", ".env.integration"
        )
        env_path = os.path.normpath(env_path)
        if not os.path.exists(env_path):
            pytest.skip(
                ".env.integration not found — set AWS credentials to run integration tests"
            )
        from dotenv import load_dotenv

        load_dotenv(env_path, override=True)

    def test_list_materials_from_real_bucket(self):
        """Verify we can list at least 1 material from the real S3 bucket."""
        config = DirectPipelineConfig(
            lematrho_bucket_name="lemat-rho",
            output_dir=tempfile.mkdtemp(),
        )
        with patch.object(LeMatRhoDirectPipeline, "_validate_tools", return_value={}):
            pipeline = LeMatRhoDirectPipeline(config=config)
        materials = pipeline._list_materials()
        assert len(materials) > 0
        # All should start with valid prefixes
        for m in materials[:10]:
            assert m.startswith(("oqmd-", "mp-", "agm"))

    def test_process_single_material(self):
        """Fetch and process a single real material end-to-end (no Bader/DDEC6)."""
        output_dir = tempfile.mkdtemp()
        config = DirectPipelineConfig(
            lematrho_bucket_name="lemat-rho",
            lematrho_grid_shape=(10, 10, 10),
            output_dir=output_dir,
        )
        no_tools = {
            "bader_path": None,
            "chargemol_path": None,
            "chgsum_script_path": None,
            "perl_path": None,
            "atomic_densities_path": None,
            "can_generate_potcar": False,
            "can_run_bader": False,
            "can_run_ddec6": False,
        }
        with patch.object(
            LeMatRhoDirectPipeline, "_validate_tools", return_value=no_tools
        ):
            pipeline = LeMatRhoDirectPipeline(config=config)

        materials = pipeline._list_materials()
        assert len(materials) > 0
        material_id = materials[0]

        result = LeMatRhoDirectPipeline._process_material(
            material_id, config, no_tools
        )
        assert result is not None
        assert result["immutable_id"] == material_id
        assert result["functional"] == "pbe"
        for col in PARQUET_COLUMNS:
            assert col in result
