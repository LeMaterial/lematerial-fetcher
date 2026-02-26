# Copyright 2025 Entalpic
import datetime
import os
import subprocess
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from pymatgen.core import Lattice, Structure

from lematerial_fetcher.fetcher.lematrho.transform import (
    LeMatRhoTransformer,
    get_cross_compatibility,
    parse_acf_dat,
    parse_ddec6_charges,
    read_potcar_zval,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional
from lematerial_fetcher.utils.config import TransformerConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def si_structure():
    """A simple Si diamond structure."""
    return Structure(
        Lattice.cubic(5.43),
        ["Si", "Si"],
        [[0, 0, 0], [0.25, 0.25, 0.25]],
    )


@pytest.fixture
def yb_structure():
    """A structure containing Yb (not cross-compatible)."""
    return Structure(
        Lattice.cubic(5.0),
        ["Yb", "O"],
        [[0, 0, 0], [0.5, 0.5, 0.5]],
    )


@pytest.fixture
def raw_structure(si_structure):
    """Raw structure from the fetch step with charge density data."""
    return RawStructure(
        id="agm000001",
        type="lematrho",
        attributes={
            "structure": si_structure.as_dict(),
            "compressed_charge_density": [[[1.0, 2.0]]],
            "compressed_aeccar0": [[[0.1]]],
            "compressed_aeccar1": [[[0.01]]],
            "compressed_aeccar2": [[[0.3]]],
            "grid_shape": [15, 15, 15],
            "s3_prefix": "agm000001",
        },
        last_modified=datetime.datetime(2025, 1, 1),
    )


@pytest.fixture
def raw_structure_yb(yb_structure):
    """Raw structure containing Yb."""
    return RawStructure(
        id="agm000002",
        type="lematrho",
        attributes={
            "structure": yb_structure.as_dict(),
            "compressed_charge_density": [[[1.0]]],
            "compressed_aeccar0": None,
            "compressed_aeccar1": None,
            "compressed_aeccar2": None,
            "grid_shape": [15, 15, 15],
            "s3_prefix": "agm000002",
        },
        last_modified=datetime.datetime(2025, 1, 1),
    )


@pytest.fixture
def mock_config():
    """TransformerConfig for testing."""
    return TransformerConfig(
        source_db_conn_str="mock://source",
        dest_db_conn_str="mock://dest",
        source_table_name="test_source",
        dest_table_name="test_dest",
        batch_size=100,
        page_offset=0,
        log_every=100,
        log_dir="./logs",
        max_retries=3,
        page_limit=10,
        num_workers=1,
        retry_delay=2,
        lematrho_bucket_name="lemat-rho",
        bader_path="/usr/bin/bader",
        chargemol_path="/usr/bin/chargemol",
        chgsum_script_path="/usr/bin/chgsum.pl",
        atomic_densities_path="/path/to/atomic_densities",
    )


@pytest.fixture
def transformer_with_tools(mock_config):
    """Transformer with all external tools available (mocked)."""
    with patch.object(LeMatRhoTransformer, "_validate_tools"):
        transformer = LeMatRhoTransformer(config=mock_config, debug=True)
    transformer._bader_path = "/usr/bin/bader"
    transformer._chargemol_path = "/usr/bin/chargemol"
    transformer._chgsum_script_path = "/usr/bin/chgsum.pl"
    transformer._perl_path = "/usr/bin/perl"
    transformer._atomic_densities_path = "/path/to/atomic_densities"
    transformer._can_generate_potcar = True
    return transformer


@pytest.fixture
def transformer_no_tools(mock_config):
    """Transformer with no external tools available."""
    with patch.object(LeMatRhoTransformer, "_validate_tools"):
        transformer = LeMatRhoTransformer(config=mock_config, debug=True)
    # All tool paths remain None (set in __init__ before _validate_tools)
    transformer._bader_path = None
    transformer._chargemol_path = None
    transformer._chgsum_script_path = None
    transformer._perl_path = None
    transformer._atomic_densities_path = None
    transformer._can_generate_potcar = False
    return transformer


# ---------------------------------------------------------------------------
# transform_row tests
# ---------------------------------------------------------------------------


class TestTransformRow:
    def test_happy_path(self, transformer_with_tools, raw_structure):
        """Full transform with Bader and DDEC6 results populated."""
        with (
            patch.object(
                transformer_with_tools, "_run_bader_analysis"
            ) as mock_bader,
            patch.object(
                transformer_with_tools, "_run_ddec6_analysis"
            ) as mock_ddec6,
        ):
            mock_bader.return_value = ([0.5, -0.5], [10.0, 12.0])
            mock_ddec6.return_value = [0.3, -0.3]

            result = transformer_with_tools.transform_row(raw_structure)

        assert len(result) == 1
        s = result[0]
        assert s.id == "agm000001"
        assert s.source == "lematrho"
        assert s.immutable_id == "agm000001"
        assert s.functional == Functional.PBE
        assert s.cross_compatibility is True
        assert s.bader_charges == [0.5, -0.5]
        assert s.bader_atomic_volume == [10.0, 12.0]
        assert s.ddec6_charges == [0.3, -0.3]
        assert s.compressed_charge_density == [[[1.0, 2.0]]]
        assert s.compressed_aeccar0 == [[[0.1]]]
        assert s.compressed_aeccar1 == [[[0.01]]]
        assert s.compressed_aeccar2 == [[[0.3]]]
        assert s.charge_density_grid_shape == [15, 15, 15]
        assert s.space_group_it_number is not None

    def test_bader_fails_ddec6_succeeds(self, transformer_with_tools, raw_structure):
        """When Bader fails, DDEC6 should still succeed independently."""
        with (
            patch.object(
                transformer_with_tools, "_run_bader_analysis"
            ) as mock_bader,
            patch.object(
                transformer_with_tools, "_run_ddec6_analysis"
            ) as mock_ddec6,
        ):
            mock_bader.return_value = (None, None)
            mock_ddec6.return_value = [0.3, -0.3]

            result = transformer_with_tools.transform_row(raw_structure)

        s = result[0]
        assert s.bader_charges is None
        assert s.bader_atomic_volume is None
        assert s.ddec6_charges == [0.3, -0.3]

    def test_both_analyses_fail(self, transformer_with_tools, raw_structure):
        """When both Bader and DDEC6 fail, structure should still be created."""
        with (
            patch.object(
                transformer_with_tools, "_run_bader_analysis"
            ) as mock_bader,
            patch.object(
                transformer_with_tools, "_run_ddec6_analysis"
            ) as mock_ddec6,
        ):
            mock_bader.return_value = (None, None)
            mock_ddec6.return_value = None

            result = transformer_with_tools.transform_row(raw_structure)

        s = result[0]
        assert s.bader_charges is None
        assert s.bader_atomic_volume is None
        assert s.ddec6_charges is None
        # Compressed grids should still be present
        assert s.compressed_charge_density == [[[1.0, 2.0]]]

    def test_no_bader_binary_skips_bader(self, transformer_no_tools, raw_structure):
        """When bader is not available, _run_bader_analysis should not be called."""
        with (
            patch.object(
                transformer_no_tools, "_run_bader_analysis"
            ) as mock_bader,
            patch.object(
                transformer_no_tools, "_run_ddec6_analysis"
            ) as mock_ddec6,
        ):
            mock_ddec6.return_value = None

            result = transformer_no_tools.transform_row(raw_structure)

        mock_bader.assert_not_called()
        mock_ddec6.assert_not_called()
        s = result[0]
        assert s.bader_charges is None
        assert s.ddec6_charges is None

    def test_functional_is_pbe(self, transformer_no_tools, raw_structure):
        """Functional should always be PBE for LeMatRho (MP settings)."""
        result = transformer_no_tools.transform_row(raw_structure)
        assert result[0].functional == Functional.PBE

    def test_cross_compatibility_excludes_yb(
        self, transformer_no_tools, raw_structure_yb
    ):
        """Yb-containing structures should not be cross-compatible."""
        result = transformer_no_tools.transform_row(raw_structure_yb)
        assert result[0].cross_compatibility is False

    def test_cross_compatibility_normal(self, transformer_no_tools, raw_structure):
        """Non-Yb structures should be cross-compatible."""
        result = transformer_no_tools.transform_row(raw_structure)
        assert result[0].cross_compatibility is True

    def test_missing_s3_prefix_skips_analyses(
        self, transformer_with_tools, si_structure
    ):
        """If s3_prefix is missing from attributes, skip Bader and DDEC6."""
        raw = RawStructure(
            id="agm000003",
            type="lematrho",
            attributes={
                "structure": si_structure.as_dict(),
                "compressed_charge_density": None,
                "grid_shape": [15, 15, 15],
                # No s3_prefix
            },
            last_modified=datetime.datetime(2025, 1, 1),
        )
        with (
            patch.object(
                transformer_with_tools, "_run_bader_analysis"
            ) as mock_bader,
            patch.object(
                transformer_with_tools, "_run_ddec6_analysis"
            ) as mock_ddec6,
        ):
            result = transformer_with_tools.transform_row(raw)

        mock_bader.assert_not_called()
        mock_ddec6.assert_not_called()


# ---------------------------------------------------------------------------
# _run_bader_analysis tests
# ---------------------------------------------------------------------------


class TestRunBaderAnalysis:
    def test_subprocess_timeout(self, transformer_with_tools, si_structure):
        """Bader analysis should return (None, None) on subprocess timeout."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.subprocess.run"
            ) as mock_run,
            patch("lematerial_fetcher.fetcher.lematrho.transform.write_potcar"),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake chgcar data"
            mock_run.side_effect = subprocess.TimeoutExpired("bader", 600)

            charges, volumes = transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert charges is None
        assert volumes is None

    def test_chgsum_nonzero_exit(self, transformer_with_tools, si_structure):
        """chgsum.pl returning non-zero exit should be handled gracefully."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.subprocess.run"
            ) as mock_run,
            patch("lematerial_fetcher.fetcher.lematrho.transform.write_potcar"),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake data"
            mock_run.side_effect = subprocess.CalledProcessError(
                1, "perl chgsum.pl", stderr=b"chgsum error"
            )

            charges, volumes = transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert charges is None
        assert volumes is None

    def test_s3_download_failure(self, transformer_with_tools, si_structure):
        """S3 download failure should be handled gracefully."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.side_effect = Exception("NoSuchKey")

            charges, volumes = transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert charges is None
        assert volumes is None

    def test_potcar_generation_failure(self, transformer_with_tools, si_structure):
        """POTCAR generation failure should be handled gracefully."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.write_potcar",
                side_effect=Exception("No PSP"),
            ),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake data"

            charges, volumes = transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert charges is None
        assert volumes is None


# ---------------------------------------------------------------------------
# _run_ddec6_analysis tests
# ---------------------------------------------------------------------------


class TestRunDdec6Analysis:
    def test_subprocess_timeout(self, transformer_with_tools, si_structure):
        """DDEC6 analysis should return None on subprocess timeout."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.subprocess.run"
            ) as mock_run,
            patch("lematerial_fetcher.fetcher.lematrho.transform.write_potcar"),
            patch.object(transformer_with_tools, "_write_chargemol_config"),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake chgcar data"
            mock_run.side_effect = subprocess.TimeoutExpired("chargemol", 600)

            result = transformer_with_tools._run_ddec6_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert result is None

    def test_chargemol_nonzero_exit(self, transformer_with_tools, si_structure):
        """chargemol returning non-zero exit should be handled gracefully."""
        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.subprocess.run"
            ) as mock_run,
            patch("lematerial_fetcher.fetcher.lematrho.transform.write_potcar"),
            patch.object(transformer_with_tools, "_write_chargemol_config"),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake data"
            mock_run.side_effect = subprocess.CalledProcessError(
                1, "chargemol", stderr=b"chargemol error"
            )

            result = transformer_with_tools._run_ddec6_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert result is None


# ---------------------------------------------------------------------------
# Temp directory cleanup tests
# ---------------------------------------------------------------------------


class TestTempDirectoryCleanup:
    def test_cleanup_on_success(self, transformer_with_tools, si_structure):
        """Temp directory should be cleaned up after successful analysis."""
        created_tmpdir = [None]

        original_tempdir = tempfile.TemporaryDirectory

        class TrackingTempDir:
            def __init__(self, *args, **kwargs):
                self._real = original_tempdir(*args, **kwargs)
                created_tmpdir[0] = self._real.name

            def __enter__(self):
                return self._real.__enter__()

            def __exit__(self, *args):
                return self._real.__exit__(*args)

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.subprocess.run"
            ),
            patch("lematerial_fetcher.fetcher.lematrho.transform.write_potcar"),
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.parse_acf_dat"
            ) as mock_parse,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.read_potcar_zval"
            ) as mock_zval,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.tempfile.TemporaryDirectory",
                TrackingTempDir,
            ),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.return_value = b"fake data"
            mock_parse.return_value = ([4.0, 4.0], [10.0, 12.0])
            mock_zval.return_value = {"Si": 4.0}

            transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert created_tmpdir[0] is not None
        assert not os.path.exists(created_tmpdir[0])

    def test_cleanup_on_failure(self, transformer_with_tools, si_structure):
        """Temp directory should be cleaned up even on failure."""
        created_tmpdir = [None]

        original_tempdir = tempfile.TemporaryDirectory

        class TrackingTempDir:
            def __init__(self, *args, **kwargs):
                self._real = original_tempdir(*args, **kwargs)
                created_tmpdir[0] = self._real.name

            def __enter__(self):
                return self._real.__enter__()

            def __exit__(self, *args):
                return self._real.__exit__(*args)

        with (
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.download_gz_file_from_s3"
            ) as mock_dl,
            patch(
                "lematerial_fetcher.fetcher.lematrho.transform.tempfile.TemporaryDirectory",
                TrackingTempDir,
            ),
            patch.object(
                type(transformer_with_tools),
                "aws_client",
                new_callable=lambda: property(lambda self: MagicMock()),
            ),
        ):
            mock_dl.side_effect = Exception("S3 error")

            transformer_with_tools._run_bader_analysis(
                si_structure, "agm000001", "agm000001"
            )

        assert created_tmpdir[0] is not None
        assert not os.path.exists(created_tmpdir[0])


# ---------------------------------------------------------------------------
# Parsing function unit tests
# ---------------------------------------------------------------------------


class TestParseAcfDat:
    def test_parse_standard_format(self, tmp_path):
        """Parse a standard ACF.dat file."""
        acf_content = (
            "    #         X           Y           Z        CHARGE      MIN DIST   ATOMIC VOL\n"
            " -----------------------------------------------------------------------\n"
            "    1    0.000000    0.000000    0.000000    3.112903    1.235698   19.382956\n"
            "    2    1.357500    1.357500    1.357500    4.887097    1.235698   13.617044\n"
            " -----------------------------------------------------------------------\n"
            "    VACUUM CHARGE:               0.0000\n"
            "    VACUUM VOLUME:               0.0000\n"
            "    NUMBER OF ELECTRONS:         8.0000\n"
        )
        acf_file = tmp_path / "ACF.dat"
        acf_file.write_text(acf_content)

        counts, volumes = parse_acf_dat(str(acf_file))

        assert len(counts) == 2
        assert len(volumes) == 2
        assert abs(counts[0] - 3.112903) < 1e-6
        assert abs(counts[1] - 4.887097) < 1e-6
        assert abs(volumes[0] - 19.382956) < 1e-6
        assert abs(volumes[1] - 13.617044) < 1e-6

    def test_parse_single_atom(self, tmp_path):
        """Parse ACF.dat with a single atom."""
        acf_content = (
            "    #         X           Y           Z        CHARGE      MIN DIST   ATOMIC VOL\n"
            " -----------------------------------------------------------------------\n"
            "    1    0.000000    0.000000    0.000000    8.000000    2.715000   40.000000\n"
            " -----------------------------------------------------------------------\n"
        )
        acf_file = tmp_path / "ACF.dat"
        acf_file.write_text(acf_content)

        counts, volumes = parse_acf_dat(str(acf_file))

        assert counts == [8.0]
        assert volumes == [40.0]


class TestReadPotcarZval:
    def test_parse_potcar(self, tmp_path):
        """Parse POTCAR for valence electron counts."""
        potcar_content = (
            "   TITEL  = PAW_PBE Si 05Jan2001\n"
            "   ZVAL   =    4.00000\n"
            "   END of PSCTR\n"
            "   TITEL  = PAW_PBE O 08Apr2002\n"
            "   ZVAL   =    6.00000\n"
            "   END of PSCTR\n"
        )
        potcar_file = tmp_path / "POTCAR"
        potcar_file.write_text(potcar_content)

        zval = read_potcar_zval(str(potcar_file))

        assert zval["Si"] == 4.0
        assert zval["O"] == 6.0

    def test_parse_potcar_with_underscore_element(self, tmp_path):
        """Handle element names like Si_d in POTCAR."""
        potcar_content = (
            "   TITEL  = PAW_PBE Si_d 05Jan2001\n"
            "   ZVAL   =    4.00000\n"
            "   END of PSCTR\n"
        )
        potcar_file = tmp_path / "POTCAR"
        potcar_file.write_text(potcar_content)

        zval = read_potcar_zval(str(potcar_file))

        assert zval["Si"] == 4.0


class TestParseDdec6Charges:
    def test_parse_standard_output(self, tmp_path):
        """Parse DDEC6 output file."""
        ddec6_content = (
            " 2\n"
            " Charge analysis\n"
            " Si    0.000000    0.000000    0.000000    0.123456\n"
            " Si    1.357500    1.357500    1.357500   -0.123456\n"
        )
        ddec6_file = tmp_path / "DDEC6_even_tempered_net_atomic_charges.xyz"
        ddec6_file.write_text(ddec6_content)

        charges = parse_ddec6_charges(str(tmp_path))

        assert len(charges) == 2
        assert abs(charges[0] - 0.123456) < 1e-6
        assert abs(charges[1] - (-0.123456)) < 1e-6


# ---------------------------------------------------------------------------
# Cross-compatibility function tests
# ---------------------------------------------------------------------------


class TestGetCrossCompatibility:
    def test_normal_elements(self):
        assert get_cross_compatibility(["Si", "O"]) is True

    def test_yb_excluded(self):
        assert get_cross_compatibility(["Yb", "O"]) is False

    def test_yb_in_larger_set(self):
        assert get_cross_compatibility(["Fe", "Yb", "O"]) is False

    def test_empty_elements(self):
        assert get_cross_compatibility([]) is True


# ---------------------------------------------------------------------------
# Tool validation tests
# ---------------------------------------------------------------------------


class TestValidateTools:
    def test_all_tools_available(self, mock_config):
        """When all tools are found, can_run_bader and can_run_ddec6 should be True."""
        with (
            patch("shutil.which", return_value="/usr/bin/tool"),
            patch("os.path.isfile", return_value=True),
            patch("os.path.isdir", return_value=True),
            patch.dict(os.environ, {"PMG_VASP_PSP_DIR": "/path/to/psp"}),
        ):
            transformer = LeMatRhoTransformer(config=mock_config, debug=True)

        assert transformer.can_run_bader is True
        assert transformer.can_run_ddec6 is True

    def test_no_tools_available(self):
        """When no tools are found, can_run_bader and can_run_ddec6 should be False."""
        config = TransformerConfig(
            source_db_conn_str="mock://source",
            dest_db_conn_str="mock://dest",
            source_table_name="test_source",
            dest_table_name="test_dest",
            batch_size=100,
            page_offset=0,
            log_every=100,
            log_dir="./logs",
            max_retries=3,
            page_limit=10,
            num_workers=1,
            retry_delay=2,
            # No tool paths set
        )
        with (
            patch("shutil.which", return_value=None),
            patch.dict(os.environ, {}, clear=True),
        ):
            transformer = LeMatRhoTransformer(config=config, debug=True)

        assert transformer.can_run_bader is False
        assert transformer.can_run_ddec6 is False

    def test_no_pmg_vasp_psp_dir(self, mock_config):
        """Without PMG_VASP_PSP_DIR, both analyses should be disabled."""
        with (
            patch("shutil.which", return_value="/usr/bin/tool"),
            patch("os.path.isfile", return_value=True),
            patch("os.path.isdir", return_value=True),
            patch.dict(os.environ, {}, clear=True),
        ):
            transformer = LeMatRhoTransformer(config=mock_config, debug=True)

        assert transformer._can_generate_potcar is False
        assert transformer.can_run_bader is False
        assert transformer.can_run_ddec6 is False
