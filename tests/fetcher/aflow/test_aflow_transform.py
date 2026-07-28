# Copyright 2025 Entalpic
import json
import math
from pathlib import Path

import pytest

from lematerial_fetcher.fetcher.aflow.transform import AflowTransformer
from lematerial_fetcher.fetcher.aflow.utils import (
    build_structure_from_aflux,
    transform_aflux_entry,
)
from lematerial_fetcher.models.models import RawStructure
from lematerial_fetcher.models.optimade import Functional
from lematerial_fetcher.utils.config import TransformerConfig

FIXTURE_PATH = Path(__file__).parent / "aflux_page.json"

# Hand-crafted rocksalt NaCl in AFLUX conventions:
# species in POSCAR order, composition aligned with species,
# positions_fractional expanded in the same order.
ROCKSALT_NACL = {
    "geometry": [5.64, 5.64, 5.64, 90.0, 90.0, 90.0],
    "species": ["Cl", "Na"],
    "composition": [4, 4],
    "positions_fractional": [
        [0.5, 0.0, 0.0],
        [0.0, 0.5, 0.0],
        [0.0, 0.0, 0.5],
        [0.5, 0.5, 0.5],
        [0.0, 0.0, 0.0],
        [0.5, 0.5, 0.0],
        [0.5, 0.0, 0.5],
        [0.0, 0.5, 0.5],
    ],
    "auid": "aflow:0000000000000000",
    "compound": "Cl4Na4",
    "catalog": "ICSD",
    "energy_cell": -27.0,
    "aflowlib_date": "20200426_10:04:20_GMT-5",
}


@pytest.fixture
def aflux_page():
    """A real AFLUX page (5 entries) captured from the live API."""
    with open(FIXTURE_PATH) as f:
        return json.load(f)


@pytest.fixture
def transformer():
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
        num_workers=2,
        retry_delay=2,
    )
    return AflowTransformer(config=config, debug=True)


def test_build_structure_from_aflux_rocksalt():
    structure = build_structure_from_aflux(ROCKSALT_NACL)
    assert len(structure) == 8
    assert structure.composition.reduced_formula == "NaCl"
    assert math.isclose(structure.lattice.a, 5.64, rel_tol=1e-9)
    assert structure.lattice.is_orthogonal


def test_build_structure_from_aflux_string_encoded_fields():
    # AFLUX may serialize species/composition as comma-separated strings
    entry = dict(ROCKSALT_NACL)
    entry["species"] = "Cl,Na"
    entry["composition"] = "4,4"
    structure = build_structure_from_aflux(entry)
    assert len(structure) == 8
    assert structure.composition.reduced_formula == "NaCl"


def test_build_structure_from_aflux_site_count_mismatch():
    entry = dict(ROCKSALT_NACL)
    entry["composition"] = [4, 3]  # 7 sites declared, 8 positions given
    with pytest.raises(ValueError, match="Site count mismatch"):
        build_structure_from_aflux(entry)


def test_transform_aflux_entry_rocksalt():
    optimade = transform_aflux_entry(ROCKSALT_NACL, ROCKSALT_NACL["auid"])

    assert optimade.source == "aflow"
    assert optimade.immutable_id == "aflow-0000000000000000"
    assert optimade.id == f"aflow-0000000000000000-{Functional.PBE.value}"
    assert optimade.functional == Functional.PBE
    # Conservative until AFLOW DFT settings are audited
    assert optimade.cross_compatibility is False

    assert optimade.elements == ["Cl", "Na"]  # alphabetical
    assert optimade.nsites == 8
    assert optimade.chemical_formula_reduced == "ClNa"
    # Rocksalt is Fm-3m (space group 225)
    assert optimade.space_group_it_number == 225
    assert optimade.bawl_fingerprint

    # energy_cell flows into energy (then MP-2020 corrections apply downstream)
    assert optimade.energy is not None
    assert optimade.last_modified.year == 2020


def is_pbe(entry):
    return all(t == "PAW_PBE" for t in entry.get("dft_type", []))


def test_transform_all_pbe_fixture_entries(aflux_page):
    pbe_entries = [e for e in aflux_page if is_pbe(e)]
    assert len(pbe_entries) >= 4
    for entry in pbe_entries:
        optimade = transform_aflux_entry(entry, entry["auid"])
        assert optimade.source == "aflow"
        assert optimade.immutable_id == f"aflow-{entry['auid'].replace('aflow:', '')}"
        assert optimade.nsites == len(entry["positions_fractional"])
        assert optimade.bawl_fingerprint
        assert 1 <= optimade.space_group_it_number <= 230


def test_transform_rejects_non_pbe_entries(aflux_page):
    """AFLOW is not uniformly PBE (the fixture contains a PAW_LDA H entry);
    such entries must not be labeled functional=pbe."""
    non_pbe = [e for e in aflux_page if not is_pbe(e)]
    assert len(non_pbe) >= 1, "fixture should contain at least one non-PBE entry"
    for entry in non_pbe:
        with pytest.raises(ValueError, match="dft_type"):
            transform_aflux_entry(entry, entry["auid"])


def test_cross_catalog_duplicates_share_fingerprint(aflux_page):
    """The same material from different AFLOW catalogs must hash identically.

    The fixture contains N4 from both LIB1 and ICSD; BAWL should collapse them,
    which is what enables cross-source dedup in LeMat-Bulk.
    """
    n4_entries = [e for e in aflux_page if e["compound"] == "N4"]
    assert len(n4_entries) >= 2, "fixture should contain N4 from LIB1 and ICSD"
    fingerprints = {
        transform_aflux_entry(e, e["auid"]).bawl_fingerprint for e in n4_entries
    }
    assert len(fingerprints) == 1


def test_transform_row_returns_structures(transformer, aflux_page):
    entry = aflux_page[0]
    raw = RawStructure(
        id=entry["auid"],
        type="aflow-structure",
        attributes=entry,
        last_modified="2020-05-24",
    )
    result = transformer.transform_row(raw)
    assert len(result) == 1
    assert result[0].source == "aflow"


def test_transform_row_skips_invalid_entry(transformer):
    raw = RawStructure(
        id="aflow:deadbeef",
        type="aflow-structure",
        attributes={"geometry": [1, 1, 1, 90, 90, 90]},  # missing everything else
        last_modified=None,
    )
    assert transformer.transform_row(raw) == []
