# Copyright 2025 Entalpic
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lematerial_fetcher.database.postgres import StructuresDatabase
from lematerial_fetcher.fetch import BatchInfo
from lematerial_fetcher.fetcher.aflow.fetch import AflowFetcher
from lematerial_fetcher.fetcher.aflow.utils import (
    AFLUX_OPTIONAL_PROPERTIES,
    AFLUX_REQUIRED_PROPERTIES,
    build_aflux_query,
    parse_aflowlib_date,
)
from lematerial_fetcher.utils.config import FetcherConfig

FIXTURE_PATH = Path(__file__).parent / "aflux_page.json"


@pytest.fixture
def aflux_page():
    """A real AFLUX page (5 entries) captured from the live API."""
    with open(FIXTURE_PATH) as f:
        return json.load(f)


@pytest.fixture
def mock_config():
    return FetcherConfig(
        base_url="https://aflow.test/API/aflux/",
        db_conn_str="postgresql://test:test@localhost:5432/test",
        table_name="test_table",
        page_limit=5,
        page_offset=0,
        mp_bucket_name="test-bucket",
        mp_bucket_prefix="test/prefix",
        log_dir="./logs",
        max_retries=3,
        num_workers=2,
        retry_delay=2,
        log_every=100,
    )


@pytest.fixture
def mock_db():
    return MagicMock(spec=StructuresDatabase)


def make_mock_session(json_payload):
    """Create a mock requests session whose GET returns the given JSON payload."""
    response = MagicMock()
    response.json.return_value = json_payload
    response.raise_for_status.return_value = None
    session = MagicMock()
    session.get.return_value = response
    return session


def test_build_aflux_query_contains_all_properties():
    url = build_aflux_query("https://aflow.test/API/aflux/", page=3, per_page=50)
    # Required properties use the existence filter so incomplete entries
    # (e.g. some LIB3 entries) are excluded server-side
    for prop in AFLUX_REQUIRED_PROPERTIES:
        assert f"{prop}(*)" in url
    for prop in AFLUX_OPTIONAL_PROPERTIES:
        assert prop in url
    assert "$paging(3,50)" in url


def test_parse_aflowlib_date_scalar():
    assert parse_aflowlib_date("20200426_10:04:20_GMT-5") == datetime(
        2020, 4, 26, 10, 4, 20
    )


def test_parse_aflowlib_date_list_takes_most_recent():
    dates = [
        "20120228_18:58:57_GMT-5",
        "20120229_03:56:50_GMT-5",
        "20210901_18:40:12_GMT-4",
    ]
    assert parse_aflowlib_date(dates) == datetime(2021, 9, 1, 18, 40, 12)


@pytest.mark.parametrize("bad_value", [None, "", [], "not-a-date"])
def test_parse_aflowlib_date_invalid(bad_value):
    assert parse_aflowlib_date(bad_value) is None


def test_get_items_to_process_uses_pagination_mode(mock_config):
    # BaseFetcher connects to the versioning table on init
    with patch("lematerial_fetcher.fetch.DatasetVersions"):
        fetcher = AflowFetcher(config=mock_config, debug=True)
    items_info = fetcher.get_items_to_process()
    assert items_info.start_offset == mock_config.page_offset
    # No cheap total count from AFLUX: rely on dynamic pagination
    assert items_info.total_count is None
    assert items_info.items is None


def test_process_batch_inserts_raw_structures(mock_config, mock_db, aflux_page):
    session = make_mock_session(aflux_page)
    with (
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase",
            return_value=mock_db,
        ),
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.create_session",
            return_value=session,
        ),
    ):
        more_data = AflowFetcher._process_batch(
            BatchInfo(offset=0, limit=5), mock_config, manager_dict={}
        )

    assert more_data is True
    # AFLUX pages are 1-based: offset 0 with limit 5 -> page 1
    requested_url = session.get.call_args[0][0]
    assert "$paging(1,5)" in requested_url

    mock_db.batch_insert_data.assert_called_once()
    structures = mock_db.batch_insert_data.call_args[0][0]
    assert len(structures) == len(aflux_page)
    for raw, entry in zip(structures, aflux_page):
        assert raw.id == entry["auid"]
        assert raw.type == "aflow-structure"
        assert raw.attributes == entry
        if entry.get("aflowlib_date"):
            # stored with day precision for versioning
            datetime.strptime(raw.last_modified, "%Y-%m-%d")


def test_process_batch_empty_page_stops_pagination(mock_config, mock_db):
    session = make_mock_session([])
    with (
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase",
            return_value=mock_db,
        ),
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.create_session",
            return_value=session,
        ),
    ):
        more_data = AflowFetcher._process_batch(
            BatchInfo(offset=100, limit=5), mock_config, manager_dict={}
        )

    assert more_data is False
    mock_db.batch_insert_data.assert_not_called()


def test_process_batch_non_list_payload_stops_pagination(mock_config, mock_db):
    # AFLUX error payloads are dicts, not lists
    session = make_mock_session({"error": "something went wrong"})
    with (
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase",
            return_value=mock_db,
        ),
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.create_session",
            return_value=session,
        ),
    ):
        more_data = AflowFetcher._process_batch(
            BatchInfo(offset=0, limit=5), mock_config, manager_dict={}
        )

    assert more_data is False
    mock_db.batch_insert_data.assert_not_called()


def test_process_batch_skips_entries_without_auid(mock_config, mock_db, aflux_page):
    page = [dict(aflux_page[0]), dict(aflux_page[1])]
    del page[0]["auid"]
    session = make_mock_session(page)
    with (
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.StructuresDatabase",
            return_value=mock_db,
        ),
        patch(
            "lematerial_fetcher.fetcher.aflow.fetch.create_session",
            return_value=session,
        ),
    ):
        more_data = AflowFetcher._process_batch(
            BatchInfo(offset=0, limit=5), mock_config, manager_dict={}
        )

    assert more_data is True
    structures = mock_db.batch_insert_data.call_args[0][0]
    assert len(structures) == 1
    assert structures[0].id == aflux_page[1]["auid"]
