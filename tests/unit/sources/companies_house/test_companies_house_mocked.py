"""Mock-based unit tests for CompaniesHouseLakeflowConnect's watchlist resolution.

Covers the static ``company_numbers`` option and the ``company_numbers_table``
reference-table lookup (Databricks SQL Statement Execution API), including
polling and multi-chunk results. These stub ``requests.Session`` so they run
without credentials or network access.
"""

from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.community_connector.sources.companies_house.companies_house import (
    CompaniesHouseLakeflowConnect,
)


def _response(status_code: int = 200, json_body=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else {}
    resp.text = str(json_body) if json_body is not None else ""
    return resp


def _conn(**extra_options):
    return CompaniesHouseLakeflowConnect({"api_key": "fake-key", **extra_options})


def test_falls_back_to_static_company_numbers():
    conn = _conn(company_numbers="00000006, SC123456")
    assert conn._get_company_numbers() == ["00000006", "SC123456"]


def test_raises_when_neither_option_is_set():
    conn = _conn()
    with pytest.raises(ValueError, match="company_numbers"):
        conn._get_company_numbers()


def test_company_numbers_table_requires_databricks_auth_options():
    conn = _conn(company_numbers_table="main.raw_reference.company_numbers")
    with pytest.raises(ValueError, match="databricks_host"):
        conn._get_company_numbers()


@patch(
    "databricks.labs.community_connector.sources.companies_house.companies_house.requests.Session"
)
def test_company_numbers_table_fetches_via_sql_statement_api(mock_session_cls):
    session = MagicMock()
    mock_session_cls.return_value = session
    statement_id = "stmt-1"
    succeeded_body = {
        "statement_id": statement_id,
        "status": {"state": "SUCCEEDED"},
        "manifest": {"total_chunk_count": 1},
        "result": {"data_array": [["07195160"], ["FC023246"]]},
    }
    session.post.return_value = _response(200, succeeded_body)
    session.get.return_value = _response(200, succeeded_body)

    conn = _conn(
        company_numbers_table="main.raw_reference.company_numbers",
        databricks_host="https://xxx.cloud.databricks.com/",
        databricks_token="fake-token",
        warehouse_id="wh-1",
    )

    assert conn._get_company_numbers() == ["07195160", "FC023246"]

    # POST body targets the configured table and warehouse.
    _, kwargs = session.post.call_args
    assert kwargs["json"]["warehouse_id"] == "wh-1"
    assert "main.raw_reference.company_numbers" in kwargs["json"]["statement"]

    # Memoized: a second call must not issue any further HTTP requests.
    session.post.reset_mock()
    session.get.reset_mock()
    assert conn._get_company_numbers() == ["07195160", "FC023246"]
    session.post.assert_not_called()
    session.get.assert_not_called()


@patch(
    "databricks.labs.community_connector.sources.companies_house.companies_house.requests.Session"
)
def test_company_numbers_table_polls_until_succeeded(mock_session_cls):
    session = MagicMock()
    mock_session_cls.return_value = session
    statement_id = "stmt-2"
    pending_body = {"statement_id": statement_id, "status": {"state": "PENDING"}}
    succeeded_body = {
        "statement_id": statement_id,
        "status": {"state": "SUCCEEDED"},
        "manifest": {"total_chunk_count": 1},
        "result": {"data_array": [["00000006"]]},
    }
    session.post.return_value = _response(200, pending_body)
    session.get.side_effect = [
        _response(200, pending_body),
        _response(200, succeeded_body),  # poll: now succeeded
        _response(200, succeeded_body),  # result fetch
    ]

    with patch(
        "databricks.labs.community_connector.sources.companies_house.companies_house.time.sleep"
    ):
        conn = _conn(
            company_numbers_table="main.raw_reference.company_numbers",
            databricks_host="https://xxx.cloud.databricks.com",
            databricks_token="fake-token",
            warehouse_id="wh-1",
        )
        assert conn._get_company_numbers() == ["00000006"]


@patch(
    "databricks.labs.community_connector.sources.companies_house.companies_house.requests.Session"
)
def test_company_numbers_table_collects_multiple_chunks(mock_session_cls):
    session = MagicMock()
    mock_session_cls.return_value = session
    statement_id = "stmt-3"
    succeeded_body = {
        "statement_id": statement_id,
        "status": {"state": "SUCCEEDED"},
        "manifest": {"total_chunk_count": 2},
        "result": {"data_array": [["00000006"]]},
    }
    chunk_1_body = {"data_array": [["SC123456"]]}
    session.post.return_value = _response(200, succeeded_body)
    session.get.side_effect = [
        _response(200, succeeded_body),  # result fetch (chunk 0 inline)
        _response(200, chunk_1_body),  # chunk 1
    ]

    conn = _conn(
        company_numbers_table="main.raw_reference.company_numbers",
        databricks_host="https://xxx.cloud.databricks.com",
        databricks_token="fake-token",
        warehouse_id="wh-1",
    )
    assert conn._get_company_numbers() == ["00000006", "SC123456"]


@patch(
    "databricks.labs.community_connector.sources.companies_house.companies_house.requests.Session"
)
def test_company_numbers_table_raises_on_failed_statement(mock_session_cls):
    session = MagicMock()
    mock_session_cls.return_value = session
    failed_body = {
        "statement_id": "stmt-4",
        "status": {"state": "FAILED", "error": {"message": "table not found"}},
    }
    session.post.return_value = _response(200, failed_body)

    conn = _conn(
        company_numbers_table="main.raw_reference.company_numbers",
        databricks_host="https://xxx.cloud.databricks.com",
        databricks_token="fake-token",
        warehouse_id="wh-1",
    )
    with pytest.raises(RuntimeError, match="table not found"):
        conn._get_company_numbers()


# ---------------------------------------------------------------------------
# _read_records_for_company — audit columns, incl. the _source_record_url
# primary-key fix for officers/PSC/charges duplicate-key failures seen
# against the live API (see companies_house_schemas.TABLE_METADATA).
# ---------------------------------------------------------------------------


def test_source_record_url_prefers_links_self_and_stays_unique_per_item():
    conn = _conn(company_numbers="00000006")
    # Two officer items sharing name/appointed_on/officer_role (the live API
    # has returned exactly this) but with distinct links.self.
    raw = [
        {"name": "A SMITH", "appointed_on": "2020-01-01", "links": {"self": "/appointments/1"}},
        {"name": "A SMITH", "appointed_on": "2020-01-01", "links": {"self": "/appointments/2"}},
    ]
    conn._read_paginated_list = MagicMock(return_value=raw)

    result = conn._read_records_for_company("officers", "00000006", {}, MagicMock())

    urls = [r["_source_record_url"] for r in result]
    assert urls == ["/appointments/1", "/appointments/2"]
    assert len(set(urls)) == len(result)


def test_source_record_url_falls_back_to_positional_index_when_links_self_missing():
    conn = _conn(company_numbers="00000006")
    # Live API has returned charge items with a null 'id' and no usable
    # links.self — the fallback must still guarantee uniqueness.
    raw = [{"id": None}, {"id": None}, {"id": None, "links": {}}]
    conn._read_non_paginated_list = MagicMock(return_value=raw)

    result = conn._read_records_for_company("charges", "00000006", {}, MagicMock())

    urls = [r["_source_record_url"] for r in result]
    assert len(set(urls)) == len(result)
    assert all(url is not None for url in urls)


def test_ingested_by_uses_pipeline_name_table_option_when_present():
    conn = _conn(company_numbers="00000006")
    conn._read_single_object = MagicMock(return_value=[{"company_number": "00000006"}])

    result = conn._read_records_for_company(
        "company_profile", "00000006", {"pipeline_name": "my_pipeline"}, MagicMock()
    )

    assert result[0]["_ingested_by"] == "my_pipeline"


def test_ingested_by_falls_back_to_constant_without_pipeline_name_option():
    conn = _conn(company_numbers="00000006")
    conn._read_single_object = MagicMock(return_value=[{"company_number": "00000006"}])

    result = conn._read_records_for_company("company_profile", "00000006", {}, MagicMock())

    assert result[0]["_ingested_by"] == "companies_house_lakeflow_connector"
