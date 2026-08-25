"""Companies House connector for the UK Companies House Public Data API.

The Companies House REST API is snapshot-only across all six modelled tables
(no cursor / incremental filter, no ``updated_since`` parameter). Each table
is keyed by ``company_number`` and produced by hitting one of six endpoints
per company. Since the natural unit of parallel work is a single
``company_number``, this connector implements ``SupportsPartitionedStream``
and partitions by company number — each partition fetches records for one
company independently on an executor.

Termination guarantee: for snapshot tables ``latest_offset`` returns a fixed
``_init_time`` cap. After the first micro-batch drains, subsequent
``latest_offset`` calls return the same value and ``Trigger.AvailableNow``
converges.
"""

# pylint: disable=too-many-lines

import time
from datetime import datetime, timezone
from typing import Iterator, Sequence

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import (
    LakeflowConnect,
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.companies_house.companies_house_schemas import (
    SUPPORTED_TABLES,
    TABLE_ENDPOINT_PATHS,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)


BASE_URL = "https://api.company-information.service.gov.uk"

# Retry policy for HTTP 429 / 5xx responses.
RETRIABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1.0

# Rate limit is 600 requests / 5-minute window (~2 req/sec).
DEFAULT_TIMEOUT_SECONDS = 30

# Pagination defaults for endpoints that expose start_index / items_per_page.
DEFAULT_ITEMS_PER_PAGE = 100  # API max is 100 for filing_history; officers accepts up to 100.

# Fallback for the ``_ingested_by`` audit column when the pipeline spec
# doesn't pass a ``pipeline_name`` table option. There is no per-request
# human identity inside a Spark Python Data Source worker (no dbutils, no
# notebook user context), so this names the connector rather than a person.
INGESTED_BY = "companies_house_lakeflow_connector"

# Databricks SQL Statement Execution API — used only when ``company_numbers_table``
# is configured, to look up the watchlist from a UC table instead of a static
# connection option. Polling / retry knobs mirror the Companies House API ones.
SQL_STATEMENT_WAIT_TIMEOUT = "30s"
SQL_STATEMENT_POLL_SECONDS = 2.0
SQL_STATEMENT_MAX_POLLS = 60  # 60 * 2s = 2 minutes before giving up


class CompaniesHouseLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Companies House UK Public Data API.

    Expected options:
        - ``api_key``: HTTP Basic Auth API key (used as username, empty password).
        - ``company_numbers``: comma-separated list of 8-char zero-padded
          company registration numbers to fetch. Non-standard prefixes
          (``SC``, ``NI``, ``OC``, ...) are preserved as-is. Required unless
          ``company_numbers_table`` is set.
        - ``base_url`` (optional): override the default API host.

    Reference-table watchlist (optional, in place of ``company_numbers``):
        - ``company_numbers_table``: fully-qualified UC table name
          (``catalog.schema.table``) with a ``company_number`` string column.
          When set, the watchlist is looked up via the Databricks SQL
          Statement Execution API on every batch instead of being fixed at
          connection-creation time, so users can maintain it by editing the
          table. Requires ``databricks_host``, ``databricks_token``, and
          ``warehouse_id``.
        - ``databricks_host``: workspace URL, e.g. ``https://xxx.cloud.databricks.com``.
        - ``databricks_token``: PAT with SELECT on the table and CAN_USE on the warehouse.
        - ``warehouse_id``: SQL warehouse to run the lookup query on.

    The connector runs inside Spark Python Data Source workers, so it never
    references SparkSession/dbutils (see class docstring); the reference-table
    lookup goes over plain HTTPS via ``requests``, same as the Companies House
    API calls themselves.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Companies House connector requires 'api_key' in options")
        self._api_key = api_key

        self._base_url = options.get("base_url", BASE_URL).rstrip("/")

        # Resolved lazily (see _get_company_numbers) so a reference-table
        # lookup — if configured — only happens where it's actually needed
        # (get_partitions / read_table, both driver-side), not on every
        # executor instantiation for read_partition.
        self._company_numbers_cache: list[str] | None = None

        # Cap the streaming offset at init time so Trigger.AvailableNow
        # terminates for snapshot tables — see class docstring.
        self._init_time = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # LakeflowConnect surface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Single-driver fallback: iterate all company numbers sequentially.

        Used by ``simpleStreamReader`` when ``is_partitioned`` returns False
        for a table, or by the batch path when partitioning fails. All
        modelled tables are snapshot, so the returned offset is always empty.
        """
        self._validate_table(table_name)

        session = self._build_session()
        try:
            records: list[dict] = []
            for company_number in self._get_company_numbers():
                records.extend(
                    self._read_records_for_company(
                        table_name, company_number, table_options, session
                    )
                )
        finally:
            session.close()

        # Snapshot tables — no cursor to advance.
        return iter(records), {}

    # ------------------------------------------------------------------
    # SupportsPartitionedStream surface
    # ------------------------------------------------------------------

    def is_partitioned(self, table_name: str) -> bool:
        """All six tables can be partitioned by company_number."""
        return table_name in SUPPORTED_TABLES

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        """Return a stable init-time snapshot offset.

        The REST API has no server-side cursor and every table is snapshot,
        so a micro-batch's job is simply to re-read all company numbers
        once. Returning a fixed ``{"snapshot": _init_time}`` means the
        second ``latest_offset`` call in the same trigger sees an
        unchanged value and ``Trigger.AvailableNow`` converges.
        """
        self._validate_table(table_name)
        return {"snapshot": self._init_time}

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        """Yield one partition descriptor per company_number.

        For streaming, when ``start_offset == end_offset`` there is nothing
        new to read (the snapshot has already been drained) — return no
        partitions so the micro-batch is empty.
        """
        self._validate_table(table_name)

        if start_offset is not None and end_offset is not None and start_offset == end_offset:
            return []

        return [
            {"company_number": company_number} for company_number in self._get_company_numbers()
        ]

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        """Fetch all records for one company_number.

        Runs on Spark executors. Must be self-contained: rebuild the HTTP
        session locally rather than reusing driver state.
        """
        self._validate_table(table_name)
        company_number = partition.get("company_number")
        if not company_number:
            raise ValueError(f"Missing 'company_number' in partition descriptor: {partition!r}")

        session = self._build_session()
        try:
            records = self._read_records_for_company(
                table_name, company_number, table_options, session
            )
        finally:
            session.close()
        return iter(records)

    # ------------------------------------------------------------------
    # Watchlist resolution (static option or UC reference table)
    # ------------------------------------------------------------------

    def _get_company_numbers(self) -> list[str]:
        """Resolve the company_number watchlist, memoized per instance.

        Prefers ``company_numbers_table`` (a UC reference table users can
        maintain) when set; falls back to the static ``company_numbers``
        option otherwise. Only called from driver-side methods
        (get_partitions / read_table) — never from read_partition — so a
        reference-table lookup runs at most once per batch, not once per
        partition.
        """
        if self._company_numbers_cache is not None:
            return self._company_numbers_cache

        table = self.options.get("company_numbers_table")
        if table:
            numbers = self._fetch_company_numbers_from_table(table)
        else:
            raw = self.options.get("company_numbers", "")
            numbers = [cn.strip() for cn in raw.split(",") if cn.strip()]

        numbers = [
            self._normalize_company_number(cn.strip()) for cn in numbers if cn and cn.strip()
        ]
        if not numbers:
            raise ValueError(
                "Companies House connector requires either 'company_numbers' "
                "(comma-separated list) or 'company_numbers_table' "
                "(a UC table with a company_number column) in options."
            )

        self._company_numbers_cache = numbers
        return numbers

    def _fetch_company_numbers_from_table(self, table: str) -> list[str]:
        """Query a UC reference table for the current watchlist.

        Uses the Databricks SQL Statement Execution API over plain HTTPS —
        the connector never touches SparkSession/dbutils (see class
        docstring), so this is a REST call, not ``spark.table(...)``.
        """
        host = self.options.get("databricks_host")
        token = self.options.get("databricks_token")
        warehouse_id = self.options.get("warehouse_id")
        missing = [
            name
            for name, value in (
                ("databricks_host", host),
                ("databricks_token", token),
                ("warehouse_id", warehouse_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"'company_numbers_table' is set but missing required options: {missing}. "
                "The reference-table watchlist needs 'databricks_host', "
                "'databricks_token', and 'warehouse_id'."
            )

        host = host.rstrip("/")
        headers = {"Authorization": f"Bearer {token}"}
        session = requests.Session()
        try:
            statement_id = self._submit_sql_statement(session, host, headers, warehouse_id, table)
            return self._collect_sql_statement_results(session, host, headers, statement_id)
        finally:
            session.close()

    def _submit_sql_statement(
        self,
        session: requests.Session,
        host: str,
        headers: dict[str, str],
        warehouse_id: str,
        table: str,
    ) -> str:
        response = session.post(
            f"{host}/api/2.0/sql/statements",
            headers=headers,
            json={
                "warehouse_id": warehouse_id,
                "statement": f"SELECT company_number FROM {table}",
                "wait_timeout": SQL_STATEMENT_WAIT_TIMEOUT,
            },
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to query company_numbers_table {table!r}: "
                f"{response.status_code} {response.text}"
            )
        body = response.json()
        return self._await_sql_statement_success(session, host, headers, body)

    def _await_sql_statement_success(
        self,
        session: requests.Session,
        host: str,
        headers: dict[str, str],
        body: dict,
    ) -> str:
        statement_id = body["statement_id"]
        state = body.get("status", {}).get("state")
        for _ in range(SQL_STATEMENT_MAX_POLLS):
            if state == "SUCCEEDED":
                return statement_id
            if state in ("FAILED", "CANCELED", "CLOSED"):
                error = body.get("status", {}).get("error", {})
                raise RuntimeError(
                    f"Query against company_numbers_table failed (state={state}): {error}"
                )
            time.sleep(SQL_STATEMENT_POLL_SECONDS)
            poll = session.get(
                f"{host}/api/2.0/sql/statements/{statement_id}",
                headers=headers,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
            if poll.status_code != 200:
                raise RuntimeError(
                    f"Failed to poll statement {statement_id}: {poll.status_code} {poll.text}"
                )
            body = poll.json()
            state = body.get("status", {}).get("state")
        raise RuntimeError(
            f"Timed out waiting for company_numbers_table query {statement_id} to complete"
        )

    def _collect_sql_statement_results(
        self,
        session: requests.Session,
        host: str,
        headers: dict[str, str],
        statement_id: str,
    ) -> list[str]:
        # Re-fetch the statement to get its manifest + first result chunk.
        response = session.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch results for statement {statement_id}: "
                f"{response.status_code} {response.text}"
            )
        body = response.json()
        manifest = body.get("manifest", {})
        total_chunks = manifest.get("total_chunk_count", 0)

        numbers: list[str] = []
        result = body.get("result", {})
        numbers.extend(row[0] for row in result.get("data_array", []) if row and row[0])

        for chunk_index in range(1, total_chunks):
            chunk_response = session.get(
                f"{host}/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}",
                headers=headers,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
            if chunk_response.status_code != 200:
                raise RuntimeError(
                    f"Failed to fetch result chunk {chunk_index} for statement "
                    f"{statement_id}: {chunk_response.status_code} {chunk_response.text}"
                )
            chunk_body = chunk_response.json()
            numbers.extend(row[0] for row in chunk_body.get("data_array", []) if row and row[0])

        return numbers

    # ------------------------------------------------------------------
    # Per-company read dispatch
    # ------------------------------------------------------------------

    def _read_records_for_company(
        self,
        table_name: str,
        company_number: str,
        table_options: dict[str, str],
        session: requests.Session,
    ) -> list[dict]:
        """Route to the right endpoint reader for one company_number.

        Every returned record gets ``_ingested_at`` / ``_ingested_by`` /
        ``_source_api_url`` audit fields stamped on here — a single
        injection point shared by all six tables, rather than repeating it
        in each endpoint reader.
        """
        if table_name == "company_profile":
            records = self._read_single_object(table_name, company_number, table_options, session)
        elif table_name == "registered_office_address":
            records = self._read_single_object(table_name, company_number, table_options, session)
        elif table_name == "charges":
            records = self._read_non_paginated_list(
                table_name, company_number, table_options, session
            )
        elif table_name in {"officers", "filing_history", "persons_with_significant_control"}:
            records = self._read_paginated_list(table_name, company_number, table_options, session)
        else:
            raise ValueError(f"Unsupported table: {table_name!r}")

        ingested_at = datetime.now(timezone.utc).isoformat()
        # ``pipeline_name`` is a table option a pipeline spec can pass
        # through (must be in the connection's externalOptionsAllowList);
        # falls back to a fixed connector identifier when not supplied.
        ingested_by = table_options.get("pipeline_name") or INGESTED_BY
        source_api_url = f"{self._base_url}{self._format_path(table_name, company_number)}"
        for index, record in enumerate(records):
            record["_ingested_at"] = ingested_at
            record["_ingested_by"] = ingested_by
            record["_source_api_url"] = source_api_url
            # Prefer the API's own resource identifier (links.self); fall
            # back to a positional id so this is *always* unique within one
            # company's fetch, even if the API omits links.self on some
            # items — used as part of the primary key for officers,
            # persons_with_significant_control, and charges (see
            # companies_house_schemas.TABLE_METADATA) because their visible
            # business fields aren't reliably unique.
            links_self = (
                (record.get("links") or {}).get("self")
                if isinstance(record.get("links"), dict)
                else None
            )
            record["_source_record_url"] = links_self or f"{source_api_url}#{index}"
        return records

    # ------------------------------------------------------------------
    # Endpoint readers
    # ------------------------------------------------------------------

    def _read_single_object(
        self,
        table_name: str,
        company_number: str,
        table_options: dict[str, str],
        session: requests.Session,
    ) -> list[dict]:
        """Fetch a single-object endpoint (company_profile / registered_office_address).

        Returns an empty list for HTTP 404 (company not found) so a
        mistyped company_number does not fail the whole batch.
        """
        path = self._format_path(table_name, company_number)
        response = self._request_with_retry(session, "GET", path)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            raise RuntimeError(
                f"Companies House API error for {table_name} "
                f"company={company_number}: {response.status_code} {response.text}"
            )

        body = response.json() or {}
        if not isinstance(body, dict):
            raise ValueError(f"Unexpected response type for {table_name}: {type(body).__name__}")

        record = dict(body)
        # The registered_office_address endpoint does not echo the
        # company_number field — inject it so downstream consumers can
        # join back to company_profile.
        record.setdefault("company_number", company_number)
        return [record]

    def _read_non_paginated_list(
        self,
        table_name: str,
        company_number: str,
        table_options: dict[str, str],
        session: requests.Session,
    ) -> list[dict]:
        """Fetch a list-envelope endpoint that returns all items in one call.

        Used for ``charges``, which has no start_index/items_per_page.
        """
        path = self._format_path(table_name, company_number)
        response = self._request_with_retry(session, "GET", path)
        if response.status_code == 404:
            return []
        if response.status_code != 200:
            raise RuntimeError(
                f"Companies House API error for {table_name} "
                f"company={company_number}: {response.status_code} {response.text}"
            )

        body = response.json() or {}
        items = body.get("items") or []
        if not isinstance(items, list):
            raise ValueError(f"Unexpected 'items' type for {table_name}: {type(items).__name__}")

        records: list[dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            record = dict(item)
            # List endpoints don't include the company_number on each item.
            record.setdefault("company_number", company_number)
            records.append(record)
        return records

    def _read_paginated_list(
        self,
        table_name: str,
        company_number: str,
        table_options: dict[str, str],
        session: requests.Session,
    ) -> list[dict]:
        """Fetch an offset-paginated list endpoint (officers / filing_history / PSC).

        Uses ``start_index`` + ``items_per_page``. Advances by the actual
        number of items returned (per API docs — last page can be short).
        Terminates when items is empty or ``start_index >= total_results``.
        """
        path = self._format_path(table_name, company_number)
        items_per_page = self._resolve_items_per_page(table_options)

        records: list[dict] = []
        start_index = 0
        while True:
            params: dict[str, str] = {
                "start_index": str(start_index),
                "items_per_page": str(items_per_page),
            }
            # filing_history supports an optional category filter.
            if table_name == "filing_history" and "category" in table_options:
                params["category"] = table_options["category"]
            # persons_with_significant_control supports optional register_view.
            if (
                table_name == "persons_with_significant_control"
                and "register_view" in table_options
            ):
                params["register_view"] = table_options["register_view"]

            response = self._request_with_retry(session, "GET", path, params=params)
            if response.status_code == 404:
                return records
            if response.status_code != 200:
                raise RuntimeError(
                    f"Companies House API error for {table_name} "
                    f"company={company_number}: {response.status_code} {response.text}"
                )

            body = response.json() or {}
            items = body.get("items") or []
            if not isinstance(items, list):
                raise ValueError(
                    f"Unexpected 'items' type for {table_name}: {type(items).__name__}"
                )

            if not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                record = dict(item)
                record.setdefault("company_number", company_number)
                records.append(record)

            # Advance by actual returned length — the last page may be short.
            start_index += len(items)

            # Envelope reports totals under different keys per endpoint:
            #   filing_history → total_count
            #   officers / PSC → total_results
            total = body.get("total_results")
            if total is None:
                total = body.get("total_count")
            if isinstance(total, int) and start_index >= total:
                break

            # Defensive: if the API returned fewer than requested, we've
            # hit the tail even without a total.
            if len(items) < items_per_page:
                break

        return records

    # ------------------------------------------------------------------
    # HTTP plumbing
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        """Build a fresh requests.Session with basic-auth pre-configured.

        A new session is created per driver / executor invocation so no
        non-picklable state (Session objects hold sockets) ships across
        the wire.
        """
        session = requests.Session()
        session.auth = (self._api_key, "")
        session.headers.update({"Accept": "application/json"})
        return session

    def _request_with_retry(
        self,
        session: requests.Session,
        method: str,
        path: str,
        params: dict | None = None,
    ) -> requests.Response:
        """Issue an HTTP request with exponential backoff on retriable statuses.

        Honours the ``Retry-After`` header on 429 responses when present;
        otherwise falls back to exponential backoff.
        """
        url = f"{self._base_url}{path}"
        backoff = INITIAL_BACKOFF_SECONDS
        last_response: requests.Response | None = None
        for attempt in range(MAX_RETRIES):
            response = session.request(method, url, params=params, timeout=DEFAULT_TIMEOUT_SECONDS)
            last_response = response
            if response.status_code not in RETRIABLE_STATUS_CODES:
                return response

            if attempt == MAX_RETRIES - 1:
                break

            # Respect Retry-After when the server provides it.
            retry_after = response.headers.get("Retry-After")
            delay = backoff
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except (TypeError, ValueError):
                    pass
            time.sleep(delay)
            backoff *= 2

        assert last_response is not None  # loop always assigns
        return last_response

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Table {table_name!r} is not supported. Supported tables: {list(SUPPORTED_TABLES)}"
            )

    def _format_path(self, table_name: str, company_number: str) -> str:
        template = TABLE_ENDPOINT_PATHS[table_name]
        return template.format(company_number=company_number)

    def _resolve_items_per_page(self, table_options: dict[str, str]) -> int:
        raw = table_options.get("items_per_page")
        if raw is None:
            return DEFAULT_ITEMS_PER_PAGE
        try:
            v = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_ITEMS_PER_PAGE
        # API max is 100.
        return max(1, min(v, 100))

    @staticmethod
    def _normalize_company_number(company_number: str) -> str:
        """Zero-pad numeric company numbers to 8 chars; preserve alpha prefixes.

        Companies House registration numbers are 8 characters. Purely
        numeric numbers (e.g. ``6``) get zero-padded to ``00000006``.
        Numbers with alpha prefixes (``SC``, ``NI``, ``OC``, ``SO``,
        ``IP``, ``IC``, ``R0``) are left as-is because the pad rule is
        different for each prefix and the caller typically already
        supplies them padded.
        """
        if not company_number:
            return company_number
        if company_number.isdigit():
            return company_number.zfill(8)
        return company_number
