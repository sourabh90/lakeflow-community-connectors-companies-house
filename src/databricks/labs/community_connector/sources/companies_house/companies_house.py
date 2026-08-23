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


class CompaniesHouseLakeflowConnect(LakeflowConnect, SupportsPartitionedStream):
    """LakeflowConnect implementation for Companies House UK Public Data API.

    Expected options:
        - ``api_key``: HTTP Basic Auth API key (used as username, empty password).
        - ``company_numbers``: comma-separated list of 8-char zero-padded
          company registration numbers to fetch. Non-standard prefixes
          (``SC``, ``NI``, ``OC``, ...) are preserved as-is.
        - ``base_url`` (optional): override the default API host.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        api_key = options.get("api_key")
        if not api_key:
            raise ValueError("Companies House connector requires 'api_key' in options")
        self._api_key = api_key

        self._base_url = options.get("base_url", BASE_URL).rstrip("/")

        # Company numbers are supplied at connection level (comma-separated).
        raw = options.get("company_numbers", "")
        self._company_numbers = [
            self._normalize_company_number(cn.strip()) for cn in raw.split(",") if cn.strip()
        ]
        if not self._company_numbers:
            raise ValueError(
                "Companies House connector requires 'company_numbers' in options "
                "(comma-separated list of company registration numbers)."
            )

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
            for company_number in self._company_numbers:
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

        return [{"company_number": company_number} for company_number in self._company_numbers]

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
    # Per-company read dispatch
    # ------------------------------------------------------------------

    def _read_records_for_company(
        self,
        table_name: str,
        company_number: str,
        table_options: dict[str, str],
        session: requests.Session,
    ) -> list[dict]:
        """Route to the right endpoint reader for one company_number."""
        if table_name == "company_profile":
            return self._read_single_object(table_name, company_number, table_options, session)
        if table_name == "registered_office_address":
            return self._read_single_object(table_name, company_number, table_options, session)
        if table_name == "charges":
            return self._read_non_paginated_list(table_name, company_number, table_options, session)
        if table_name in {"officers", "filing_history", "persons_with_significant_control"}:
            return self._read_paginated_list(table_name, company_number, table_options, session)
        raise ValueError(f"Unsupported table: {table_name!r}")

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
