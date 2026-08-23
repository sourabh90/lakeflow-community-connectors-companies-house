"""Simulate-mode tests for CompaniesHouseLakeflowConnect.

Runs against ``source_simulator/specs/companies_house/`` — the connector
never touches the real Companies House API in this suite. The simulator
serves synthetic corpus records generated from the connector's own
``TABLE_SCHEMAS`` (see ``tests/conftest.py``-style bootstrap in
``source_simulator/tools/corpus_from_schema.py``).

All six modelled tables are snapshot-only and partitioned by
``company_number`` — the connector implements
``SupportsPartitionedStream``.
"""

from databricks.labs.community_connector.sources.companies_house.companies_house import (
    CompaniesHouseLakeflowConnect,
)
from tests.unit.sources.test_partition_suite import SupportsPartitionedStreamTests
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestCompaniesHouseConnector(LakeflowConnectTests, SupportsPartitionedStreamTests):
    connector_class = CompaniesHouseLakeflowConnect
    simulator_source = "companies_house"
    # Stand-in credentials — the simulator ignores auth. The
    # ``company_numbers`` option is required by
    # ``CompaniesHouseLakeflowConnect.__init__`` and drives the partition
    # count for every table (one partition per company number).
    replay_config = {
        "api_key": "test_api_key_123",
        "company_numbers": "00000006,SC123456",
    }
