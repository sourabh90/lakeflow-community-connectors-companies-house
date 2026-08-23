"""Companies House source connector."""

from databricks.labs.community_connector.sources.companies_house.companies_house import (
    CompaniesHouseLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class CompaniesHouseDataSource(LakeflowSource):
    _lakeflow_connect_cls = CompaniesHouseLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "companies_house"


__all__ = [
    "CompaniesHouseLakeflowConnect",
    "CompaniesHouseDataSource",
]
