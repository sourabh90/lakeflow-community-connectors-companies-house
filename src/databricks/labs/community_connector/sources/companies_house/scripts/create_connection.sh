#!/usr/bin/env bash
# Create the Unity Catalog COMMUNITY connection for the companies_house connector.
#
# Usage:
#   COMPANIES_HOUSE_API_KEY=<key> COMPANIES_HOUSE_COMPANY_NUMBERS=<numbers> \
#     ./create_connection.sh [connection_name] [databricks_profile]
#
# Env vars:
#   COMPANIES_HOUSE_API_KEY               required. Companies House API key (secret).
#   COMPANIES_HOUSE_COMPANY_NUMBERS       comma-separated company numbers,
#                                         e.g. "00000006,SC123456". Required
#                                         unless COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE
#                                         is set.
#   COMPANIES_HOUSE_BASE_URL              optional. Override the API base URL.
#
#   Reference-table watchlist (alternative to COMPANIES_HOUSE_COMPANY_NUMBERS —
#   see README.md "Reference-Table Watchlist"). All four required together:
#   COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE  UC table (catalog.schema.table) with
#                                          a company_number column.
#   COMPANIES_HOUSE_DATABRICKS_HOST        workspace URL, e.g.
#                                          https://xxx.cloud.databricks.com
#   COMPANIES_HOUSE_DATABRICKS_TOKEN       PAT with SELECT on the table and
#                                          CAN_USE on the warehouse (secret).
#   COMPANIES_HOUSE_WAREHOUSE_ID           SQL warehouse ID for the lookup query.
#
# Args:
#   connection_name    optional. Defaults to "companies_house_conn".
#   databricks_profile optional. Defaults to "cognizant".
#
# Credentials are read from the environment, never hardcoded here or passed as
# literal arguments, so they don't end up in shell history or this file.

set -euo pipefail

CONNECTION_NAME="${1:-companies_house_conn}"
DATABRICKS_PROFILE="${2:-cognizant}"

if [[ -z "${COMPANIES_HOUSE_API_KEY:-}" ]]; then
  echo "Error: COMPANIES_HOUSE_API_KEY is not set." >&2
  exit 1
fi

if [[ -z "${COMPANIES_HOUSE_COMPANY_NUMBERS:-}" && -z "${COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE:-}" ]]; then
  echo "Error: set either COMPANIES_HOUSE_COMPANY_NUMBERS or COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE." >&2
  exit 1
fi

if [[ -n "${COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE:-}" ]]; then
  missing=()
  [[ -z "${COMPANIES_HOUSE_DATABRICKS_HOST:-}" ]] && missing+=("COMPANIES_HOUSE_DATABRICKS_HOST")
  [[ -z "${COMPANIES_HOUSE_DATABRICKS_TOKEN:-}" ]] && missing+=("COMPANIES_HOUSE_DATABRICKS_TOKEN")
  [[ -z "${COMPANIES_HOUSE_WAREHOUSE_ID:-}" ]] && missing+=("COMPANIES_HOUSE_WAREHOUSE_ID")
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Error: COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE is set but missing: ${missing[*]}" >&2
    exit 1
  fi
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../../../../.." && pwd)"

OPTIONS_JSON=$(python3 -c '
import json, os, sys
options = {"api_key": os.environ["COMPANIES_HOUSE_API_KEY"]}
if os.environ.get("COMPANIES_HOUSE_COMPANY_NUMBERS"):
    options["company_numbers"] = os.environ["COMPANIES_HOUSE_COMPANY_NUMBERS"]
if os.environ.get("COMPANIES_HOUSE_BASE_URL"):
    options["base_url"] = os.environ["COMPANIES_HOUSE_BASE_URL"]
if os.environ.get("COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE"):
    options["company_numbers_table"] = os.environ["COMPANIES_HOUSE_COMPANY_NUMBERS_TABLE"]
    options["databricks_host"] = os.environ["COMPANIES_HOUSE_DATABRICKS_HOST"]
    options["databricks_token"] = os.environ["COMPANIES_HOUSE_DATABRICKS_TOKEN"]
    options["warehouse_id"] = os.environ["COMPANIES_HOUSE_WAREHOUSE_ID"]
json.dump(options, sys.stdout)
')

source "${REPO_ROOT}/.venv/bin/activate"

DATABRICKS_CONFIG_PROFILE="${DATABRICKS_PROFILE}" community-connector create_connection \
  companies_house "${CONNECTION_NAME}" \
  -o "${OPTIONS_JSON}"
