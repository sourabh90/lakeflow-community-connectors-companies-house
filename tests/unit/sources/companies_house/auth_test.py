"""Auth verification test for the Companies House connector.

Loads credentials from credentials.json in the project root, makes a
GET /company/{company_number} request with HTTP Basic Auth (api_key as
username, empty string as password), asserts HTTP 200, and prints the
company name on success.
"""

from __future__ import annotations

import sys
from pathlib import Path

import requests

# Allow imports from the repo root (for tests.unit.sources.test_utils)
_REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(_REPO_ROOT))

from tests.unit.sources.test_utils import load_config  # noqa: E402

BASE_URL = "https://api.company-information.service.gov.uk"
CREDENTIALS_PATH = _REPO_ROOT / "credentials.json"


def main() -> None:
    config = load_config(default_path=str(CREDENTIALS_PATH))

    api_key: str = config["api_key"]
    company_numbers_raw: str = config["company_numbers"]
    first_company_number = company_numbers_raw.split(",")[0].strip()

    url = f"{BASE_URL}/company/{first_company_number}"
    print(f"Verifying auth against: GET {url}")

    response = requests.get(url, auth=(api_key, ""))

    assert response.status_code == 200, (
        f"Expected HTTP 200 but got {response.status_code}. Response body: {response.text[:500]}"
    )

    data = response.json()
    company_name = data.get("company_name", "<not found in response>")
    print(f"Auth verification successful. Company name: {company_name}")


if __name__ == "__main__":
    main()
