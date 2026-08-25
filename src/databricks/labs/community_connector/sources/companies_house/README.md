# Lakeflow Companies House Community Connector

This documentation describes how to configure and use the **Companies House** Lakeflow community connector to ingest UK company registry data from the [Companies House Public Data REST API](https://developer.company-information.service.gov.uk/) into Databricks.

## Overview

**Companies House** is the United Kingdom's official registrar of companies. Every limited company, LLP, and other registrable entity incorporated in the UK is required to file information with Companies House, and that information is made available through a free public REST API keyed by an 8-character **company registration number**.

This connector wraps the Companies House Public Data API and exposes six company-centric tables that are commonly used for compliance, due-diligence, risk, KYC/AML, and beneficial-ownership analytics:

- `company_profile` — headline company record (name, status, incorporation date, accounts, addresses).
- `registered_office_address` — the current registered office address.
- `officers` — directors, secretaries, LLP members, and other officers.
- `filing_history` — every document filed by the company with Companies House.
- `persons_with_significant_control` — beneficial owners / PSC register entries.
- `charges` — registered mortgages and other charges against company assets.

All six tables are **snapshot** reads keyed by `company_number`, and the connector partitions work across companies (one Spark partition per `company_number`) using the `SupportsPartitionedStream` interface.

## Prerequisites

- **A Companies House Developer account and API key**:
  - Register at the [Companies House Developer Hub](https://developer.company-information.service.gov.uk/).
  - Create an application of type **"Live application"** (the "Test application" type only works against the sandbox environment).
  - Copy the generated **API key** — this is the credential you will supply to the connector as `api_key`.
- **A list of UK company registration numbers to ingest**:
  - The public REST API does not expose a "list all companies" endpoint. You must provide the connector with an explicit comma-separated list of `company_numbers` to fetch.
  - You can discover company numbers via the [Companies House search UI](https://find-and-update.company-information.service.gov.uk/) or from the [Companies House bulk data products](http://download.companieshouse.gov.uk/en_output.html).
- **Network access**: The environment running the connector must be able to reach `https://api.company-information.service.gov.uk`.
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `api_key` | string (secret) | yes | Companies House Public Data API key. Used as the username in HTTP Basic Auth with an empty password. | `abcd1234-ab12-cd34-ef56-abcdef123456` |
| `company_numbers` | string | conditional | Comma-separated list of UK company registration numbers to ingest. Purely numeric numbers are auto zero-padded to 8 characters (e.g. `6` becomes `00000006`). Alpha-prefixed numbers (`SC`, `NI`, `OC`, `SO`, `IP`, `IC`, `R0`) are passed through as-is and must already be correctly formatted. Required unless `company_numbers_table` is set. | `00000006,SC123456,NI012345` |
| `base_url` | string | no | Override the Companies House API base URL. Defaults to `https://api.company-information.service.gov.uk`. Only set for a proxy or non-production endpoint. | `https://api.company-information.service.gov.uk` |
| `company_numbers_table` | string | no | Fully-qualified UC table (`catalog.schema.table`) with a `company_number` string column, used **instead of** `company_numbers`. See [Reference-Table Watchlist](#reference-table-watchlist-alternative-to-company_numbers) below. Requires `databricks_host`, `databricks_token`, `warehouse_id`. | `main.raw_reference.company_numbers` |
| `databricks_host` | string | conditional | Workspace URL, used only to query `company_numbers_table`. Required if `company_numbers_table` is set. | `https://xxx.cloud.databricks.com` |
| `databricks_token` | string (secret) | conditional | Databricks PAT with `SELECT` on `company_numbers_table` and `CAN_USE` on `warehouse_id`. Required if `company_numbers_table` is set. | — |
| `warehouse_id` | string | conditional | SQL warehouse ID used to run the `company_numbers_table` lookup query. Required if `company_numbers_table` is set. | `d84369d67ab84390` |
| `externalOptionsAllowList` | string | conditional | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. Only required if you plan to override defaults via table options (see below). | `items_per_page,category,register_view,pipeline_name` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`items_per_page,category,register_view,pipeline_name`

> **Note**: Table-specific options such as `items_per_page`, `category`, and `register_view` are **not** connection parameters. They are provided per-table via `table_configuration` in the pipeline spec. Their names must be included in `externalOptionsAllowList` for the connection to allow them to reach the connector.

### Reference-Table Watchlist (alternative to `company_numbers`)

Instead of a fixed `company_numbers` string on the connection, you can point the
connector at a UC table that users maintain (add/remove rows) without ever
touching the connection:

```sql
CREATE TABLE IF NOT EXISTS main.raw_reference.company_numbers (
  company_number STRING
);
INSERT INTO main.raw_reference.company_numbers VALUES
  ('07195160'), ('FC023246'), ('BR007627');
```

Optionally, add `inserted_at`/`inserted_by` audit columns so it's clear who
added each company and when — Delta doesn't allow a `DEFAULT` on a column in
the same statement that adds it, so this is two steps:

```sql
ALTER TABLE main.raw_reference.company_numbers
  ADD COLUMNS (inserted_at TIMESTAMP, inserted_by STRING);
ALTER TABLE main.raw_reference.company_numbers
  SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE main.raw_reference.company_numbers
  ALTER COLUMN inserted_at SET DEFAULT current_timestamp();
ALTER TABLE main.raw_reference.company_numbers
  ALTER COLUMN inserted_by SET DEFAULT current_user();
```

After this, a plain `INSERT INTO ... VALUES ('12345678')` auto-stamps both
columns — no need to specify them per insert.

Then set on the connection: `company_numbers_table=main.raw_reference.company_numbers`,
plus `databricks_host`, `databricks_token`, and `warehouse_id`.

The connector queries `SELECT company_number FROM <company_numbers_table>` via
the Databricks SQL Statement Execution API **once per pipeline run** (not per
company) — it runs inside Spark Python Data Source workers and, like the rest
of the connector, never uses SparkSession or `dbutils` directly. Editing the
table takes effect on the next run; no connection update or pipeline update
needed. If both `company_numbers` and `company_numbers_table` are set,
`company_numbers_table` wins.

`databricks_token` needs `SELECT` on the reference table and `CAN_USE` on the
warehouse — nothing more. Rotate it the same way as `api_key`, via
`update_connection`.

### Obtaining the Required Parameters

- **Companies House API Key** (`api_key`):
  1. Go to [https://developer.company-information.service.gov.uk/](https://developer.company-information.service.gov.uk/) and sign in (create a free account if you do not have one).
  2. Navigate to **Your applications** and click **Create an application**.
  3. Choose environment **Live** (production data). "Test" only serves the sandbox.
  4. After the application is created, open it and click **View API key**.
  5. Copy the API key and store it securely. Use this as the `api_key` connection option.
- **Company numbers** (`company_numbers`):
  - Numbers are 8 characters. Numeric-only numbers must be zero-padded (`6` → `00000006`); the connector will pad on your behalf if you supply a shorter number.
  - Scottish (`SC`), Northern Ireland (`NI`), overseas / establishment (`OC`, `SO`, `IP`, `IC`, `R0`) prefixed numbers must be supplied already in the correct 8-character format.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source, or create a new one and supply `api_key`, `company_numbers`, and (optionally) `base_url`.
3. If you intend to use any of the table-specific options (`items_per_page`, `category`, `register_view`, `pipeline_name`), set `externalOptionsAllowList` to `items_per_page,category,register_view,pipeline_name`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Companies House connector exposes a **static list** of six tables. All are `snapshot` ingestion and keyed by `company_number`.

| Table | Description | Endpoint | Ingestion Type | Primary Key |
|---|---|---|---|---|
| `company_profile` | Headline company record: name, status, type, jurisdiction, SIC codes, accounts and confirmation-statement filing deadlines, inline registered office address, and navigation links to related resources. | `GET /company/{company_number}` | `snapshot` | `company_number` |
| `registered_office_address` | Current registered office address, returned as a standalone record. Useful when tracking address changes independently from the full profile. | `GET /company/{company_number}/registered-office-address` | `snapshot` | `company_number` |
| `officers` | Directors, secretaries, LLP members, and other officers, both currently appointed and resigned. Includes service address, nationality, occupation, partial date of birth for natural persons, and identification details for corporate officers. | `GET /company/{company_number}/officers` | `snapshot` | `(company_number, _source_record_url)` |
| `filing_history` | Every document ever filed by the company with Companies House: accounts, confirmation statements, appointment / resignation notices, resolutions, address changes, mortgages, etc. | `GET /company/{company_number}/filing-history` | `snapshot` | `(company_number, transaction_id)` |
| `persons_with_significant_control` | UK Persons with Significant Control (PSC) register — beneficial owners and controlling parties, including natures of control, partial date of birth (natural persons), and identification (corporate / legal-person PSCs). | `GET /company/{company_number}/persons-with-significant-control` | `snapshot` | `(company_number, _source_record_url)` |
| `charges` | Registered mortgages and other charges over the company's assets — creation, satisfaction, and secured details. | `GET /company/{company_number}/charges` | `snapshot` | `(company_number, _source_record_url)` |

### Notes on primary keys

- The list endpoints for `officers`, `filing_history`, `persons_with_significant_control`, and `charges` do **not** include the `company_number` in each item. The connector injects `company_number` on every row before writing so the compound primary keys shown above are meaningful.
- `officers`, `persons_with_significant_control`, and `charges` do **not** have a reliably unique business key in the live API response — visible fields like `(name, appointed_on, officer_role)` or `(notified_on, name, kind)` have been observed to collide across two distinct records for the same company, and `charges.id` has been observed `null`. The connector uses `_source_record_url` instead (see [Audit Columns](#audit-columns)) — the item's own `links.self` from the API, falling back to a positional id if `links.self` is absent, so it's always unique within one company's fetch. Override with `primary_keys` in `table_configuration` if your downstream model requires a different key.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system — one of the six supported names above. |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default). |
| `destination_schema` | No | Target schema (defaults to pipeline's default). |
| `destination_table` | No | Target table name (defaults to `source_table`). |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. All six tables use `snapshot` ingestion and therefore support both. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |
| `cluster_by` | No | List of columns to cluster the destination Delta table by (Liquid Clustering). Consumed by the pipeline; not forwarded to the source. |

### Source-specific `table_configuration` options

Table-specific options are passed via the pipeline spec under `table_configuration` and must be present in the connection's `externalOptionsAllowList` to reach the connector.

| Option | Applicable Tables | Type | Description |
|---|---|---|---|
| `items_per_page` | `officers`, `filing_history`, `persons_with_significant_control` | integer (1–100) | Overrides the pagination page size. Defaults to `100`. Values outside `[1, 100]` are clamped by the connector. Ignored by `company_profile`, `registered_office_address`, and `charges` (which are not paginated). |
| `category` | `filing_history` | string | Filters filings by category. Valid values include `accounts`, `address`, `annual-return`, `capital`, `change-of-name`, `incorporation`, `liquidation`, `miscellaneous`, `mortgage`, `officers`, `persons-with-significant-control`, `resolution`, `confirmation-statement`. |
| `register_view` | `persons_with_significant_control` | string | When set to `true`, returns the PSC register view rather than the PSC filings view. |
| `pipeline_name` | all tables | string | Stamped into the `_ingested_by` audit column on every record (see [Audit Columns](#audit-columns)) instead of the connector's default identifier. |

Neither `company_profile` nor `registered_office_address` require any table-specific options.

## Audit Columns

Every table gets four connector-injected columns, stamped once per record in `_read_records_for_company` (`companies_house.py`):

| Column | Type | Description |
|---|---|---|
| `_ingested_at` | timestamp | UTC time the record was fetched from the API. |
| `_ingested_by` | string | The `pipeline_name` table option, if set (must be in `externalOptionsAllowList`); otherwise a fixed connector identifier (`companies_house_lakeflow_connector`). There is no per-request human identity available inside a Spark Python Data Source worker, so this names a process, not a person. |
| `_source_api_url` | string | The Companies House endpoint (e.g. `https://api.company-information.service.gov.uk/company/07195160`) the record's `company_number`/table pair was fetched from. For paginated tables this is the base endpoint, not a specific page URL. |
| `_source_record_url` | string | The item's own `links.self` from the API response — its resource identifier, distinct from `_source_api_url` above. Falls back to `<_source_api_url>#<index>` if the item has no `links.self`, so it's always populated and unique within one company's fetch. Used as part of the primary key for `officers`, `persons_with_significant_control`, and `charges` (see [Notes on primary keys](#notes-on-primary-keys)). |

To have `_ingested_by` reflect the actual pipeline instead of the default, set `pipeline_name` as a table option in the pipeline spec:

```yaml
connector_options:
  community_connector_options:
    options:
      pipeline_name: companies_house_api_ingestion_pl
```

This requires `pipeline_name` to be in the connection's `externalOptionsAllowList` (added automatically by `create_connection`/`update_connection` from this file's `external_options_allowlist`).

## Data Type Mapping

Companies House JSON fields are mapped to Spark types as follows:

| API Type | Example Fields | Spark Type | Notes |
|---|---|---|---|
| string | `company_number`, `company_name`, `company_status`, `type`, `name`, `officer_role`, `kind` | `StringType` | Enum-like string values are stored as-is; enumerations may evolve. |
| integer | `pages`, `charge_number`, `date_of_birth.month`, `date_of_birth.year`, `super_secure_managing_officer_count`, `accounting_reference_date.day` | `LongType` | The connector uses `LongType` (not `IntegerType`) throughout to guard against unexpectedly large values. |
| boolean | `can_file`, `has_charges`, `paper_filed`, `ceased`, `is_sanctioned`, `contains_fixed_charge`, `overdue` | `BooleanType` | Standard `true`/`false` values. |
| date (`YYYY-MM-DD`) | `date_of_creation`, `date_of_cessation`, `appointed_on`, `resigned_on`, `notified_on`, `delivered_on`, `satisfied_on` | `StringType` | Returned by the API as ISO 8601 date strings; cast to `date` downstream if needed. |
| object | `accounts`, `confirmation_statement`, `registered_office_address` (inline in profile), `date_of_birth`, `name_elements`, `identification`, `particulars`, `secured_details`, `links` | `StructType` | Nested objects are preserved as structs rather than flattened, so nested schemas survive API additions. |
| array | `sic_codes`, `previous_company_names`, `former_names`, `natures_of_control`, `annotations`, `associated_filings`, `resolutions`, `classification`, `persons_entitled`, `transactions` | `ArrayType` (of primitive or `StructType`) | Nested arrays of structs are preserved as-is. |

**Notable schema quirks (preserved literally from the source API):**

- `charges.assests_ceased_released` — the field name is misspelled (`assests`, not `assets`) in the official API; the connector keeps the misspelling so the schema matches API responses byte-for-byte.
- `date_of_birth` on `officers` and `persons_with_significant_control` contains only `month` and `year` — the birth day is never returned by the API for natural persons (privacy restriction). Do not attempt to synthesise a full date.

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Use the Lakeflow Community Connector UI to copy or reference the Companies House connector source into your workspace. This places the connector code (for example, `companies_house.py`) under a project path that Lakeflow can load.

### Step 2: Configure Your Pipeline

In your pipeline code (e.g. `ingestion_pipeline.py`), configure a `pipeline_spec` that references:

- A **Unity Catalog connection** that uses this Companies House connector (with `api_key` and `company_numbers` set).
- One or more **tables** to ingest, each with optional `table_configuration`.

Example `pipeline_spec` for a small watchlist of companies covering all six tables:

```json
{
  "pipeline_spec": {
    "connection_name": "companies_house_connection",
    "object": [
      {
        "table": {
          "source_table": "company_profile"
        }
      },
      {
        "table": {
          "source_table": "registered_office_address"
        }
      },
      {
        "table": {
          "source_table": "officers",
          "table_configuration": {
            "items_per_page": "100"
          }
        }
      },
      {
        "table": {
          "source_table": "filing_history",
          "table_configuration": {
            "items_per_page": "100",
            "category": "accounts"
          }
        }
      },
      {
        "table": {
          "source_table": "persons_with_significant_control",
          "table_configuration": {
            "register_view": "true"
          }
        }
      },
      {
        "table": {
          "source_table": "charges"
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your `api_key` and `company_numbers`.
- For each `table`:
  - `source_table` must be one of the six supported names above.
  - Optional table-specific options go under `table_configuration` and their names must be listed in the connection's `externalOptionsAllowList`.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g. a scheduled job or workflow).

#### Ingestion mode

All six tables use **snapshot** ingestion — each pipeline run re-reads the full response for every configured `company_number`. The Companies House REST API does not support server-side incremental filtering, so there is no cursor and no `updated_since` parameter.

- On every run, one HTTP call is made per company for `company_profile`, `registered_office_address`, and `charges`. Paginated tables (`officers`, `filing_history`, `persons_with_significant_control`) can make multiple calls per company (roughly `ceil(total_results / items_per_page)`).
- Combine snapshot ingestion with `SCD_TYPE_2` under `table_configuration` if you want to preserve a history of changes to profiles, addresses, officers, or PSCs.

#### Rate limits and throttling

- Companies House enforces a limit of **600 requests per 5-minute sliding window** (~120 req/min, ~2 req/sec) per API key. Exceeding this returns HTTP `429 Too Many Requests`.
- The connector handles `429` and `5xx` responses automatically with **exponential backoff** (initial delay 1 s, doubling, up to 5 attempts). It also honours the `Retry-After` header when the server provides one.
- For a large watchlist:
  - `company_profile`, `registered_office_address`, and `charges` are each 1 call per company.
  - `officers`, `filing_history`, and `persons_with_significant_control` are typically 1 call per company for small companies, growing with the number of officers / filings / PSCs.
  - At the default `items_per_page=100`, a full sync of ~100 companies covering all six tables comfortably fits within the 5-minute rate window; larger watchlists should be split across schedules or across multiple API keys.

#### Best Practices

- **Start small**: Begin with a handful of `company_numbers` and one or two tables (e.g. `company_profile`, `officers`) to validate the configuration.
- **Zero-pad numeric company numbers**: The connector will pad on your behalf, but explicit padding (`00000006`) removes ambiguity.
- **Batch companies by schedule**: For watchlists that would exceed the 5-min rate window, run partial syncs on a rolling schedule (e.g. 100 companies every 5 minutes) rather than a single large run.
- **Use SCD Type 2 when tracking change**: Because ingestion is snapshot-only, choosing `SCD_TYPE_2` in `table_configuration` is the recommended way to capture history (director resignations, address changes, new charges, etc.).
- **Respect API guidelines**: Never embed the `api_key` in source code; keep it in the UC connection's secret storage. Rotate the key periodically.

#### Troubleshooting

Common issues and how to address them:

- **`401 Unauthorized` on every request**:
  - The `api_key` is missing, malformed, revoked, or belongs to a `Test` application (the Test key only works against the sandbox).
  - Confirm you created a **Live** application in the Developer Hub and that the key is copied verbatim.
- **`404 Not Found` for a specific company**:
  - The connector treats `404` as "company not found / no records" and returns zero rows for that company — the batch will not fail.
  - Verify `company_number` is 8 characters and zero-padded (or has the correct alpha prefix such as `SC`, `NI`, `OC`).
- **`429 Too Many Requests`**:
  - The connector retries with exponential backoff automatically; persistent `429`s mean you are consistently exceeding 600 req / 5 min.
  - Reduce watchlist size per run, stagger schedules, or contact Companies House to request a higher limit.
- **Missing fields on some records**:
  - The Companies House API omits optional fields when they do not apply — this is expected. Address fields in particular are sparse and inconsistently populated.
  - Handle `null` explicitly downstream; do not concatenate address components without null-checking.
- **`date_of_birth.day` is always null**:
  - This is by design. The REST API only returns `month` and `year` for natural persons as a privacy protection; the day is never exposed.

## Known Limitations

- **Snapshot-only ingestion**: The Companies House REST Public Data API does not support server-side incremental filtering on any endpoint (no `updated_since`, `modified_after`, ETag-based conditional reads at the collection level, etc.). Every pipeline run re-reads the full response for every configured company. For change-tracking analytics, combine snapshot ingestion with SCD Type 2 in `table_configuration`.
- **No streaming / no CDC from REST**: Real-time change events are only available via the separate [Companies House Streaming API](https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview) (Server-Sent Events, distinct stream key, max 2 concurrent connections). The Streaming API is **not** implemented in this connector.
- **No "list all companies" endpoint**: The public REST API is keyed by `company_number`; there is no discovery endpoint that enumerates all companies. You must supply the connector with an explicit `company_numbers` list. For a full-registry sync, use the Companies House [bulk data products](http://download.companieshouse.gov.uk/en_output.html) to obtain the initial set.
- **`date_of_birth` privacy**: Only `month` and `year` are ever returned for natural-person officers and PSCs. The birth day is deliberately withheld.
- **No hard-delete signal**: Dissolved companies return HTTP `200` with `company_status: "dissolved"` (never a deletion marker). Resigned officers, ceased PSCs, and satisfied charges all remain in their respective lists with a status field populated — they are never removed. Downstream consumers must interpret status fields rather than expecting rows to disappear.
- **Non-standard company number prefixes**: The connector zero-pads purely numeric numbers to 8 characters, but numbers with alpha prefixes (`SC`, `NI`, `OC`, `SO`, `IP`, `IC`, `R0`) are passed through as-is — you must supply them already correctly formatted.
- **Sparse address fields and API typos**: Address fields are inconsistently populated across companies (e.g. `premises` vs `address_line_1`). Two field names in the `charges` API are misspelled in the upstream schema (`assests_ceased_released`, `unfiletered_count`) and are preserved literally in this connector.
- **Deferred endpoints**: `insolvency`, `exemptions`, `registers`, `uk-establishments`, PSC statements, and the `/search/*` and `/disqualified-officers/*` endpoints are not exposed by this connector.

## References

- Connector implementation: `src/databricks/labs/community_connector/sources/companies_house/companies_house.py`
- Schema definitions: `src/databricks/labs/community_connector/sources/companies_house/companies_house_schemas.py`
- Full API research notes: `src/databricks/labs/community_connector/sources/companies_house/companies_house_api_doc.md`
- Connection spec: `src/databricks/labs/community_connector/sources/companies_house/connector_spec.yaml`
- Official Companies House documentation:
  - Developer Hub (obtain API key): [https://developer.company-information.service.gov.uk/](https://developer.company-information.service.gov.uk/)
  - Public Data API reference: [https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference)
  - Rate limiting: [https://developer-specs.company-information.service.gov.uk/guides/rateLimiting](https://developer-specs.company-information.service.gov.uk/guides/rateLimiting)
  - Streaming API (for real-time change events, not implemented here): [https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview](https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview)
  - Filing description enumerations: [https://github.com/companieshouse/api-enumerations](https://github.com/companieshouse/api-enumerations)
  - Bulk data products (full-registry snapshots): [http://download.companieshouse.gov.uk/en_output.html](http://download.companieshouse.gov.uk/en_output.html)
