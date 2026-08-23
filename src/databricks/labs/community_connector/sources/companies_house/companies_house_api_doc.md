# Companies House Public Data API Documentation

## Authorization

**Preferred method: HTTP Basic Auth with API Key**

The Companies House Public Data API uses HTTP Basic Access Authentication (RFC 2617). The API key is supplied as the username; the password is left empty.

- **Header:** `Authorization: Basic <base64(api_key + ":")>`
- The colon (`:`) after the key is required by the Basic Auth spec to denote an empty password.
- All requests must be made over TLS. TLS 1.2 is recommended.
- API keys are obtained from the [Companies House Developer Hub](https://developer.company-information.service.gov.uk/).

**Python example (requests library):**
```python
import requests

API_KEY = "your_api_key_here"
BASE_URL = "https://api.company-information.service.gov.uk"

response = requests.get(
    f"{BASE_URL}/company/00000006",
    auth=(API_KEY, "")   # username=api_key, password="" (empty)
)
```

**Alternative: OAuth 2.0 Bearer Token**
OAuth 2.0 is available for web applications requiring end-user authorization flows. For connector/batch use cases, API Key auth is preferred because it does not require user interaction. OAuth is not documented further here.

**Security notes from official guidelines:**
- Never embed the API key in source code; use environment variables or config files.
- Keep keys out of version control repositories.
- Optionally restrict the key to specific IP addresses.
- Rotate keys periodically.

---

## Object List

The Companies House Public Data API exposes company-centric data organized around a **company number** (an 8-character string, e.g., `00000006`). The object list is **static** — the available resource types are fixed and known at connector build time. There is no "list all resource types" API call.

The connector targets these six core streams (in recommended implementation order):

| Stream Name | API Endpoint Pattern | Notes |
|---|---|---|
| `company_profile` | `GET /company/{company_number}` | Single object per company |
| `officers` | `GET /company/{company_number}/officers` | Paginated list |
| `filing_history` | `GET /company/{company_number}/filing-history` | Paginated list |
| `persons_with_significant_control` | `GET /company/{company_number}/persons-with-significant-control` | Paginated list |
| `charges` | `GET /company/{company_number}/charges` | Non-paginated list (total_count only) |
| `registered_office_address` | `GET /company/{company_number}/registered-office-address` | Single object per company |

**Key design constraint:** Every endpoint is keyed by `company_number`. The connector must be seeded with a list of company numbers to fetch (there is no "list all companies" endpoint in the free-tier REST API — see the search endpoints for discovery). Alternatively, the [Companies House bulk data products](https://developer.company-information.service.gov.uk/bulk-data) provide a full snapshot of all company numbers.

---

## Object Schema

Schemas are **static** — the structure of each resource is defined by the API specification and does not change per company. Optional fields may be absent from responses when they do not apply.

### `company_profile`

**Endpoint:** `GET /company/{company_number}`
**Response type:** Single JSON object (not a list)
**Primary key:** `company_number`

**Top-level fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `company_number` | string | Yes | 8-character company registration identifier |
| `company_name` | string | Yes | Official registered name |
| `company_status` | string (enum) | Yes | Current status: `active`, `dissolved`, `liquidation`, `receivership`, `administration`, `voluntary-arrangement`, `converted-closed`, `insolvency-proceedings` |
| `company_status_detail` | string | No | Additional detail on status |
| `type` | string (enum) | Yes | Entity type: `ltd`, `plc`, `llp`, `private-unlimited`, `old-public-company`, `private-limited-guarant-nsc-limited-exemption`, `limited-partnership`, `registered-overseas-entity`, etc. |
| `subtype` | string | No | Sub-classification: `community-interest-company`, `private-fund-limited-partnership` |
| `date_of_creation` | date (YYYY-MM-DD) | Yes | Incorporation date |
| `date_of_cessation` | date (YYYY-MM-DD) | No | Date company was dissolved, converted, or closed |
| `jurisdiction` | string (enum) | Yes | Governing jurisdiction: `england-wales`, `scotland`, `northern-ireland`, `european-union`, `united-kingdom`, `wales`, `england`, `noneu` |
| `sic_codes` | array[string] | No | UK SIC industry classification codes (e.g., `["62020"]`) |
| `can_file` | boolean | No | Whether the company can file via the Companies House Service |
| `has_been_liquidated` | boolean | No | Deprecated — use `links.insolvency` instead |
| `has_insolvency_history` | boolean | No | Deprecated — use `links.insolvency` instead |
| `has_charges` | boolean | No | Deprecated — use `links.charges` instead |
| `has_super_secure_pscs` | boolean | No | Whether company has super-secure PSC entries |
| `super_secure_managing_officer_count` | integer | No | Count of super-secure managing officers |
| `is_community_interest_company` | boolean | No | Deprecated — use `subtype` |
| `registered_office_is_in_dispute` | boolean | No | Whether the registered address is in dispute |
| `undeliverable_registered_office_address` | boolean | No | Whether post cannot be delivered to the registered address |
| `last_full_members_list_date` | date | No | Date of last full members list update |
| `external_registration_number` | string | No | Registration number from an external body |
| `partial_data_available` | string | No | Returned when Companies House is not the primary data source |
| `etag` | string | Yes | Resource version identifier (for caching) |

**`accounts` sub-object:**

| Field | Type | Description |
|---|---|---|
| `accounts.accounting_reference_date.day` | integer | ARD day |
| `accounts.accounting_reference_date.month` | integer | ARD month |
| `accounts.last_accounts.period_start_on` | date | Start of most recently filed accounting period |
| `accounts.last_accounts.period_end_on` | date | End of most recently filed accounting period |
| `accounts.last_accounts.made_up_to` | date | Deprecated |
| `accounts.last_accounts.type` | string (enum) | Account type: `full`, `small`, `medium`, `group`, `dormant`, `interim`, `initial`, `total-exemption-full`, `total-exemption-small`, `partial-exemption`, `audit-exemption-subsidiary`, `filing-exemption-subsidiary`, `micro-entity` |
| `accounts.next_accounts.period_start_on` | date | Start of next accounting period |
| `accounts.next_accounts.period_end_on` | date | End of next accounting period |
| `accounts.next_accounts.due_on` | date | Filing deadline |
| `accounts.next_accounts.overdue` | boolean | Whether accounts are overdue |
| `accounts.next_due` | date | Deprecated |
| `accounts.next_made_up_to` | date | Deprecated |
| `accounts.overdue` | boolean | Deprecated |

**`confirmation_statement` sub-object:**

| Field | Type | Description |
|---|---|---|
| `confirmation_statement.last_made_up_to` | date | Date of last confirmation statement |
| `confirmation_statement.next_due` | date | Deadline for next confirmation statement |
| `confirmation_statement.next_made_up_to` | date | Date to which next statement must be made up |
| `confirmation_statement.overdue` | boolean | Whether confirmation statement is overdue |

**`registered_office_address` sub-object (inline in profile):**

| Field | Type | Description |
|---|---|---|
| `registered_office_address.premises` | string | Property name or number |
| `registered_office_address.address_line_1` | string | First line of address |
| `registered_office_address.address_line_2` | string | Second line of address |
| `registered_office_address.locality` | string | City or town |
| `registered_office_address.region` | string | County or region |
| `registered_office_address.postal_code` | string | Postcode (mandatory for UK companies as of 15 Sep 2025) |
| `registered_office_address.country` | string | Country (e.g., `England`, `Wales`, `Scotland`, `United Kingdom`) |
| `registered_office_address.care_of` | string | Care of name |
| `registered_office_address.po_box` | string | PO Box number |

**`links` sub-object:**

| Field | Type | Description |
|---|---|---|
| `links.self` | string | URL of this resource |
| `links.filing_history` | string | URL of filing history list |
| `links.officers` | string | URL of officers list |
| `links.persons_with_significant_control` | string | URL of PSC list |
| `links.persons_with_significant_control_statements` | string | URL of PSC statements list |
| `links.charges` | string | URL of charges list |
| `links.insolvency` | string | URL of insolvency resource |
| `links.exemptions` | string | URL of exemptions resource |
| `links.registers` | string | URL of registers resource |
| `links.overseas` | string | URL of overseas details resource |
| `links.uk-establishments` | string | URL of UK establishments list |

**`previous_company_names` array items:**

| Field | Type | Description |
|---|---|---|
| `name` | string | Previous company name |
| `effective_from` | date | Date this name became effective |
| `ceased_on` | date | Date this name ceased |

**`branch_company_details` sub-object (UK establishment of foreign company):**

| Field | Type | Description |
|---|---|---|
| `business_activity` | string | Business undertaken by UK establishment |
| `parent_company_name` | string | Parent company name |
| `parent_company_number` | string | Parent company number |

---

### `registered_office_address`

**Endpoint:** `GET /company/{company_number}/registered-office-address`
**Response type:** Single JSON object
**Primary key:** `company_number` (must be injected by the connector)

Note: The `registered_office_address` object returned by this endpoint has the same fields as the `registered_office_address` sub-object embedded in the company profile. This endpoint is useful for fetching address changes independently.

| Field | Type | Description |
|---|---|---|
| `premises` | string | Property name or number |
| `address_line_1` | string | First line |
| `address_line_2` | string | Second line |
| `locality` | string | City/town |
| `region` | string | County/region |
| `postal_code` | string | Postcode |
| `country` | string | Country |
| `care_of` | string | Care of name |
| `po_box` | string | PO Box |
| `etag` | string | Resource version identifier |

**Important note:** Address fields are inconsistently populated across companies. Never assume all fields are present. The `premises` and `address_line_1` split varies by company — some use `premises` for building number and `address_line_1` for street; others combine them into `address_line_1`. Filter nulls dynamically.

---

### `officers`

**Endpoint:** `GET /company/{company_number}/officers`
**Response type:** Paginated list
**Primary key:** `(company_number, name, appointed_on, officer_role)` — no stable unique `id` field is returned; `person_number` is present in bulk data but may be absent in the REST API. TBD: verify `person_number` availability in REST responses.

**Top-level response envelope fields:**

| Field | Type | Description |
|---|---|---|
| `active_count` | integer | Number of currently active officers |
| `resigned_count` | integer | Number of resigned officers |
| `total_results` | integer | Total officers matching the query |
| `items_per_page` | integer | Officers returned in this page |
| `start_index` | integer | Zero-based offset of this page |
| `kind` | string | Always `officer-list` |
| `etag` | string | Resource version identifier |
| `links` | object | Navigation links |

**`items[]` array — officer record fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Officer full name (corporate or natural) |
| `officer_role` | string (enum) | Yes | Role: `director`, `secretary`, `nominee-director`, `nominee-secretary`, `managing-officer`, `llp-member`, `llp-designated-member`, `corporate-llp-member`, `corporate-director`, `corporate-secretary`, `judicial-factor`, `receiver-and-manager`, `cic-manager`, `corporate-managing-officer` |
| `appointed_on` | date | Yes (usually) | Date of appointment |
| `resigned_on` | date | No | Date of resignation |
| `is_pre_1992_appointment` | boolean | No | True for appointments pre-dating 1992 (exact date unknown) |
| `appointed_before` | string | No | Date reference for legacy pre-1992 appointments |
| `person_number` | string | No | Unique person identifier (from bulk products; may be absent in REST) |
| `nationality` | string | No | Officer's nationality |
| `occupation` | string | No | Occupation or job title |
| `country_of_residence` | string | No | Country of residence |
| `responsibilities` | string | No | Description of managing officer responsibilities |
| `etag` | string | No | Officer resource version identifier |
| `links.officer.appointments` | string | No | URL to all appointments for this officer |
| `links.self` | string | No | URL for this officer appointment |

**`address` sub-object (service address):**

| Field | Type | Description |
|---|---|---|
| `address.premises` | string | Building name/number |
| `address.address_line_1` | string | Street |
| `address.address_line_2` | string | Second line |
| `address.locality` | string | Town/city |
| `address.region` | string | County |
| `address.postal_code` | string | Postcode |
| `address.country` | string | Country |
| `address.care_of` | string | Care of |
| `address.po_box` | string | PO Box |

**`date_of_birth` sub-object:**

| Field | Type | Description |
|---|---|---|
| `date_of_birth.month` | integer | Birth month only (privacy-restricted) |
| `date_of_birth.year` | integer | Birth year only (privacy-restricted) |

Note: Full birth day is never returned by the API for natural persons (privacy requirement).

**`former_names[]` array items:**

| Field | Type | Description |
|---|---|---|
| `forenames` | string | Former given names |
| `surname` | string | Former surname |

**`identification` sub-object (corporate officers):**

| Field | Type | Description |
|---|---|---|
| `identification.identification_type` | string | Type: `eea`, `non-eea`, `uk-limited-company`, `other-corporate-body-or-firm`, `registered-overseas-entity-corporate-managing-officer` |
| `identification.legal_authority` | string | Governing legal authority |
| `identification.legal_form` | string | Legal form |
| `identification.place_registered` | string | Place of registration |
| `identification.registration_number` | string | Registration number |

---

### `filing_history`

**Endpoint:** `GET /company/{company_number}/filing-history`
**Response type:** Paginated list
**Primary key:** `(company_number, transaction_id)`

**Query parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `items_per_page` | integer | No | Number of items per page (default: 25, max: 100) |
| `start_index` | integer | No | Zero-based offset (default: 0) |
| `category` | string | No | Filter by category: `accounts`, `address`, `annual-return`, `capital`, `change-of-name`, `incorporation`, `liquidation`, `miscellaneous`, `mortgage`, `officers`, `persons-with-significant-control`, `resolution`, `confirmation-statement` |

**Top-level response envelope fields:**

| Field | Type | Description |
|---|---|---|
| `filing_history_status` | string | Status; value: `filing-history-available` |
| `total_count` | integer | Total number of filings for the company |
| `items_per_page` | integer | Items in this page |
| `start_index` | integer | Zero-based offset of this page |
| `kind` | string | Always `filing-history` |
| `etag` | string | Resource version identifier |

**`items[]` array — filing record fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `transaction_id` | string | Yes | Unique identifier for this filing |
| `type` | string | Yes | Filing type code (e.g., `CS01`, `AA`, `TM01`, `AP01`) |
| `category` | string (enum) | Yes | Filing category (accounts, officers, address, etc.) |
| `subcategory` | string | No | Sub-category; value: `resolution` |
| `date` | date | Yes | Date the filing was processed by Companies House |
| `description` | string | Yes | Description key (snake_case enum — must be mapped to human-readable text using the [CH description enumerations](https://github.com/companieshouse/api-enumerations)) |
| `barcode` | string | No | Document barcode |
| `pages` | integer | No | Number of pages in the PDF document |
| `paper_filed` | boolean | No | True if this was a paper (non-electronic) filing |
| `links.self` | string | No | URL to this filing item |
| `links.document_metadata` | string | No | URL to document metadata (for downloading the PDF) |

**`annotations[]` array items:**

| Field | Type | Description |
|---|---|---|
| `annotation` | string | Annotation text |
| `date` | date | Date annotation was added |
| `description` | string | Annotation description key |

**`associated_filings[]` array items:**

| Field | Type | Description |
|---|---|---|
| `date` | date | Date the associated filing was processed |
| `description` | string | Description key of the associated filing |
| `type` | string | Type of the associated filing |

**`resolutions[]` array items:**

| Field | Type | Description |
|---|---|---|
| `category` | string | Always `miscellaneous` |
| `subcategory` | string | Always `resolution` |
| `type` | string | Resolution type |
| `description` | string | Description key |
| `document_id` | string | Document identifier |
| `receive_date` | date | Date the resolution was received |

---

### `persons_with_significant_control`

**Endpoint:** `GET /company/{company_number}/persons-with-significant-control`
**Response type:** Paginated list
**Primary key:** `(company_number, notified_on, name, kind)` — no single stable unique field. TBD: verify if `links.self` contains a stable notification ID.

**Query parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `items_per_page` | string | No | Number of items per page |
| `start_index` | string | No | Zero-based offset |
| `register_view` | string | No | Whether to view the register — set to `true` to view the PSC register rather than filings |

**Top-level response envelope fields:**

| Field | Type | Description |
|---|---|---|
| `active_count` | integer | Number of active PSCs |
| `ceased_count` | integer | Number of ceased PSCs |
| `total_results` | integer | Total PSCs in the result set |
| `items_per_page` | integer | Items in this page |
| `start_index` | integer | Zero-based offset of this page |
| `links` | object | Navigation links |

**`items[]` array — PSC record fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | string (enum) | Yes | PSC type: `individual-person-with-significant-control`, `corporate-entity-person-with-significant-control`, `legal-person-with-significant-control`, `super-secure-person-with-significant-control`, `individual-beneficial-owner`, `corporate-entity-beneficial-owner`, `legal-person-beneficial-owner`, `super-secure-beneficial-owner` |
| `name` | string | Yes | Full name of the PSC |
| `notified_on` | date | Yes | Date Companies House was notified about this PSC |
| `ceased` | boolean | No | True if this PSC has ceased |
| `ceased_on` | date | No | Date cessation was notified to Companies House |
| `natures_of_control` | array[string] | Yes | Control type indicators (see enumeration list below) |
| `nationality` | string | No | Nationality (natural persons) |
| `country_of_residence` | string | No | Country of residence (natural persons) |
| `description` | string | No | Descriptor for super-secure PSC entries |
| `is_sanctioned` | boolean | No | Whether a sanctions declaration has been made |
| `etag` | string | No | Resource version identifier |
| `links.self` | string | No | URL to this PSC notification |

**`date_of_birth` sub-object (natural persons):**

| Field | Type | Description |
|---|---|---|
| `date_of_birth.month` | integer | Birth month only |
| `date_of_birth.year` | integer | Birth year only |

**`name_elements` sub-object (natural persons):**

| Field | Type | Description |
|---|---|---|
| `name_elements.title` | string | Title (Mr, Mrs, Dr, etc.) |
| `name_elements.forename` | string | First name |
| `name_elements.middle_name` | string | Middle name(s) |
| `name_elements.surname` | string | Surname |

**`address` sub-object (service address):**
Same structure as officer `address` sub-object above.

**`identification` sub-object (corporate/legal-person PSCs):**

| Field | Type | Description |
|---|---|---|
| `identification.country_registered` | string | Country of registration |
| `identification.legal_authority` | string | Governing legal authority |
| `identification.legal_form` | string | Legal form |
| `identification.place_registered` | string | Place of registration |
| `identification.registration_number` | string | Registration number |

**Common `natures_of_control` enum values:**

- `ownership-of-shares-25-to-50-percent`
- `ownership-of-shares-50-to-75-percent`
- `ownership-of-shares-75-to-100-percent`
- `voting-rights-25-to-50-percent`
- `voting-rights-50-to-75-percent`
- `voting-rights-75-to-100-percent`
- `right-to-appoint-and-remove-directors`
- `significant-influence-or-control`
- `ownership-of-shares-25-to-50-percent-as-trust`
- `ownership-of-shares-25-to-50-percent-as-firm`
- (Additional trust/firm/overseas-entity variants exist — see [CH API Enumerations](https://github.com/companieshouse/api-enumerations))

---

### `charges`

**Endpoint:** `GET /company/{company_number}/charges`
**Response type:** Non-paginated list (all charges returned in one response)
**Primary key:** `(company_number, charge_number)` or `(company_number, id)`

Note: The charges list endpoint does not support `start_index`/`items_per_page` pagination like the officers and filing history endpoints. All charges for a company are returned in a single response.

**Top-level response envelope fields:**

| Field | Type | Description |
|---|---|---|
| `total_count` | integer | Total number of charges returned |
| `unfiletered_count` | integer | Count before any filtering (note: typo in API — `unfiletered` not `unfiltered`) |
| `satisfied_count` | integer | Number of satisfied charges |
| `part_satisfied_count` | integer | Number of part-satisfied charges |
| `etag` | string | Resource version identifier |

**`items[]` array — charge record fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `id` | string | Yes | Charge identifier |
| `charge_number` | integer | Yes | Sequential charge number |
| `charge_code` | string | No | Charge code (replaces old mortgage description) |
| `status` | string (enum) | Yes | `outstanding`, `fully-satisfied`, `part-satisfied`, `satisfied` |
| `created_on` | date | No | Date the charge was created |
| `delivered_on` | date | No | Date the charge was submitted to Companies House |
| `acquired_on` | date | No | Date the property/undertaking was acquired |
| `satisfied_on` | date | No | Date the charge was satisfied |
| `resolved_on` | date | No | Date the issue was resolved |
| `covering_instrument_date` | date | No | Date by which the series of debentures were created |
| `more_than_four_persons_entitled` | boolean | No | True if more than four persons are entitled |
| `assests_ceased_released` | string | No | Cease/release information (note: typo in API — `assests` not `assets`) |
| `etag` | string | No | Charge entity tag |

**`classification[]` array items:**

| Field | Type | Description |
|---|---|---|
| `type` | string | Classification type |
| `description` | string | Description of the classification |

**`persons_entitled[]` array items:**

| Field | Type | Description |
|---|---|---|
| `name` | string | Name of the person entitled to the charge |

**`particulars[]` sub-object:**

| Field | Type | Description |
|---|---|---|
| `type` | string | Particulars type |
| `description` | string | Description of what is charged |
| `contains_fixed_charge` | boolean | Whether a fixed charge is included |
| `contains_floating_charge` | boolean | Whether a floating charge is included |
| `contains_negative_pledge` | boolean | Whether a negative pledge is included |
| `floating_charge_covers_all` | boolean | Whether floating charge covers all assets |

**`secured_details[]` sub-object:**

| Field | Type | Description |
|---|---|---|
| `type` | string | Type of secured obligation |
| `description` | string | Description of what is secured |

**`transactions[]` array items:**

| Field | Type | Description |
|---|---|---|
| `filing_type` | string | Type of filing |
| `delivered_on` | date | Date the transaction was delivered |
| `links.filing` | string | URL to the filing document |
| `insolvency_case_number` | integer | Associated insolvency case number (if applicable) |

**`scottish_alterations[]` sub-object (Scottish companies):**

| Field | Type | Description |
|---|---|---|
| `type` | string | Alteration type |
| `description` | string | Alteration description |
| `has_alterations_to_order` | boolean | Whether alterations to order exist |
| `has_alterations_to_prohibitions` | boolean | Whether alterations to prohibitions exist |
| `has_alterations_to_provisions` | boolean | Whether alterations to provisions exist |

---

## Get Object Primary Keys

| Stream | Primary Key | Notes |
|---|---|---|
| `company_profile` | `company_number` | Stable 8-character string |
| `registered_office_address` | `company_number` | Connector must add this field; endpoint does not return it |
| `officers` | `(company_number, name, appointed_on, officer_role)` | No single stable unique ID; `person_number` exists in bulk data but unreliable in REST |
| `filing_history` | `(company_number, transaction_id)` | `transaction_id` is stable and unique per filing |
| `persons_with_significant_control` | `(company_number, notified_on, name, kind)` | No single stable unique ID from list endpoint |
| `charges` | `(company_number, charge_number)` or `(company_number, id)` | Both `id` and `charge_number` are unique per company |

**Note:** For all streams, the connector must inject `company_number` into list endpoint item records since individual items in the list response do not include the company number.

---

## Object Ingestion Type

| Stream | Ingestion Type | Rationale |
|---|---|---|
| `company_profile` | `snapshot` | No cursor or incremental filter on the profile endpoint. Must re-fetch all companies each sync to detect changes. |
| `registered_office_address` | `snapshot` | Same constraint as company profile — point-in-time fetch only. |
| `officers` | `snapshot` | No `updated_at` or cursor field. List is complete per request. Detect changes by full compare. |
| `filing_history` | `append` | Filings are ordered by `date` descending. New filings always appear at the start of the list. Fetching page 1 on each sync is sufficient to capture new filings; however, the API provides no cursor field to filter by date, so the connector must either (a) fetch all pages and deduplicate or (b) stop early when a known `transaction_id` is encountered. |
| `persons_with_significant_control` | `snapshot` | No cursor field. `ceased_on` is present for ceased PSCs but cannot be used as an incremental filter in the API query. |
| `charges` | `snapshot` | No cursor field. `delivered_on` and `satisfied_on` exist on items but are not filterable via query parameters. |

**Important:** The Companies House REST Public Data API does **not** support server-side incremental filtering (no `updated_since`, `modified_after`, or similar query parameters on any of these endpoints). All streams are effectively snapshot-only when using the REST API.

**For true incremental sync:** Companies House provides a separate [Streaming API](https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview) (`https://stream.companieshouse.gov.uk`) that pushes real-time change events. This requires a separate stream key and a different connection model (long-lived SSE connection, max 2 concurrent connections). The Streaming API is not covered in this document but is the correct approach for near-real-time change capture.

---

## Read API for Data Retrieval

### Common Patterns

- **Base URL:** `https://api.company-information.service.gov.uk`
- **Method:** `GET` for all read operations
- **Auth:** `auth=(api_key, "")` in Python requests
- **Response format:** JSON
- **Response tolerance:** Responses may include unknown fields in the future; connectors must tolerate new keys gracefully (do not use strict schema validation that rejects extra fields).

### Rate Limits

| Limit | Value |
|---|---|
| Requests per 5-minute window | 600 |
| Error on limit exceeded | HTTP 429 Too Many Requests |
| Reset interval | 5 minutes (sliding window) |
| Higher limits | Available on request from Companies House |
| Streaming API — max concurrent connections | 2 |
| Streaming API — retry after 429 | Wait 1 minute before reconnecting |

At 600 requests per 5 minutes (120 req/min, 2 req/sec), fetching full data for a large company with officers, filing history, and PSCs may consume 4–6 API calls. For a batch of 100 companies, that is 400–600 calls — at or near the 5-minute rate limit. Implement exponential backoff with jitter on 429 responses.

### Pagination

Paginated endpoints (`officers`, `filing_history`, `persons_with_significant_control`) use **offset-based pagination** with `start_index` and `items_per_page` query parameters.

| Parameter | Type | Default | Max | Notes |
|---|---|---|---|---|
| `start_index` | integer | 0 | — | Zero-based page offset |
| `items_per_page` | integer | 25 | 100 | Officers endpoint default is 35 on the web UI; API default is 25 |

**Pagination algorithm:**
```python
start_index = 0
items_per_page = 100

while True:
    response = requests.get(
        f"{BASE_URL}/company/{company_number}/officers",
        params={"start_index": start_index, "items_per_page": items_per_page},
        auth=(api_key, "")
    ).json()

    items = response.get("items", [])
    if not items:
        break

    yield from items

    start_index += len(items)
    if start_index >= response["total_results"]:
        break
```

Note: Use `len(items)` rather than `items_per_page` to advance `start_index` — the API may return fewer items than requested on the last page.

### `charges` — No Pagination

The charges endpoint returns all charges in a single response (no pagination parameters). Check `total_count` in the response to confirm all charges were returned.

### `registered_office_address` and `company_profile` — Single Object

These endpoints return a single JSON object, not a list. No pagination is needed.

### Example Requests

**Company Profile:**
```python
GET https://api.company-information.service.gov.uk/company/00000006
Authorization: Basic bXlfYXBpX2tleTo=
```

**Officers (page 1, 100 per page):**
```python
GET https://api.company-information.service.gov.uk/company/00000006/officers?start_index=0&items_per_page=100
Authorization: Basic bXlfYXBpX2tleTo=
```

**Filing History (only annual accounts category):**
```python
GET https://api.company-information.service.gov.uk/company/00000006/filing-history?category=accounts&start_index=0&items_per_page=100
Authorization: Basic bXlfYXBpX2tleTo=
```

**PSC list:**
```python
GET https://api.company-information.service.gov.uk/company/00000006/persons-with-significant-control?start_index=0&items_per_page=100
Authorization: Basic bXlfYXBpX2tleTo=
```

**Charges:**
```python
GET https://api.company-information.service.gov.uk/company/00000006/charges
Authorization: Basic bXlfYXBpX2tleTo=
```

**Registered Office Address:**
```python
GET https://api.company-information.service.gov.uk/company/00000006/registered-office-address
Authorization: Basic bXlfYXBpX2tleTo=
```

### Officers `order_by` Parameter

The officers endpoint supports optional `order_by` sorting:

| Value | Description |
|---|---|
| `appointed_on` | Sort by appointment date |
| `resigned_on` | Sort by resignation date |
| `surname` | Sort by surname alphabetically |

### Handling Deleted Records

- The REST API does **not** expose deleted company records — dissolved companies return HTTP 200 with `company_status: "dissolved"`, not a deletion marker.
- Resigned officers remain in the list with a `resigned_on` date populated; they are not removed from the response.
- Ceased PSCs remain in the list with `ceased: true` and `ceased_on` populated; they are not removed.
- Satisfied charges remain in the list with `status: "fully-satisfied"` and `satisfied_on` populated.
- There is no soft-delete or tombstone pattern in the REST API. Hard deletes (e.g., company data corrections) are not exposed.

---

## Field Type Mapping

| API Type | Python/Spark Type | Notes |
|---|---|---|
| `string` | `StringType` | Most identifiers, names, codes |
| `integer` | `LongType` | Counts, charge numbers, date components |
| `boolean` | `BooleanType` | Flags and indicators |
| `date` | `DateType` (or `StringType`) | Returned as ISO 8601 string `YYYY-MM-DD`; parse to date in ETL |
| `array` | `ArrayType` | `sic_codes`, `natures_of_control`, `former_names`, etc. |
| `object` | `StructType` or flatten to columns | Nested sub-objects; flatten for columnar storage |
| `string (enum)` | `StringType` | Store as string; enumerations can change over time |

**Special behaviors:**

- **`date_of_birth`:** Returns only `month` and `year` for natural persons. The `day` is never returned (privacy restriction). Do not attempt to construct a full date.
- **`description` in `filing_history`:** Returns a snake_case key (e.g., `annual-return-made-up-to`), not a human-readable string. Human-readable text requires a lookup against the [Companies House API Enumerations](https://github.com/companieshouse/api-enumerations).
- **`etag`:** A cache-busting version hash. Useful for conditional GET requests (`If-None-Match` header) to avoid re-processing unchanged resources.
- **`sic_codes`:** Array of 5-digit strings. Must be cross-referenced against the UK SIC 2007 code list for descriptions.
- **`company_number`:** Must be zero-padded to 8 characters (e.g., `00000006`, not `6`). Scottish companies use `SC` prefix; Northern Ireland use `NI` prefix.
- **`assests_ceased_released` / `unfiletered_count`:** These are known typos in the official API schema — document and handle them literally.

---

## Known Quirks and Special Notes

1. **No incremental/delta filter on REST API:** All endpoints are point-in-time snapshot reads. Use the Streaming API for change events.

2. **Company number formatting:** Zero-pad to 8 characters. Handle `SC`, `NI`, `OC`, `SO`, `IP`, `IC`, `R0` and other prefixes for non-standard company types.

3. **Dissolved companies return HTTP 200:** Always check `company_status` rather than relying on error codes to detect dissolved companies.

4. **Officers date of birth:** Only month and year are returned. Never the day.

5. **Filing description is a key, not text:** The `description` field in filing history is a code like `annual-return-made-up-to`. Map to human text using the [api-enumerations repo](https://github.com/companieshouse/api-enumerations).

6. **Address fields are sparse and inconsistent:** Do not concatenate address fields without null-checking each component.

7. **`company_number` must be injected into list records:** The officers, filing history, PSC, and charges list endpoints do not include `company_number` in each item. The connector must add it to each row before writing.

8. **API schema is additive:** The official docs state: "Your application must tolerate the order of document members changing over time, and expect to receive members it hasn't seen before." Use schema evolution-friendly parsing.

9. **`charges` pagination:** The charges list has a `total_count` field but no `items_per_page`/`start_index` pagination. If a company has many charges, they are all returned at once. Monitor response sizes.

10. **`has_been_liquidated`, `has_charges`, `has_insolvency_history`:** These top-level boolean fields on the company profile are deprecated. Use the `links` sub-object (`links.insolvency`, `links.charges`) to determine presence instead.

11. **Super-secure PSCs:** Some PSCs are marked as `super-secure-person-with-significant-control`. These return minimal data (only a `description` field) for protection. The `has_super_secure_pscs` boolean on the company profile indicates if any exist.

---

## Deferred Tables

The following endpoints exist in the Companies House Public Data API but are deferred from the initial connector implementation. They have lower business value, more complex schemas, or limited coverage in comparable connectors (Airbyte, dltHub).

| Endpoint / Stream | Reason Deferred |
|---|---|
| `GET /company/{number}/insolvency` | Niche data; complex nested schema; only relevant for a small subset of companies |
| `GET /company/{number}/exemptions` | Very limited coverage; rarely used in analytics pipelines |
| `GET /company/{number}/registers` | Rarely surfaced in comparable connectors |
| `GET /company/{number}/uk-establishments` | Foreign branch data only; niche use case |
| `GET /search/companies` | Discovery endpoint; not an analytics stream |
| `GET /search/officers` | Discovery endpoint; not an analytics stream |
| `GET /search/disqualified-officers` | Niche compliance use case |
| `GET /advanced-search/companies` | Discovery endpoint; overlaps with company_profile |
| `GET /company/{number}/persons-with-significant-control-statements` | Supplemental to PSC list; lower priority |
| `GET /disqualified-officers/natural/{officer_id}` | Niche compliance use case |
| `GET /disqualified-officers/corporate/{officer_id}` | Niche compliance use case |
| Streaming API endpoints | Different connection model (SSE); separate implementation effort |

---

## Research Log

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|---|---|---|---|---|
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference | 2026-08-23 | High | Full list of available endpoints |
| Official Docs | https://developer-specs.company-information.service.gov.uk/guides/rateLimiting | 2026-08-23 | High | Rate limits: 600 req/5 min, 429 on exceed |
| Official Docs | https://developer-specs.company-information.service.gov.uk/guides/developerGuidelines | 2026-08-23 | High | Auth method, security best practices, schema tolerance |
| Official Docs | https://developer.company-information.service.gov.uk/authentication | 2026-08-23 | High | HTTP Basic Auth with API key as username, empty password |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/companyprofile?v=latest | 2026-08-23 | High | Full company profile schema with all nested sub-objects |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/officerlist?v=latest | 2026-08-23 | High | Officer list schema, pagination fields, officer item fields |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/filinghistorylist?v=latest | 2026-08-23 | High | Filing history list schema, pagination, item fields |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/chargelist?v=latest | 2026-08-23 | High | Charge list schema, charge item fields |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/chargedetails?v=latest | 2026-08-23 | High | Charge detail fields (acquired_on, delivered_on, etc.) |
| Official Docs | https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/resources/list?v=latest | 2026-08-23 | High | PSC list schema with active_count, ceased_count, item fields |
| Official Docs | https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview | 2026-08-23 | High | Streaming API overview, confirms REST API is snapshot-only |
| Reference Implementation | https://dlthub.com/context/source/companies-house | 2026-08-23 | High | Confirmed 6 core endpoint targets; data_selector="items" for lists |
| Community Guide | https://chguide.co.uk/rest-api/data-endpoints/company-profile | 2026-08-23 | Medium | Company profile fields cross-reference |
| Technical Blog | https://paul-walsh.co.uk/companies-house-api-gotchas/ | 2026-08-23 | Medium | Gotchas: zero-padding, dissolved=200, address fields, description keys |
| Technical Blog | https://dev.to/openregistry/companies-house-via-api-what-the-uk-registry-actually-returns-436k | 2026-08-23 | Medium | Confirmed field names for officers, PSC, filing history |
| Community Forum | https://forum.companieshouse.gov.uk/t/unable-to-get-all-the-officers-data-from-the-api/5568 | 2026-08-23 | Medium | Confirmed pagination behavior with start_index and items_per_page |
| Search Result | https://medium.com/@alistairboyer/rate-limiting-and-the-companies-house-api-b16ef36365a6 | 2026-08-23 | Medium | Confirmed 600 req/5 min rate limit detail |
