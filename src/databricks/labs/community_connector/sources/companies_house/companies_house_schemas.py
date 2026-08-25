"""Static schema definitions for Companies House connector tables.

All six tables are snapshot reads. Schemas are derived directly from the
Companies House Public Data API resource specifications (see
``companies_house_api_doc.md``).

Design notes:
    - ``LongType`` (not ``IntegerType``) is used for all integer fields to
      guard against unexpected large values.
    - Dates are exposed as ``StringType`` — the API returns ISO 8601
      ``YYYY-MM-DD`` strings and the framework can coerce them downstream.
    - Nested sub-objects use ``StructType`` (not ``MapType``) to preserve
      explicit typing.
    - Each list endpoint gets a connector-injected ``company_number`` field
      because the item envelopes do not carry it.
    - Every table also gets connector-injected audit columns (see
      ``_read_records_for_company`` in ``companies_house.py``):
      ``_ingested_at`` (fetch timestamp), ``_ingested_by`` (the ingesting
      pipeline name, if passed via the ``pipeline_name`` table option;
      otherwise a fixed connector identifier — there is no per-request
      human identity available inside a Spark Python Data Source worker),
      ``_source_api_url`` (the Companies House endpoint the record for
      that company_number/table was fetched from), and
      ``_source_record_url`` (the item's own ``links.self`` from the API
      response, when present — the API's resource identifier for that
      specific record, used as part of the primary key for officers,
      persons_with_significant_control, and charges; see TABLE_METADATA).
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Appended to every table schema — see module docstring.
AUDIT_FIELDS = [
    StructField("_ingested_at", TimestampType(), True),
    StructField("_ingested_by", StringType(), True),
    StructField("_source_api_url", StringType(), True),
    StructField("_source_record_url", StringType(), True),
]


# =============================================================================
# Reusable nested struct definitions
# =============================================================================

ADDRESS_STRUCT = StructType(
    [
        StructField("premises", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("locality", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("care_of", StringType(), True),
        StructField("po_box", StringType(), True),
    ]
)
"""Address struct used across profile, officers, PSC, and standalone address."""


DATE_OF_BIRTH_STRUCT = StructType(
    [
        StructField("month", LongType(), True),
        StructField("year", LongType(), True),
    ]
)
"""Partial DOB (privacy-restricted: no day) for natural-person officers / PSCs."""


NAME_ELEMENTS_STRUCT = StructType(
    [
        StructField("title", StringType(), True),
        StructField("forename", StringType(), True),
        StructField("middle_name", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)


# =============================================================================
# company_profile
# =============================================================================

ACCOUNTING_REFERENCE_DATE_STRUCT = StructType(
    [
        StructField("day", LongType(), True),
        StructField("month", LongType(), True),
    ]
)

LAST_ACCOUNTS_STRUCT = StructType(
    [
        StructField("period_start_on", StringType(), True),
        StructField("period_end_on", StringType(), True),
        StructField("made_up_to", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

NEXT_ACCOUNTS_STRUCT = StructType(
    [
        StructField("period_start_on", StringType(), True),
        StructField("period_end_on", StringType(), True),
        StructField("due_on", StringType(), True),
        StructField("overdue", BooleanType(), True),
    ]
)

ACCOUNTS_STRUCT = StructType(
    [
        StructField("accounting_reference_date", ACCOUNTING_REFERENCE_DATE_STRUCT, True),
        StructField("last_accounts", LAST_ACCOUNTS_STRUCT, True),
        StructField("next_accounts", NEXT_ACCOUNTS_STRUCT, True),
        StructField("next_due", StringType(), True),
        StructField("next_made_up_to", StringType(), True),
        StructField("overdue", BooleanType(), True),
    ]
)

CONFIRMATION_STATEMENT_STRUCT = StructType(
    [
        StructField("last_made_up_to", StringType(), True),
        StructField("next_due", StringType(), True),
        StructField("next_made_up_to", StringType(), True),
        StructField("overdue", BooleanType(), True),
    ]
)

COMPANY_PROFILE_LINKS_STRUCT = StructType(
    [
        StructField("self", StringType(), True),
        StructField("filing_history", StringType(), True),
        StructField("officers", StringType(), True),
        StructField("persons_with_significant_control", StringType(), True),
        StructField("persons_with_significant_control_statements", StringType(), True),
        StructField("charges", StringType(), True),
        StructField("insolvency", StringType(), True),
        StructField("exemptions", StringType(), True),
        StructField("registers", StringType(), True),
        StructField("overseas", StringType(), True),
        StructField("uk_establishments", StringType(), True),
    ]
)

PREVIOUS_COMPANY_NAME_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("effective_from", StringType(), True),
        StructField("ceased_on", StringType(), True),
    ]
)

BRANCH_COMPANY_DETAILS_STRUCT = StructType(
    [
        StructField("business_activity", StringType(), True),
        StructField("parent_company_name", StringType(), True),
        StructField("parent_company_number", StringType(), True),
    ]
)


COMPANY_PROFILE_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("company_status", StringType(), True),
        StructField("company_status_detail", StringType(), True),
        StructField("type", StringType(), True),
        StructField("subtype", StringType(), True),
        StructField("date_of_creation", StringType(), True),
        StructField("date_of_cessation", StringType(), True),
        StructField("jurisdiction", StringType(), True),
        StructField("sic_codes", ArrayType(StringType()), True),
        StructField("can_file", BooleanType(), True),
        StructField("has_been_liquidated", BooleanType(), True),
        StructField("has_insolvency_history", BooleanType(), True),
        StructField("has_charges", BooleanType(), True),
        StructField("has_super_secure_pscs", BooleanType(), True),
        StructField("super_secure_managing_officer_count", LongType(), True),
        StructField("is_community_interest_company", BooleanType(), True),
        StructField("registered_office_is_in_dispute", BooleanType(), True),
        StructField("undeliverable_registered_office_address", BooleanType(), True),
        StructField("last_full_members_list_date", StringType(), True),
        StructField("external_registration_number", StringType(), True),
        StructField("partial_data_available", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("accounts", ACCOUNTS_STRUCT, True),
        StructField("confirmation_statement", CONFIRMATION_STATEMENT_STRUCT, True),
        StructField("registered_office_address", ADDRESS_STRUCT, True),
        StructField("links", COMPANY_PROFILE_LINKS_STRUCT, True),
        StructField(
            "previous_company_names",
            ArrayType(PREVIOUS_COMPANY_NAME_STRUCT),
            True,
        ),
        StructField("branch_company_details", BRANCH_COMPANY_DETAILS_STRUCT, True),
    ]
)


# =============================================================================
# registered_office_address
# =============================================================================

REGISTERED_OFFICE_ADDRESS_LINKS_STRUCT = StructType(
    [
        StructField("self", StringType(), True),
    ]
)


REGISTERED_OFFICE_ADDRESS_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("premises", StringType(), True),
        StructField("address_line_1", StringType(), True),
        StructField("address_line_2", StringType(), True),
        StructField("locality", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("care_of", StringType(), True),
        StructField("po_box", StringType(), True),
        StructField("etag", StringType(), True),
        # Envelope metadata the API returns on this endpoint.
        StructField("kind", StringType(), True),
        StructField("links", REGISTERED_OFFICE_ADDRESS_LINKS_STRUCT, True),
    ]
)


# =============================================================================
# officers
# =============================================================================

FORMER_NAME_STRUCT = StructType(
    [
        StructField("forenames", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

OFFICER_IDENTIFICATION_STRUCT = StructType(
    [
        StructField("identification_type", StringType(), True),
        StructField("legal_authority", StringType(), True),
        StructField("legal_form", StringType(), True),
        StructField("place_registered", StringType(), True),
        StructField("registration_number", StringType(), True),
    ]
)

OFFICER_LINKS_STRUCT = StructType(
    [
        StructField(
            "officer",
            StructType(
                [
                    StructField("appointments", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("self", StringType(), True),
    ]
)

# Post-ECCTA (Economic Crime and Corporate Transparency Act) identity
# verification metadata. Present on officers whose identity has been
# verified via an Authorised Corporate Service Provider (ACSP).
OFFICER_IDENTITY_VERIFICATION_DETAILS_STRUCT = StructType(
    [
        StructField(
            "anti_money_laundering_supervisory_bodies",
            ArrayType(StringType()),
            True,
        ),
        StructField("appointment_verification_end_on", StringType(), True),
        StructField("appointment_verification_start_on", StringType(), True),
        StructField("appointment_verification_statement_date", StringType(), True),
        StructField("appointment_verification_statement_due_on", StringType(), True),
        StructField("authorised_corporate_service_provider_name", StringType(), True),
        StructField("identity_verified_on", StringType(), True),
        StructField("preferred_name", StringType(), True),
    ]
)


OFFICERS_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("name", StringType(), True),
        StructField("officer_role", StringType(), True),
        StructField("appointed_on", StringType(), True),
        StructField("resigned_on", StringType(), True),
        StructField("is_pre_1992_appointment", BooleanType(), True),
        StructField("appointed_before", StringType(), True),
        StructField("person_number", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("country_of_residence", StringType(), True),
        StructField("responsibilities", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("address", ADDRESS_STRUCT, True),
        StructField("date_of_birth", DATE_OF_BIRTH_STRUCT, True),
        StructField("former_names", ArrayType(FORMER_NAME_STRUCT), True),
        StructField("identification", OFFICER_IDENTIFICATION_STRUCT, True),
        StructField("links", OFFICER_LINKS_STRUCT, True),
        StructField(
            "identity_verification_details",
            OFFICER_IDENTITY_VERIFICATION_DETAILS_STRUCT,
            True,
        ),
    ]
)


# =============================================================================
# filing_history
# =============================================================================

FILING_LINKS_STRUCT = StructType(
    [
        StructField("self", StringType(), True),
        StructField("document_metadata", StringType(), True),
    ]
)

ANNOTATION_STRUCT = StructType(
    [
        StructField("annotation", StringType(), True),
        StructField("date", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

ASSOCIATED_FILING_STRUCT = StructType(
    [
        StructField("date", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

RESOLUTION_STRUCT = StructType(
    [
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("document_id", StringType(), True),
        StructField("receive_date", StringType(), True),
    ]
)

# ``description_values`` is a heterogeneous, description-code-specific
# object — different filings surface different subsets of keys. Modelled
# as an explicit struct with all observed keys (plus room for the
# ``capital`` array-of-currency values that alterations to share capital
# filings emit). Anything unmodelled here is silently dropped by the
# framework's struct coercion.
FILING_HISTORY_CAPITAL_STRUCT = StructType(
    [
        StructField("currency", StringType(), True),
        StructField("figure", StringType(), True),
    ]
)

FILING_HISTORY_DESCRIPTION_VALUES_STRUCT = StructType(
    [
        StructField("capital", ArrayType(FILING_HISTORY_CAPITAL_STRUCT), True),
        StructField("date", StringType(), True),
        StructField("description", StringType(), True),
        StructField("made_up_date", StringType(), True),
        StructField("new_date", StringType(), True),
        StructField("officer_name", StringType(), True),
        StructField("termination_date", StringType(), True),
    ]
)


FILING_HISTORY_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("transaction_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("date", StringType(), True),
        StructField("description", StringType(), True),
        StructField("barcode", StringType(), True),
        StructField("pages", LongType(), True),
        StructField("paper_filed", BooleanType(), True),
        StructField("links", FILING_LINKS_STRUCT, True),
        StructField("annotations", ArrayType(ANNOTATION_STRUCT), True),
        StructField("associated_filings", ArrayType(ASSOCIATED_FILING_STRUCT), True),
        StructField("resolutions", ArrayType(RESOLUTION_STRUCT), True),
        # Effective date the filing acts on — often distinct from ``date``
        # (submission timestamp). Present on most filings in the live API.
        StructField("action_date", StringType(), True),
        # Description-template variables that give context to the
        # description text (e.g. officer_name for TM01 terminations,
        # made_up_date for accounts, capital for SH01 alterations).
        StructField(
            "description_values",
            FILING_HISTORY_DESCRIPTION_VALUES_STRUCT,
            True,
        ),
    ]
)


# =============================================================================
# persons_with_significant_control
# =============================================================================

PSC_IDENTIFICATION_STRUCT = StructType(
    [
        StructField("country_registered", StringType(), True),
        StructField("legal_authority", StringType(), True),
        StructField("legal_form", StringType(), True),
        StructField("place_registered", StringType(), True),
        StructField("registration_number", StringType(), True),
    ]
)

PSC_LINKS_STRUCT = StructType(
    [
        StructField("self", StringType(), True),
    ]
)


PERSONS_WITH_SIGNIFICANT_CONTROL_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("kind", StringType(), True),
        StructField("name", StringType(), True),
        StructField("notified_on", StringType(), True),
        StructField("ceased", BooleanType(), True),
        StructField("ceased_on", StringType(), True),
        StructField("natures_of_control", ArrayType(StringType()), True),
        StructField("nationality", StringType(), True),
        StructField("country_of_residence", StringType(), True),
        StructField("description", StringType(), True),
        StructField("is_sanctioned", BooleanType(), True),
        StructField("etag", StringType(), True),
        StructField("address", ADDRESS_STRUCT, True),
        StructField("date_of_birth", DATE_OF_BIRTH_STRUCT, True),
        StructField("name_elements", NAME_ELEMENTS_STRUCT, True),
        StructField("identification", PSC_IDENTIFICATION_STRUCT, True),
        StructField("links", PSC_LINKS_STRUCT, True),
    ]
)


# =============================================================================
# charges
# =============================================================================

CHARGE_CLASSIFICATION_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

CHARGE_PERSON_ENTITLED_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
    ]
)

CHARGE_PARTICULARS_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("contains_fixed_charge", BooleanType(), True),
        StructField("contains_floating_charge", BooleanType(), True),
        StructField("contains_negative_pledge", BooleanType(), True),
        StructField("floating_charge_covers_all", BooleanType(), True),
    ]
)

CHARGE_SECURED_DETAILS_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

CHARGE_TRANSACTION_LINKS_STRUCT = StructType(
    [
        StructField("filing", StringType(), True),
    ]
)

CHARGE_TRANSACTION_STRUCT = StructType(
    [
        StructField("filing_type", StringType(), True),
        StructField("delivered_on", StringType(), True),
        StructField("insolvency_case_number", LongType(), True),
        StructField("links", CHARGE_TRANSACTION_LINKS_STRUCT, True),
    ]
)

CHARGE_SCOTTISH_ALTERATIONS_STRUCT = StructType(
    [
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("has_alterations_to_order", BooleanType(), True),
        StructField("has_alterations_to_prohibitions", BooleanType(), True),
        StructField("has_alterations_to_provisions", BooleanType(), True),
    ]
)

# Per-charge item envelope link. The live API returns a single ``self``
# link pointing at the charge detail resource.
CHARGE_LINKS_STRUCT = StructType(
    [
        StructField("self", StringType(), True),
    ]
)


CHARGES_SCHEMA = StructType(
    [
        StructField("company_number", StringType(), False),
        StructField("id", StringType(), True),
        StructField("charge_number", LongType(), True),
        StructField("charge_code", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_on", StringType(), True),
        StructField("delivered_on", StringType(), True),
        StructField("acquired_on", StringType(), True),
        StructField("satisfied_on", StringType(), True),
        StructField("resolved_on", StringType(), True),
        StructField("covering_instrument_date", StringType(), True),
        StructField("more_than_four_persons_entitled", BooleanType(), True),
        # Documented typo in the source API — kept literally.
        StructField("assests_ceased_released", StringType(), True),
        StructField("etag", StringType(), True),
        # The live API returns ``classification`` as a single object per
        # charge, not the array documented in the developer specification.
        # Modelled as a struct to match reality.
        StructField("classification", CHARGE_CLASSIFICATION_STRUCT, True),
        StructField("persons_entitled", ArrayType(CHARGE_PERSON_ENTITLED_STRUCT), True),
        StructField("particulars", CHARGE_PARTICULARS_STRUCT, True),
        StructField("secured_details", CHARGE_SECURED_DETAILS_STRUCT, True),
        StructField("transactions", ArrayType(CHARGE_TRANSACTION_STRUCT), True),
        StructField("scottish_alterations", CHARGE_SCOTTISH_ALTERATIONS_STRUCT, True),
        StructField("links", CHARGE_LINKS_STRUCT, True),
    ]
)


# =============================================================================
# Registry maps
# =============================================================================

TABLE_SCHEMAS: dict[str, StructType] = {
    "company_profile": StructType(COMPANY_PROFILE_SCHEMA.fields + AUDIT_FIELDS),
    "officers": StructType(OFFICERS_SCHEMA.fields + AUDIT_FIELDS),
    "filing_history": StructType(FILING_HISTORY_SCHEMA.fields + AUDIT_FIELDS),
    "persons_with_significant_control": StructType(
        PERSONS_WITH_SIGNIFICANT_CONTROL_SCHEMA.fields + AUDIT_FIELDS
    ),
    "charges": StructType(CHARGES_SCHEMA.fields + AUDIT_FIELDS),
    "registered_office_address": StructType(REGISTERED_OFFICE_ADDRESS_SCHEMA.fields + AUDIT_FIELDS),
}
"""Mapping of table names to their StructType schemas. Every schema has the
``_ingested_at`` / ``_ingested_by`` audit columns appended — see AUDIT_FIELDS."""


TABLE_METADATA: dict[str, dict] = {
    "company_profile": {
        "primary_keys": ["company_number"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "officers": {
        # (name, appointed_on, officer_role) is not reliably unique — the
        # live API has returned two distinct appointments sharing all
        # three. _source_record_url (the item's own links.self) is the
        # API's actual resource identifier for one appointment.
        "primary_keys": ["company_number", "_source_record_url"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "filing_history": {
        "primary_keys": ["company_number", "transaction_id"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "persons_with_significant_control": {
        # (notified_on, name, kind) is not reliably unique — same rationale
        # as officers above.
        "primary_keys": ["company_number", "_source_record_url"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "charges": {
        # The live API has returned charge items with a null 'id' field,
        # so (company_number, id) collides across a company's charges.
        # _source_record_url (the item's own links.self) is unique per charge.
        "primary_keys": ["company_number", "_source_record_url"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
    "registered_office_address": {
        "primary_keys": ["company_number"],
        "cursor_field": None,
        "ingestion_type": "snapshot",
    },
}
"""Metadata for each table: primary keys, cursor field, ingestion type."""


TABLE_ENDPOINT_PATHS: dict[str, str] = {
    "company_profile": "/company/{company_number}",
    "officers": "/company/{company_number}/officers",
    "filing_history": "/company/{company_number}/filing-history",
    "persons_with_significant_control": (
        "/company/{company_number}/persons-with-significant-control"
    ),
    "charges": "/company/{company_number}/charges",
    "registered_office_address": ("/company/{company_number}/registered-office-address"),
}
"""URL path template per table (formatted with a ``company_number``)."""


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())
"""Ordered list of tables exposed to LakeflowConnect.list_tables()."""
