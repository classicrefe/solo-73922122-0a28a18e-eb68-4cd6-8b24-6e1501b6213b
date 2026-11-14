"""Pydantic request/response models. All datetimes are timezone-aware UTC."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator, model_validator


def _check_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must include an explicit timezone (use UTC, e.g. 2026-01-10T08:00:00Z)")
    return value.astimezone(timezone.utc)


class ApplicationIn(BaseModel):
    application_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    application_time: datetime

    @field_validator("application_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)


class ApplicationsImport(BaseModel):
    applications: list[ApplicationIn] = Field(min_length=1)


class CreditEventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    customer_id: str = Field(min_length=1, max_length=128)
    business_time: datetime
    known_time: datetime
    income: int | None = Field(default=None)
    withdrawn: bool = False
    # Optional explicit source line number from the uploader's file. When
    # omitted the server assigns the 1-based position inside the payload.
    source_line: int | None = Field(default=None, ge=1)

    @field_validator("business_time", "known_time")
    @classmethod
    def _utc(cls, v: datetime) -> datetime:
        return _check_utc(v)

    @field_validator("withdrawn")
    @classmethod
    def _income_consistency(cls, withdrawn: bool, info) -> bool:
        income = info.data.get("income")
        if withdrawn and income is not None:
            raise ValueError("withdrawn event must not carry an income")
        if not withdrawn and income is None:
            raise ValueError("non-withdrawn event requires an integer income")
        return withdrawn


class CreditEventsImport(BaseModel):
    events: list[CreditEventIn] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_source_lines(self) -> "CreditEventsImport":
        explicit = [e.source_line for e in self.events if e.source_line is not None]
        if len(set(explicit)) != len(explicit):
            dupes = sorted({ln for ln in explicit if explicit.count(ln) > 1})
            raise ValueError(f"duplicate source_line values in payload: {dupes}")
        return self

    def resolved_lines(self) -> list[tuple[int, "CreditEventIn"]]:
        """Return (source_line, event) pairs; missing line numbers default to
        the event's 1-based position in the payload."""
        return [(e.source_line if e.source_line is not None else idx, e)
                for idx, e in enumerate(self.events, start=1)]


class RejectedResponse(BaseModel):
    detail: str
    ingest_batch_id: uuid.UUID
    conflicts: list[dict] | None = None


class BatchCreateRequest(BaseModel):
    application_ids: list[str] = Field(min_length=1)


class BatchResponse(BaseModel):
    batch_id: uuid.UUID
    status: str
    total_items: int
    processed_items: int
    frozen_ingest_at: datetime
    created_at: datetime
    worker_id: str | None = None
    error: str | None = None


class ResultRow(BaseModel):
    application_id: str
    customer_id: str
    application_time: datetime
    income: int | None
    source_event_id: str | None
    source_version_known_time: datetime | None
    source_business_time: datetime | None
    source_ingest_batch_id: uuid.UUID | None
    basis: str  # "event" or "no_known_event"


class ResultsPage(BaseModel):
    batch_id: uuid.UUID
    status: str
    limit: int
    next_cursor: str | None
    items: list[ResultRow]


class ImportAccepted(BaseModel):
    ingest_batch_id: uuid.UUID
    imported: int
    kind: str
    # Number of submitted source lines persisted for line-by-line
    # reconciliation (equals the payload size for accepted credit imports).
    source_rows: int | None = None


# --------------------------------------------------------------------------- #
# Line-by-line reconciliation ("逐行核账")
# --------------------------------------------------------------------------- #
# Row-level outcomes:
#   MATCHED    — the stored version keyed by (event_id, known_time) has exactly
#                the submitted fields;
#   MISMATCHED — the key exists but at least one field differs (a successful
#                receipt can never make a differing value count as passed);
#   MISSING    — no stored version exists for the submitted key.
RECON_MATCHED = "MATCHED"
RECON_MISMATCHED = "MISMATCHED"
RECON_MISSING = "MISSING"

# Batch-level reconciliation states:
#   RECONCILED      — source rows exist and every line was checked;
#   NOT_RECONCILED  — discrepancies found (see row-level statuses);
#   UNRECONCILABLE  — the batch predates source-row auditing, so its submitted
#                     lines cannot be reconstructed; it must never be passed.
RECON_STATE_RECONCILED = "RECONCILED"
RECON_STATE_NOT_RECONCILED = "NOT_RECONCILED"
RECON_STATE_UNRECONCILABLE = "UNRECONCILABLE"


class ReconciliationRow(BaseModel):
    source_line: int
    status: str  # MATCHED | MISMATCHED | MISSING
    # Exactly what the caller submitted on this line.
    submitted: dict
    # The version actually stored for (event_id, known_time); None when MISSING.
    actual: dict | None = None
    # First import batch that ingested the stored version; None when MISSING.
    first_ingest_batch_id: uuid.UUID | None = None
    # Subset of field names whose values differ (only present for MISMATCHED).
    field_differences: list[str] = Field(default_factory=list)


class ReconciliationReport(BaseModel):
    ingest_batch_id: uuid.UUID
    import_kind: str
    import_status: str  # accepted | rejected
    # RECONCILED | NOT_RECONCILED | UNRECONCILABLE
    reconciliation_state: str
    # Overall training verdict. True only when every submitted source line is
    # MATCHED against a stored version; missing/differing lines and legacy
    # batches without source rows all yield False.
    training_eligible: bool
    reason: str | None = None
    total_rows: int
    matched_rows: int
    mismatched_rows: int
    missing_rows: int
    limit: int
    next_cursor: str | None
    rows: list[ReconciliationRow]


class ImportBatchSummary(BaseModel):
    ingest_batch_id: uuid.UUID
    import_kind: str
    row_count: int
    status: str
    reason: str | None
    received_at: datetime
    # False for batches created before source-row auditing existed.
    reconcilable: bool
