"""FastAPI application: imports, immutable batch generation, paged results.

Run two processes against the same DATABASE_URL (see docker-compose.yml);
each serves the API and runs one cooperating worker.
"""
from __future__ import annotations

import base64
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse

from . import config
from .db import close_pool, get_pool, init_pool, init_schema
from .models import (
    RECON_MISMATCHED,
    RECON_MISSING,
    RECON_MATCHED,
    RECON_STATE_NOT_RECONCILED,
    RECON_STATE_RECONCILED,
    RECON_STATE_UNRECONCILABLE,
    ApplicationsImport,
    BatchCreateRequest,
    BatchResponse,
    CreditEventsImport,
    ImportAccepted,
    ImportBatchSummary,
    ReconciliationReport,
    ReconciliationRow,
    ResultsPage,
    ResultRow,
)
from .worker import Worker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("income.api")


class _ConcurrentConflict(Exception):
    """Raised inside the import savepoint when a re-read shows that a
    concurrent transaction committed a different value for one of this
    payload's version keys. Rolls the savepoint back; the outer transaction
    then records the rejection instead of returning a success receipt."""

    def __init__(self, conflicts: list[dict], row_count: int) -> None:
        super().__init__("concurrent conflicting versions committed")
        self.conflicts = conflicts
        self.row_count = row_count


# Fields compared line by line during reconciliation.
_RECON_FIELDS = ("customer_id", "business_time", "income", "withdrawn")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    pool = await init_pool()
    await init_schema()
    _worker = Worker(pool, config.WORKER_ID)
    _worker.start()
    logger.info("api process up; worker id = %s", config.WORKER_ID)
    try:
        yield
    finally:
        await _worker.stop()
        await close_pool()


app = FastAPI(
    title="Point-in-time Income Backfill API",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "历史贷款申请的申请时点收入回填服务。征信事件按获知时间保留全部版本，"
        "生成批次固化受理时已提交的输入，两个 API 进程共享数据库并协作续算。\n\n"
        "征信导入支持逐行核账：每条上传记录连同原字段与行号随回执留存，"
        "可按成功导入批次号逐行对照库内实际版本与首次入库批次，"
        "标出 MATCHED/MISMATCHED/MISSING 行并给出整批能否用于训练的结论；"
        "旧批次无源行记录时明确返回 UNRECONCILABLE，绝不默认通过。"
    ),
)
_worker: Worker | None = None


@app.get("/health", tags=["ops"])
async def health() -> dict:
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "worker_id": config.WORKER_ID}


# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
async def _record_import(conn, kind: str, row_count: int, ok: bool, reason: str | None) -> uuid.UUID:
    ingest_id = uuid.uuid4()
    await conn.execute(
        """INSERT INTO import_batches
               (ingest_batch_id, import_kind, row_count, status, reason)
           VALUES ($1, $2, $3, $4, $5)""",
        ingest_id, kind, row_count, "accepted" if ok else "rejected", reason,
    )
    return ingest_id


@app.post(
    "/api/v1/imports/applications",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入历史贷款申请（整批事务，失败全拒绝）",
)
async def import_applications(payload: ApplicationsImport) -> ImportAccepted:
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Duplicates inside the payload.
            seen: set[str] = set()
            dupes = [a.application_id for a in payload.applications
                     if a.application_id in seen or seen.add(a.application_id)]
            if dupes:
                ingest_id = await _record_import(
                    conn, "applications", len(payload.applications), False,
                    f"duplicate application_id in payload: {sorted(set(dupes))}",
                )
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "whole import rejected: duplicate application_id in payload",
                        "ingest_batch_id": str(ingest_id),
                        "conflicts": sorted(set(dupes)),
                    },
                )

            existing = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1)",
                [a.application_id for a in payload.applications],
            )
            if existing:
                ids = [r["application_id"] for r in existing]
                ingest_id = await _record_import(
                    conn, "applications", len(payload.applications), False,
                    f"application_id already imported: {ids}",
                )
                return JSONResponse(
                    status_code=409,
                    content={
                        "detail": "whole import rejected: application_id already exists",
                        "ingest_batch_id": str(ingest_id),
                        "conflicts": ids,
                    },
                )

            ingest_id = await _record_import(conn, "applications", len(payload.applications), True, None)
            await conn.executemany(
                """INSERT INTO applications
                       (application_id, customer_id, application_time,
                        ingested_at, ingest_batch_id)
                   VALUES ($1, $2, $3, now(), $4)""",
                [
                    (a.application_id, a.customer_id, a.application_time, ingest_id)
                    for a in payload.applications
                ],
            )
    return ImportAccepted(ingest_batch_id=ingest_id, imported=len(payload.applications), kind="applications")


@app.post(
    "/api/v1/imports/credit-events",
    response_model=ImportAccepted,
    status_code=status.HTTP_201_CREATED,
    tags=["imports"],
    summary="导入征信变更日志（同事件同获知时间异值则整批拒绝；逐行留存源行）",
)
async def import_credit_events(payload: CreditEventsImport) -> ImportAccepted:
    events = payload.events
    resolved = payload.resolved_lines()  # [(source_line, CreditEventIn), ...]

    # ---- validation: identical duplicates collapse; differing versions on
    # the same (event_id, known_time) reject the entire import batch. ----
    versions: dict[tuple[str, datetime], dict] = {}
    conflicts: list[dict] = []
    customer_of_event: dict[str, str] = {}
    for e in events:
        key = (e.event_id, e.known_time)
        signature = {
            "customer_id": e.customer_id,
            "business_time": e.business_time,
            "income": e.income,
            "withdrawn": e.withdrawn,
        }
        if key in versions and versions[key] != signature:
            conflicts.append({"event_id": e.event_id, "known_time": e.known_time.isoformat(),
                              "reason": "different values for same event_id+known_time"})
        else:
            versions.setdefault(key, signature)
        prior_customer = customer_of_event.get(e.event_id)
        if prior_customer and prior_customer != e.customer_id:
            conflicts.append({"event_id": e.event_id,
                              "reason": "event_id mapped to multiple customer_id values"})
        customer_of_event.setdefault(e.event_id, e.customer_id)

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "conflicting versions in payload")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: conflicting event versions",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            # Cross-check against already-stored versions. FOR UPDATE locks the
            # matched versions so two concurrent imports touching the same key
            # serialize here instead of both seeing the pre-import state.
            db_rows = await conn.fetch(
                """SELECT event_id, known_time, customer_id, business_time,
                          income, withdrawn
                     FROM credit_event_versions
                    WHERE (event_id, known_time) IN (
                        SELECT v.e, v.k FROM unnest($1::text[], $2::timestamptz[])
                            AS v(e, k)
                    )
                    FOR UPDATE""",
                [k[0] for k in versions], [k[1] for k in versions],
            )
            for row in db_rows:
                sig = versions[(row["event_id"], row["known_time"])]
                if (sig["customer_id"] != row["customer_id"]
                        or sig["business_time"] != row["business_time"]
                        or sig["income"] != row["income"]
                        or sig["withdrawn"] != row["withdrawn"]):
                    conflicts.append({
                        "event_id": row["event_id"],
                        "known_time": row["known_time"].isoformat(),
                        "reason": "different values already stored for event_id+known_time",
                    })
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "conflicts with stored versions")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: conflicts with stored versions",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            # Customer consistency for events that already exist under other versions.
            known_events = await conn.fetch(
                "SELECT DISTINCT event_id, customer_id FROM credit_event_versions WHERE event_id = ANY($1)",
                list(customer_of_event),
            )
            for row in known_events:
                if customer_of_event.get(row["event_id"]) != row["customer_id"]:
                    conflicts.append({"event_id": row["event_id"],
                                      "reason": "event_id belongs to another customer_id"})
            if conflicts:
                ingest_id = await _record_import(conn, "credit_events", len(events), False,
                                                 "event/customer mismatch")
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: event/customer mismatch",
                             "ingest_batch_id": str(ingest_id), "conflicts": conflicts},
                )

            # Everything so far is validated. The version inserts, the import
            # batch audit row and the per-line source snapshot must commit or
            # roll back together: a crash or a concurrent conflicting commit in
            # the middle leaves no receipt and no source rows behind.
            try:
                async with conn.transaction():
                    ingest_id = await _record_import(
                        conn, "credit_events", len(events), True, None
                    )
                    inserted = 0
                    for (event_id, known_time), sig in versions.items():
                        # Skip exact duplicates already present (idempotent
                        # re-import: same key + same value adds no new version).
                        tag = await conn.execute(
                            """INSERT INTO credit_event_versions
                                   (event_id, customer_id, business_time, known_time,
                                    income, withdrawn, ingested_at, ingest_batch_id)
                               VALUES ($1,$2,$3,$4,$5,$6,now(),$7)
                               ON CONFLICT (event_id, known_time) DO NOTHING""",
                            event_id, sig["customer_id"], sig["business_time"], known_time,
                            sig["income"], sig["withdrawn"], ingest_id,
                        )
                        if tag.endswith("0 1"):
                            inserted += 1

                    # Re-read every key under the same transaction. A concurrent
                    # transaction that committed a DIFFERENT value for a key this
                    # payload inserts would otherwise be hidden by the ON CONFLICT
                    # check at plan time; the actual stored value is the truth.
                    stored_now = await conn.fetch(
                        """SELECT event_id, known_time, customer_id, business_time,
                                  income, withdrawn
                             FROM credit_event_versions
                            WHERE (event_id, known_time) IN (
                                SELECT v.e, v.k FROM unnest($1::text[], $2::timestamptz[])
                                    AS v(e, k)
                            )""",
                        [k[0] for k in versions], [k[1] for k in versions],
                    )
                    race_conflicts = []
                    for row in stored_now:
                        sig = versions[(row["event_id"], row["known_time"])]
                        if (sig["customer_id"] != row["customer_id"]
                                or sig["business_time"] != row["business_time"]
                                or sig["income"] != row["income"]
                                or sig["withdrawn"] != row["withdrawn"]):
                            race_conflicts.append({
                                "event_id": row["event_id"],
                                "known_time": row["known_time"].isoformat(),
                                "reason": "different values committed concurrently for event_id+known_time",
                            })
                    if race_conflicts:
                        # Roll back only the version inserts + accepted audit row;
                        # the outer transaction then records the rejection, so the
                        # caller never receives a success receipt for an异值 batch.
                        raise _ConcurrentConflict(race_conflicts, len(events))

                    # Persist EVERY submitted source line (exact duplicate lines
                    # included) with its original fields and line number, so the
                    # batch can later be reconciled line by line even when it
                    # inserted zero new versions.
                    await conn.executemany(
                        """INSERT INTO credit_import_rows
                               (ingest_batch_id, source_line, event_id, customer_id,
                                business_time, known_time, income, withdrawn)
                           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                        [
                            (ingest_id, line, e.event_id, e.customer_id, e.business_time,
                             e.known_time, e.income, e.withdrawn)
                            for line, e in resolved
                        ],
                    )
            except _ConcurrentConflict as exc:
                # Savepoint rolled back: no version row, no accepted audit row,
                # no source rows survive. Record the rejection in the outer
                # transaction and return the conflict as a normal 409 receipt.
                ingest_id = await _record_import(
                    conn, "credit_events", exc.row_count, False,
                    "conflicts with versions committed concurrently",
                )
                return JSONResponse(
                    status_code=409,
                    content={"detail": "whole import rejected: conflicts with stored versions",
                             "ingest_batch_id": str(ingest_id), "conflicts": exc.conflicts},
                )
    return ImportAccepted(ingest_batch_id=ingest_id, imported=inserted, kind="credit_events",
                          source_rows=len(resolved))


# --------------------------------------------------------------------------- #
# Line-by-line reconciliation ("逐行核账")
# --------------------------------------------------------------------------- #
@app.get(
    "/api/v1/imports/credit-events",
    response_model=list[ImportBatchSummary],
    tags=["reconciliation"],
    summary="列出征信事件导入批次（标注是否留有可核账的源行记录）",
)
async def list_credit_imports() -> list[ImportBatchSummary]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT ib.ingest_batch_id, ib.import_kind, ib.row_count, ib.status,
                      ib.reason, ib.received_at,
                      EXISTS (SELECT 1 FROM credit_import_rows cir
                               WHERE cir.ingest_batch_id = ib.ingest_batch_id) AS reconcilable
                 FROM import_batches ib
                WHERE ib.import_kind = 'credit_events'
                ORDER BY ib.received_at""",
        )
    return [ImportBatchSummary(**dict(r.items())) for r in rows]


@app.get(
    "/api/v1/imports/credit-events/{ingest_batch_id}/reconciliation",
    response_model=ReconciliationReport,
    tags=["reconciliation"],
    summary="按成功导入批次号逐行核账（源行 vs 库内实际版本，给出能否用于训练）",
)
async def reconcile_credit_import(
    ingest_batch_id: uuid.UUID,
    limit: int = Query(default=config.DEFAULT_PAGE_SIZE, ge=1, le=config.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 next_cursor"),
) -> ReconciliationReport:
    after_line = _decode_cursor(cursor) if cursor else 0
    pool = get_pool()
    async with pool.acquire() as conn:
        batch = await conn.fetchrow(
            """SELECT ingest_batch_id, import_kind, row_count, status, reason, received_at
                 FROM import_batches WHERE ingest_batch_id = $1""",
            ingest_batch_id,
        )
        if not batch:
            raise HTTPException(404, "import batch not found")
        if batch["import_kind"] != "credit_events":
            raise HTTPException(409, "not a credit-events import batch; reconciliation is credit-only")

        # A rejected import never persisted any source rows: there is nothing
        # to reconcile, and it must never be treated as a passing batch.
        if batch["status"] != "accepted":
            raise HTTPException(
                409,
                f"import batch was {batch['status']}"
                + (f": {batch['reason']}" if batch["reason"] else "")
                + "; rejected imports carry no source rows and cannot be reconciled",
            )

        source_count = await conn.fetchval(
            "SELECT count(*) FROM credit_import_rows WHERE ingest_batch_id = $1",
            ingest_batch_id,
        )

        # Legacy batches created before source-row auditing existed: the
        # submitted lines cannot be reconstructed, so the honest answer is
        # "unable to reconcile" — never a pass.
        if not source_count:
            return ReconciliationReport(
                ingest_batch_id=ingest_batch_id,
                import_kind=batch["import_kind"],
                import_status=batch["status"],
                reconciliation_state=RECON_STATE_UNRECONCILABLE,
                training_eligible=False,
                reason=("no source rows were recorded for this batch (it predates "
                        "line-level auditing); reconciliation is impossible, so the "
                        "batch cannot be certified for training"),
                total_rows=0, matched_rows=0, mismatched_rows=0, missing_rows=0,
                limit=limit, next_cursor=None, rows=[],
            )

        # Whole-batch counts are computed over ALL source rows, independent of
        # the current page, so the training verdict is per-batch not per-page.
        stats = await conn.fetchrow(
            """
            WITH checked AS (
                SELECT cir.source_line,
                       CASE
                           WHEN cev.event_id IS NULL THEN $2::text
                           WHEN cir.customer_id   IS DISTINCT FROM cev.customer_id
                             OR cir.business_time IS DISTINCT FROM cev.business_time
                             OR cir.income        IS DISTINCT FROM cev.income
                             OR cir.withdrawn     IS DISTINCT FROM cev.withdrawn
                           THEN $3::text
                           ELSE $1::text
                       END AS row_status
                  FROM credit_import_rows cir
                  LEFT JOIN credit_event_versions cev
                    ON cev.event_id = cir.event_id
                   AND cev.known_time = cir.known_time
                 WHERE cir.ingest_batch_id = $4
            )
            SELECT count(*) AS total_rows,
                   count(*) FILTER (WHERE row_status = $1) AS matched_rows,
                   count(*) FILTER (WHERE row_status = $3) AS mismatched_rows,
                   count(*) FILTER (WHERE row_status = $2) AS missing_rows
              FROM checked
            """,
            RECON_MATCHED, RECON_MISSING, RECON_MISMATCHED, ingest_batch_id,
        )

        # One keyset page of source lines joined to the actually stored version.
        page_rows = await conn.fetch(
            """
            SELECT cir.source_line,
                   cir.event_id       AS sub_event_id,
                   cir.customer_id    AS sub_customer_id,
                   cir.business_time  AS sub_business_time,
                   cir.known_time     AS sub_known_time,
                   cir.income         AS sub_income,
                   cir.withdrawn      AS sub_withdrawn,
                   cev.event_id       AS act_event_id,
                   cev.customer_id    AS act_customer_id,
                   cev.business_time  AS act_business_time,
                   cev.known_time     AS act_known_time,
                   cev.income         AS act_income,
                   cev.withdrawn      AS act_withdrawn,
                   cev.ingest_batch_id AS first_ingest_batch_id
              FROM credit_import_rows cir
              LEFT JOIN credit_event_versions cev
                ON cev.event_id = cir.event_id
               AND cev.known_time = cir.known_time
             WHERE cir.ingest_batch_id = $1
               AND cir.source_line > $2
             ORDER BY cir.source_line
             LIMIT $3
            """,
            ingest_batch_id, after_line, limit + 1,
        )

    next_cursor = None
    if len(page_rows) > limit:
        next_cursor = _encode_cursor(page_rows[limit - 1]["source_line"])
        page_rows = page_rows[:limit]

    recon_rows: list[ReconciliationRow] = []
    for r in page_rows:
        submitted = {
            "event_id": r["sub_event_id"],
            "customer_id": r["sub_customer_id"],
            "business_time": r["sub_business_time"],
            "known_time": r["sub_known_time"],
            "income": r["sub_income"],
            "withdrawn": r["sub_withdrawn"],
        }
        if r["act_event_id"] is None:
            row_status = RECON_MISSING
            actual = None
            first_batch = None
            diffs: list[str] = []
        else:
            actual = {
                "event_id": r["act_event_id"],
                "customer_id": r["act_customer_id"],
                "business_time": r["act_business_time"],
                "known_time": r["act_known_time"],
                "income": r["act_income"],
                "withdrawn": r["act_withdrawn"],
            }
            first_batch = r["first_ingest_batch_id"]
            diffs = [
                f for f in _RECON_FIELDS
                if submitted[f] != actual[f]
            ]
            row_status = RECON_MISMATCHED if diffs else RECON_MATCHED
        recon_rows.append(ReconciliationRow(
            source_line=r["source_line"], status=row_status, submitted=submitted,
            actual=actual, first_ingest_batch_id=first_batch, field_differences=diffs,
        ))

    mismatched_rows = stats["mismatched_rows"]
    missing_rows = stats["missing_rows"]
    training_eligible = mismatched_rows == 0 and missing_rows == 0
    reconciliation_state = (
        RECON_STATE_RECONCILED if training_eligible else RECON_STATE_NOT_RECONCILED
    )
    reason = None
    if not training_eligible:
        problems = []
        if mismatched_rows:
            problems.append(f"{mismatched_rows} line(s) with field values differing from the stored version")
        if missing_rows:
            problems.append(f"{missing_rows} line(s) whose event_id+known_time has no stored version")
        reason = "batch fails line-by-line reconciliation: " + "; ".join(problems)

    return ReconciliationReport(
        ingest_batch_id=ingest_batch_id,
        import_kind=batch["import_kind"],
        import_status=batch["status"],
        reconciliation_state=reconciliation_state,
        training_eligible=training_eligible,
        reason=reason,
        total_rows=stats["total_rows"],
        matched_rows=stats["matched_rows"],
        mismatched_rows=mismatched_rows,
        missing_rows=missing_rows,
        limit=limit,
        next_cursor=next_cursor,
        rows=recon_rows,
    )


# --------------------------------------------------------------------------- #
# Batch generation
# --------------------------------------------------------------------------- #
@app.post(
    "/api/v1/batches",
    response_model=BatchResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["batches"],
    summary="生成回填批次（固化受理时刻已提交的输入）",
)
async def create_batch(payload: BatchCreateRequest) -> BatchResponse:
    ids = payload.application_ids
    if len(set(ids)) != len(ids):
        raise HTTPException(400, "duplicate application_ids in request")

    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT application_id FROM applications WHERE application_id = ANY($1) ORDER BY application_id",
                ids,
            )
            found = {r["application_id"] for r in rows}
            missing = sorted(set(ids) - found)
            if missing:
                raise HTTPException(400, f"unknown application_ids: {missing}")

            batch_id = uuid.uuid4()
            batch_row = await conn.fetchrow(
                """INSERT INTO batches (batch_id, frozen_ingest_at, total_items)
                   VALUES ($1, now(), $2)
                   RETURNING batch_id, created_at, frozen_ingest_at, status,
                             total_items, processed_items, worker_id, error""",
                batch_id, len(ids),
            )
            await conn.executemany(
                """INSERT INTO batch_items (batch_id, application_id, item_seq)
                   VALUES ($1, $2, $3)""",
                [(batch_id, app_id, seq) for seq, app_id in enumerate(sorted(ids))],
            )
    return BatchResponse(**dict(batch_row.items()))


@app.get("/api/v1/batches", response_model=list[BatchResponse], tags=["batches"],
         summary="列出全部批次及状态")
async def list_batches() -> list[BatchResponse]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches ORDER BY created_at""",
        )
    return [BatchResponse(**dict(r.items())) for r in rows]


@app.get("/api/v1/batches/{batch_id}", response_model=BatchResponse, tags=["batches"],
         summary="查询单个批次状态")
async def get_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error
                 FROM batches WHERE batch_id = $1""",
            batch_id,
        )
    if not row:
        raise HTTPException(404, "batch not found")
    return BatchResponse(**dict(row.items()))


@app.post("/api/v1/batches/{batch_id}/resume", response_model=BatchResponse, tags=["batches"],
          summary="将 FAILED 批次重新置为 PENDING（续算，不改变已固化输入）")
async def resume_batch(batch_id: uuid.UUID) -> BatchResponse:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE batches
                  SET status = 'PENDING', worker_id = NULL,
                      lease_expires_at = NULL, error = NULL
                WHERE batch_id = $1 AND status = 'FAILED'
            RETURNING batch_id, created_at, frozen_ingest_at, status, total_items,
                      processed_items, worker_id, error""",
            batch_id,
        )
        if not row:
            raise HTTPException(409, "only FAILED batches can be resumed")
        await conn.execute(
            """UPDATE batch_items
                  SET status = 'PENDING', worker_id = NULL, lease_expires_at = NULL
                WHERE batch_id = $1 AND status <> 'DONE'""",
            batch_id,
        )
    return BatchResponse(**dict(row.items()))


# --------------------------------------------------------------------------- #
# Results — keyset pagination, only after COMPLETED
# --------------------------------------------------------------------------- #
def _encode_cursor(item_seq: int) -> str:
    return base64.urlsafe_b64encode(str(item_seq).encode()).decode()


def _decode_cursor(cursor: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception as exc:
        raise HTTPException(400, "invalid cursor") from exc


@app.get("/api/v1/batches/{batch_id}/results", response_model=ResultsPage, tags=["results"],
         summary="分页查询结果（批次全部完成后才开放；成功批次内容不再变化）")
async def get_results(
    batch_id: uuid.UUID,
    limit: int = Query(default=config.DEFAULT_PAGE_SIZE, ge=1, le=config.MAX_PAGE_SIZE),
    cursor: str | None = Query(default=None, description="上一页返回的 next_cursor"),
) -> ResultsPage:
    after_seq = _decode_cursor(cursor) if cursor else -1
    pool = get_pool()
    async with pool.acquire() as conn:
        b = await conn.fetchrow("SELECT status, error FROM batches WHERE batch_id = $1", batch_id)
        if not b:
            raise HTTPException(404, "batch not found")
        if b["status"] != "COMPLETED":
            raise HTTPException(
                409,
                f"results not open: batch is {b['status']}"
                + (f" ({b['error']})" if b["error"] else ""),
            )
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time, income,
                      source_event_id, source_version_known_time,
                      source_business_time, source_ingest_batch_id, item_seq
                 FROM batch_results
                WHERE batch_id = $1 AND item_seq > $2
                ORDER BY item_seq
                LIMIT $3""",
            batch_id, after_seq, limit + 1,
        )
    next_cursor = None
    if len(rows) > limit:
        next_cursor = _encode_cursor(rows[limit - 1]["item_seq"])
        rows = rows[:limit]
    items = [
        ResultRow(
            application_id=r["application_id"],
            customer_id=r["customer_id"],
            application_time=r["application_time"],
            income=r["income"],
            source_event_id=r["source_event_id"],
            source_version_known_time=r["source_version_known_time"],
            source_business_time=r["source_business_time"],
            source_ingest_batch_id=r["source_ingest_batch_id"],
            basis="event" if r["source_event_id"] else "no_known_event",
        )
        for r in rows
    ]
    return ResultsPage(batch_id=batch_id, status="COMPLETED", limit=limit,
                       next_cursor=next_cursor, items=items)


# --------------------------------------------------------------------------- #
# Debug / inspection helpers (used by the demo script and /docs)
# --------------------------------------------------------------------------- #
@app.get("/api/v1/applications", tags=["inspect"], summary="查看已导入申请")
async def list_applications() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT application_id, customer_id, application_time,
                      ingested_at, ingest_batch_id
                 FROM applications ORDER BY application_id""",
        )
    return [dict(r.items()) for r in rows]


@app.get("/api/v1/events", tags=["inspect"], summary="查看征信事件全部版本")
async def list_events() -> list[dict]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT event_id, customer_id, business_time, known_time, income,
                      withdrawn, ingested_at, ingest_batch_id
                 FROM credit_event_versions
                ORDER BY event_id, known_time""",
        )
    return [dict(r.items()) for r in rows]
