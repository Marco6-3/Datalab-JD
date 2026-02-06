from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

VALID_JOB_STATUSES = {"queued", "running", "succeeded", "failed"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SQLiteJobStore:
    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs (
                        job_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        outputs_json TEXT NOT NULL DEFAULT '{}',
                        error_message TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.commit()

    def create_job(self, job_id: str, status: str = "queued") -> None:
        if status not in VALID_JOB_STATUSES:
            raise ValueError(f"Invalid job status: {status}")
        now = _utc_now_iso()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO jobs (job_id, status, outputs_json, error_message, created_at, updated_at)
                    VALUES (?, ?, '{}', NULL, ?, ?)
                    """,
                    (job_id, status, now, now),
                )
                conn.commit()

    def update_job(
        self,
        job_id: str,
        *,
        status: str,
        outputs: dict[str, str] | None = None,
        error_message: str | None = None,
    ) -> None:
        if status not in VALID_JOB_STATUSES:
            raise ValueError(f"Invalid job status: {status}")

        payload = json.dumps(outputs or {}, ensure_ascii=True)
        now = _utc_now_iso()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = ?, outputs_json = ?, error_message = ?, updated_at = ?
                    WHERE job_id = ?
                    """,
                    (status, payload, error_message, now, job_id),
                )
                conn.commit()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT job_id, status, outputs_json, error_message FROM jobs WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
        if row is None:
            return None
        return {
            "job_id": row["job_id"],
            "status": row["status"],
            "outputs": json.loads(row["outputs_json"] or "{}"),
            "error_message": row["error_message"],
        }
