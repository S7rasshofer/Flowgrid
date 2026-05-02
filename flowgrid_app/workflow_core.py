from __future__ import annotations

import csv
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator

from flowgrid_app import PermissionDeniedError, PermissionService, UserRepository
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.paths import (
    ASSETS_DIR_NAME,
    ASSET_ADMIN_ICON_DIR_NAME,
    ASSET_AGENT_ICON_DIR_NAME,
    ASSET_PART_FLAG_IMAGE_DIR_NAME,
    ASSET_QA_FLAG_ICON_DIR_NAME,
    ASSET_UI_ICON_COMPAT_DIR_NAME,
    DEPOT_BACKGROUND_AUTO_REFRESH_MS,
    DEPOT_DB_REOPEN_COOLDOWN_MS,
    _data_file_path,
)
from flowgrid_app.runtime_logging import _json_safe, _runtime_log_event
from flowgrid_app.ui_utils import clamp, safe_int
from flowgrid_app.window.constants import DEPOT_VIEW_TTL_MS
from flowgrid_app.window.query_support import (
    _alert_quiet_active,
    _dedupe_part_detail_rows,
    _installed_key_set_from_text,
    _merged_part_detail_rows,
    _next_alert_quiet_until,
    _parse_iso_datetime_local,
    _part_detail_row_key,
    _serialize_part_detail_rows,
    _serialized_installed_keys,
    _submission_entry_date_sql,
    _submission_latest_ts_sql,
)

_RUNTIME_ESCALATED_EVENTS: set[str] = set()


def _escalate_runtime_issue_once(
    event_key: str,
    summary: str,
    *,
    details: str = "",
    context: dict[str, Any] | None = None,
) -> None:
    if event_key in _RUNTIME_ESCALATED_EVENTS:
        return
    _RUNTIME_ESCALATED_EVENTS.add(event_key)
    message = str(summary or "").strip()
    if details:
        message = f"{message}\n\nDetails:\n{details}"
    if context:
        message = f"{message}\n\nContext:\n{json.dumps(_json_safe(context), ensure_ascii=False, indent=2)}"
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.user32.MessageBoxW(None, message[:1800], "Flowgrid Runtime Issue", 0x10 | 0x1000)
            return
        except Exception:
            pass


QA_FLAG_OPTIONS: tuple[str, ...] = (
    "None",
    "Follow Up",
    "Need Parts",
    "Escalation",
    "Client Callback",
    "Return Visit",
    "Safety",
    "Other",
)

QA_FLAG_SEVERITY_OPTIONS: tuple[str, ...] = ("Low", "Medium", "High", "Critical")

TRACKER_DASHBOARD_TABLES: tuple[tuple[str, str], ...] = (
    ("submissions", "Submissions"),
    ("parts", "Parts"),
    ("rtvs", "RTVs"),
    ("client_jo", "Client JO"),
    ("client_parts", "Client Parts"),
)

@dataclass(slots=True)
class DepotRefreshViewState:
    state_key: str = ""
    last_refresh_ms: float = 0.0
    last_reason: str = ""

class DepotRefreshCoordinator:
    """Centralized refresh policy for shared-drive workflow views."""

    _SKIP_LOG_INTERVAL_MS = 2000

    def __init__(self) -> None:
        self._view_states: dict[str, DepotRefreshViewState] = {}
        self._invalidated_views: set[str] = set()
        self._cached_payloads: dict[tuple[str, str], tuple[float, Any]] = {}
        self._last_skip_log_ms: dict[str, float] = {}

    @staticmethod
    def _now_ms() -> float:
        return time.monotonic() * 1000.0

    @staticmethod
    def _normalize_key(value: Any) -> str:
        text = str(value or "").strip()
        return text

    @staticmethod
    def _normalize_state_key(value: Any) -> str:
        if isinstance(value, (dict, list, tuple, set)):
            try:
                return json.dumps(_json_safe(value), sort_keys=True, ensure_ascii=True, separators=(",", ":"))
            except Exception:
                return repr(value)
        return str(value or "")

    def invalidate_views(self, *view_keys: str, reason: str = "") -> None:
        normalized_keys = [self._normalize_key(view_key) for view_key in view_keys if self._normalize_key(view_key)]
        if not normalized_keys:
            return
        for view_key in normalized_keys:
            self._invalidated_views.add(view_key)
            self._cached_payloads = {
                cache_key: cache_value
                for cache_key, cache_value in self._cached_payloads.items()
                if cache_key[0] != view_key
            }
        _runtime_log_event(
            "sync.depot_views_invalidated",
            severity="info",
            summary="Shared workflow view invalidation was triggered.",
            context={
                "views": normalized_keys,
                "reason": str(reason or ""),
            },
        )

    def should_refresh_view(
        self,
        view_key: str,
        state_key: Any,
        *,
        force: bool = False,
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
        reason: str = "",
    ) -> bool:
        normalized_view = self._normalize_key(view_key)
        normalized_state = self._normalize_state_key(state_key)
        if not normalized_view:
            return True
        if force:
            _runtime_log_event(
                "sync.depot_view_refresh_forced",
                severity="info",
                summary="A shared workflow view refresh was forced.",
                context={"view": normalized_view, "reason": str(reason or ""), "state_key": normalized_state[:160]},
            )
            return True
        if normalized_view in self._invalidated_views:
            return True

        entry = self._view_states.get(normalized_view)
        if entry is None:
            return True
        if entry.state_key != normalized_state:
            return True

        elapsed_ms = self._now_ms() - float(entry.last_refresh_ms or 0.0)
        if elapsed_ms >= int(max(0, ttl_ms)):
            return True

        last_skip_log = float(self._last_skip_log_ms.get(normalized_view, 0.0))
        now_ms = self._now_ms()
        if (now_ms - last_skip_log) >= self._SKIP_LOG_INTERVAL_MS:
            self._last_skip_log_ms[normalized_view] = now_ms
            _runtime_log_event(
                "sync.depot_view_refresh_skipped_fresh",
                severity="info",
                summary="A shared workflow view refresh was skipped because the cached state is still fresh.",
                context={
                    "view": normalized_view,
                    "reason": str(reason or ""),
                    "ttl_ms": int(ttl_ms),
                    "age_ms": int(max(0.0, elapsed_ms)),
                },
            )
        return False

    def mark_view_refreshed(
        self,
        view_key: str,
        state_key: Any,
        *,
        payload: Any = None,
        reason: str = "",
        duration_ms: float | None = None,
        row_count: int | None = None,
    ) -> None:
        normalized_view = self._normalize_key(view_key)
        normalized_state = self._normalize_state_key(state_key)
        if not normalized_view:
            return
        self._view_states[normalized_view] = DepotRefreshViewState(
            state_key=normalized_state,
            last_refresh_ms=self._now_ms(),
            last_reason=str(reason or ""),
        )
        self._invalidated_views.discard(normalized_view)
        self._cached_payloads[(normalized_view, normalized_state)] = (self._now_ms(), payload)
        context = {
            "view": normalized_view,
            "reason": str(reason or ""),
        }
        if duration_ms is not None:
            context["duration_ms"] = int(max(0.0, float(duration_ms)))
        if row_count is not None:
            context["row_count"] = int(max(0, row_count))
        _runtime_log_event(
            "sync.depot_view_refreshed",
            severity="info",
            summary="A shared workflow view refresh completed.",
            context=context,
        )

    def get_cached_payload(
        self,
        view_key: str,
        state_key: Any,
        *,
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> Any | None:
        normalized_view = self._normalize_key(view_key)
        normalized_state = self._normalize_state_key(state_key)
        cache_entry = self._cached_payloads.get((normalized_view, normalized_state))
        if cache_entry is None or normalized_view in self._invalidated_views:
            return None
        cached_at_ms, payload = cache_entry
        if (self._now_ms() - float(cached_at_ms)) > int(max(0, ttl_ms)):
            self._cached_payloads.pop((normalized_view, normalized_state), None)
            return None
        return payload

class DepotDB:
    _READ_ONLY_WRITE_PREFIXES: tuple[str, ...] = (
        "INSERT",
        "UPDATE",
        "DELETE",
        "REPLACE",
        "CREATE",
        "ALTER",
        "DROP",
        "VACUUM",
        "REINDEX",
        "ATTACH",
        "DETACH",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "ANALYZE",
    )

    def __init__(self, db_path: Path, *, read_only: bool = False, ensure_schema: bool = True):
        self.db_path = db_path
        self.read_only = bool(read_only)
        self.ensure_schema_on_open = bool(ensure_schema) and not self.read_only
        self._transaction_depth = 0
        self._connection_unhealthy = False
        self._last_reopen_attempt_ms = 0.0
        self._closed = False
        self._close_reason = ""
        self.conn: sqlite3.Connection | None = self._open_connection()
        self._create_tables()

    def _read_only_connection_target(self) -> str:
        raw_path = str(self.db_path)
        if raw_path.startswith("\\\\"):
            return f"file:{raw_path}?mode=ro"
        try:
            return f"{self.db_path.resolve().as_uri()}?mode=ro"
        except Exception:
            return f"file:{str(self.db_path).replace(os.sep, '/')}?mode=ro"

    def _read_write_existing_connection_target(self) -> str:
        raw_path = str(self.db_path)
        if raw_path.startswith("\\\\"):
            return f"file:{raw_path}?mode=rw"
        try:
            return f"{self.db_path.absolute().as_uri()}?mode=rw"
        except Exception:
            return f"file:{str(self.db_path).replace(os.sep, '/')}?mode=rw"

    def _open_connection(self) -> sqlite3.Connection:
        use_uri = self.read_only
        if self.read_only:
            connect_target = self._read_only_connection_target()
        elif self.ensure_schema_on_open:
            connect_target = str(self.db_path)
        else:
            connect_target = self._read_write_existing_connection_target()
            use_uri = True
        connection = sqlite3.connect(
            connect_target,
            timeout=30.0,
            isolation_level=None,
            uri=use_uri,
        )
        connection.row_factory = sqlite3.Row
        self._apply_connection_pragmas(connection, read_only=self.read_only)
        self._connection_unhealthy = False
        return connection

    @staticmethod
    def _apply_connection_pragmas(connection: sqlite3.Connection, *, read_only: bool = False) -> None:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 20000")
        if not read_only:
            connection.execute("PRAGMA journal_mode = DELETE")

    def _closed_database_error(self, operation_name: str) -> RuntimeError:
        close_reason = str(self._close_reason or "").strip() or "shutdown"
        message = (
            "Shared workflow database connection is closed; "
            f"blocked {operation_name} after {close_reason}."
        )
        _runtime_log_event(
            "depot.db.use_after_close_blocked",
            severity="warning",
            summary="Blocked a database operation because the shared workflow database connection is closed.",
            context={
                "db_path": str(self.db_path),
                "operation": str(operation_name or ""),
                "close_reason": close_reason,
                "read_only": bool(self.read_only),
            },
        )
        return RuntimeError(message)

    def close(self, reason: str = "shutdown") -> bool:
        if self._closed:
            return True
        close_reason = str(reason or "").strip() or "shutdown"
        self._closed = True
        self._close_reason = close_reason
        connection = self.conn
        self.conn = None
        self._connection_unhealthy = False
        if connection is None:
            _runtime_log_event(
                "depot.db.close_succeeded",
                severity="info",
                summary="The shared workflow database connection was already released during close.",
                context={
                    "db_path": str(self.db_path),
                    "reason": close_reason,
                    "read_only": bool(self.read_only),
                    "already_closed": True,
                },
            )
            return True
        try:
            connection.close()
        except Exception as exc:
            _runtime_log_event(
                "depot.db.close_failed",
                severity="error",
                summary="Closing the shared workflow database connection failed.",
                exc=exc,
                context={
                    "db_path": str(self.db_path),
                    "reason": close_reason,
                    "read_only": bool(self.read_only),
                },
            )
            return False
        _runtime_log_event(
            "depot.db.close_succeeded",
            severity="info",
            summary="The shared workflow database connection was closed successfully.",
            context={
                "db_path": str(self.db_path),
                "reason": close_reason,
                "read_only": bool(self.read_only),
                "already_closed": False,
            },
        )
        return True

    def _should_reopen_for_error(self, exc: BaseException, *, lock_retry_exhausted: bool = False) -> bool:
        if self._transaction_depth > 0:
            return False
        if self._closed:
            return False
        if lock_retry_exhausted:
            return True
        message = str(exc or "").strip().lower()
        recoverable_fragments = (
            "cannot operate on a closed database",
            "unable to open database file",
            "disk i/o error",
            "database disk image is malformed",
            "readonly database",
        )
        return any(fragment in message for fragment in recoverable_fragments)

    def reopen_connection(self, reason: str) -> bool:
        if self._transaction_depth > 0:
            return False
        if self._closed:
            return False
        now_ms = time.monotonic() * 1000.0
        if (now_ms - float(self._last_reopen_attempt_ms or 0.0)) < DEPOT_DB_REOPEN_COOLDOWN_MS:
            return False
        self._last_reopen_attempt_ms = now_ms
        old_conn = getattr(self, "conn", None)
        _runtime_log_event(
            "depot.db.reopen_requested",
            severity="warning",
            summary="The shared workflow database connection is being reopened.",
            context={"db_path": str(self.db_path), "reason": str(reason or "")},
        )
        try:
            new_conn = self._open_connection()
        except Exception as exc:
            self._connection_unhealthy = True
            _runtime_log_event(
                "depot.db.reopen_failed",
                severity="error",
                summary="The shared workflow database connection reopen attempt failed.",
                exc=exc,
                context={"db_path": str(self.db_path), "reason": str(reason or "")},
            )
            return False
        self.conn = new_conn
        try:
            if old_conn is not None:
                old_conn.close()
        except Exception as exc:
            _runtime_log_event(
                "depot.db.reopen_previous_close_failed",
                severity="warning",
                summary="Closing the previous shared workflow database connection failed after a reopen.",
                exc=exc,
                context={"db_path": str(self.db_path)},
            )
        _runtime_log_event(
            "depot.db.reopen_succeeded",
            severity="info",
            summary="The shared workflow database connection was reopened successfully.",
            context={"db_path": str(self.db_path), "reason": str(reason or "")},
        )
        return True

    def _run_sql_with_retry(
        self,
        operation_name: str,
        query: str,
        params: tuple,
        runner: Callable[[sqlite3.Cursor], Any],
        *,
        allow_reopen: bool = False,
    ) -> Any:
        attempt = 0
        reopened = False
        while True:
            connection = self.conn
            if self._closed or connection is None:
                raise self._closed_database_error(operation_name)
            try:
                cursor = connection.cursor()
                result = runner(cursor)
                self._connection_unhealthy = False
                return result
            except sqlite3.OperationalError as exc:
                attempt += 1
                message = str(exc).lower()
                is_locked = "locked" in message
                if is_locked and attempt < 6:
                    time.sleep(min(1.5, 0.15 * (2 ** (attempt - 1))))
                    continue
                self._connection_unhealthy = True
                if allow_reopen and not reopened and self._should_reopen_for_error(exc, lock_retry_exhausted=is_locked):
                    reopened = self.reopen_connection(f"{operation_name}:{'locked' if is_locked else 'operational_error'}")
                    if reopened:
                        attempt = 0
                        continue
                if is_locked:
                    _runtime_log_event(
                        "depot.db.lock_retry_exhausted",
                        severity="warning",
                        summary="Shared workflow database lock retries were exhausted.",
                        exc=exc,
                        context={
                            "db_path": str(self.db_path),
                            "operation": operation_name,
                            "attempts": attempt,
                            "query_preview": " ".join(str(query).split())[:240],
                            "param_count": len(params),
                            "reopened": bool(reopened),
                        },
                    )
                raise
            except (sqlite3.ProgrammingError, sqlite3.DatabaseError) as exc:
                self._connection_unhealthy = True
                if self._closed or self.conn is None:
                    raise self._closed_database_error(operation_name) from exc
                if allow_reopen and not reopened and self._should_reopen_for_error(exc):
                    reopened = self.reopen_connection(f"{operation_name}:connection_recovery")
                    if reopened:
                        continue
                raise

    def _execute_transaction_command(self, command: str) -> None:
        self._run_sql_with_retry(
            "transaction",
            command,
            (),
            lambda cursor: cursor.execute(command),
        )

    @staticmethod
    def _normalize_query_for_intent(query: str) -> str:
        text = str(query or "")
        text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
        text = re.sub(r"^\s*--.*?$", " ", text, flags=re.MULTILINE)
        return text.strip().upper()

    @classmethod
    def _query_attempts_write(cls, query: str) -> bool:
        normalized = cls._normalize_query_for_intent(query)
        if not normalized:
            return False
        if normalized.startswith("PRAGMA"):
            return "=" in normalized
        if normalized.startswith("WITH"):
            return bool(re.search(r"\b(INSERT|UPDATE|DELETE|REPLACE)\b", normalized))
        return any(normalized.startswith(prefix) for prefix in cls._READ_ONLY_WRITE_PREFIXES)

    def _raise_read_only_write_block(self, *, purpose: str, query: str = "", param_count: int = 0) -> None:
        query_preview = " ".join(str(query or "").split())[:240]
        summary = "Blocked a write attempt because the shared workflow database is open read-only."
        _runtime_log_event(
            "depot.db.read_only_write_blocked",
            severity="warning",
            summary=summary,
            context={
                "db_path": str(self.db_path),
                "purpose": str(purpose or ""),
                "query_preview": query_preview,
                "param_count": int(param_count),
            },
        )
        raise RuntimeError(
            f"Shared workflow database is open read-only; blocked write attempt for {purpose}."
        )

    @contextmanager
    def write_transaction(self, purpose: str = "workflow.write") -> Iterator[None]:
        if self.read_only:
            self._raise_read_only_write_block(purpose=purpose)
        is_outer = self._transaction_depth == 0
        if is_outer:
            self._execute_transaction_command("BEGIN IMMEDIATE")
        self._transaction_depth += 1
        try:
            yield
        except Exception:
            if is_outer:
                try:
                    self._execute_transaction_command("ROLLBACK")
                except Exception as rollback_exc:
                    _runtime_log_event(
                        "depot.db.transaction_rollback_failed",
                        severity="critical",
                        summary="A database transaction rollback failed after an exception.",
                        exc=rollback_exc,
                        context={"db_path": str(self.db_path), "purpose": str(purpose)},
                    )
            raise
        else:
            if is_outer:
                try:
                    self._execute_transaction_command("COMMIT")
                except Exception:
                    try:
                        self._execute_transaction_command("ROLLBACK")
                    except Exception as rollback_exc:
                        _runtime_log_event(
                            "depot.db.transaction_rollback_failed",
                            severity="critical",
                            summary="A database transaction rollback failed after a commit error.",
                            exc=rollback_exc,
                            context={"db_path": str(self.db_path), "purpose": str(purpose)},
                        )
                    raise
        finally:
            self._transaction_depth = max(0, self._transaction_depth - 1)

    def _create_tables(self) -> None:
        if not self.ensure_schema_on_open:
            return
        DepotSchema.ensure_schema(self)
        DepotSchema.apply_migrations(self)
        DepotSchema.run_backfills(self)

    def _ensure_column(self, table_name: str, column_name: str, column_sql: str) -> None:
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = {str(row[1]).lower() for row in cursor.fetchall()}
        if column_name.lower() in columns:
            return
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")

    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        if self.read_only and self._query_attempts_write(query):
            self._raise_read_only_write_block(purpose="execute", query=query, param_count=len(params))
        return self._run_sql_with_retry(
            "execute",
            query,
            params,
            lambda cursor: cursor.execute(query, params),
        )

    def fetchall(self, query: str, params: tuple = ()) -> list[sqlite3.Row]:
        return self._run_sql_with_retry(
            "fetchall",
            query,
            params,
            lambda cursor: cursor.execute(query, params).fetchall(),
            allow_reopen=True,
        )

    def fetchone(self, query: str, params: tuple = ()) -> sqlite3.Row | None:
        return self._run_sql_with_retry(
            "fetchone",
            query,
            params,
            lambda cursor: cursor.execute(query, params).fetchone(),
            allow_reopen=True,
        )

class DepotSchema:
    @staticmethod
    def ensure_schema(db: DepotDB) -> None:
        with db.write_transaction("schema.ensure"):
            cursor = db.conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT '',
                    user_id TEXT NOT NULL,
                    work_order TEXT NOT NULL,
                    touch TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT '',
                    client_unit INTEGER NOT NULL DEFAULT 0,
                    entry_date TEXT NOT NULL,
                    serial_number TEXT NOT NULL DEFAULT '',
                    part_order_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submissions_user ON submissions(user_id)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submissions_work_order ON submissions(work_order)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submissions_touch ON submissions(touch)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_submissions_entry_date ON submissions(entry_date)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS parts (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    assigned_user_id TEXT NOT NULL,
                    source_submission_id INTEGER NOT NULL DEFAULT 0,
                    missing_part_order_followup INTEGER NOT NULL DEFAULT 0,
                    missing_part_order_logged_at TEXT NOT NULL DEFAULT '',
                    missing_part_order_logged_by TEXT NOT NULL DEFAULT '',
                    missing_part_order_resolved_at TEXT NOT NULL DEFAULT '',
                    missing_part_order_resolved_by TEXT NOT NULL DEFAULT '',
                    work_order TEXT NOT NULL,
                    client_unit INTEGER NOT NULL DEFAULT 0,
                    category TEXT NOT NULL,
                    comments TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    working_user_id TEXT NOT NULL DEFAULT '',
                    working_updated_at TEXT NOT NULL DEFAULT '',
                    alert_quiet_until TEXT NOT NULL DEFAULT '',
                    parts_on_hand INTEGER NOT NULL DEFAULT 0,
                    parts_installed INTEGER NOT NULL DEFAULT 0,
                    parts_installed_by TEXT NOT NULL DEFAULT '',
                    parts_installed_at TEXT NOT NULL DEFAULT ''
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS part_details (
                    id INTEGER PRIMARY KEY,
                    part_id INTEGER NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    lpn TEXT NOT NULL,
                    part_number TEXT NOT NULL DEFAULT '',
                    part_description TEXT NOT NULL DEFAULT '',
                    shipping_info TEXT NOT NULL DEFAULT '',
                    installed_keys TEXT NOT NULL DEFAULT '',
                    delivered INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(part_id) REFERENCES parts(id) ON DELETE CASCADE
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_part_details_lpn ON part_details(lpn)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_parts_user ON parts(user_id)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_parts_work_order ON parts(work_order)
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS rtvs (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    work_order TEXT NOT NULL,
                    comments TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS client_jo (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    work_order TEXT NOT NULL,
                    comments TEXT
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS client_parts (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    assigned_user_id TEXT,
                    work_order TEXT NOT NULL UNIQUE,
                    comments TEXT,
                    alert_quiet_until TEXT NOT NULL DEFAULT '',
                    followup_last_action TEXT NOT NULL DEFAULT '',
                    followup_last_action_at TEXT NOT NULL DEFAULT '',
                    followup_last_actor TEXT NOT NULL DEFAULT '',
                    followup_no_contact_count INTEGER NOT NULL DEFAULT 0,
                    followup_stage_logged INTEGER NOT NULL DEFAULT -1
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    id INTEGER PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    user_id TEXT NOT NULL UNIQUE,
                    tier INTEGER NOT NULL DEFAULT 1,
                    location TEXT NOT NULL DEFAULT '',
                    icon_path TEXT NOT NULL DEFAULT ''
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS admin_users (
                    id INTEGER PRIMARY KEY,
                    user_id TEXT NOT NULL UNIQUE,
                    admin_name TEXT NOT NULL DEFAULT '',
                    position TEXT NOT NULL DEFAULT '',
                    location TEXT NOT NULL DEFAULT '',
                    icon_path TEXT NOT NULL DEFAULT '',
                    access_level TEXT NOT NULL DEFAULT 'admin'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS role_definitions (
                    id INTEGER PRIMARY KEY,
                    role_name TEXT NOT NULL UNIQUE,
                    role_slot TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS qa_flags (
                    id INTEGER PRIMARY KEY,
                    flag_name TEXT NOT NULL UNIQUE,
                    severity TEXT NOT NULL DEFAULT 'Medium',
                    icon_path TEXT NOT NULL DEFAULT '',
                    sort_order INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_qa_flags_sort ON qa_flags(sort_order, severity, flag_name)
                """
            )

    @staticmethod
    def apply_migrations(db: DepotDB) -> None:
        with db.write_transaction("schema.migrate"):
            cursor = db.conn.cursor()
            db._ensure_column("agents", "tier", "INTEGER NOT NULL DEFAULT 1")
            db._ensure_column("agents", "location", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("agents", "icon_path", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("submissions", "category", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("submissions", "updated_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("submissions", "serial_number", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("admin_users", "admin_name", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("admin_users", "position", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("admin_users", "location", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("admin_users", "icon_path", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("admin_users", "access_level", "TEXT NOT NULL DEFAULT 'admin'")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS role_definitions (
                    id INTEGER PRIMARY KEY,
                    role_name TEXT NOT NULL UNIQUE,
                    role_slot TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cursor.execute(
                "UPDATE admin_users SET access_level='' "
                "WHERE access_level IS NULL"
            )
            DepotSchema._seed_default_role_definitions(cursor)
            db._ensure_column("parts", "qa_comment", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "agent_comment", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "qa_flag", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "qa_flag_image_path", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "working_user_id", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "working_updated_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "alert_quiet_until", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "parts_on_hand", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("parts", "parts_installed", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("parts", "parts_installed_by", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "parts_installed_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "source_submission_id", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("parts", "missing_part_order_followup", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("parts", "missing_part_order_logged_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "missing_part_order_logged_by", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "missing_part_order_resolved_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("parts", "missing_part_order_resolved_by", "TEXT NOT NULL DEFAULT ''")
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_parts_source_submission ON parts(source_submission_id)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_parts_missing_part_order_followup
                ON parts(is_active, missing_part_order_followup)
                """
            )
            db._ensure_column("part_details", "installed_keys", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("qa_flags", "severity", "TEXT NOT NULL DEFAULT 'Medium'")
            db._ensure_column("qa_flags", "icon_path", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("qa_flags", "sort_order", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("client_parts", "followup_last_action", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("client_parts", "followup_last_action_at", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("client_parts", "followup_last_actor", "TEXT NOT NULL DEFAULT ''")
            db._ensure_column("client_parts", "followup_no_contact_count", "INTEGER NOT NULL DEFAULT 0")
            db._ensure_column("client_parts", "followup_stage_logged", "INTEGER NOT NULL DEFAULT -1")
            db._ensure_column("client_parts", "alert_quiet_until", "TEXT NOT NULL DEFAULT ''")
            cursor.execute(
                "UPDATE submissions "
                "SET entry_date=SUBSTR(COALESCE(created_at, ''), 1, 10) "
                "WHERE TRIM(COALESCE(entry_date, ''))=''"
            )
            cursor.execute(
                "UPDATE submissions "
                "SET updated_at=COALESCE(NULLIF(TRIM(updated_at), ''), created_at) "
                "WHERE TRIM(COALESCE(updated_at, ''))=''"
            )
            DepotSchema._collapse_duplicate_submissions(db, cursor)
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_entry_date_user_work_order
                ON submissions(entry_date, user_id, work_order)
                """
            )

    @staticmethod
    def _seed_default_role_definitions(cursor: sqlite3.Cursor) -> None:
        for sort_order, (role_name, role_slot) in enumerate(DepotRules.DEFAULT_ROLE_DEFINITIONS, start=1):
            cursor.execute(
                "INSERT OR IGNORE INTO role_definitions (role_name, role_slot, sort_order) VALUES (?, ?, ?)",
                (
                    str(role_name or "").strip(),
                    DepotRules.normalize_role_slot(role_slot, default=DepotRules.ROLE_SLOT_NONE),
                    int(sort_order),
                ),
            )

    @staticmethod
    def _collapse_duplicate_submissions(db: DepotDB, cursor: sqlite3.Cursor) -> None:
        duplicate_groups = cursor.execute(
            """
            SELECT entry_date, user_id, work_order, COUNT(*) AS duplicate_count
            FROM submissions
            WHERE TRIM(COALESCE(entry_date, '')) <> ''
            GROUP BY entry_date, user_id, work_order
            HAVING COUNT(*) > 1
            ORDER BY entry_date ASC, user_id ASC, work_order ASC
            """
        ).fetchall()
        if not duplicate_groups:
            return

        merged_groups = 0
        removed_rows = 0
        for group in duplicate_groups:
            entry_date = str(group["entry_date"] or "").strip()
            user_id = str(group["user_id"] or "").strip()
            work_order = str(group["work_order"] or "").strip()
            rows = cursor.execute(
                f"""
                SELECT
                    id,
                    COALESCE(created_at, '') AS created_at,
                    {_submission_latest_ts_sql()} AS latest_stamp,
                    COALESCE(touch, '') AS touch,
                    COALESCE(category, '') AS category,
                    COALESCE(client_unit, 0) AS client_unit,
                    COALESCE(serial_number, '') AS serial_number
                FROM submissions
                WHERE entry_date=? AND user_id=? AND work_order=?
                ORDER BY created_at ASC, id ASC
                """,
                (entry_date, user_id, work_order),
            ).fetchall()
            if len(rows) < 2:
                continue

            keep_id = int(rows[0]["id"])
            latest_row = max(
                rows,
                key=lambda row: (
                    str(row["latest_stamp"] or row["created_at"] or ""),
                    int(row["id"]),
                ),
            )
            latest_stamp = str(latest_row["latest_stamp"] or latest_row["created_at"] or "").strip()
            cursor.execute(
                "UPDATE submissions "
                "SET touch=?, category=?, client_unit=?, updated_at=?, serial_number=? "
                "WHERE id=?",
                (
                    str(latest_row["touch"] or "").strip(),
                    str(latest_row["category"] or "").strip(),
                    int(max(0, safe_int(latest_row["client_unit"], 0))),
                    latest_stamp,
                    str(latest_row["serial_number"] or "").strip().upper(),
                    keep_id,
                ),
            )

            duplicate_ids = [int(row["id"]) for row in rows[1:]]
            placeholders = ",".join("?" for _ in duplicate_ids)
            cursor.execute(
                f"UPDATE parts SET source_submission_id=? WHERE source_submission_id IN ({placeholders})",
                (keep_id, *duplicate_ids),
            )
            cursor.execute(f"DELETE FROM submissions WHERE id IN ({placeholders})", tuple(duplicate_ids))
            merged_groups += 1
            removed_rows += len(duplicate_ids)

        _runtime_log_event(
            "depot.db.submissions_same_day_duplicates_collapsed",
            severity="info",
            summary="Collapsed historical duplicate same-day submissions to the newest payload per user/work-order/day.",
            context={
                "db_path": str(db.db_path),
                "merged_groups": int(merged_groups),
                "removed_rows": int(removed_rows),
            },
        )

    @staticmethod
    def run_backfills(db: DepotDB) -> None:
        with db.write_transaction("schema.backfill"):
            cursor = db.conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE parts
                    SET source_submission_id=COALESCE((
                        SELECT s.id
                        FROM submissions s
                        WHERE s.work_order=parts.work_order AND s.touch=?
                        ORDER BY COALESCE(NULLIF(TRIM(s.updated_at), ''), s.created_at) DESC, s.id DESC
                        LIMIT 1
                    ), 0)
                    WHERE COALESCE(source_submission_id, 0)=0
                    """,
                    (DepotRules.TOUCH_PART_ORDER,),
                )
                cursor.execute("UPDATE parts SET source_submission_id=0 WHERE source_submission_id IS NULL")
                cursor.execute(
                    """
                    UPDATE parts
                    SET assigned_user_id=COALESCE((
                        SELECT COALESCE(s.user_id, '')
                        FROM submissions s
                        WHERE s.id=parts.source_submission_id
                        LIMIT 1
                    ), '')
                    WHERE TRIM(COALESCE(assigned_user_id, ''))=''
                      AND COALESCE(source_submission_id, 0)<>0
                    """
                )
                cursor.execute(
                    "UPDATE parts SET qa_comment=COALESCE(comments, '') "
                    "WHERE (qa_comment IS NULL OR qa_comment='') AND comments IS NOT NULL AND TRIM(comments) <> ''"
                )
                cursor.execute("UPDATE parts SET missing_part_order_followup=0 WHERE missing_part_order_followup IS NULL")
                cursor.execute("UPDATE parts SET missing_part_order_logged_at='' WHERE missing_part_order_logged_at IS NULL")
                cursor.execute("UPDATE parts SET missing_part_order_logged_by='' WHERE missing_part_order_logged_by IS NULL")
                cursor.execute("UPDATE parts SET missing_part_order_resolved_at='' WHERE missing_part_order_resolved_at IS NULL")
                cursor.execute("UPDATE parts SET missing_part_order_resolved_by='' WHERE missing_part_order_resolved_by IS NULL")
                cursor.execute("UPDATE parts SET qa_comment='' WHERE qa_comment IS NULL")
                cursor.execute("UPDATE parts SET agent_comment='' WHERE agent_comment IS NULL")
                unresolved_source_row = cursor.execute(
                    "SELECT COUNT(*) AS c FROM parts WHERE COALESCE(source_submission_id, 0)=0"
                ).fetchone()
                unresolved_source_count = int(unresolved_source_row["c"] if unresolved_source_row is not None else 0)
                if unresolved_source_count > 0:
                    _runtime_log_event(
                        "depot.db.parts_source_submission_backfill_incomplete",
                        severity="warning",
                        summary="Parts migration could not link every row to a Part Order submission; leaving unresolved rows usable.",
                        context={"db_path": str(db.db_path), "unresolved_source_submission_count": unresolved_source_count},
                    )
            except Exception as exc:
                context = {"db_path": str(db.db_path)}
                _runtime_log_event(
                    "depot.db.backfill_migration_failed",
                    severity="critical",
                    summary="DB schema backfill failed while creating or upgrading tables.",
                    exc=exc,
                    context=context,
                )
                _escalate_runtime_issue_once(
                    "depot.db.backfill_migration_failed",
                    "Flowgrid database migration encountered an error. Some historical fields may be incomplete.",
                    details=f"{type(exc).__name__}: {exc}",
                    context=context,
                )

class DepotTracker:
    DASHBOARD_NOTE_TARGET_SPECS: dict[str, dict[str, Any]] = {
        "submissions.rows": {
            "label": "Submissions - Rows",
            "table": "submissions",
            "column": "",
            "order_by": f"{_submission_latest_ts_sql()} DESC, id DESC",
            "mode": "submission_rows",
            "sync_comments_with_column": False,
        },
        "parts.qa_comment": {
            "label": "Parts - QA Note",
            "table": "parts",
            "column": "qa_comment",
            "order_by": "created_at DESC, id DESC",
            "sync_comments_with_column": True,
        },
        "parts.agent_comment": {
            "label": "Parts - Agent Note",
            "table": "parts",
            "column": "agent_comment",
            "order_by": "created_at DESC, id DESC",
            "sync_comments_with_column": False,
        },
        "client_parts.comments": {
            "label": "Client Parts - Comment",
            "table": "client_parts",
            "column": "comments",
            "order_by": "created_at DESC, id DESC",
            "sync_comments_with_column": False,
        },
        "rtvs.comments": {
            "label": "RTVs - Comment",
            "table": "rtvs",
            "column": "comments",
            "order_by": "created_at DESC, id DESC",
            "sync_comments_with_column": False,
        },
        "client_jo.comments": {
            "label": "Client JO - Comment",
            "table": "client_jo",
            "column": "comments",
            "order_by": "created_at DESC, id DESC",
            "sync_comments_with_column": False,
        },
    }

    def __init__(
        self,
        db: DepotDB,
        *,
        startup_repairs_enabled: bool = True,
        allow_metadata_repairs: bool = True,
    ):
        self.db = db
        self.startup_repairs_enabled = bool(startup_repairs_enabled) and not self.db.read_only
        self.allow_metadata_repairs = bool(allow_metadata_repairs) and not self.db.read_only
        self.user_repository: UserRepository | None = None
        self.permission_service: PermissionService | None = None
        if self.startup_repairs_enabled:
            self._ensure_default_qa_flags()
            self._repair_closed_workorder_queues()

    def _can_persist_metadata_repairs(self) -> bool:
        return bool(self.allow_metadata_repairs and not self.db.read_only)

    def _repair_closed_workorder_queues(self) -> None:
        try:
            close_statuses = (
                DepotRules.TOUCH_COMPLETE,
                DepotRules.TOUCH_JUNK,
                DepotRules.TOUCH_RTV,
            )
            # Do not auto-close active parts on startup based only on latest submission touch.
            # Reason: teams frequently reuse work-order IDs in testing/iteration cycles; applying
            # a historical "Complete/Junk Out/RTV" touch during app startup can incorrectly
            # deactivate newly queued QA parts and make lists appear empty after restart.
            #
            # Active-part closure remains enforced at submit time in submit_work().
            self.db.execute(
                "DELETE FROM client_parts "
                "WHERE COALESCE(("
                "SELECT s.touch FROM submissions s WHERE s.work_order=client_parts.work_order "
                f"ORDER BY {_submission_latest_ts_sql('s')} DESC, s.id DESC LIMIT 1"
                "), '') IN (?, ?, ?)",
                close_statuses,
            )
        except Exception as exc:
            _runtime_log_event(
                "depot.closed_queue_repair_failed",
                severity="warning",
                summary="Failed repairing closed work orders in active queues during startup.",
                exc=exc,
            )

    def dashboard_category_options(self) -> list[str]:
        rows = self.db.fetchall(
            "SELECT category FROM ("
            "SELECT TRIM(COALESCE(category, '')) AS category FROM submissions "
            "WHERE TRIM(COALESCE(category, '')) <> '' "
            "UNION "
            "SELECT TRIM(COALESCE(category, '')) AS category FROM parts "
            "WHERE TRIM(COALESCE(category, '')) <> ''"
            ") ORDER BY category COLLATE NOCASE ASC"
        )
        categories: list[str] = list(DepotRules.CATEGORY_OPTIONS)
        seen = {str(category).strip().casefold() for category in categories if str(category).strip()}
        for row in rows:
            category_text = str(row["category"] or "").strip()
            if not category_text:
                continue
            key = category_text.casefold()
            if key in seen:
                continue
            seen.add(key)
            categories.append(category_text)
        return categories

    def dashboard_note_target_options(self) -> list[tuple[str, str]]:
        return [
            (key, str(spec.get("label", key)))
            for key, spec in self.DASHBOARD_NOTE_TARGET_SPECS.items()
        ]

    def _dashboard_note_target_spec(self, target_key: str) -> dict[str, Any]:
        normalized_key = str(target_key or "").strip()
        spec = self.DASHBOARD_NOTE_TARGET_SPECS.get(normalized_key)
        if spec is None:
            raise ValueError("Invalid dashboard note target.")
        return spec

    def _dashboard_resolved_category_expr(self, work_order_expr: str) -> str:
        return (
            "COALESCE("
            "NULLIF(TRIM(("
            "SELECT ds.category FROM submissions ds "
            f"WHERE ds.work_order={work_order_expr} AND TRIM(COALESCE(ds.category, '')) <> '' "
            f"ORDER BY {_submission_latest_ts_sql('ds')} DESC, ds.id DESC LIMIT 1"
            ")), ''), "
            "NULLIF(TRIM(("
            "SELECT dp.category FROM parts dp "
            f"WHERE dp.work_order={work_order_expr} AND TRIM(COALESCE(dp.category, '')) <> '' "
            "ORDER BY dp.is_active DESC, dp.created_at DESC, dp.id DESC LIMIT 1"
            ")), ''), "
            "''"
            ")"
        )

    def _dashboard_parts_category_expr(self, work_order_expr: str, category_expr: str) -> str:
        return (
            "COALESCE("
            f"NULLIF(TRIM(COALESCE({category_expr}, '')), ''), "
            f"{self._dashboard_resolved_category_expr(work_order_expr)}"
            ")"
        )

    @staticmethod
    def _append_dashboard_category_filter(
        where_parts: list[str],
        params: list[Any],
        category_filter: str | None,
        category_expr: str,
    ) -> None:
        normalized_category = str(category_filter or "").strip()
        if not normalized_category:
            return
        where_parts.append(f"UPPER(TRIM(COALESCE({category_expr}, ''))) = ?")
        params.append(normalized_category.upper())

    def _build_submission_metrics_filter(
        self,
        *,
        alias: str = "s0",
        start_date: str | None = None,
        end_date: str | None = None,
        user_id: str | None = None,
        touch: str | None = None,
        client_only: bool | None = None,
        category: str | None = None,
    ) -> tuple[str, list[Any], str]:
        where: list[str] = []
        params: list[Any] = []
        entry_date_expr = _submission_entry_date_sql(alias)

        if start_date:
            where.append(f"{entry_date_expr} >= ?")
            params.append(str(start_date))
        if end_date:
            where.append(f"{entry_date_expr} <= ?")
            params.append(str(end_date))
        normalized_user = DepotRules.normalize_user_id(user_id or "")
        if normalized_user:
            where.append(f"{alias}.user_id = ?")
            params.append(normalized_user)
        normalized_touch = str(touch or "").strip()
        if normalized_touch:
            where.append(f"{alias}.touch = ?")
            params.append(normalized_touch)
        if client_only is not None:
            where.append(f"{alias}.client_unit = ?")
            params.append(1 if client_only else 0)
        self._append_dashboard_category_filter(
            where,
            params,
            category,
            self._dashboard_resolved_category_expr(f"{alias}.work_order"),
        )
        where_clause = "WHERE " + " AND ".join(where) if where else ""
        return where_clause, params, entry_date_expr

    @staticmethod
    def _touch_count_map(rows: list[sqlite3.Row]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            touch_name = str(row["touch"] or "").strip()
            if not touch_name:
                continue
            counts[touch_name] = int(max(0, safe_int(row["c"], 0)))
        return counts

    def _collect_submission_touch_mix_metrics(
        self,
        *,
        alias: str,
        where_clause: str,
        params: list[Any],
        include_latest_workload_mix: bool = False,
    ) -> dict[str, Any]:
        from_clause = f"FROM submissions {alias}"
        params_tuple = tuple(params)
        total_row = self.db.fetchone(f"SELECT COUNT(*) AS c {from_clause} {where_clause}", params_tuple)
        total_units_row = self.db.fetchone(
            f"SELECT COUNT(DISTINCT {alias}.work_order) AS c {from_clause} {where_clause}",
            params_tuple,
        )
        by_touch_rows = self.db.fetchall(
            f"SELECT {alias}.touch AS touch, COUNT(*) AS c {from_clause} {where_clause} GROUP BY {alias}.touch",
            params_tuple,
        )
        latest_by_touch_map: dict[str, int] = {}
        if include_latest_workload_mix:
            latest_by_touch_rows = self.db.fetchall(
                f"""
                WITH filtered_work_orders AS (
                    SELECT DISTINCT {alias}.work_order AS work_order
                    {from_clause}
                    {where_clause}
                ),
                latest_ranked AS (
                    SELECT
                        fwo.work_order AS work_order,
                        COALESCE(s1.touch, '') AS touch,
                        ROW_NUMBER() OVER (
                            PARTITION BY fwo.work_order
                            ORDER BY {_submission_latest_ts_sql('s1')} DESC, s1.id DESC
                        ) AS rn
                    FROM filtered_work_orders fwo
                    JOIN submissions s1 ON s1.work_order = fwo.work_order
                )
                SELECT touch, COUNT(*) AS c
                FROM latest_ranked
                WHERE rn = 1 AND TRIM(COALESCE(touch, '')) <> ''
                GROUP BY touch
                """,
                params_tuple,
            )
            latest_by_touch_map = self._touch_count_map(latest_by_touch_rows)
        return {
            "total_submissions": int(max(0, safe_int(total_row["c"], 0))) if total_row is not None else 0,
            "total_units": int(max(0, safe_int(total_units_row["c"], 0))) if total_units_row is not None else 0,
            "by_touch": self._touch_count_map(by_touch_rows),
            "latest_by_touch": latest_by_touch_map,
        }

    def get_touch_mix_metrics(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        user_id: str | None = None,
        touch: str | None = None,
        client_only: bool | None = None,
        category: str | None = None,
        include_latest_workload_mix: bool = False,
    ) -> dict[str, Any]:
        alias = "s0"
        where_clause, params, _entry_date_expr = self._build_submission_metrics_filter(
            alias=alias,
            start_date=start_date,
            end_date=end_date,
            user_id=user_id,
            touch=touch,
            client_only=client_only,
            category=category,
        )
        return self._collect_submission_touch_mix_metrics(
            alias=alias,
            where_clause=where_clause,
            params=params,
            include_latest_workload_mix=include_latest_workload_mix,
        )

    def fetch_dashboard_table_rows(
        self,
        table_name: str,
        *,
        limit: int = 300,
        start_date: str | None = None,
        end_date: str | None = None,
        user_id: str | None = None,
        category_filter: str | None = None,
    ) -> list[sqlite3.Row]:
        normalized_table = str(table_name or "").strip()
        allowed_tables = {name for name, _label in TRACKER_DASHBOARD_TABLES}
        if normalized_table not in allowed_tables:
            raise ValueError("Invalid dashboard table selection.")

        max_rows = int(clamp(safe_int(limit, 300), 1, 5000))
        params: list[Any] = []

        if normalized_table == "submissions":
            where_parts: list[str] = []
            entry_date_expr = _submission_entry_date_sql("s0")
            if start_date:
                where_parts.append(f"{entry_date_expr} >= ?")
                params.append(str(start_date))
            if end_date:
                where_parts.append(f"{entry_date_expr} <= ?")
                params.append(str(end_date))
            normalized_user = DepotRules.normalize_user_id(user_id or "")
            if normalized_user:
                where_parts.append("s0.user_id = ?")
                params.append(normalized_user)
            self._append_dashboard_category_filter(
                where_parts,
                params,
                category_filter,
                self._dashboard_resolved_category_expr("s0.work_order"),
            )
            where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
            query = (
                "SELECT s0.id, s0.created_at, s0.user_id, s0.work_order, s0.touch, s0.client_unit, s0.entry_date, "
                "CASE "
                "WHEN s0.touch='Part Order' THEN "
                "SUM(CASE WHEN s0.touch='Part Order' THEN 1 ELSE 0 END) OVER ("
                f"PARTITION BY s0.user_id, s0.work_order ORDER BY {_submission_latest_ts_sql('s0')}, s0.id "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                ") "
                "ELSE 0 "
                "END AS part_order_count "
                f"FROM submissions s0{where_clause} ORDER BY {_submission_latest_ts_sql('s0')} DESC, s0.id DESC LIMIT ?"
            )
            params.append(max_rows)
            return self.db.fetchall(query, tuple(params))

        where_parts = []
        if normalized_table == "parts":
            category_expr = self._dashboard_parts_category_expr("t.work_order", "t.category")
        else:
            category_expr = self._dashboard_resolved_category_expr("t.work_order")
        self._append_dashboard_category_filter(where_parts, params, category_filter, category_expr)
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        order_clause = " ORDER BY created_at DESC"
        query = f"SELECT t.* FROM {normalized_table} t{where_clause}{order_clause} LIMIT ?"
        params.append(max_rows)
        return self.db.fetchall(query, tuple(params))

    def fetch_dashboard_note_rows(
        self,
        target_key: str,
        *,
        limit: int = 200,
        work_order_filter: str | None = None,
        category_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        spec = self._dashboard_note_target_spec(target_key)
        if str(spec.get("mode", "") or "").strip() == "submission_rows":
            return self.fetch_dashboard_submission_rows(
                limit=limit,
                work_order_filter=work_order_filter,
                category_filter=category_filter,
            )
        table_name = str(spec.get("table", "")).strip()
        column_name = str(spec.get("column", "")).strip()
        order_by = str(spec.get("order_by", "id DESC")).strip() or "id DESC"
        if not table_name or not column_name:
            raise ValueError("Dashboard note target configuration is incomplete.")

        where_parts: list[str] = []
        params: list[Any] = []
        normalized_work_order = DepotRules.normalize_work_order(str(work_order_filter or ""))
        if normalized_work_order:
            where_parts.append("UPPER(COALESCE(t.work_order, '')) LIKE ?")
            params.append(f"%{normalized_work_order}%")
        if table_name == "parts":
            category_expr = self._dashboard_parts_category_expr("t.work_order", "t.category")
        else:
            category_expr = self._dashboard_resolved_category_expr("t.work_order")
        self._append_dashboard_category_filter(where_parts, params, category_filter, category_expr)
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

        max_rows = int(clamp(safe_int(limit, 200), 1, 5000))
        query = (
            "SELECT t.id, COALESCE(t.created_at, '') AS created_at, COALESCE(t.user_id, '') AS user_id, "
            "COALESCE(t.work_order, '') AS work_order, COALESCE("
            f"{column_name}, '') AS note_text "
            f"FROM {table_name} t{where_clause} ORDER BY {order_by} LIMIT ?"
        )
        params.append(max_rows)
        rows = self.db.fetchall(query, tuple(params))
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "id": int(max(0, safe_int(row["id"], 0))),
                    "created_at": str(row["created_at"] or "").strip(),
                    "user_id": str(row["user_id"] or "").strip(),
                    "work_order": str(row["work_order"] or "").strip(),
                    "note_text": str(row["note_text"] or "").strip(),
                }
            )
        return result

    def fetch_dashboard_submission_rows(
        self,
        *,
        limit: int = 200,
        work_order_filter: str | None = None,
        category_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        where_parts: list[str] = []
        params: list[Any] = []
        normalized_work_order = DepotRules.normalize_work_order(str(work_order_filter or ""))
        if normalized_work_order:
            where_parts.append("UPPER(COALESCE(s0.work_order, '')) LIKE ?")
            params.append(f"%{normalized_work_order}%")
        self._append_dashboard_category_filter(
            where_parts,
            params,
            category_filter,
            self._dashboard_resolved_category_expr("s0.work_order"),
        )
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        max_rows = int(clamp(safe_int(limit, 200), 1, 5000))
        params.append(max_rows)
        rows = self.db.fetchall(
            "SELECT s0.id, COALESCE(s0.created_at, '') AS created_at, COALESCE(s0.updated_at, '') AS updated_at, "
            "COALESCE(s0.user_id, '') AS user_id, COALESCE(s0.work_order, '') AS work_order, "
            "COALESCE(s0.touch, '') AS touch, COALESCE(s0.category, '') AS category, "
            "COALESCE(s0.client_unit, 0) AS client_unit, COALESCE(s0.entry_date, '') AS entry_date "
            f"FROM submissions s0{where_clause} ORDER BY {_submission_latest_ts_sql('s0')} DESC, s0.id DESC LIMIT ?",
            tuple(params),
        )
        return [
            {
                "id": int(max(0, safe_int(row["id"], 0))),
                "created_at": str(row["created_at"] or "").strip(),
                "updated_at": str(row["updated_at"] or "").strip(),
                "user_id": DepotRules.normalize_user_id(str(row["user_id"] or "")),
                "work_order": DepotRules.normalize_work_order(str(row["work_order"] or "")),
                "touch": str(row["touch"] or "").strip(),
                "category": str(row["category"] or "").strip(),
                "client_unit": int(max(0, safe_int(row["client_unit"], 0))),
                "entry_date": str(row["entry_date"] or "").strip(),
            }
            for row in rows
        ]

    def update_dashboard_note_value(self, target_key: str, row_id: int, note_text: str) -> None:
        spec = self._dashboard_note_target_spec(target_key)
        if str(spec.get("mode", "") or "").strip() == "submission_rows":
            raise ValueError("Submission rows use the table row editor.")
        table_name = str(spec.get("table", "")).strip()
        column_name = str(spec.get("column", "")).strip()
        if not table_name or not column_name:
            raise ValueError("Dashboard note target configuration is incomplete.")

        normalized_row_id = int(row_id)
        existing = self.db.fetchone(f"SELECT id FROM {table_name} WHERE id=? LIMIT 1", (normalized_row_id,))
        if existing is None:
            raise ValueError("Selected row no longer exists.")

        normalized_note = str(note_text or "").strip()
        if bool(spec.get("sync_comments_with_column", False)) and column_name != "comments":
            self.db.execute(
                f"UPDATE {table_name} SET {column_name}=?, comments=? WHERE id=?",
                (normalized_note, normalized_note, normalized_row_id),
            )
            return
        self.db.execute(
            f"UPDATE {table_name} SET {column_name}=? WHERE id=?",
            (normalized_note, normalized_row_id),
        )

    def _require_dashboard_submission_admin(self, actor_user_id: str) -> str:
        actor = DepotRules.normalize_user_id(actor_user_id)
        if not actor:
            raise PermissionDeniedError("A valid administrator is required.")
        if self.permission_service is not None:
            self.permission_service.require_admin_access(actor)
        elif not self.is_admin_user(actor):
            raise PermissionDeniedError(PermissionService.ADMIN_ACCESS_DENIED_MESSAGE)
        return actor

    def update_dashboard_submission_row(self, submission_id: int, values: dict[str, Any], actor_user_id: str) -> dict[str, Any]:
        actor = self._require_dashboard_submission_admin(actor_user_id)
        target_id = int(max(0, safe_int(submission_id, 0)))
        if target_id <= 0:
            raise ValueError("A valid submission row is required.")
        existing = self.get_submission_record(target_id)
        if existing is None:
            raise ValueError("Selected submission row no longer exists.")

        linked_parts_count_row = self.db.fetchone(
            "SELECT COUNT(*) AS c FROM parts WHERE COALESCE(source_submission_id, 0)=?",
            (target_id,),
        )
        linked_parts_count = int(max(0, safe_int(linked_parts_count_row["c"], 0))) if linked_parts_count_row is not None else 0
        old_work_order = DepotRules.normalize_work_order(str(existing.get("work_order", "") or ""))
        old_touch = str(existing.get("touch", "") or "").strip()

        created_at = str(values.get("created_at", existing.get("created_at", "")) or "").strip()
        updated_at = str(values.get("updated_at", "") or "").strip() or datetime.now().isoformat(timespec="seconds")
        user_id = DepotRules.normalize_user_id(str(values.get("user_id", existing.get("user_id", "")) or ""))
        work_order = DepotRules.normalize_work_order(str(values.get("work_order", existing.get("work_order", "")) or ""))
        touch = str(values.get("touch", existing.get("touch", "")) or "").strip()
        category = str(values.get("category", existing.get("category", "")) or "").strip()
        client_unit = 1 if str(values.get("client_unit", existing.get("client_unit", 0)) or "").strip().lower() in {"1", "true", "yes", "y", "on"} else 0
        entry_date = str(values.get("entry_date", existing.get("entry_date", "")) or "").strip()
        if not entry_date and len(created_at) >= 10:
            entry_date = created_at[:10]

        if not created_at:
            raise ValueError("created_at is required.")
        if not user_id:
            raise ValueError("user_id is required.")
        if not work_order:
            raise ValueError("work_order is required.")
        if not touch:
            raise ValueError("touch is required.")
        if not entry_date:
            raise ValueError("entry_date is required.")
        if linked_parts_count > 0 and old_touch == DepotRules.TOUCH_PART_ORDER and touch != DepotRules.TOUCH_PART_ORDER:
            raise ValueError("This Part Order submission is linked to parts; keep touch as Part Order or relink/delete parts first.")
        if linked_parts_count > 0 and old_work_order != work_order:
            raise ValueError("This submission is linked to parts; keep the work order unchanged or relink/delete parts first.")

        with self.db.write_transaction("tracker.update_dashboard_submission_row"):
            self.db.execute(
                "UPDATE submissions SET created_at=?, updated_at=?, user_id=?, work_order=?, touch=?, category=?, client_unit=?, entry_date=? "
                "WHERE id=?",
                (created_at, updated_at, user_id, work_order, touch, category, int(client_unit), entry_date, target_id),
            )
            if linked_parts_count > 0:
                self.db.execute(
                    "UPDATE parts SET assigned_user_id=? WHERE COALESCE(source_submission_id, 0)=?",
                    (user_id, target_id),
                )

        _runtime_log_event(
            "depot.dashboard_submission_updated",
            severity="info",
            summary="A dashboard submission row was edited.",
            context={
                "submission_id": int(target_id),
                "actor_user_id": actor,
                "work_order": work_order,
                "touch": touch,
                "linked_parts_count": int(linked_parts_count),
            },
        )
        return self.get_submission_record(target_id) or {"id": target_id}

    def delete_dashboard_submission(self, submission_id: int, actor_user_id: str) -> dict[str, Any]:
        actor = self._require_dashboard_submission_admin(actor_user_id)
        target_id = int(max(0, safe_int(submission_id, 0)))
        if target_id <= 0:
            raise ValueError("A valid submission row is required.")
        submission = self.get_submission_record(target_id)
        if submission is None:
            raise ValueError("Selected submission row no longer exists.")

        work_order = DepotRules.normalize_work_order(str(submission.get("work_order", "") or ""))
        touch = str(submission.get("touch", "") or "").strip()
        client_unit = bool(submission.get("client_unit", False))
        linked_parts_count_row = self.db.fetchone(
            "SELECT COUNT(*) AS c FROM parts WHERE COALESCE(source_submission_id, 0)=?",
            (target_id,),
        )
        linked_parts_count = int(max(0, safe_int(linked_parts_count_row["c"], 0))) if linked_parts_count_row is not None else 0
        fallback_part_order = None
        if touch == DepotRules.TOUCH_PART_ORDER and linked_parts_count > 0:
            fallback_part_order = self.db.fetchone(
                "SELECT id, COALESCE(user_id, '') AS user_id "
                "FROM submissions WHERE work_order=? AND touch=? AND id<>? "
                f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                (work_order, DepotRules.TOUCH_PART_ORDER, target_id),
            )
            if fallback_part_order is None:
                raise ValueError(
                    "This Part Order is linked to live parts and cannot be deleted because there is no earlier Part Order to relink."
                )

        with self.db.write_transaction("tracker.delete_dashboard_submission"):
            self._delete_submission_row_and_aux_logs(submission)
            if touch == DepotRules.TOUCH_PART_ORDER and linked_parts_count > 0 and fallback_part_order is not None:
                fallback_submission_id = int(max(0, safe_int(fallback_part_order["id"], 0)))
                fallback_user_id = DepotRules.normalize_user_id(str(fallback_part_order["user_id"] or ""))
                self.db.execute(
                    "UPDATE parts SET source_submission_id=?, assigned_user_id=? WHERE COALESCE(source_submission_id, 0)=?",
                    (fallback_submission_id, fallback_user_id, target_id),
                )

            if client_unit and touch in DepotRules.FOLLOW_UP_TOUCHES:
                fallback_client_submission = self.db.fetchone(
                    "SELECT COALESCE(user_id, '') AS user_id, "
                    f"{_submission_latest_ts_sql()} AS latest_stamp "
                    "FROM submissions "
                    "WHERE work_order=? AND COALESCE(client_unit, 0)=1 AND touch IN (?, ?) "
                    f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                    (work_order, DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER),
                )
                if fallback_client_submission is None:
                    self.db.execute("DELETE FROM client_parts WHERE work_order=?", (work_order,))
                else:
                    fallback_user = DepotRules.normalize_user_id(str(fallback_client_submission["user_id"] or ""))
                    fallback_stamp = str(fallback_client_submission["latest_stamp"] or "").strip()
                    existing_client_row = self.db.fetchone("SELECT id FROM client_parts WHERE work_order=?", (work_order,))
                    if existing_client_row is None:
                        self.db.execute(
                            "INSERT INTO client_parts (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
                            (fallback_stamp, fallback_user, work_order, ""),
                        )
                    else:
                        self.db.execute(
                            "UPDATE client_parts SET user_id=?, created_at=? WHERE work_order=?",
                            (fallback_user, fallback_stamp, work_order),
                        )

        _runtime_log_event(
            "depot.dashboard_submission_deleted",
            severity="warning",
            summary="A dashboard submission row was deleted by an administrator.",
            context={
                "submission_id": int(target_id),
                "actor_user_id": actor,
                "work_order": work_order,
                "touch": touch,
                "client_unit": bool(client_unit),
                "linked_parts_count": int(linked_parts_count),
            },
        )
        return {"submission_id": int(target_id), "work_order": work_order, "touch": touch, "client_unit": bool(client_unit)}

    def get_admin_access_level(self, user_id: str) -> str:
        snapshot = self._user_repository_or_fallback().get_role_snapshot(user_id)
        if not snapshot.user_id:
            return DepotRules.ADMIN_ACCESS_NONE
        return DepotRules.normalize_admin_access_level(
            snapshot.access_level,
            default=DepotRules.ADMIN_ACCESS_NONE,
        )

    def _user_repository_or_fallback(self) -> UserRepository:
        repository = self.user_repository
        if repository is not None:
            return repository
        return UserRepository(self, DepotRules)

    def is_admin_user(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().is_admin_user(user_id))

    def can_open_agent_window(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().can_open_agent_window(user_id))

    def can_open_qa_window(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().get_role_snapshot(user_id).can_open_qa_window)

    def can_access_hidden_tabs(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().can_access_hidden_tabs(user_id))

    def can_access_dashboard(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().can_access_dashboard(user_id))

    def get_agent_tier(self, user_id: str, default: int = 1) -> int:
        repository = self._user_repository_or_fallback()
        return int(repository.get_agent_tier(user_id, default=default))

    def can_access_missing_po_followups(self, user_id: str) -> bool:
        return bool(self._user_repository_or_fallback().can_access_missing_po_followups(user_id))

    def _asset_subdir(self, folder_name: str) -> Path:
        folder = str(folder_name or "").strip()
        if not folder:
            return self.db.db_path.parent / ASSETS_DIR_NAME
        path = self.db.db_path.parent / ASSETS_DIR_NAME / folder
        if self._can_persist_metadata_repairs():
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _resolve_stored_asset_path(self, stored_path: str, folder_name: str, *, allow_external_absolute: bool = False) -> Path | None:
        raw = str(stored_path or "").strip()
        if not raw:
            return None
        data_root = self.db.db_path.parent
        folder = str(folder_name or "").strip()
        asset_dir = self._asset_subdir(folder)
        raw_norm = raw.replace("\\", "/").lstrip("./")
        path_obj = Path(raw_norm)

        candidates: list[Path] = []
        if path_obj.is_absolute():
            resolved_abs = path_obj.resolve()
            if allow_external_absolute:
                candidates.append(resolved_abs)
            else:
                try:
                    candidates.append(data_root / resolved_abs.relative_to(data_root))
                except Exception:
                    pass
            candidates.append(asset_dir / path_obj.name)
        else:
            candidates.append(data_root / path_obj)
            candidates.append(asset_dir / path_obj.name)
            parts = [p for p in path_obj.parts if p]
            if parts:
                if parts[0].lower() == folder.lower():
                    tail = Path(*parts[1:]) if len(parts) > 1 else Path(path_obj.name)
                    candidates.append(asset_dir / tail)
                elif parts[0].lower() == ASSETS_DIR_NAME.lower() and len(parts) > 1 and parts[1].lower() == folder.lower():
                    tail = Path(*parts[2:]) if len(parts) > 2 else Path(path_obj.name)
                    candidates.append(asset_dir / tail)

        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate).replace("\\", "/").lower()
            if key in seen:
                continue
            seen.add(key)
            if candidate.exists() and candidate.is_file():
                return candidate
        return None

    def _admin_icons_dir(self) -> Path:
        return self._asset_subdir(ASSET_ADMIN_ICON_DIR_NAME)

    def _stored_admin_icon_to_abs_path(self, stored_path: str) -> Path | None:
        return self._resolve_stored_asset_path(stored_path, ASSET_ADMIN_ICON_DIR_NAME)

    def _find_icon_for_admin_user(self, user_id: str) -> Path | None:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return None
        icon_dir = self._admin_icons_dir()
        candidates = [p for p in icon_dir.glob(f"{normalized}.*") if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.suffix.lower())
        return candidates[0]

    def _relative_admin_icon_store_path(self, abs_path: Path) -> str:
        try:
            rel = abs_path.relative_to(self.db.db_path.parent)
            return str(rel).replace("\\", "/")
        except Exception as exc:
            if not isinstance(exc, ValueError):
                _runtime_log_event(
                    "depot.admin_icon_relative_path_failed",
                    severity="warning",
                    summary="Failed converting admin icon path to relative path; storing absolute path.",
                    exc=exc,
                    context={"abs_path": str(abs_path), "data_root": str(self.db.db_path.parent)},
                )
            return str(abs_path)

    def _store_admin_icon(self, user_id: str, icon_path: str, existing_stored_path: str = "") -> str:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return ""
        requested = str(icon_path or "").strip()
        icon_dir = self._admin_icons_dir()

        if not requested:
            for existing in icon_dir.glob(f"{normalized}.*"):
                try:
                    if existing.is_file():
                        existing.unlink()
                except Exception as exc:
                    _runtime_log_event(
                        "depot.admin_icon_cleanup_failed",
                        severity="warning",
                        summary="Failed deleting existing admin icon during clear operation.",
                        exc=exc,
                        context={"user_id": normalized, "path": str(existing)},
                    )
            return ""

        source = Path(requested)
        if not source.is_absolute():
            source = (self.db.db_path.parent / source).resolve()
        if not source.exists() or not source.is_file():
            existing_abs = self._stored_admin_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_admin_icon_store_path(existing_abs)
            fallback = self._find_icon_for_admin_user(normalized)
            return self._relative_admin_icon_store_path(fallback) if fallback is not None else ""

        suffix = source.suffix.lower() or ".img"
        target = icon_dir / f"{normalized}{suffix}"
        for existing in icon_dir.glob(f"{normalized}.*"):
            if existing.resolve() == target.resolve():
                continue
            try:
                if existing.is_file():
                    existing.unlink()
            except Exception as exc:
                _runtime_log_event(
                    "depot.admin_icon_replace_cleanup_failed",
                    severity="warning",
                    summary="Failed deleting stale admin icon during replacement.",
                    exc=exc,
                    context={"user_id": normalized, "path": str(existing), "target_path": str(target)},
                )
        try:
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            elif not target.exists():
                shutil.copy2(source, target)
        except Exception as exc:
            _runtime_log_event(
                "depot.admin_icon_copy_failed",
                severity="warning",
                summary="Failed copying admin icon; keeping previous icon path when possible.",
                exc=exc,
                context={"user_id": normalized, "source_path": str(source), "target_path": str(target)},
            )
            existing_abs = self._stored_admin_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_admin_icon_store_path(existing_abs)
            return ""
        return self._relative_admin_icon_store_path(target)

    def list_role_definitions(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT role_name, role_slot, sort_order FROM role_definitions "
            "ORDER BY sort_order ASC, role_name COLLATE NOCASE ASC"
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            role_name = str(row["role_name"] or "").strip()
            if not role_name:
                continue
            result.append(
                {
                    "role_name": role_name,
                    "role_slot": DepotRules.normalize_role_slot(
                        row["role_slot"],
                        default=DepotRules.ROLE_SLOT_NONE,
                    ),
                    "sort_order": int(row["sort_order"] or 0),
                }
            )
        return result

    def _role_definition_maps(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        by_name: dict[str, dict[str, Any]] = {}
        preferred_by_slot: dict[str, dict[str, Any]] = {}
        for row in self.list_role_definitions():
            role_name = str(row.get("role_name", "") or "").strip()
            role_slot = DepotRules.normalize_role_slot(
                row.get("role_slot", ""),
                default=DepotRules.ROLE_SLOT_NONE,
            )
            if not role_name:
                continue
            entry = {
                "role_name": role_name,
                "role_slot": role_slot,
                "sort_order": int(row.get("sort_order", 0) or 0),
            }
            by_name[role_name.casefold()] = entry
            preferred_by_slot.setdefault(role_slot, entry)
        return by_name, preferred_by_slot

    def _resolve_role_assignment(self, stored_role_name: str, tech_tier: int) -> tuple[str, str]:
        normalized_role_name = str(stored_role_name or "").strip()
        by_name, preferred_by_slot = self._role_definition_maps()
        if normalized_role_name:
            matched = by_name.get(normalized_role_name.casefold())
            if matched is not None:
                return str(matched.get("role_name", "") or ""), str(matched.get("role_slot", "") or "")
        if int(tech_tier) > 0:
            role_slot = DepotRules.role_slot_from_agent_tier(tech_tier, default=DepotRules.ROLE_SLOT_NONE)
            fallback = preferred_by_slot.get(role_slot)
            if fallback is not None:
                return str(fallback.get("role_name", "") or ""), str(fallback.get("role_slot", "") or "")
        return "", DepotRules.ROLE_SLOT_NONE

    def list_admin_users(self) -> list[dict[str, Any]]:
        if self.user_repository is not None:
            return self.user_repository.list_admin_users()
        rows = self.db.fetchall(
            "SELECT user_id, admin_name, position, location, icon_path, COALESCE(access_level, '') AS access_level "
            "FROM admin_users ORDER BY user_id ASC"
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            user_id = DepotRules.normalize_user_id(str(row["user_id"] or ""))
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self._stored_admin_icon_to_abs_path(stored_icon)
            if abs_icon is None:
                fallback = self._find_icon_for_admin_user(user_id)
                if fallback is not None:
                    fallback_stored = self._relative_admin_icon_store_path(fallback)
                    if fallback_stored != stored_icon and self._can_persist_metadata_repairs():
                        self.db.execute("UPDATE admin_users SET icon_path=? WHERE user_id=?", (fallback_stored, user_id))
                    abs_icon = fallback
            out.append(
                {
                    "user_id": user_id,
                    "admin_name": str(row["admin_name"] or "").strip(),
                    "position": str(row["position"] or "").strip(),
                    "role_name": str(row["position"] or "").strip(),
                    "location": str(row["location"] or "").strip(),
                    "access_level": DepotRules.normalize_admin_access_level(
                        row["access_level"],
                        default=DepotRules.ADMIN_ACCESS_NONE,
                    ),
                    "icon_path": str(abs_icon) if abs_icon is not None else "",
                }
            )
        return out

    def add_admin_user(
        self,
        user_id: str,
        admin_name: str = "",
        position: str = "",
        location: str = "",
        icon_path: str = "",
        *,
        access_level: str = "admin",
    ) -> str:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return ""
        normalized_name = str(admin_name or "").strip()
        normalized_position = str(position or "").strip()
        normalized_location = str(location or "").strip()
        normalized_icon = str(icon_path or "").strip()
        raw_access_level = str(access_level or "").strip()
        normalized_access_level = DepotRules.normalize_admin_access_level(
            access_level,
            default=DepotRules.ADMIN_ACCESS_NONE if not raw_access_level else DepotRules.ADMIN_ACCESS_ADMIN,
        )

        existing = self.db.fetchone("SELECT icon_path FROM admin_users WHERE user_id=?", (normalized,))
        existing_stored = str(existing["icon_path"] or "").strip() if existing is not None else ""
        stored_icon = self._store_admin_icon(normalized, normalized_icon, existing_stored)

        self.db.execute(
            "INSERT INTO admin_users (user_id, admin_name, position, location, icon_path, access_level) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "admin_name=excluded.admin_name, position=excluded.position, location=excluded.location, "
            "icon_path=excluded.icon_path, access_level=excluded.access_level",
            (
                normalized,
                normalized_name,
                normalized_position,
                normalized_location,
                stored_icon,
                normalized_access_level,
            ),
        )
        return stored_icon

    def upsert_role_definition(
        self,
        role_name: str,
        role_slot: str,
        original_role_name: str = "",
    ) -> dict[str, Any]:
        normalized_name = str(role_name or "").strip()
        if not normalized_name:
            raise ValueError("Role name is required.")
        normalized_slot = DepotRules.normalize_role_slot(
            role_slot,
            default=DepotRules.ROLE_SLOT_NONE,
        )
        original_name = str(original_role_name or "").strip()
        lookup_name = original_name or normalized_name
        existing_row = self.db.fetchone(
            "SELECT id, role_name, sort_order FROM role_definitions WHERE LOWER(TRIM(role_name))=LOWER(TRIM(?)) LIMIT 1",
            (lookup_name,),
        )
        conflicting_row = self.db.fetchone(
            "SELECT id, role_name, sort_order FROM role_definitions WHERE LOWER(TRIM(role_name))=LOWER(TRIM(?)) LIMIT 1",
            (normalized_name,),
        )
        if existing_row is not None and conflicting_row is not None and int(existing_row["id"] or 0) != int(conflicting_row["id"] or 0):
            raise ValueError("A role with that name already exists.")

        if existing_row is None and conflicting_row is not None:
            existing_row = conflicting_row

        if existing_row is None:
            row = self.db.fetchone("SELECT COALESCE(MAX(sort_order), 0) AS max_sort FROM role_definitions")
            sort_order = int(row["max_sort"] or 0) + 1 if row is not None else 1
            self.db.execute(
                "INSERT INTO role_definitions (role_name, role_slot, sort_order) VALUES (?, ?, ?)",
                (normalized_name, normalized_slot, int(sort_order)),
            )
        else:
            sort_order = int(existing_row["sort_order"] or 0)
            self.db.execute(
                "UPDATE role_definitions SET role_name=?, role_slot=? WHERE id=?",
                (normalized_name, normalized_slot, int(existing_row["id"] or 0)),
            )

        assigned_rows = self.db.fetchall(
            "SELECT user_id, admin_name, location, icon_path, COALESCE(access_level, '') AS access_level "
            "FROM admin_users WHERE LOWER(TRIM(position))=LOWER(TRIM(?)) ORDER BY user_id ASC",
            (lookup_name,),
        )
        for row in assigned_rows:
            user_id = DepotRules.normalize_user_id(str(row["user_id"] or ""))
            if not user_id:
                continue
            agent_row = self.db.fetchone(
                "SELECT agent_name, location, icon_path FROM agents WHERE user_id=? LIMIT 1",
                (user_id,),
            )
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self._stored_admin_icon_to_abs_path(stored_icon)
            if abs_icon is None:
                stored_agent_icon = str(agent_row["icon_path"] or "").strip() if agent_row is not None else ""
                abs_icon = self._stored_icon_to_abs_path(stored_agent_icon)
            if abs_icon is None:
                fallback = self._find_icon_for_admin_user(user_id)
                if fallback is not None:
                    abs_icon = fallback
            if abs_icon is None:
                fallback = self._find_icon_for_user(user_id)
                if fallback is not None:
                    abs_icon = fallback
            self.upsert_setup_user(
                user_id,
                str(row["admin_name"] or "").strip()
                or (str(agent_row["agent_name"] or "").strip() if agent_row is not None else "")
                or user_id,
                normalized_name,
                str(row["location"] or "").strip()
                or (str(agent_row["location"] or "").strip() if agent_row is not None else ""),
                DepotRules.normalize_admin_access_level(
                    row["access_level"],
                    default=DepotRules.ADMIN_ACCESS_NONE,
                ),
                str(abs_icon) if abs_icon is not None else "",
            )

        return {
            "role_name": normalized_name,
            "role_slot": normalized_slot,
            "sort_order": int(sort_order),
        }

    def delete_role_definition(self, role_name: str) -> None:
        normalized_name = str(role_name or "").strip()
        if not normalized_name:
            return
        assigned_row = self.db.fetchone(
            "SELECT user_id FROM admin_users WHERE LOWER(TRIM(position))=LOWER(TRIM(?)) LIMIT 1",
            (normalized_name,),
        )
        if assigned_row is not None:
            raise ValueError("That role is assigned to one or more users.")
        self.db.execute(
            "DELETE FROM role_definitions WHERE LOWER(TRIM(role_name))=LOWER(TRIM(?))",
            (normalized_name,),
        )

    def remove_admin_user(self, user_id: str) -> None:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return
        self.db.execute("DELETE FROM admin_users WHERE user_id=?", (normalized,))
        for existing in self._admin_icons_dir().glob(f"{normalized}.*"):
            try:
                if existing.is_file():
                    existing.unlink()
            except Exception as exc:
                _runtime_log_event(
                    "depot.admin_icon_delete_failed",
                    severity="warning",
                    summary="Failed deleting admin icon file during admin deletion.",
                    exc=exc,
                    context={"user_id": normalized, "path": str(existing)},
                )

    def list_setup_users(self) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for row in self.list_agents():
            user_id = DepotRules.normalize_user_id(str(row.get("user_id", "") or ""))
            if not user_id:
                continue
            entry = merged.setdefault(
                user_id,
                {
                    "user_id": user_id,
                    "name": "",
                    "role_name": "",
                    "role_slot": DepotRules.ROLE_SLOT_NONE,
                    "location": "",
                    "tech_tier": 0,
                    "access_level": DepotRules.ADMIN_ACCESS_NONE,
                    "icon_path": "",
                    "agent_name": "",
                    "admin_name": "",
                    "agent_icon_path": "",
                    "admin_icon_path": "",
                    "stored_role_name": "",
                },
            )
            entry["agent_name"] = str(row.get("agent_name", "") or "").strip()
            entry["name"] = entry["agent_name"] or str(entry.get("name", "") or "").strip()
            entry["location"] = str(row.get("location", "") or "").strip() or str(entry.get("location", "") or "").strip()
            entry["tech_tier"] = int(DepotRules.normalize_agent_tier(row.get("tier", 1)))
            entry["agent_icon_path"] = str(row.get("icon_path", "") or "").strip()
            entry["icon_path"] = entry["agent_icon_path"] or str(entry.get("icon_path", "") or "").strip()

        for row in self.list_admin_users():
            user_id = DepotRules.normalize_user_id(str(row.get("user_id", "") or ""))
            if not user_id:
                continue
            entry = merged.setdefault(
                user_id,
                {
                    "user_id": user_id,
                    "name": "",
                    "role_name": "",
                    "role_slot": DepotRules.ROLE_SLOT_NONE,
                    "location": "",
                    "tech_tier": 0,
                    "access_level": DepotRules.ADMIN_ACCESS_NONE,
                    "icon_path": "",
                    "agent_name": "",
                    "admin_name": "",
                    "agent_icon_path": "",
                    "admin_icon_path": "",
                    "stored_role_name": "",
                },
            )
            entry["admin_name"] = str(row.get("admin_name", "") or "").strip()
            if not str(entry.get("name", "") or "").strip():
                entry["name"] = entry["admin_name"]
            entry["stored_role_name"] = str(row.get("role_name", "") or row.get("position", "") or "").strip()
            if not str(entry.get("location", "") or "").strip():
                entry["location"] = str(row.get("location", "") or "").strip()
            entry["access_level"] = DepotRules.normalize_admin_access_level(
                row.get("access_level", ""),
                default=DepotRules.ADMIN_ACCESS_NONE,
            )
            entry["admin_icon_path"] = str(row.get("icon_path", "") or "").strip()
            if not str(entry.get("icon_path", "") or "").strip():
                entry["icon_path"] = entry["admin_icon_path"]

        repository = self.user_repository
        for user_id, entry in merged.items():
            if repository is not None:
                snapshot = repository.get_role_snapshot(user_id)
                entry["role_name"] = str(snapshot.role_name or "")
                entry["role_slot"] = str(snapshot.role_slot or DepotRules.ROLE_SLOT_NONE)
                entry["tech_tier"] = int(snapshot.agent_tier)
                entry["access_level"] = str(snapshot.access_level or "")
            else:
                role_name, role_slot = self._resolve_role_assignment(
                    str(entry.get("stored_role_name", "") or ""),
                    int(entry.get("tech_tier", 0) or 0),
                )
                entry["role_name"] = role_name
                entry["role_slot"] = role_slot
                entry["tech_tier"] = int(DepotRules.role_slot_to_agent_tier(role_slot, default=0))
            entry["name"] = str(entry.get("name", "") or "").strip() or user_id

        return sorted(
            merged.values(),
            key=lambda row: (
                str(row.get("user_id", "") or "").strip().casefold(),
            ),
        )

    def upsert_setup_user(
        self,
        user_id: str,
        name: str,
        role_name: str,
        location: str,
        access_level: str,
        icon_path: str = "",
    ) -> dict[str, Any]:
        normalized_user = DepotRules.normalize_user_id(user_id)
        normalized_name = str(name or "").strip()
        normalized_role_name = str(role_name or "").strip()
        normalized_location = str(location or "").strip()
        normalized_icon = str(icon_path or "").strip()
        normalized_access = DepotRules.normalize_admin_access_level(
            access_level,
            default=DepotRules.ADMIN_ACCESS_NONE,
        )
        role_row = self.db.fetchone(
            "SELECT role_name, role_slot FROM role_definitions WHERE LOWER(TRIM(role_name))=LOWER(TRIM(?)) LIMIT 1",
            (normalized_role_name,),
        )
        if role_row is None:
            raise ValueError("Selected role is no longer configured.")
        stored_role_name = str(role_row["role_name"] or "").strip()
        normalized_role_slot = DepotRules.normalize_role_slot(
            role_row["role_slot"],
            default=DepotRules.ROLE_SLOT_NONE,
        )
        normalized_tier = int(DepotRules.role_slot_to_agent_tier(normalized_role_slot, default=0))

        agent_icon = ""
        admin_icon = self.add_admin_user(
            normalized_user,
            normalized_name or normalized_user,
            stored_role_name,
            normalized_location,
            normalized_icon,
            access_level=normalized_access,
        )
        if normalized_tier > 0:
            agent_icon = self.upsert_agent(
                normalized_user,
                normalized_name or normalized_user,
                normalized_tier,
                normalized_icon,
                normalized_location,
            )
        else:
            self.delete_agent(normalized_user)
        merged_icon = str(agent_icon or admin_icon or normalized_icon or "").strip()
        return {
            "user_id": normalized_user,
            "name": normalized_name or normalized_user,
            "role_name": stored_role_name,
            "role_slot": normalized_role_slot,
            "location": normalized_location,
            "tech_tier": normalized_tier,
            "access_level": normalized_access,
            "icon_path": merged_icon,
        }

    def delete_setup_user(self, user_id: str) -> None:
        normalized_user = DepotRules.normalize_user_id(user_id)
        if not normalized_user:
            return
        self.remove_admin_user(normalized_user)
        self.delete_agent(normalized_user)

    @staticmethod
    def _normalize_flag_name(value: str) -> str:
        return str(value or "").strip()

    @staticmethod
    def _normalize_flag_severity(value: str) -> str:
        raw = str(value or "").strip().title()
        return raw if raw in QA_FLAG_SEVERITY_OPTIONS else "Medium"

    def _qa_flag_icons_dir(self) -> Path:
        return self._asset_subdir(ASSET_QA_FLAG_ICON_DIR_NAME)

    def _stored_qa_flag_icon_to_abs_path(self, stored_path: str) -> Path | None:
        return self._resolve_stored_asset_path(stored_path, ASSET_QA_FLAG_ICON_DIR_NAME)

    def _find_icon_for_qa_flag_id(self, flag_id: int) -> Path | None:
        icon_dir = self._qa_flag_icons_dir()
        candidates = [p for p in icon_dir.glob(f"flag_{int(flag_id)}.*") if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.suffix.lower())
        return candidates[0]

    def _relative_qa_flag_icon_store_path(self, abs_path: Path) -> str:
        try:
            rel = abs_path.relative_to(self.db.db_path.parent)
            return str(rel).replace("\\", "/")
        except Exception as exc:
            if not isinstance(exc, ValueError):
                _runtime_log_event(
                    "depot.qa_flag_relative_path_failed",
                    severity="warning",
                    summary="Failed to convert QA flag icon path to data-root-relative path; storing absolute path.",
                    exc=exc,
                    context={"abs_path": str(abs_path), "data_root": str(self.db.db_path.parent)},
                )
            return str(abs_path)

    def _store_qa_flag_icon(self, flag_id: int, icon_path: str, existing_stored_path: str = "") -> str:
        fid = int(flag_id)
        requested = str(icon_path or "").strip()
        icon_dir = self._qa_flag_icons_dir()

        if not requested:
            for existing in icon_dir.glob(f"flag_{fid}.*"):
                try:
                    if existing.is_file():
                        existing.unlink()
                except Exception as exc:
                    _runtime_log_event(
                        "depot.qa_flag_icon_cleanup_failed",
                        severity="warning",
                        summary="Failed deleting existing QA flag icon during clear operation.",
                        exc=exc,
                        context={"flag_id": fid, "path": str(existing)},
                    )
            return ""

        source = Path(requested)
        if not source.is_absolute():
            source = (self.db.db_path.parent / source).resolve()
        if not source.exists() or not source.is_file():
            existing_abs = self._stored_qa_flag_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_qa_flag_icon_store_path(existing_abs)
            fallback = self._find_icon_for_qa_flag_id(fid)
            return self._relative_qa_flag_icon_store_path(fallback) if fallback is not None else ""

        suffix = source.suffix.lower() or ".img"
        target = icon_dir / f"flag_{fid}{suffix}"
        for existing in icon_dir.glob(f"flag_{fid}.*"):
            if existing.resolve() == target.resolve():
                continue
            try:
                if existing.is_file():
                    existing.unlink()
            except Exception as exc:
                _runtime_log_event(
                    "depot.qa_flag_icon_replace_cleanup_failed",
                    severity="warning",
                    summary="Failed deleting stale QA flag icon while replacing icon.",
                    exc=exc,
                    context={"flag_id": fid, "path": str(existing), "target_path": str(target)},
                )
        try:
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            elif not target.exists():
                shutil.copy2(source, target)
        except Exception as exc:
            _runtime_log_event(
                "depot.qa_flag_icon_copy_failed",
                severity="warning",
                summary="Failed to copy QA flag icon; keeping previous icon path when possible.",
                exc=exc,
                context={"flag_id": fid, "source_path": str(source), "target_path": str(target)},
            )
            existing_abs = self._stored_qa_flag_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_qa_flag_icon_store_path(existing_abs)
            return ""
        return self._relative_qa_flag_icon_store_path(target)

    def _ensure_default_qa_flags(self) -> None:
        row = self.db.fetchone("SELECT COUNT(*) AS c FROM qa_flags")
        if row is not None and int(row["c"] or 0) > 0:
            return
        default_names = [name for name in QA_FLAG_OPTIONS if name and name.lower() != "none"]
        for idx, name in enumerate(default_names, start=1):
            severity = "High" if name in {"Escalation", "Safety"} else "Medium"
            self.db.execute(
                "INSERT OR IGNORE INTO qa_flags (flag_name, severity, icon_path, sort_order) VALUES (?, ?, '', ?)",
                (name, severity, idx),
            )

    def list_qa_flags(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT id, flag_name, severity, icon_path, sort_order "
            "FROM qa_flags ORDER BY sort_order ASC, flag_name ASC"
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            flag_id = int(row["id"])
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self._stored_qa_flag_icon_to_abs_path(stored_icon)
            if abs_icon is None:
                fallback = self._find_icon_for_qa_flag_id(flag_id)
                if fallback is not None:
                    fallback_stored = self._relative_qa_flag_icon_store_path(fallback)
                    if fallback_stored != stored_icon and self._can_persist_metadata_repairs():
                        self.db.execute("UPDATE qa_flags SET icon_path=? WHERE id=?", (fallback_stored, flag_id))
                    abs_icon = fallback
            out.append(
                {
                    "id": flag_id,
                    "flag_name": str(row["flag_name"] or "").strip(),
                    "severity": self._normalize_flag_severity(str(row["severity"] or "Medium")),
                    "icon_path": str(abs_icon) if abs_icon is not None else "",
                    "sort_order": int(row["sort_order"] or 0),
                }
            )
        return out

    def get_qa_flag_options(self, include_none: bool = True) -> list[str]:
        flags = [row["flag_name"] for row in self.list_qa_flags() if row.get("flag_name")]
        if include_none:
            return ["None", *flags]
        return flags

    def resolve_qa_flag_icon(self, qa_flag: str, legacy_part_image_path: str = "") -> str:
        flag_name = self._normalize_flag_name(qa_flag)
        if flag_name:
            row = self.db.fetchone(
                "SELECT icon_path FROM qa_flags WHERE flag_name=? LIMIT 1",
                (flag_name,),
            )
            if row is not None:
                stored = str(row["icon_path"] or "").strip()
                abs_icon = self._stored_qa_flag_icon_to_abs_path(stored)
                if abs_icon is not None:
                    return str(abs_icon)
        # Backward compatibility for old per-unit icons
        return self.resolve_part_flag_image_path(legacy_part_image_path)

    def upsert_qa_flag(self, flag_name: str, severity: str, icon_path: str = "") -> int:
        normalized_name = self._normalize_flag_name(flag_name)
        normalized_severity = self._normalize_flag_severity(severity)
        if not normalized_name:
            return 0
        with self.db.write_transaction("tracker.upsert_qa_flag"):
            existing = self.db.fetchone(
                "SELECT id, icon_path, sort_order FROM qa_flags WHERE flag_name=?",
                (normalized_name,),
            )
            if existing is None:
                order_row = self.db.fetchone("SELECT COALESCE(MAX(sort_order), 0) AS m FROM qa_flags")
                next_order = int(order_row["m"] or 0) + 1 if order_row is not None else 1
                cur = self.db.execute(
                    "INSERT INTO qa_flags (flag_name, severity, icon_path, sort_order) VALUES (?, ?, '', ?)",
                    (normalized_name, normalized_severity, next_order),
                )
                flag_id = int(cur.lastrowid or 0)
                existing_stored = ""
            else:
                flag_id = int(existing["id"] or 0)
                existing_stored = str(existing["icon_path"] or "").strip()
                self.db.execute(
                    "UPDATE qa_flags SET severity=? WHERE id=?",
                    (normalized_severity, flag_id),
                )
            if flag_id <= 0:
                return 0
            stored_icon = self._store_qa_flag_icon(flag_id, icon_path, existing_stored)
            self.db.execute(
                "UPDATE qa_flags SET flag_name=?, severity=?, icon_path=? WHERE id=?",
                (normalized_name, normalized_severity, stored_icon, flag_id),
            )
        return flag_id

    def delete_qa_flag(self, flag_name: str) -> None:
        normalized_name = self._normalize_flag_name(flag_name)
        if not normalized_name:
            return
        row = self.db.fetchone("SELECT id FROM qa_flags WHERE flag_name=?", (normalized_name,))
        if row is None:
            return
        flag_id = int(row["id"] or 0)
        self.db.execute("DELETE FROM qa_flags WHERE id=?", (flag_id,))
        if flag_id > 0:
            icon_dir = self._qa_flag_icons_dir()
            for existing in icon_dir.glob(f"flag_{flag_id}.*"):
                try:
                    if existing.is_file():
                        existing.unlink()
                except Exception as exc:
                    _runtime_log_event(
                        "depot.qa_flag_icon_delete_failed",
                        severity="warning",
                        summary="Failed deleting QA flag icon file during QA flag deletion.",
                        exc=exc,
                        context={"flag_id": flag_id, "path": str(existing)},
                    )

    def ensure_shared_editable_asset_dirs(self) -> None:
        if not self._can_persist_metadata_repairs():
            return
        for folder in (ASSET_AGENT_ICON_DIR_NAME, ASSET_ADMIN_ICON_DIR_NAME, ASSET_QA_FLAG_ICON_DIR_NAME):
            self._asset_subdir(folder)

    def shared_editable_icon_snapshot(self) -> tuple[tuple[str, str, int, int], ...]:
        snapshot: list[tuple[str, str, int, int]] = []
        data_root = self.db.db_path.parent
        for folder in (ASSET_AGENT_ICON_DIR_NAME, ASSET_ADMIN_ICON_DIR_NAME, ASSET_QA_FLAG_ICON_DIR_NAME):
            folder_path = self._asset_subdir(folder)
            try:
                files = sorted((path for path in folder_path.iterdir() if path.is_file()), key=lambda item: item.name.lower())
            except Exception as exc:
                _runtime_log_event(
                    "depot.editable_icon_snapshot_failed",
                    severity="warning",
                    summary="Failed scanning a shared editable icon folder during refresh.",
                    exc=exc,
                    context={"folder": folder, "folder_path": str(folder_path)},
                )
                continue
            for path in files:
                try:
                    stat = path.stat()
                    rel = str(path.relative_to(data_root)).replace("\\", "/")
                    snapshot.append((folder, rel, int(stat.st_mtime_ns), int(stat.st_size)))
                except Exception as exc:
                    _runtime_log_event(
                        "depot.editable_icon_file_snapshot_failed",
                        severity="warning",
                        summary="Failed reading a shared editable icon file state during refresh.",
                        exc=exc,
                        context={"folder": folder, "path": str(path)},
                    )
        snapshot.sort()
        return tuple(snapshot)

    def reconcile_shared_editable_icons(self) -> None:
        if not self._can_persist_metadata_repairs():
            return
        self.ensure_shared_editable_asset_dirs()
        try:
            self.list_agents()
        except Exception as exc:
            _runtime_log_event(
                "depot.agent_icon_reconcile_failed",
                severity="warning",
                summary="Failed reconciling shared agent icons.",
                exc=exc,
            )
        try:
            self.list_admin_users()
        except Exception as exc:
            _runtime_log_event(
                "depot.admin_icon_reconcile_failed",
                severity="warning",
                summary="Failed reconciling shared admin icons.",
                exc=exc,
            )
        try:
            self.list_qa_flags()
        except Exception as exc:
            _runtime_log_event(
                "depot.qa_flag_icon_reconcile_failed",
                severity="warning",
                summary="Failed reconciling shared QA flag icons.",
                exc=exc,
            )

    def _agent_icons_dir(self) -> Path:
        return self._asset_subdir(ASSET_AGENT_ICON_DIR_NAME)

    def _stored_icon_to_abs_path(self, stored_path: str) -> Path | None:
        return self._resolve_stored_asset_path(stored_path, ASSET_AGENT_ICON_DIR_NAME)

    def _find_icon_for_user(self, user_id: str) -> Path | None:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return None
        icon_dir = self._agent_icons_dir()
        candidates = [p for p in icon_dir.glob(f"{normalized}.*") if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.suffix.lower())
        return candidates[0]

    def _relative_icon_store_path(self, abs_path: Path) -> str:
        try:
            rel = abs_path.relative_to(self.db.db_path.parent)
            return str(rel).replace("\\", "/")
        except Exception as exc:
            if not isinstance(exc, ValueError):
                _runtime_log_event(
                    "depot.agent_icon_relative_path_failed",
                    severity="warning",
                    summary="Failed to convert agent icon path to data-root-relative path; storing absolute path.",
                    exc=exc,
                    context={"abs_path": str(abs_path), "data_root": str(self.db.db_path.parent)},
                )
            return str(abs_path)

    def _part_flag_images_dir(self) -> Path:
        return self._asset_subdir(ASSET_PART_FLAG_IMAGE_DIR_NAME)

    def _stored_part_flag_image_to_abs_path(self, stored_path: str) -> Path | None:
        return self._resolve_stored_asset_path(stored_path, ASSET_PART_FLAG_IMAGE_DIR_NAME, allow_external_absolute=True)

    def resolve_part_flag_image_path(self, stored_path: str) -> str:
        abs_path = self._stored_part_flag_image_to_abs_path(stored_path)
        return str(abs_path) if abs_path is not None else ""

    def _find_flag_image_for_part(self, part_id: int) -> Path | None:
        flag_dir = self._part_flag_images_dir()
        candidates = [p for p in flag_dir.glob(f"part_{int(part_id)}.*") if p.is_file()]
        if not candidates:
            return None
        candidates.sort(key=lambda p: p.suffix.lower())
        return candidates[0]

    def _relative_part_flag_image_store_path(self, abs_path: Path) -> str:
        try:
            rel = abs_path.relative_to(self.db.db_path.parent)
            return str(rel).replace("\\", "/")
        except Exception as exc:
            if not isinstance(exc, ValueError):
                _runtime_log_event(
                    "depot.part_flag_relative_path_failed",
                    severity="warning",
                    summary="Failed to convert part flag image path to data-root-relative path; storing absolute path.",
                    exc=exc,
                    context={"abs_path": str(abs_path), "data_root": str(self.db.db_path.parent)},
                )
            return str(abs_path)

    def _store_agent_icon(self, user_id: str, icon_path: str, existing_stored_path: str = "") -> str:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return ""

        requested = str(icon_path or "").strip()
        icon_dir = self._agent_icons_dir()

        # Empty icon path means explicit clear.
        if not requested:
            for existing in icon_dir.glob(f"{normalized}.*"):
                try:
                    if existing.is_file():
                        existing.unlink()
                except Exception as exc:
                    _runtime_log_event(
                        "depot.agent_icon_cleanup_failed",
                        severity="warning",
                        summary="Failed deleting existing agent icon during clear operation.",
                        exc=exc,
                        context={"user_id": normalized, "path": str(existing)},
                    )
            return ""

        source = Path(requested)
        if not source.is_absolute():
            source = (self.db.db_path.parent / source).resolve()

        if not source.exists() or not source.is_file():
            existing_abs = self._stored_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_icon_store_path(existing_abs)
            fallback = self._find_icon_for_user(normalized)
            return self._relative_icon_store_path(fallback) if fallback is not None else ""

        suffix = source.suffix.lower() or ".img"
        target = icon_dir / f"{normalized}{suffix}"

        for existing in icon_dir.glob(f"{normalized}.*"):
            if existing.resolve() == target.resolve():
                continue
            try:
                if existing.is_file():
                    existing.unlink()
            except Exception as exc:
                _runtime_log_event(
                    "depot.agent_icon_replace_cleanup_failed",
                    severity="warning",
                    summary="Failed deleting stale agent icon during replacement.",
                    exc=exc,
                    context={"user_id": normalized, "path": str(existing), "target_path": str(target)},
                )

        try:
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
            elif not target.exists():
                shutil.copy2(source, target)
        except Exception as exc:
            _runtime_log_event(
                "depot.agent_icon_copy_failed",
                severity="warning",
                summary="Failed copying agent icon; keeping previous icon path when possible.",
                exc=exc,
                context={"user_id": normalized, "source_path": str(source), "target_path": str(target)},
            )
            existing_abs = self._stored_icon_to_abs_path(existing_stored_path)
            if existing_abs is not None:
                return self._relative_icon_store_path(existing_abs)
            return ""

        return self._relative_icon_store_path(target)

    def list_agents(self, tier_filter: int | None = None) -> list[dict[str, Any]]:
        if self.user_repository is not None:
            return self.user_repository.list_agents(tier_filter=tier_filter)
        params: list[Any] = []
        where = ""
        if tier_filter in (1, 2, 3, 4):
            where = "WHERE tier=?"
            params.append(int(tier_filter))
        rows = self.db.fetchall(
            f"SELECT agent_name, user_id, tier, location, icon_path FROM agents {where} ORDER BY tier ASC, agent_name ASC, user_id ASC",
            tuple(params),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            user_id = DepotRules.normalize_user_id(row["user_id"])
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self._stored_icon_to_abs_path(stored_icon)

            if abs_icon is None:
                fallback = self._find_icon_for_user(user_id)
                if fallback is not None:
                    fallback_stored = self._relative_icon_store_path(fallback)
                    if fallback_stored != stored_icon and self._can_persist_metadata_repairs():
                        self.db.execute("UPDATE agents SET icon_path=? WHERE user_id=?", (fallback_stored, user_id))
                    abs_icon = fallback

            result.append(
                {
                    "agent_name": str(row["agent_name"] or ""),
                    "user_id": user_id,
                    "tier": DepotRules.normalize_agent_tier(row["tier"]),
                    "location": str(row["location"] or "").strip(),
                    "icon_path": str(abs_icon) if abs_icon is not None else "",
                }
            )
        return result

    def part_owner_choice_items(self, work_order: str = "") -> tuple[list[str], dict[str, str], int]:
        if self.user_repository is not None:
            return self.user_repository.part_owner_choice_items(work_order)
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        current_owner = ""
        if normalized_work_order:
            existing_row = self.db.fetchone(
                "SELECT COALESCE(assigned_user_id, '') AS assigned_user_id "
                "FROM parts WHERE is_active=1 AND work_order=? ORDER BY id DESC LIMIT 1",
                (normalized_work_order,),
            )
            if existing_row is not None:
                current_owner = DepotRules.normalize_user_id(str(existing_row["assigned_user_id"] or ""))

        items: list[str] = []
        item_lookup: dict[str, str] = {}
        current_index = 0
        for agent_row in self.list_agents():
            agent_user = DepotRules.normalize_user_id(str(agent_row.get("user_id", "") or ""))
            if not agent_user:
                continue
            agent_name = str(agent_row.get("agent_name", "") or "").strip()
            display_text = f"{agent_user} - {agent_name}" if agent_name else agent_user
            item_lookup[display_text] = agent_user
            items.append(display_text)
            if current_owner and agent_user == current_owner:
                current_index = len(items) - 1
        return items, item_lookup, current_index

    def agent_display_map(self) -> dict[str, tuple[str, str]]:
        if self.user_repository is not None:
            return self.user_repository.agent_display_map()
        agent_meta: dict[str, tuple[str, str]] = {}
        for agent_row in self.list_agents():
            agent_user = DepotRules.normalize_user_id(str(agent_row.get("user_id", "") or ""))
            if not agent_user:
                continue
            agent_meta[agent_user] = (
                str(agent_row.get("agent_name", "") or "").strip(),
                str(agent_row.get("icon_path", "") or "").strip(),
            )
        return agent_meta

    def get_part_note_context(self, part_id: int) -> dict[str, Any] | None:
        row = self.db.fetchone(
            "SELECT id, work_order, category, client_unit, COALESCE(qa_comment, '') AS qa_comment, "
            "COALESCE(agent_comment, '') AS agent_comment, COALESCE(qa_flag, '') AS qa_flag, "
            "COALESCE(qa_flag_image_path, '') AS qa_flag_image_path, COALESCE(comments, '') AS comments, "
            "COALESCE(working_user_id, '') AS working_user_id, COALESCE(working_updated_at, '') AS working_updated_at "
            "FROM parts WHERE id=?",
            (int(part_id),),
        )
        if row is None:
            return None
        qa_comment = str(row["qa_comment"] or row["comments"] or "").strip()
        image_path = self.resolve_qa_flag_icon(
            str(row["qa_flag"] or "").strip(),
            str(row["qa_flag_image_path"] or ""),
        )
        resolved_category = self.resolve_work_order_category(
            str(row["work_order"] or ""),
            str(row["category"] or "").strip(),
        )
        return {
            "id": int(row["id"]),
            "work_order": str(row["work_order"] or ""),
            "category": resolved_category,
            "client_unit": bool(int(row["client_unit"] or 0)),
            "qa_comment": qa_comment,
            "agent_comment": str(row["agent_comment"] or "").strip(),
            "qa_flag": str(row["qa_flag"] or "").strip(),
            "qa_flag_image_path": image_path,
            "working_user_id": DepotRules.normalize_user_id(str(row["working_user_id"] or "")),
            "working_updated_at": str(row["working_updated_at"] or "").strip(),
        }

    @staticmethod
    def next_alert_quiet_until() -> str:
        return _next_alert_quiet_until()

    @staticmethod
    def is_alert_quiet(raw_value: str) -> bool:
        return _alert_quiet_active(raw_value)

    def quiet_part_alert_until_next_morning(self, part_id: int) -> str:
        quiet_until = self.next_alert_quiet_until()
        self.db.execute(
            "UPDATE parts SET alert_quiet_until=? WHERE id=?",
            (quiet_until, int(part_id)),
        )
        return quiet_until

    def quiet_client_followup_until_next_morning(self, client_part_id: int) -> str:
        quiet_until = self.next_alert_quiet_until()
        self.db.execute(
            "UPDATE client_parts SET alert_quiet_until=? WHERE id=?",
            (quiet_until, int(client_part_id)),
        )
        return quiet_until

    def list_agent_active_parts(self, user_id: str, search_text: str = "") -> list[sqlite3.Row]:
        normalized_user = DepotRules.normalize_user_id(user_id)
        query = (
            "SELECT p.id, p.created_at, p.work_order, p.category, p.client_unit, COALESCE(p.qa_comment, '') AS qa_comment, "
            "COALESCE(p.agent_comment, '') AS agent_comment, COALESCE(p.comments, '') AS comments, "
            "COALESCE(p.qa_flag, '') AS qa_flag, COALESCE(p.qa_flag_image_path, '') AS qa_flag_image_path, "
            "COALESCE(p.working_user_id, '') AS working_user_id, COALESCE(p.working_updated_at, '') AS working_updated_at, "
            "COALESCE(p.alert_quiet_until, '') AS alert_quiet_until, "
            "COALESCE(p.parts_installed, 0) AS parts_installed, "
            "COALESCE(p.parts_installed_by, '') AS parts_installed_by, COALESCE(p.parts_installed_at, '') AS parts_installed_at "
            "FROM parts p "
            "WHERE p.assigned_user_id=? AND p.is_active=1 "
            "AND p.id=("
            "SELECT MAX(p2.id) FROM parts p2 WHERE p2.is_active=1 AND p2.work_order=p.work_order"
            ")"
        )
        params: list[Any] = [normalized_user]
        if search_text:
            query += " AND p.work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += " ORDER BY p.created_at ASC, p.id ASC LIMIT 300"
        return self.db.fetchall(query, tuple(params))

    def list_category_active_parts(self, search_text: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT p.id, p.created_at, p.work_order, COALESCE(p.assigned_user_id, '') AS assigned_user_id, "
            "p.category, p.client_unit, COALESCE(p.qa_comment, '') AS qa_comment, "
            "COALESCE(p.agent_comment, '') AS agent_comment, COALESCE(p.comments, '') AS comments, "
            "COALESCE(p.qa_flag, '') AS qa_flag, COALESCE(p.qa_flag_image_path, '') AS qa_flag_image_path, "
            "COALESCE(p.working_user_id, '') AS working_user_id, COALESCE(p.working_updated_at, '') AS working_updated_at, "
            "COALESCE(p.alert_quiet_until, '') AS alert_quiet_until, "
            "COALESCE(p.parts_installed, 0) AS parts_installed, "
            "COALESCE(p.parts_installed_by, '') AS parts_installed_by, COALESCE(p.parts_installed_at, '') AS parts_installed_at "
            "FROM parts p "
            "WHERE p.is_active=1 "
            "AND p.id=("
            "SELECT MAX(p2.id) FROM parts p2 WHERE p2.is_active=1 AND p2.work_order=p.work_order"
            ")"
        )
        params: list[Any] = []
        if search_text:
            query += " AND p.work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += (
            " ORDER BY p.client_unit DESC, CASE WHEN TRIM(COALESCE(p.qa_flag, '')) <> '' THEN 1 ELSE 0 END DESC, "
            "p.created_at ASC, p.id ASC LIMIT 300"
        )
        return self.db.fetchall(query, tuple(params))

    def list_qa_assigned_parts(self, search_text: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT p.id, p.created_at, p.work_order, p.assigned_user_id, p.category, p.client_unit, COALESCE(p.qa_comment, '') AS qa_comment, "
            "COALESCE(p.agent_comment, '') AS agent_comment, COALESCE(p.comments, '') AS comments, "
            "COALESCE(p.qa_flag, '') AS qa_flag, COALESCE(p.qa_flag_image_path, '') AS qa_flag_image_path, "
            "COALESCE(p.working_user_id, '') AS working_user_id, COALESCE(p.working_updated_at, '') AS working_updated_at, "
            "COALESCE(p.alert_quiet_until, '') AS alert_quiet_until "
            "FROM parts p WHERE p.is_active=1 "
            "AND p.id=("
            "SELECT MAX(p2.id) FROM parts p2 WHERE p2.is_active=1 AND p2.work_order=p.work_order"
            ")"
        )
        params: list[Any] = []
        if search_text:
            query += " AND p.work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += " ORDER BY p.created_at ASC, p.id ASC LIMIT 300"
        return self.db.fetchall(query, tuple(params))

    def list_qa_delivered_parts(self, search_text: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT p.id, p.created_at, p.work_order, p.assigned_user_id, p.category, "
            "COALESCE(p.qa_comment, '') AS qa_comment, COALESCE(p.comments, '') AS comments, "
            "COALESCE(p.parts_installed, 0) AS parts_installed, "
            "COALESCE(p.parts_installed_by, '') AS parts_installed_by, COALESCE(p.parts_installed_at, '') AS parts_installed_at "
            "FROM parts p "
            "WHERE p.is_active=1 "
            "AND p.id=("
            "SELECT MAX(p2.id) FROM parts p2 WHERE p2.is_active=1 AND p2.work_order=p.work_order"
            ") "
            "AND EXISTS ("
            "SELECT 1 FROM part_details d2 "
            "JOIN parts p3 ON p3.id=d2.part_id "
            "WHERE p3.is_active=1 AND p3.work_order=p.work_order AND COALESCE(d2.delivered, 0)=1"
            ")"
        )
        params: list[Any] = []
        if search_text:
            query += " AND p.work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += " ORDER BY COALESCE(p.parts_installed, 0) ASC, p.created_at ASC, p.id ASC LIMIT 400"
        return self.db.fetchall(query, tuple(params))

    def list_delivered_part_details(self, work_order: str) -> list[sqlite3.Row]:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return []
        return list(self.list_delivered_part_details_bulk([normalized_work_order]).get(normalized_work_order, []))

    def list_delivered_part_details_bulk(
        self,
        work_orders: list[str] | tuple[str, ...] | set[str],
    ) -> dict[str, list[sqlite3.Row]]:
        normalized_orders: list[str] = []
        seen: set[str] = set()
        for raw_value in work_orders:
            work_order = DepotRules.normalize_work_order(str(raw_value or ""))
            if not work_order or work_order in seen:
                continue
            seen.add(work_order)
            normalized_orders.append(work_order)
        if not normalized_orders:
            return {}

        result: dict[str, list[sqlite3.Row]] = {work_order: [] for work_order in normalized_orders}
        chunk_size = 300
        for start in range(0, len(normalized_orders), chunk_size):
            chunk = normalized_orders[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.db.fetchall(
                f"""
                SELECT
                    COALESCE(p.work_order, '') AS work_order,
                    COALESCE(d.lpn, '') AS lpn,
                    COALESCE(d.part_number, '') AS part_number,
                    COALESCE(d.part_description, '') AS part_description,
                    COALESCE(d.installed_keys, '') AS installed_keys,
                    COALESCE(d.shipping_info, '') AS shipping_info
                FROM part_details d
                JOIN parts p ON p.id=d.part_id
                WHERE p.is_active=1
                  AND p.work_order IN ({placeholders})
                  AND COALESCE(d.delivered, 0)=1
                ORDER BY p.work_order ASC, d.id ASC
                """,
                tuple(chunk),
            )
            for row in rows:
                work_order = DepotRules.normalize_work_order(str(row["work_order"] or ""))
                if not work_order:
                    continue
                result.setdefault(work_order, []).append(row)
        return result

    def list_completed_parts(self, search_text: str = "", category_filter: str | None = None) -> list[sqlite3.Row]:
        query = (
            "SELECT p.id, p.created_at, p.work_order, p.assigned_user_id, p.category, p.client_unit, "
            "COALESCE(p.qa_comment, '') AS qa_comment, COALESCE(p.agent_comment, '') AS agent_comment, "
            "COALESCE(p.comments, '') AS comments, COALESCE(p.qa_flag, '') AS qa_flag, COALESCE(p.qa_flag_image_path, '') AS qa_flag_image_path, "
            "COALESCE(p.working_user_id, '') AS working_user_id, COALESCE(p.working_updated_at, '') AS working_updated_at, "
            "COALESCE(ls.touch, '') AS latest_touch, "
            f"COALESCE(NULLIF(TRIM(ls.updated_at), ''), COALESCE(ls.created_at, '')) AS latest_touch_at "
            "FROM parts p "
            "LEFT JOIN submissions ls ON ls.id = ("
            "SELECT s2.id FROM submissions s2 WHERE s2.work_order = p.work_order "
            f"ORDER BY {_submission_latest_ts_sql('s2')} DESC, s2.id DESC LIMIT 1"
            ") "
            "WHERE p.is_active=0 AND COALESCE(ls.touch, '') IN (?, ?, ?)"
        )
        params: list[Any] = [
            DepotRules.TOUCH_COMPLETE,
            DepotRules.TOUCH_JUNK,
            DepotRules.TOUCH_RTV,
        ]
        if search_text:
            query += " AND p.work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        normalized_category = str(category_filter or "").strip()
        if normalized_category:
            query += (
                " AND UPPER(TRIM(COALESCE("
                + self._dashboard_resolved_category_expr("p.work_order")
                + ", ''))) = ?"
            )
            params.append(normalized_category.upper())
        query += f" ORDER BY COALESCE(NULLIF(TRIM(ls.updated_at), ''), COALESCE(ls.created_at, p.created_at)) DESC, p.id DESC LIMIT 400"
        return self.db.fetchall(query, tuple(params))

    def list_team_client_followups(self) -> list[sqlite3.Row]:
        return self.list_client_followups("", include_all=True)

    def list_client_followups(self, user_id: str = "", *, include_all: bool = False) -> list[sqlite3.Row]:
        normalized_user = DepotRules.normalize_user_id(user_id)
        if not include_all and not normalized_user:
            return []
        submission_user_filter = "" if include_all else "s.user_id=cp.user_id AND "
        where_clause = "" if include_all else "WHERE cp.user_id=? "
        params: tuple[Any, ...] = () if include_all else (normalized_user,)
        limit = 600 if include_all else 300
        return self.db.fetchall(
            "SELECT cp.id, COALESCE(cp.user_id, '') AS user_id, cp.work_order, cp.created_at, "
            "COALESCE(cp.comments, '') AS comments, "
            "COALESCE(cp.alert_quiet_until, '') AS alert_quiet_until, "
            "COALESCE(cp.followup_last_action, '') AS followup_last_action, "
            "COALESCE(cp.followup_last_action_at, '') AS followup_last_action_at, "
            "COALESCE(cp.followup_last_actor, '') AS followup_last_actor, "
            "COALESCE(cp.followup_no_contact_count, 0) AS followup_no_contact_count, "
            "COALESCE(cp.followup_stage_logged, -1) AS followup_stage_logged, "
            "COALESCE(("
            "SELECT s.touch FROM submissions s "
            f"WHERE {submission_user_filter}s.work_order=cp.work_order "
            "AND s.client_unit=1 AND s.touch IN ('Part Order', 'Other') "
            f"ORDER BY {_submission_latest_ts_sql('s')} DESC, s.id DESC LIMIT 1"
            "), '') AS latest_touch, "
            "COALESCE(("
            f"SELECT {_submission_entry_date_sql('s')} FROM submissions s "
            f"WHERE {submission_user_filter}s.work_order=cp.work_order "
            "AND s.client_unit=1 AND s.touch IN ('Part Order', 'Other') "
            f"ORDER BY {_submission_latest_ts_sql('s')} DESC, s.id DESC LIMIT 1"
            "), '') AS latest_touch_date, "
            "COALESCE(("
            f"SELECT MAX({_submission_entry_date_sql('s')}) FROM submissions s "
            f"WHERE {submission_user_filter}s.work_order=cp.work_order "
            "AND s.client_unit=1 AND s.touch='Part Order'"
            "), '') AS last_part_order_date "
            f"FROM client_parts cp {where_clause}"
            f"ORDER BY cp.created_at DESC, cp.id DESC LIMIT {limit}",
            params,
        )

    def get_part_work_order(self, part_id: int) -> str:
        row = self.db.fetchone(
            "SELECT COALESCE(work_order, '') AS work_order FROM parts WHERE id=?",
            (int(part_id),),
        )
        if row is None:
            return ""
        return DepotRules.normalize_work_order(str(row["work_order"] or ""))

    def list_client_jo_rows(self, search_text: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT id, COALESCE(created_at, '') AS created_at, COALESCE(user_id, '') AS user_id, "
            "COALESCE(work_order, '') AS work_order, COALESCE(comments, '') AS comments "
            "FROM client_jo"
        )
        params: list[Any] = []
        if search_text:
            query += " WHERE work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += " ORDER BY created_at DESC, id DESC LIMIT 400"
        return self.db.fetchall(query, tuple(params))

    def list_junk_out_rows(
        self,
        search_text: str = "",
        *,
        client_filter: str = "all",
        start_date: str = "",
        end_date: str = "",
    ) -> list[sqlite3.Row]:
        query = (
            "SELECT s.id, COALESCE(s.created_at, '') AS created_at, COALESCE(s.updated_at, '') AS updated_at, "
            "COALESCE(s.user_id, '') AS user_id, COALESCE(s.work_order, '') AS work_order, "
            "COALESCE(s.category, '') AS category, COALESCE(s.client_unit, 0) AS client_unit, "
            "COALESCE(s.entry_date, SUBSTR(COALESCE(s.created_at, ''), 1, 10)) AS entry_date, "
            "COALESCE(s.serial_number, '') AS serial_number, COALESCE(cj.comments, '') AS comments "
            "FROM submissions s "
            "LEFT JOIN client_jo cj ON cj.user_id=s.user_id AND cj.work_order=s.work_order "
            "AND SUBSTR(COALESCE(cj.created_at, ''), 1, 10)=COALESCE(s.entry_date, SUBSTR(COALESCE(s.created_at, ''), 1, 10)) "
            "WHERE s.touch=?"
        )
        params: list[Any] = [DepotRules.TOUCH_JUNK]
        normalized_search = DepotRules.normalize_work_order(str(search_text or ""))
        if normalized_search:
            query += " AND UPPER(COALESCE(s.work_order, '')) LIKE ?"
            params.append(f"%{normalized_search}%")
        normalized_filter = str(client_filter or "all").strip().lower()
        if normalized_filter in {"client", "client_only", "client only"}:
            query += " AND COALESCE(s.client_unit, 0)=1"
        elif normalized_filter in {"non_client", "non-client", "non client", "without_client", "without client"}:
            query += " AND COALESCE(s.client_unit, 0)=0"
        normalized_start = str(start_date or "").strip()
        if normalized_start:
            query += " AND COALESCE(s.entry_date, SUBSTR(COALESCE(s.created_at, ''), 1, 10)) >= ?"
            params.append(normalized_start[:10])
        normalized_end = str(end_date or "").strip()
        if normalized_end:
            query += " AND COALESCE(s.entry_date, SUBSTR(COALESCE(s.created_at, ''), 1, 10)) <= ?"
            params.append(normalized_end[:10])
        query += f" ORDER BY {_submission_latest_ts_sql('s')} DESC, s.id DESC LIMIT 600"
        return self.db.fetchall(query, tuple(params))

    def list_rtv_rows(self, search_text: str = "") -> list[sqlite3.Row]:
        query = (
            "SELECT id, COALESCE(created_at, '') AS created_at, COALESCE(user_id, '') AS user_id, "
            "COALESCE(work_order, '') AS work_order, COALESCE(comments, '') AS comments "
            "FROM rtvs"
        )
        params: list[Any] = []
        if search_text:
            query += " WHERE work_order LIKE ?"
            params.append(f"%{str(search_text).strip()}%")
        query += " ORDER BY created_at DESC, id DESC LIMIT 400"
        return self.db.fetchall(query, tuple(params))

    def list_recent_user_submissions(self, user_id: str, limit: int = 3) -> list[sqlite3.Row]:
        normalized_user = DepotRules.normalize_user_id(user_id)
        max_rows = int(clamp(safe_int(limit, 3), 1, 20))
        return self.db.fetchall(
            "SELECT id, work_order, touch, COALESCE(category, '') AS category, client_unit, "
            "COALESCE(serial_number, '') AS serial_number, "
            "COALESCE(created_at, '') AS created_at, COALESCE(updated_at, '') AS updated_at, "
            f"{_submission_latest_ts_sql()} AS latest_stamp "
            "FROM submissions WHERE UPPER(TRIM(COALESCE(user_id, '')))=? "
            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT ?",
            (normalized_user, max_rows),
        )

    def get_submission_record(self, submission_id: int) -> dict[str, Any] | None:
        row = self.db.fetchone(
            "SELECT id, COALESCE(created_at, '') AS created_at, COALESCE(updated_at, '') AS updated_at, "
            "COALESCE(user_id, '') AS user_id, COALESCE(work_order, '') AS work_order, "
            "COALESCE(touch, '') AS touch, COALESCE(category, '') AS category, "
            "COALESCE(client_unit, 0) AS client_unit, COALESCE(entry_date, '') AS entry_date, "
            "COALESCE(serial_number, '') AS serial_number "
            "FROM submissions WHERE id=?",
            (int(submission_id),),
        )
        if row is None:
            return None
        latest_stamp = str(row["updated_at"] or "").strip() or str(row["created_at"] or "").strip()
        return {
            "id": int(max(0, safe_int(row["id"], 0))),
            "created_at": str(row["created_at"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
            "latest_stamp": latest_stamp,
            "user_id": DepotRules.normalize_user_id(str(row["user_id"] or "")),
            "work_order": DepotRules.normalize_work_order(str(row["work_order"] or "")),
            "touch": str(row["touch"] or "").strip(),
            "category": str(row["category"] or "").strip(),
            "client_unit": bool(int(max(0, safe_int(row["client_unit"], 0)))),
            "entry_date": str(row["entry_date"] or "").strip(),
            "serial_number": str(row["serial_number"] or "").strip(),
        }

    def get_latest_work_order_submission(self, work_order: str) -> dict[str, Any] | None:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return None
        row = self.db.fetchone(
            "SELECT id, COALESCE(created_at, '') AS created_at, COALESCE(updated_at, '') AS updated_at, "
            "COALESCE(user_id, '') AS user_id, COALESCE(work_order, '') AS work_order, "
            "COALESCE(touch, '') AS touch, COALESCE(category, '') AS category, COALESCE(client_unit, 0) AS client_unit, "
            "COALESCE(entry_date, '') AS entry_date, COALESCE(serial_number, '') AS serial_number "
            "FROM submissions WHERE work_order=? "
            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
            (normalized_work_order,),
        )
        if row is None:
            return None
        latest_stamp = str(row["updated_at"] or "").strip() or str(row["created_at"] or "").strip()
        return {
            "id": int(max(0, safe_int(row["id"], 0))),
            "created_at": str(row["created_at"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
            "latest_stamp": latest_stamp,
            "user_id": DepotRules.normalize_user_id(str(row["user_id"] or "")),
            "work_order": DepotRules.normalize_work_order(str(row["work_order"] or "")),
            "touch": str(row["touch"] or "").strip(),
            "category": str(row["category"] or "").strip(),
            "client_unit": bool(int(max(0, safe_int(row["client_unit"], 0)))),
            "entry_date": str(row["entry_date"] or "").strip(),
            "serial_number": str(row["serial_number"] or "").strip(),
        }

    def _delete_submission_row_and_aux_logs(self, submission: dict[str, Any]) -> None:
        target_id = int(max(0, safe_int(submission.get("id", 0), 0)))
        if target_id <= 0:
            return
        actor = DepotRules.normalize_user_id(str(submission.get("user_id", "") or ""))
        work_order = DepotRules.normalize_work_order(str(submission.get("work_order", "") or ""))
        touch = str(submission.get("touch", "") or "").strip()
        entry_date = str(submission.get("entry_date", "") or "").strip()
        client_unit = bool(submission.get("client_unit", False))

        if touch == DepotRules.TOUCH_RTV:
            self.db.execute(
                "DELETE FROM rtvs WHERE user_id=? AND work_order=? AND SUBSTR(COALESCE(created_at, ''), 1, 10)=?",
                (actor, work_order, entry_date),
            )
        if client_unit and touch == DepotRules.TOUCH_JUNK:
            self.db.execute(
                "DELETE FROM client_jo WHERE user_id=? AND work_order=? AND SUBSTR(COALESCE(created_at, ''), 1, 10)=?",
                (actor, work_order, entry_date),
            )
        self.db.execute("DELETE FROM submissions WHERE id=?", (target_id,))

    def get_blocking_work_submission(
        self,
        work_order: str,
        actor_user_id: str = "",
        *,
        next_touch: str = "",
        submitted_at: datetime | date | str | None = None,
    ) -> dict[str, Any] | None:
        latest_submission = self.get_latest_work_order_submission(work_order)
        if latest_submission is None:
            return None
        latest_touch = str(latest_submission.get("touch", "") or "").strip()
        if not latest_touch or latest_touch in DepotRules.FOLLOW_UP_TOUCHES:
            return None
        actor = DepotRules.normalize_user_id(actor_user_id)
        if actor:
            entry_date = self._normalize_submission_timestamp(submitted_at).date().isoformat()
            if self._find_existing_same_day_submission(entry_date, actor, work_order) is not None:
                return None
            latest_entry_date = str(latest_submission.get("entry_date", "") or "").strip()
            requested_touch = str(next_touch or "").strip()
            if (
                latest_touch in DepotRules.CLOSING_TOUCHES
                and requested_touch not in DepotRules.CLOSING_TOUCHES
                and latest_entry_date
                and latest_entry_date < entry_date
            ):
                return None
        return latest_submission

    def delete_user_submission(self, submission_id: int, actor_user_id: str) -> dict[str, Any]:
        target_id = int(max(0, safe_int(submission_id, 0)))
        actor = DepotRules.normalize_user_id(actor_user_id)
        if target_id <= 0 or not actor:
            raise ValueError("A valid submission and user are required.")

        submission = self.get_submission_record(target_id)
        if submission is None:
            raise ValueError("That submission no longer exists.")
        if str(submission.get("user_id", "") or "") != actor:
            raise PermissionDeniedError("Users can only remove their own submissions.")

        work_order = DepotRules.normalize_work_order(str(submission.get("work_order", "") or ""))
        touch = str(submission.get("touch", "") or "").strip()
        entry_date = str(submission.get("entry_date", "") or "").strip()
        client_unit = bool(submission.get("client_unit", False))

        fallback_part_order = None
        linked_parts_count_row = self.db.fetchone(
            "SELECT COUNT(*) AS c FROM parts WHERE COALESCE(source_submission_id, 0)=?",
            (target_id,),
        )
        linked_parts_count = int(max(0, safe_int(linked_parts_count_row["c"], 0))) if linked_parts_count_row is not None else 0
        if touch == DepotRules.TOUCH_PART_ORDER and linked_parts_count > 0:
            fallback_part_order = self.db.fetchone(
                "SELECT id, COALESCE(user_id, '') AS user_id "
                "FROM submissions WHERE work_order=? AND touch=? AND id<>? "
                f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                (work_order, DepotRules.TOUCH_PART_ORDER, target_id),
            )
            if fallback_part_order is None:
                raise ValueError(
                    "This Part Order is linked to live parts and cannot be removed because there is no earlier Part Order to relink."
                )

        reopened_part_id = 0
        restored_client_followup = False
        with self.db.write_transaction("tracker.delete_user_submission"):
            self._delete_submission_row_and_aux_logs(submission)

            if touch == DepotRules.TOUCH_PART_ORDER and linked_parts_count > 0 and fallback_part_order is not None:
                fallback_submission_id = int(max(0, safe_int(fallback_part_order["id"], 0)))
                fallback_user_id = DepotRules.normalize_user_id(str(fallback_part_order["user_id"] or ""))
                self.db.execute(
                    "UPDATE parts SET source_submission_id=?, assigned_user_id=? WHERE COALESCE(source_submission_id, 0)=?",
                    (fallback_submission_id, fallback_user_id, target_id),
                )

            if client_unit and touch in DepotRules.FOLLOW_UP_TOUCHES:
                fallback_client_submission = self.db.fetchone(
                    "SELECT COALESCE(user_id, '') AS user_id, "
                    f"{_submission_latest_ts_sql()} AS latest_stamp "
                    "FROM submissions "
                    "WHERE work_order=? AND COALESCE(client_unit, 0)=1 AND touch IN (?, ?) "
                    f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                    (work_order, DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER),
                )
                if fallback_client_submission is None:
                    self.db.execute("DELETE FROM client_parts WHERE work_order=?", (work_order,))
                else:
                    fallback_user = DepotRules.normalize_user_id(str(fallback_client_submission["user_id"] or ""))
                    fallback_stamp = str(fallback_client_submission["latest_stamp"] or "").strip()
                    existing_client_row = self.db.fetchone(
                        "SELECT id FROM client_parts WHERE work_order=?",
                        (work_order,),
                    )
                    if existing_client_row is None:
                        self.db.execute(
                            "INSERT INTO client_parts (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
                            (fallback_stamp, fallback_user, work_order, ""),
                        )
                    else:
                        self.db.execute(
                            "UPDATE client_parts SET user_id=?, created_at=? WHERE work_order=?",
                            (fallback_user, fallback_stamp, work_order),
                        )

            if touch in DepotRules.CLOSING_TOUCHES:
                latest_after_close_delete = self.db.fetchone(
                    "SELECT COALESCE(touch, '') AS touch FROM submissions WHERE work_order=? "
                    f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                    (work_order,),
                )
                latest_after_touch = (
                    str(latest_after_close_delete["touch"] or "").strip()
                    if latest_after_close_delete is not None
                    else ""
                )
                if latest_after_touch not in DepotRules.CLOSING_TOUCHES:
                    part_row = self.db.fetchone(
                        "SELECT id, COALESCE(assigned_user_id, '') AS assigned_user_id "
                        "FROM parts WHERE work_order=? AND is_active=0 ORDER BY id DESC LIMIT 1",
                        (work_order,),
                    )
                    if part_row is not None:
                        reopened_part_id = int(max(0, safe_int(part_row["id"], 0)))
                        fallback_source = self.db.fetchone(
                            "SELECT id, COALESCE(user_id, '') AS user_id FROM submissions WHERE work_order=? AND touch=? "
                            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                            (work_order, DepotRules.TOUCH_PART_ORDER),
                        )
                        fallback_source_id = (
                            int(max(0, safe_int(fallback_source["id"], 0)))
                            if fallback_source is not None
                            else 0
                        )
                        fallback_assigned_user = (
                            DepotRules.normalize_user_id(str(fallback_source["user_id"] or ""))
                            if fallback_source is not None
                            else DepotRules.normalize_user_id(str(part_row["assigned_user_id"] or ""))
                        )
                        self.db.execute(
                            "UPDATE parts SET is_active=1, source_submission_id=?, assigned_user_id=?, "
                            "working_user_id='', working_updated_at='' WHERE id=?",
                            (fallback_source_id, fallback_assigned_user, reopened_part_id),
                        )
                    fallback_client_submission = self.db.fetchone(
                        "SELECT COALESCE(user_id, '') AS user_id, "
                        f"{_submission_latest_ts_sql()} AS latest_stamp "
                        "FROM submissions "
                        "WHERE work_order=? AND COALESCE(client_unit, 0)=1 AND touch IN (?, ?) "
                        f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                        (work_order, DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER),
                    )
                    if fallback_client_submission is not None:
                        fallback_user = DepotRules.normalize_user_id(str(fallback_client_submission["user_id"] or ""))
                        fallback_stamp = str(fallback_client_submission["latest_stamp"] or "").strip()
                        existing_client_row = self.db.fetchone("SELECT id FROM client_parts WHERE work_order=?", (work_order,))
                        if existing_client_row is None:
                            self.db.execute(
                                "INSERT INTO client_parts (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
                                (fallback_stamp, fallback_user, work_order, ""),
                            )
                        else:
                            self.db.execute(
                                "UPDATE client_parts SET user_id=?, created_at=? WHERE work_order=?",
                                (fallback_user, fallback_stamp, work_order),
                            )
                        restored_client_followup = True

        _runtime_log_event(
            "depot.user_submission_deleted",
            severity="info",
            summary="A recent user submission was removed.",
            context={
                "submission_id": int(target_id),
                "actor_user_id": actor,
                "work_order": work_order,
                "touch": touch,
                "client_unit": bool(client_unit),
                "linked_parts_count": int(linked_parts_count),
                "reopened_part_id": int(reopened_part_id),
                "restored_client_followup": bool(restored_client_followup),
            },
        )
        return {
            "submission_id": int(target_id),
            "work_order": work_order,
            "touch": touch,
            "client_unit": bool(client_unit),
            "reopened_part_id": int(reopened_part_id),
        }

    def upsert_agent(self, user_id: str, agent_name: str, tier: int, icon_path: str = "", location: str = "") -> str:
        normalized_user = DepotRules.normalize_user_id(user_id)
        normalized_name = str(agent_name or "").strip()
        normalized_tier = DepotRules.normalize_agent_tier(tier)
        normalized_icon = str(icon_path or "").strip()
        normalized_location = str(location or "").strip()
        if not normalized_user or not normalized_name:
            return ""

        with self.db.write_transaction("tracker.upsert_agent"):
            existing = self.db.fetchone("SELECT icon_path FROM agents WHERE user_id=?", (normalized_user,))
            existing_stored = str(existing["icon_path"] or "").strip() if existing else ""
            stored_icon = self._store_agent_icon(normalized_user, normalized_icon, existing_stored)
            self.db.execute(
                """
                INSERT INTO agents (agent_name, user_id, tier, location, icon_path)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  agent_name=excluded.agent_name,
                  tier=excluded.tier,
                  location=excluded.location,
                  icon_path=excluded.icon_path
                """,
                (normalized_name, normalized_user, normalized_tier, normalized_location, stored_icon),
            )
        return stored_icon

    def delete_agent(self, user_id: str) -> None:
        normalized_user = DepotRules.normalize_user_id(user_id)
        if not normalized_user:
            return
        self.db.execute("DELETE FROM agents WHERE user_id=?", (normalized_user,))
        for existing in self._agent_icons_dir().glob(f"{normalized_user}.*"):
            try:
                if existing.is_file():
                    existing.unlink()
            except Exception as exc:
                _runtime_log_event(
                    "depot.agent_icon_delete_failed",
                    severity="warning",
                    summary="Failed deleting agent icon file during agent deletion.",
                    exc=exc,
                    context={"user_id": normalized_user, "path": str(existing)},
                )

    @staticmethod
    def _normalize_submission_timestamp(submitted_at: datetime | date | str | None) -> datetime:
        if isinstance(submitted_at, datetime):
            if submitted_at.tzinfo is not None:
                return submitted_at.astimezone().replace(tzinfo=None)
            return submitted_at
        if isinstance(submitted_at, date):
            return datetime.combine(submitted_at, datetime.min.time())
        raw_value = str(submitted_at or "").strip()
        if not raw_value:
            return datetime.now()
        try:
            parsed = datetime.fromisoformat(raw_value)
            if parsed.tzinfo is not None:
                return parsed.astimezone().replace(tzinfo=None)
            return parsed
        except Exception:
            try:
                return datetime.combine(datetime.strptime(raw_value, "%Y-%m-%d").date(), datetime.min.time())
            except Exception:
                return datetime.now()

    def _find_existing_same_day_submission(
        self,
        entry_date: str,
        user_id: str,
        work_order: str,
    ) -> sqlite3.Row | None:
        return self.db.fetchone(
            "SELECT id, COALESCE(created_at, '') AS created_at, COALESCE(updated_at, '') AS updated_at, "
            "COALESCE(touch, '') AS touch, COALESCE(client_unit, 0) AS client_unit "
            "FROM submissions "
            "WHERE entry_date=? AND user_id=? AND work_order=? "
            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
            (str(entry_date or "").strip(), DepotRules.normalize_user_id(user_id), DepotRules.normalize_work_order(work_order)),
        )

    def _upsert_same_day_submission(
        self,
        entry_date: str,
        user_id: str,
        work_order: str,
        touch: str,
        category_text: str,
        client_unit_int: int,
        stamp_text: str,
        serial_number: str = "",
    ) -> int:
        serial_text = str(serial_number or "").strip().upper()
        existing = self._find_existing_same_day_submission(entry_date, user_id, work_order)
        if existing is not None:
            self.db.execute(
                "UPDATE submissions SET touch=?, category=?, client_unit=?, updated_at=?, serial_number=? WHERE id=?",
                (touch, category_text, int(client_unit_int), str(stamp_text or "").strip(), serial_text, int(existing["id"])),
            )
            return int(existing["id"])
        try:
            cursor = self.db.execute(
                "INSERT INTO submissions (created_at, updated_at, user_id, work_order, touch, category, client_unit, entry_date, serial_number) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(stamp_text or "").strip(),
                    str(stamp_text or "").strip(),
                    DepotRules.normalize_user_id(user_id),
                    DepotRules.normalize_work_order(work_order),
                    str(touch or "").strip(),
                    str(category_text or "").strip(),
                    int(client_unit_int),
                    str(entry_date or "").strip(),
                    serial_text,
                ),
            )
            return int(cursor.lastrowid or 0)
        except sqlite3.IntegrityError:
            existing = self._find_existing_same_day_submission(entry_date, user_id, work_order)
            if existing is None:
                raise
            self.db.execute(
                "UPDATE submissions SET touch=?, category=?, client_unit=?, updated_at=?, serial_number=? WHERE id=?",
                (touch, category_text, int(client_unit_int), str(stamp_text or "").strip(), serial_text, int(existing["id"])),
            )
            return int(existing["id"])

    def _upsert_same_day_aux_log(
        self,
        table_name: str,
        entry_date: str,
        user_id: str,
        work_order: str,
        comments: str,
        stamp_text: str,
    ) -> None:
        normalized_table = str(table_name or "").strip().lower()
        if normalized_table not in {"rtvs", "client_jo"}:
            raise ValueError(f"Unsupported same-day aux log table: {table_name}")
        existing = self.db.fetchone(
            f"SELECT id FROM {normalized_table} "
            "WHERE user_id=? AND work_order=? AND SUBSTR(COALESCE(created_at, ''), 1, 10)=? "
            "ORDER BY created_at DESC, id DESC LIMIT 1",
            (
                DepotRules.normalize_user_id(user_id),
                DepotRules.normalize_work_order(work_order),
                str(entry_date or "").strip(),
            ),
        )
        if existing is not None:
            self.db.execute(
                f"UPDATE {normalized_table} SET created_at=?, comments=? WHERE id=?",
                (str(stamp_text or "").strip(), str(comments or ""), int(existing["id"])),
            )
            return
        self.db.execute(
            f"INSERT INTO {normalized_table} (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
            (
                str(stamp_text or "").strip(),
                DepotRules.normalize_user_id(user_id),
                DepotRules.normalize_work_order(work_order),
                str(comments or ""),
            ),
        )

    def submit_work(
        self,
        user_id: str,
        work_order: str,
        touch: str,
        client_unit: bool,
        comments: str | None = None,
        category: str | None = "",
        submitted_at: datetime | date | str | None = None,
        serial_number: str | None = "",
    ) -> None:
        stamp_dt = self._normalize_submission_timestamp(submitted_at)
        stamp_text = stamp_dt.isoformat(timespec="seconds")
        user_id = DepotRules.normalize_user_id(user_id)
        work_order = DepotRules.normalize_work_order(work_order)
        entry_date = stamp_dt.date().isoformat()
        category_text = str(category or "").strip()
        comments_text = str(comments or "")
        client_unit_int = 1 if client_unit else 0
        serial_text = str(serial_number or "").strip().upper()
        if touch == DepotRules.TOUCH_JUNK and not serial_text:
            raise ValueError("Serial number is required for Junk Out submissions.")

        with self.db.write_transaction("tracker.submit_work"):
            existing_same_day_submission = self._find_existing_same_day_submission(entry_date, user_id, work_order)
            prior_same_day_touch = str(existing_same_day_submission["touch"] or "").strip() if existing_same_day_submission is not None else ""
            prior_same_day_client_unit = (
                bool(int(max(0, safe_int(existing_same_day_submission["client_unit"], 0))))
                if existing_same_day_submission is not None
                else False
            )
            latest_submission = self.get_latest_work_order_submission(work_order)
            if (
                existing_same_day_submission is None
                and touch not in DepotRules.CLOSING_TOUCHES
                and latest_submission is not None
            ):
                latest_touch = str(latest_submission.get("touch", "") or "").strip()
                latest_entry_date = str(latest_submission.get("entry_date", "") or "").strip()
                if latest_touch in DepotRules.CLOSING_TOUCHES and latest_entry_date and latest_entry_date < entry_date:
                    self._delete_submission_row_and_aux_logs(latest_submission)
                    _runtime_log_event(
                        "depot.superseded_closing_submission_deleted",
                        severity="info",
                        summary="A previous-day closing submission was removed to allow a reopened work-order update.",
                        context={
                            "removed_submission_id": int(max(0, safe_int(latest_submission.get("id", 0), 0))),
                            "removed_touch": latest_touch,
                            "removed_entry_date": latest_entry_date,
                            "new_touch": str(touch or "").strip(),
                            "new_entry_date": entry_date,
                            "work_order": work_order,
                            "actor_user_id": user_id,
                        },
                    )
            submission_id = self._upsert_same_day_submission(
                entry_date,
                user_id,
                work_order,
                touch,
                category_text,
                client_unit_int,
                stamp_text,
                serial_text,
            )

            if prior_same_day_touch == DepotRules.TOUCH_RTV and touch != DepotRules.TOUCH_RTV:
                self.db.execute(
                    "DELETE FROM rtvs WHERE user_id=? AND work_order=? AND SUBSTR(COALESCE(created_at, ''), 1, 10)=?",
                    (user_id, work_order, entry_date),
                )
            if (
                prior_same_day_touch == DepotRules.TOUCH_JUNK
                and prior_same_day_client_unit
                and not (client_unit and touch == DepotRules.TOUCH_JUNK)
            ):
                self.db.execute(
                    "DELETE FROM client_jo WHERE user_id=? AND work_order=? AND SUBSTR(COALESCE(created_at, ''), 1, 10)=?",
                    (user_id, work_order, entry_date),
                )
            if (
                prior_same_day_touch in DepotRules.FOLLOW_UP_TOUCHES
                and prior_same_day_client_unit
                and not (client_unit and touch in DepotRules.FOLLOW_UP_TOUCHES)
            ):
                self.db.execute("DELETE FROM client_parts WHERE work_order=?", (work_order,))

            if touch == DepotRules.TOUCH_PART_ORDER and submission_id > 0:
                self.db.execute(
                    "UPDATE parts SET assigned_user_id=?, source_submission_id=?, "
                    "missing_part_order_followup=0, "
                    "missing_part_order_resolved_at=CASE "
                    "WHEN COALESCE(missing_part_order_followup, 0)=1 THEN ? "
                    "ELSE COALESCE(missing_part_order_resolved_at, '') END, "
                    "missing_part_order_resolved_by=CASE "
                    "WHEN COALESCE(missing_part_order_followup, 0)=1 THEN ? "
                    "ELSE COALESCE(missing_part_order_resolved_by, '') END "
                    "WHERE work_order=? AND is_active=1",
                    (user_id, submission_id, stamp_text, user_id, work_order),
                )
                if category_text:
                    self.db.execute(
                        "UPDATE parts SET category=? WHERE work_order=? AND is_active=1",
                        (category_text, work_order),
                    )

            if touch == DepotRules.TOUCH_RTV:
                self._upsert_same_day_aux_log("rtvs", entry_date, user_id, work_order, comments_text, stamp_text)

            if client_unit and touch == DepotRules.TOUCH_JUNK:
                self._upsert_same_day_aux_log("client_jo", entry_date, user_id, work_order, comments_text, stamp_text)

            if client_unit and touch in (DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER):
                existing = self.db.fetchone(
                    "SELECT id FROM client_parts WHERE work_order = ?", (work_order,)
                )
                if existing:
                    self.db.execute(
                        "UPDATE client_parts SET user_id=?, comments=?, created_at=? WHERE work_order = ?",
                        (user_id, comments_text, stamp_text, work_order),
                    )
                else:
                    self.db.execute(
                        "INSERT INTO client_parts (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
                        (stamp_text, user_id, work_order, comments_text),
                    )

            if touch in DepotRules.CLOSING_TOUCHES:
                self.db.execute(
                    "DELETE FROM part_details WHERE part_id IN (SELECT id FROM parts WHERE work_order=?)",
                    (work_order,),
                )
                self.db.execute(
                    "UPDATE parts SET is_active=0, working_user_id='', working_updated_at='' WHERE work_order=?",
                    (work_order,),
                )
                self.db.execute(
                    "DELETE FROM client_parts WHERE work_order = ?", (work_order,)
                )

    def resolve_work_order_category(self, work_order: str, fallback: str = "") -> str:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return str(fallback or "").strip()

        submission_row = self.db.fetchone(
            "SELECT COALESCE(category, '') AS category "
            "FROM submissions "
            "WHERE work_order=? AND TRIM(COALESCE(category, '')) <> '' "
            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
            (normalized_work_order,),
        )
        if submission_row is not None:
            category_text = str(submission_row["category"] or "").strip()
            if category_text:
                return category_text

        part_row = self.db.fetchone(
            "SELECT COALESCE(category, '') AS category "
            "FROM parts "
            "WHERE work_order=? AND TRIM(COALESCE(category, '')) <> '' "
            "ORDER BY is_active DESC, created_at DESC, id DESC LIMIT 1",
            (normalized_work_order,),
        )
        if part_row is not None:
            category_text = str(part_row["category"] or "").strip()
            if category_text:
                return category_text

        return str(fallback or "").strip()

    def resolve_work_order_categories_bulk(
        self,
        work_orders: list[str] | tuple[str, ...] | set[str],
        fallback_map: dict[str, str] | None = None,
    ) -> dict[str, str]:
        normalized_orders: list[str] = []
        seen: set[str] = set()
        for raw_value in work_orders:
            work_order = DepotRules.normalize_work_order(str(raw_value or ""))
            if not work_order or work_order in seen:
                continue
            seen.add(work_order)
            normalized_orders.append(work_order)
        if not normalized_orders:
            return {}

        fallback_lookup: dict[str, str] = {}
        if fallback_map:
            for raw_key, raw_value in fallback_map.items():
                normalized_key = DepotRules.normalize_work_order(str(raw_key or ""))
                if normalized_key:
                    fallback_lookup[normalized_key] = str(raw_value or "").strip()

        resolved: dict[str, str] = {
            work_order: str(fallback_lookup.get(work_order, "") or "").strip()
            for work_order in normalized_orders
        }
        chunk_size = 300
        for start in range(0, len(normalized_orders), chunk_size):
            chunk = normalized_orders[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            submission_rows = self.db.fetchall(
                f"""
                SELECT
                    work_order,
                    COALESCE(category, '') AS category,
                    {_submission_latest_ts_sql()} AS latest_stamp,
                    id
                FROM submissions
                WHERE work_order IN ({placeholders})
                  AND TRIM(COALESCE(category, '')) <> ''
                ORDER BY work_order ASC, {_submission_latest_ts_sql()} DESC, id DESC
                """,
                tuple(chunk),
            )
            seen_submission: set[str] = set()
            for row in submission_rows:
                work_order = DepotRules.normalize_work_order(str(row["work_order"] or ""))
                if not work_order or work_order in seen_submission:
                    continue
                category_text = str(row["category"] or "").strip()
                if category_text:
                    resolved[work_order] = category_text
                    seen_submission.add(work_order)

            unresolved = [work_order for work_order in chunk if not str(resolved.get(work_order, "") or "").strip()]
            if not unresolved:
                continue
            unresolved_placeholders = ",".join("?" for _ in unresolved)
            part_rows = self.db.fetchall(
                f"""
                SELECT
                    work_order,
                    COALESCE(category, '') AS category,
                    is_active,
                    COALESCE(created_at, '') AS created_at,
                    id
                FROM parts
                WHERE work_order IN ({unresolved_placeholders})
                  AND TRIM(COALESCE(category, '')) <> ''
                ORDER BY work_order ASC, is_active DESC, created_at DESC, id DESC
                """,
                tuple(unresolved),
            )
            seen_parts: set[str] = set()
            for row in part_rows:
                work_order = DepotRules.normalize_work_order(str(row["work_order"] or ""))
                if not work_order or work_order in seen_parts:
                    continue
                category_text = str(row["category"] or "").strip()
                if category_text:
                    resolved[work_order] = category_text
                    seen_parts.add(work_order)

        return {work_order: str(value or "").strip() for work_order, value in resolved.items()}

    def active_part_category_options(self) -> list[str]:
        rows = self.db.fetchall(
            "SELECT DISTINCT work_order, COALESCE(category, '') AS category "
            "FROM parts WHERE is_active=1 ORDER BY work_order ASC"
        )
        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        resolved_map = self.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        categories: list[str] = list(DepotRules.CATEGORY_OPTIONS)
        seen_categories = {str(category).strip().casefold() for category in categories if str(category).strip()}
        for work_order in fallback_map.keys():
            category_text = str(resolved_map.get(work_order, "") or "").strip()
            if not category_text:
                continue
            category_key = category_text.casefold()
            if category_key in seen_categories:
                continue
            seen_categories.add(category_key)
            categories.append(category_text)
        return categories

    def get_latest_part_order_submission(self, work_order: str) -> dict[str, Any] | None:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return None
        row = self.db.fetchone(
            "SELECT id, user_id, COALESCE(category, '') AS category, COALESCE(created_at, '') AS created_at, "
            "COALESCE(updated_at, '') AS updated_at "
            "FROM submissions "
            "WHERE work_order=? AND touch=? "
            f"ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
            (normalized_work_order, DepotRules.TOUCH_PART_ORDER),
        )
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "user_id": DepotRules.normalize_user_id(str(row["user_id"] or "")),
            "category": str(row["category"] or "").strip(),
            "created_at": str(row["created_at"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
        }

    def update_work_order_category(self, work_order: str, category: str) -> str:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        category_text = str(category or "").strip()
        if not normalized_work_order or not category_text:
            return ""

        latest_part_order = self.get_latest_part_order_submission(normalized_work_order)
        target_row = {"id": int(latest_part_order["id"])} if latest_part_order is not None else None
        if target_row is None:
            target_row = self.db.fetchone(
                f"SELECT id FROM submissions WHERE work_order=? ORDER BY {_submission_latest_ts_sql()} DESC, id DESC LIMIT 1",
                (normalized_work_order,),
            )
        if target_row is None:
            return ""

        self.db.execute(
            "UPDATE submissions SET category=? WHERE id=?",
            (category_text, int(target_row["id"])),
        )
        return category_text

    def submit_part(
        self,
        user_id: str,
        work_order: str,
        category: str,
        client_unit: bool,
        qa_comment: str | None = "",
        qa_flag: str | None = "",
        parts_on_hand: bool = False,
        fallback_assigned_user_id: str = "",
    ) -> int:
        now = datetime.utcnow().isoformat()
        user_id = DepotRules.normalize_user_id(user_id)
        fallback_assigned_user_id = DepotRules.normalize_user_id(fallback_assigned_user_id)
        work_order = DepotRules.normalize_work_order(work_order)
        category = str(category or "").strip()
        client_unit_int = 1 if client_unit else 0
        parts_on_hand_int = 1 if bool(parts_on_hand) else 0
        qa_comment_text = str(qa_comment or "").strip()
        qa_flag_text = str(qa_flag or "").strip()
        source_submission = self.get_latest_part_order_submission(work_order)
        source_submission_id = int(source_submission["id"]) if source_submission is not None else 0
        assigned_user_id = (
            DepotRules.normalize_user_id(str(source_submission.get("user_id", "") or ""))
            if source_submission is not None
            else fallback_assigned_user_id
        )
        if not category and source_submission is not None:
            category = str(source_submission.get("category", "") or "").strip()
        if source_submission is None and not fallback_assigned_user_id:
            raise ValueError("A Part Order work submission is required before parts can be submitted for this work order.")
        if qa_flag_text.lower() == "none":
            qa_flag_text = ""
        existing = self.db.fetchone(
            "SELECT id FROM parts WHERE is_active=1 AND work_order=? ORDER BY id DESC LIMIT 1",
            (work_order,),
        )
        if existing is not None:
            existing_id = int(existing["id"])
            self.db.execute(
                "UPDATE parts SET user_id=?, assigned_user_id=?, source_submission_id=?, client_unit=?, category=?, comments=?, qa_comment=?, "
                "qa_flag=?, qa_flag_image_path='', "
                "parts_on_hand=CASE WHEN ?=1 THEN 1 ELSE parts_on_hand END "
                "WHERE id=?",
                (
                    user_id,
                    assigned_user_id,
                    source_submission_id,
                    client_unit_int,
                    category,
                    qa_comment_text,
                    qa_comment_text,
                    qa_flag_text,
                    parts_on_hand_int,
                    existing_id,
                ),
            )
            return existing_id
        insert_cursor = self.db.execute(
            "INSERT INTO parts (created_at, user_id, assigned_user_id, source_submission_id, work_order, client_unit, category, comments, qa_comment, "
            "agent_comment, qa_flag, qa_flag_image_path, is_active, parts_on_hand, parts_installed, parts_installed_by, parts_installed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0, '', '')",
            (
                now,
                user_id,
                assigned_user_id,
                source_submission_id,
                work_order,
                client_unit_int,
                category,
                qa_comment_text,
                qa_comment_text,
                "",
                qa_flag_text,
                "",
                parts_on_hand_int,
            ),
        )
        return int(insert_cursor.lastrowid or 0)

    def submit_part_missing_part_order(
        self,
        user_id: str,
        work_order: str,
        category: str,
        client_unit: bool,
        assigned_user_id: str,
        qa_comment: str | None = "",
        qa_flag: str | None = "",
        parts_on_hand: bool = False,
    ) -> int:
        now = datetime.utcnow().isoformat()
        followup_stamp = datetime.now().isoformat(timespec="seconds")
        user_id = DepotRules.normalize_user_id(user_id)
        assigned_user_id = DepotRules.normalize_user_id(assigned_user_id)
        work_order = DepotRules.normalize_work_order(work_order)
        category = str(category or "").strip()
        client_unit_int = 1 if client_unit else 0
        parts_on_hand_int = 1 if bool(parts_on_hand) else 0
        qa_comment_text = str(qa_comment or "").strip()
        qa_flag_text = str(qa_flag or "").strip()
        if qa_flag_text.lower() == "none":
            qa_flag_text = ""
        if not assigned_user_id:
            raise ValueError("An assigned agent is required when no Part Order submission exists.")
        if self.get_latest_part_order_submission(work_order) is not None:
            raise ValueError("A Part Order submission already exists for this work order.")
        agent_row = self.db.fetchone("SELECT 1 FROM agents WHERE user_id=?", (assigned_user_id,))
        if agent_row is None:
            raise ValueError("Selected agent is no longer configured.")

        existing = self.db.fetchone(
            "SELECT id FROM parts WHERE is_active=1 AND work_order=? ORDER BY id DESC LIMIT 1",
            (work_order,),
        )
        if existing is not None:
            existing_id = int(existing["id"])
            self.db.execute(
                "UPDATE parts SET user_id=?, assigned_user_id=?, source_submission_id=0, client_unit=?, category=?, comments=?, qa_comment=?, "
                "qa_flag=?, qa_flag_image_path='', missing_part_order_followup=1, missing_part_order_logged_at=?, "
                "missing_part_order_logged_by=?, missing_part_order_resolved_at='', missing_part_order_resolved_by='', "
                "parts_on_hand=CASE WHEN ?=1 THEN 1 ELSE parts_on_hand END "
                "WHERE id=?",
                (
                    user_id,
                    assigned_user_id,
                    client_unit_int,
                    category,
                    qa_comment_text,
                    qa_comment_text,
                    qa_flag_text,
                    followup_stamp,
                    user_id,
                    parts_on_hand_int,
                    existing_id,
                ),
            )
            part_id = existing_id
        else:
            insert_cursor = self.db.execute(
                "INSERT INTO parts (created_at, user_id, assigned_user_id, source_submission_id, "
                "missing_part_order_followup, missing_part_order_logged_at, missing_part_order_logged_by, "
                "missing_part_order_resolved_at, missing_part_order_resolved_by, work_order, client_unit, category, "
                "comments, qa_comment, agent_comment, qa_flag, qa_flag_image_path, is_active, parts_on_hand, "
                "parts_installed, parts_installed_by, parts_installed_at) "
                "VALUES (?, ?, ?, 0, 1, ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0, '', '')",
                (
                    now,
                    user_id,
                    assigned_user_id,
                    followup_stamp,
                    user_id,
                    work_order,
                    client_unit_int,
                    category,
                    qa_comment_text,
                    qa_comment_text,
                    "",
                    qa_flag_text,
                    "",
                    parts_on_hand_int,
                ),
            )
            part_id = int(insert_cursor.lastrowid or 0)

        _runtime_log_event(
            "depot.part_missing_part_order_followup_logged",
            severity="warning",
            summary="QA parts submission proceeded without a Part Order submission and was logged for admin follow up.",
            context={
                "part_id": int(part_id),
                "qa_user_id": str(user_id),
                "assigned_user_id": str(assigned_user_id),
                "work_order": str(work_order),
                "client_unit": int(client_unit_int),
            },
        )
        return int(part_id)

    def list_missing_part_order_followups(self) -> list[dict[str, Any]]:
        rows = self.db.fetchall(
            "SELECT p.id, p.created_at, p.work_order, COALESCE(p.user_id, '') AS user_id, "
            "COALESCE(p.assigned_user_id, '') AS assigned_user_id, COALESCE(p.category, '') AS category, "
            "COALESCE(p.client_unit, 0) AS client_unit, COALESCE(p.qa_comment, '') AS qa_comment, "
            "COALESCE(p.comments, '') AS comments, COALESCE(p.missing_part_order_logged_at, '') AS missing_part_order_logged_at, "
            "COALESCE(p.missing_part_order_logged_by, '') AS missing_part_order_logged_by "
            "FROM parts p "
            "WHERE p.is_active=1 AND COALESCE(p.missing_part_order_followup, 0)=1 AND COALESCE(p.source_submission_id, 0)=0 "
            "AND p.id=("
            "SELECT MAX(p2.id) FROM parts p2 WHERE p2.is_active=1 AND p2.work_order=p.work_order"
            ") "
            "ORDER BY COALESCE(NULLIF(TRIM(p.missing_part_order_logged_at), ''), p.created_at) ASC, p.id ASC"
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "id": int(row["id"]),
                    "created_at": str(row["created_at"] or "").strip(),
                    "work_order": str(row["work_order"] or "").strip(),
                    "user_id": DepotRules.normalize_user_id(str(row["user_id"] or "")),
                    "assigned_user_id": DepotRules.normalize_user_id(str(row["assigned_user_id"] or "")),
                    "category": str(row["category"] or "").strip(),
                    "client_unit": int(row["client_unit"] or 0),
                    "qa_comment": str(row["qa_comment"] or row["comments"] or "").strip(),
                    "logged_at": str(row["missing_part_order_logged_at"] or "").strip(),
                    "logged_by": DepotRules.normalize_user_id(str(row["missing_part_order_logged_by"] or "")),
                }
            )
        return result

    def reassign_part_owner(self, part_id: int, assigned_user_id: str) -> None:
        normalized_agent = DepotRules.normalize_user_id(assigned_user_id)
        if not normalized_agent:
            raise ValueError("An agent is required.")
        agent_row = self.db.fetchone("SELECT 1 FROM agents WHERE user_id=?", (normalized_agent,))
        if agent_row is None:
            raise ValueError("Selected agent is no longer configured.")
        row = self.db.fetchone("SELECT id, work_order FROM parts WHERE id=?", (int(part_id),))
        if row is None:
            raise ValueError("Selected part no longer exists.")
        self.db.execute(
            "UPDATE parts SET assigned_user_id=? WHERE id=?",
            (normalized_agent, int(part_id)),
        )
        _runtime_log_event(
            "depot.part_owner_reassigned",
            severity="info",
            summary="A parts row owner was reassigned.",
            context={
                "part_id": int(part_id),
                "assigned_user_id": str(normalized_agent),
                "work_order": str(row["work_order"] or ""),
            },
        )

    def resolve_missing_part_order_followup(self, part_id: int, actor_user_id: str) -> None:
        actor = DepotRules.normalize_user_id(actor_user_id)
        row = self.db.fetchone("SELECT id, work_order FROM parts WHERE id=?", (int(part_id),))
        if row is None:
            raise ValueError("Selected part no longer exists.")
        stamp = datetime.now().isoformat(timespec="seconds")
        self.db.execute(
            "UPDATE parts SET missing_part_order_followup=0, missing_part_order_resolved_at=?, missing_part_order_resolved_by=? "
            "WHERE id=?",
            (stamp, actor, int(part_id)),
        )
        _runtime_log_event(
            "depot.part_missing_part_order_followup_resolved",
            severity="info",
            summary="A missing-Part-Order follow-up item was resolved.",
            context={
                "part_id": int(part_id),
                "actor_user_id": str(actor),
                "work_order": str(row["work_order"] or ""),
            },
        )

    def upsert_part_detail(
        self,
        part_id: int,
        lpn: str,
        part_number: str,
        part_description: str,
        shipping_info: str,
        delivered: bool,
    ) -> None:
        now = datetime.utcnow().isoformat()
        normalized_lpn = DepotRules.normalize_work_order(lpn)
        part_no_text = str(part_number or "").strip()
        part_desc_text = str(part_description or "").strip()
        shipping_text = str(shipping_info or "").strip()
        incoming_row = (normalized_lpn, part_no_text, part_desc_text, shipping_text)
        incoming_key = _part_detail_row_key(*incoming_row)

        existing = self.db.fetchone(
            "SELECT COALESCE(lpn, '') AS lpn, COALESCE(part_number, '') AS part_number, "
            "COALESCE(part_description, '') AS part_description, COALESCE(shipping_info, '') AS shipping_info, "
            "COALESCE(installed_keys, '') AS installed_keys, COALESCE(delivered, 0) AS delivered "
            "FROM part_details WHERE part_id=?",
            (int(part_id),),
        )
        if existing is None:
            if not incoming_key:
                return
            self.db.execute(
                "INSERT INTO part_details (part_id, created_at, lpn, part_number, part_description, shipping_info, delivered) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    int(part_id),
                    now,
                    normalized_lpn,
                    part_no_text,
                    part_desc_text,
                    shipping_text,
                    1 if bool(delivered) else 0,
                ),
            )
            return

        line_rows = _merged_part_detail_rows(
            str(existing["lpn"] or ""),
            str(existing["part_number"] or ""),
            str(existing["part_description"] or ""),
            str(existing["shipping_info"] or ""),
        )
        non_empty_ship = [str(row[3] or "").strip() for row in line_rows if str(row[3] or "").strip()]
        ship_seen: set[str] = set()
        ship_unique: list[str] = []
        for ship in non_empty_ship:
            key = str(ship or "").strip().casefold()
            if key in ship_seen:
                continue
            ship_seen.add(key)
            ship_unique.append(str(ship or "").strip())
        if len(ship_unique) == 1:
            shared_ship = ship_unique[0]
            line_rows = [
                (row[0], row[1], row[2], shared_ship if not str(row[3] or "").strip() else str(row[3] or "").strip())
                for row in line_rows
            ]
        line_rows = _dedupe_part_detail_rows(line_rows)

        appended_new_row = False
        if incoming_key:
            replaced = False
            for idx, row in enumerate(line_rows):
                if _part_detail_row_key(*row) == incoming_key:
                    line_rows[idx] = incoming_row
                    replaced = True
                    break
            if not replaced:
                line_rows.append(incoming_row)
                appended_new_row = True
        line_rows = _dedupe_part_detail_rows(line_rows)

        installed_key_set = _installed_key_set_from_text(str(existing["installed_keys"] or "").strip())

        retained_installed_keys: list[str] = []
        for row in line_rows:
            key = _part_detail_row_key(row[0], row[1], row[2], row[3])
            if key in installed_key_set:
                retained_installed_keys.append(key)

        merged_lpn, merged_part_number, merged_part_desc, merged_shipping = _serialize_part_detail_rows(line_rows)
        merged_installed_keys = (
            json.dumps(retained_installed_keys, ensure_ascii=True, separators=(",", ":"))
            if retained_installed_keys
            else ""
        )
        delivered_value = 1 if (bool(delivered) or bool(int(existing["delivered"] or 0))) else 0
        with self.db.write_transaction("tracker.upsert_part_detail"):
            self.db.execute(
                "UPDATE part_details SET lpn=?, part_number=?, part_description=?, shipping_info=?, installed_keys=?, delivered=? WHERE part_id=?",
                (
                    merged_lpn,
                    merged_part_number,
                    merged_part_desc,
                    merged_shipping,
                    merged_installed_keys,
                    delivered_value,
                    int(part_id),
                ),
            )
            if appended_new_row:
                # New delivered rows become pending install by default.
                self.db.execute(
                    "UPDATE parts SET parts_installed=0, parts_installed_by='', parts_installed_at='' WHERE id=?",
                    (int(part_id),),
                )

    def find_active_parts_by_work_orders(self, work_orders: list[str]) -> dict[str, int]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in work_orders:
            work_order = DepotRules.normalize_work_order(str(raw_value or ""))
            if not work_order or work_order in seen:
                continue
            seen.add(work_order)
            normalized.append(work_order)
        if not normalized:
            return {}

        by_work_order: dict[str, int] = {}
        chunk_size = 400
        for start in range(0, len(normalized), chunk_size):
            chunk = normalized[start : start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.db.fetchall(
                f"SELECT id, work_order FROM parts WHERE is_active=1 AND work_order IN ({placeholders}) "
                "ORDER BY id DESC",
                tuple(chunk),
            )
            for row in rows:
                existing_work_order = DepotRules.normalize_work_order(str(row["work_order"] or ""))
                if not existing_work_order or existing_work_order in by_work_order:
                    continue
                by_work_order[existing_work_order] = int(row["id"])
        return by_work_order

    def get_active_part_qa_flag(self, work_order: str) -> str:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return ""
        row = self.db.fetchone(
            "SELECT COALESCE(qa_flag, '') AS qa_flag FROM parts WHERE is_active=1 AND work_order=? ORDER BY id DESC LIMIT 1",
            (normalized_work_order,),
        )
        if row is None:
            return ""
        return str(row["qa_flag"] or "").strip()

    def update_part_agent_comment(self, part_id: int, agent_comment: str) -> None:
        self.db.execute(
            "UPDATE parts SET agent_comment=? WHERE id=?",
            (str(agent_comment or "").strip(), int(part_id)),
        )

    def set_part_working_user(self, part_id: int, working_user_id: str) -> None:
        normalized = DepotRules.normalize_user_id(working_user_id)
        stamp = datetime.now().isoformat(timespec="seconds") if normalized else ""
        quiet_until = self.next_alert_quiet_until() if normalized else ""
        # One agent may actively "work" only one part row at a time.
        with self.db.write_transaction("tracker.set_part_working_user"):
            if normalized:
                self.db.execute(
                    "UPDATE parts SET working_user_id='', working_updated_at='' "
                    "WHERE working_user_id=? AND id<>?",
                    (normalized, int(part_id)),
                )
            self.db.execute(
                "UPDATE parts SET working_user_id=?, working_updated_at=?, alert_quiet_until=? WHERE id=?",
                (normalized, stamp, quiet_until, int(part_id)),
            )

    def set_part_installed(self, part_id: int, installed_row_keys: list[str] | tuple[str, ...] | set[str], actor_user_id: str = "") -> None:
        detail = self.db.fetchone(
            "SELECT COALESCE(lpn, '') AS lpn, COALESCE(part_number, '') AS part_number, "
            "COALESCE(part_description, '') AS part_description, COALESCE(shipping_info, '') AS shipping_info "
            "FROM part_details WHERE part_id=?",
            (int(part_id),),
        )
        if detail is None:
            raise ValueError("Delivered part details were not found for the selected work order.")

        detail_rows = _dedupe_part_detail_rows(
            _merged_part_detail_rows(
                str(detail["lpn"] or ""),
                str(detail["part_number"] or ""),
                str(detail["part_description"] or ""),
                str(detail["shipping_info"] or ""),
            )
        )
        valid_keys = {
            row_key
            for row_key in (_part_detail_row_key(*row) for row in detail_rows)
            if row_key
        }
        selected_keys: list[str] = []
        seen: set[str] = set()
        for value in installed_row_keys:
            row_key = str(value or "").strip()
            if not row_key or row_key not in valid_keys or row_key in seen:
                continue
            seen.add(row_key)
            selected_keys.append(row_key)

        actor = DepotRules.normalize_user_id(actor_user_id)
        stamp = datetime.now().isoformat(timespec="seconds")
        installed = bool(selected_keys)
        with self.db.write_transaction("tracker.set_part_installed"):
            self.db.execute(
                "UPDATE part_details SET installed_keys=? WHERE part_id=?",
                (_serialized_installed_keys(selected_keys), int(part_id)),
            )
            self.db.execute(
                "UPDATE parts SET parts_on_hand=1, parts_installed=?, parts_installed_by=?, parts_installed_at=?, "
                "working_user_id='', working_updated_at='', alert_quiet_until=? WHERE id=?",
                (
                    1 if installed else 0,
                    actor if installed else "",
                    stamp if installed else "",
                    self.next_alert_quiet_until(),
                    int(part_id),
                ),
            )

    def reopen_completed_part(self, part_id: int, actor_user_id: str) -> dict[str, Any]:
        actor = self._require_dashboard_submission_admin(actor_user_id)
        target_id = int(max(0, safe_int(part_id, 0)))
        if target_id <= 0:
            raise ValueError("A valid completed part row is required.")
        part = self.db.fetchone(
            "SELECT id, COALESCE(work_order, '') AS work_order, COALESCE(assigned_user_id, '') AS assigned_user_id, "
            "COALESCE(category, '') AS category, COALESCE(client_unit, 0) AS client_unit, COALESCE(is_active, 0) AS is_active "
            "FROM parts WHERE id=?",
            (target_id,),
        )
        if part is None:
            raise ValueError("Selected completed part row no longer exists.")
        if int(max(0, safe_int(part["is_active"], 0))) != 0:
            raise ValueError("Selected part row is already active.")

        work_order = DepotRules.normalize_work_order(str(part["work_order"] or ""))
        if not work_order:
            raise ValueError("Selected completed part row is missing a work order.")
        assigned_user = DepotRules.normalize_user_id(str(part["assigned_user_id"] or ""))
        submission_user = assigned_user or actor
        category = str(part["category"] or "").strip()
        client_unit_int = int(max(0, safe_int(part["client_unit"], 0)))
        stamp_dt = datetime.now()
        stamp = stamp_dt.isoformat(timespec="seconds")
        entry_date = stamp_dt.date().isoformat()

        with self.db.write_transaction("tracker.reopen_completed_part"):
            submission_id = self._upsert_same_day_submission(
                entry_date,
                submission_user,
                work_order,
                DepotRules.TOUCH_PART_ORDER,
                category,
                client_unit_int,
                stamp,
                "",
            )
            self.db.execute(
                "UPDATE parts SET is_active=1, assigned_user_id=?, source_submission_id=?, "
                "parts_installed=0, parts_installed_by='', parts_installed_at='', "
                "working_user_id='', working_updated_at='', alert_quiet_until='' WHERE id=?",
                (submission_user, int(submission_id), target_id),
            )
            self.db.execute("UPDATE part_details SET installed_keys='' WHERE part_id=?", (target_id,))
            if client_unit_int:
                existing_client_row = self.db.fetchone("SELECT id FROM client_parts WHERE work_order=?", (work_order,))
                if existing_client_row is None:
                    self.db.execute(
                        "INSERT INTO client_parts (created_at, user_id, work_order, comments) VALUES (?, ?, ?, ?)",
                        (stamp, submission_user, work_order, ""),
                    )
                else:
                    self.db.execute(
                        "UPDATE client_parts SET user_id=?, created_at=? WHERE work_order=?",
                        (submission_user, stamp, work_order),
                    )

        _runtime_log_event(
            "depot.completed_part_reopened",
            severity="warning",
            summary="A dashboard completed part row was reopened into the active Part Order queue.",
            context={
                "part_id": int(target_id),
                "work_order": work_order,
                "actor_user_id": actor,
                "assigned_user_id": submission_user,
                "source_submission_id": int(submission_id),
                "client_unit": bool(client_unit_int),
            },
        )
        return {
            "part_id": int(target_id),
            "work_order": work_order,
            "assigned_user_id": submission_user,
            "source_submission_id": int(submission_id),
            "client_unit": bool(client_unit_int),
        }

    def mark_client_followup_action(self, client_part_id: int, action: str, actor_user_id: str) -> int:
        normalized_action = DepotRules.normalize_followup_action(action)
        if not normalized_action:
            raise ValueError("Invalid follow-up action.")
        row = self.db.fetchone(
            "SELECT COALESCE(followup_no_contact_count, 0) AS followup_no_contact_count, "
            "COALESCE(followup_last_action, '') AS followup_last_action, "
            "COALESCE(followup_last_action_at, '') AS followup_last_action_at, "
            "COALESCE(comments, '') AS comments "
            "FROM client_parts WHERE id=?",
            (int(client_part_id),),
        )
        if row is None:
            raise ValueError("Client follow-up row no longer exists.")
        existing_count = int(max(0, safe_int(row["followup_no_contact_count"], 0)))
        previous_action = DepotRules.normalize_followup_action(str(row["followup_last_action"] or ""))
        previous_stamp = str(row["followup_last_action_at"] or "").strip()
        previous_day = previous_stamp[:10] if len(previous_stamp) >= 10 else ""
        stamp_dt = datetime.now()
        stamp = stamp_dt.isoformat(timespec="seconds")
        today_iso = stamp_dt.date().isoformat()
        same_day_replacement = bool(previous_day == today_iso)

        if normalized_action in DepotRules.CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS:
            if same_day_replacement:
                if previous_action in DepotRules.CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS:
                    existing_count = int(max(1, existing_count))
                else:
                    existing_count = 1
            else:
                existing_count += 1
        else:
            existing_count = 0

        actor = DepotRules.normalize_user_id(actor_user_id)
        action_tag = f"Follow-up {today_iso}: {normalized_action} - [{actor or 'UNKNOWN'}]"
        stage_logged = -1
        existing_comments = str(row["comments"] or "").strip()
        lines = [line.rstrip() for line in existing_comments.splitlines() if str(line).strip()]
        action_prefix = f"Follow-up {today_iso}:"
        stage_prefix = f"Waiting Stage {today_iso}:"
        lines = [line for line in lines if not line.startswith(action_prefix)]
        lines = [line for line in lines if not line.startswith(stage_prefix)]
        stage_lines: list[str] = [action_tag]
        if normalized_action in DepotRules.CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS:
            stage_logged = 0
            stage_lines.append(f"{stage_prefix} {DepotRules.followup_stage_label(0)} - [SYSTEM]")
        lines.extend(stage_lines)
        updated_comments = "\n".join([line for line in lines if str(line).strip()])
        self.db.execute(
            "UPDATE client_parts SET followup_last_action=?, followup_last_action_at=?, "
            "followup_last_actor=?, followup_no_contact_count=?, followup_stage_logged=?, comments=?, alert_quiet_until=? WHERE id=?",
            (
                normalized_action,
                stamp,
                actor,
                int(existing_count),
                int(stage_logged),
                updated_comments,
                self.next_alert_quiet_until(),
                int(client_part_id),
            ),
        )
        return int(existing_count)

    def update_client_followup_stage(self, client_part_id: int, stage_index: int) -> str:
        next_stage = int(clamp(int(stage_index), 0, 2))
        row = self.db.fetchone(
            "SELECT COALESCE(comments, '') AS comments, "
            "COALESCE(followup_stage_logged, -1) AS followup_stage_logged "
            "FROM client_parts WHERE id=?",
            (int(client_part_id),),
        )
        if row is None:
            raise ValueError("Client follow-up row no longer exists.")
        current_stage = int(safe_int(row["followup_stage_logged"], -1))
        current_comments = str(row["comments"] or "").strip()
        if next_stage <= current_stage:
            return current_comments

        stamp_day = datetime.now().date().isoformat()
        stage_prefix = f"Waiting Stage {stamp_day}:"
        chunks: list[str] = [line.rstrip() for line in current_comments.splitlines() if str(line).strip()]
        chunks = [line for line in chunks if not line.startswith(stage_prefix)]
        for idx in range(max(0, current_stage + 1), next_stage + 1):
            chunks.append(f"{stage_prefix} {DepotRules.followup_stage_label(idx)} - [SYSTEM]")
        updated_comments = "\n".join([chunk for chunk in chunks if str(chunk).strip()])
        self.db.execute(
            "UPDATE client_parts SET followup_stage_logged=?, comments=? WHERE id=?",
            (int(next_stage), updated_comments, int(client_part_id)),
        )
        return updated_comments

    def update_part_qa_fields(self, part_id: int, qa_comment: str, qa_flag: str) -> None:
        qa_flag_text = str(qa_flag or "").strip()
        if qa_flag_text.lower() == "none":
            qa_flag_text = ""
        qa_comment_text = str(qa_comment or "").strip()
        self.db.execute(
            "UPDATE parts SET qa_comment=?, comments=?, qa_flag=?, qa_flag_image_path=? WHERE id=?",
            (qa_comment_text, qa_comment_text, qa_flag_text, "", int(part_id)),
        )

    def get_dashboard_metrics(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        user_id: str | None = None,
        touch: str | None = None,
        client_only: bool | None = None,
        category: str | None = None,
        include_latest_workload_mix: bool = False,
    ) -> dict[str, Any]:
        alias = "s0"
        where_clause, params, entry_date_expr = self._build_submission_metrics_filter(
            alias=alias,
            start_date=start_date,
            end_date=end_date,
            user_id=user_id,
            touch=touch,
            client_only=client_only,
            category=category,
        )
        from_clause = f"FROM submissions {alias}"
        touch_mix_metrics = self._collect_submission_touch_mix_metrics(
            alias=alias,
            where_clause=where_clause,
            params=params,
            include_latest_workload_mix=include_latest_workload_mix,
        )
        total_submissions = int(max(0, safe_int(touch_mix_metrics.get("total_submissions", 0), 0)))
        total_units = int(max(0, safe_int(touch_mix_metrics.get("total_units", 0), 0)))
        by_touch_map = touch_mix_metrics.get("by_touch", {})
        if not isinstance(by_touch_map, dict):
            by_touch_map = {}
        latest_by_touch_map = touch_mix_metrics.get("latest_by_touch", {})
        if not isinstance(latest_by_touch_map, dict):
            latest_by_touch_map = {}
        by_user = self.db.fetchall(
            f"SELECT {alias}.user_id AS user_id, COUNT(*) AS c {from_clause} {where_clause} GROUP BY {alias}.user_id",
            tuple(params),
        )
        daily = self.db.fetchall(
            f"SELECT {entry_date_expr} AS entry_date, COUNT(*) AS c {from_clause} {where_clause} "
            f"GROUP BY {entry_date_expr} ORDER BY {entry_date_expr} DESC LIMIT 30",
            tuple(params),
        )
        trend_daily = self.db.fetchall(
            f"""
            WITH filtered AS (
                SELECT
                    {entry_date_expr} AS effective_date,
                    s0.work_order AS work_order,
                    s0.touch AS touch
                {from_clause}
                {where_clause}
            )
            SELECT
                effective_date AS entry_date,
                COUNT(*) AS total_rows,
                COUNT(DISTINCT work_order) AS units,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS complete,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS junk,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS part_order,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS rtv,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS triaged,
                SUM(CASE WHEN touch = ? THEN 1 ELSE 0 END) AS other_units
            FROM filtered
            WHERE COALESCE(effective_date, '') <> ''
            GROUP BY effective_date
            ORDER BY effective_date ASC
            """,
            (
                *params,
                DepotRules.TOUCH_COMPLETE,
                DepotRules.TOUCH_JUNK,
                DepotRules.TOUCH_PART_ORDER,
                DepotRules.TOUCH_RTV,
                "Triaged",
                DepotRules.TOUCH_OTHER,
            ),
        )

        if start_date and end_date:
            try:
                start_dt = datetime.strptime(str(start_date), "%Y-%m-%d").date()
                end_dt = datetime.strptime(str(end_date), "%Y-%m-%d").date()
                day_span = max(1, (end_dt - start_dt).days + 1)
            except Exception:
                day_span = max(1, len(daily))
        else:
            day_span = max(1, len(daily))

        complete_count = int(max(0, safe_int(by_touch_map.get(DepotRules.TOUCH_COMPLETE, 0), 0)))
        junk_count = int(max(0, safe_int(by_touch_map.get(DepotRules.TOUCH_JUNK, 0), 0)))
        part_order_count = int(max(0, safe_int(by_touch_map.get(DepotRules.TOUCH_PART_ORDER, 0), 0)))
        rtv_count = int(max(0, safe_int(by_touch_map.get(DepotRules.TOUCH_RTV, 0), 0)))
        triaged_count = int(max(0, safe_int(by_touch_map.get("Triaged", 0), 0)))
        other_touch_count = int(max(0, safe_int(by_touch_map.get(DepotRules.TOUCH_OTHER, 0), 0)))
        latest_complete_count = int(max(0, safe_int(latest_by_touch_map.get(DepotRules.TOUCH_COMPLETE, 0), 0)))
        latest_junk_count = int(max(0, safe_int(latest_by_touch_map.get(DepotRules.TOUCH_JUNK, 0), 0)))
        latest_part_order_count = int(max(0, safe_int(latest_by_touch_map.get(DepotRules.TOUCH_PART_ORDER, 0), 0)))
        latest_rtv_count = int(max(0, safe_int(latest_by_touch_map.get(DepotRules.TOUCH_RTV, 0), 0)))
        latest_triaged_count = int(max(0, safe_int(latest_by_touch_map.get("Triaged", 0), 0)))
        latest_other_touch_count = int(max(0, safe_int(latest_by_touch_map.get(DepotRules.TOUCH_OTHER, 0), 0)))

        return {
            "total_submissions": int(total_submissions),
            "total_units": int(total_units),
            "by_touch": by_touch_map,
            "latest_by_touch": {str(key): int(max(0, safe_int(value, 0))) for key, value in latest_by_touch_map.items()},
            "by_user": {str(row["user_id"] or "").strip(): int(max(0, safe_int(row["c"], 0))) for row in by_user},
            "daily": [{"entry_date": str(r["entry_date"] or ""), "count": int(max(0, safe_int(r["c"], 0)))} for r in daily],
            "trend_daily": [
                {
                    "entry_date": str(row["entry_date"] or ""),
                    "total_rows": int(max(0, safe_int(row["total_rows"], 0))),
                    "units": int(max(0, safe_int(row["units"], 0))),
                    "complete": int(max(0, safe_int(row["complete"], 0))),
                    "junk": int(max(0, safe_int(row["junk"], 0))),
                    "part_order": int(max(0, safe_int(row["part_order"], 0))),
                    "rtv": int(max(0, safe_int(row["rtv"], 0))),
                    "triaged": int(max(0, safe_int(row["triaged"], 0))),
                    "other": int(max(0, safe_int(row["other_units"], 0))),
                }
                for row in trend_daily
            ],
            "day_span": int(day_span),
            "complete_count": int(complete_count),
            "junk_count": int(junk_count),
            "part_order_count": int(part_order_count),
            "rtv_count": int(rtv_count),
            "triaged_count": int(triaged_count),
            "other_touch_count": int(other_touch_count),
            "latest_complete_count": int(latest_complete_count),
            "latest_junk_count": int(latest_junk_count),
            "latest_part_order_count": int(latest_part_order_count),
            "latest_rtv_count": int(latest_rtv_count),
            "latest_triaged_count": int(latest_triaged_count),
            "latest_other_touch_count": int(latest_other_touch_count),
            "avg_submission_rows_per_day": float(total_submissions / day_span) if day_span > 0 else 0.0,
            "avg_units_per_day": float(total_units / day_span) if day_span > 0 else 0.0,
            "avg_complete_per_day": float(complete_count / day_span) if day_span > 0 else 0.0,
            "avg_junk_per_day": float(junk_count / day_span) if day_span > 0 else 0.0,
            "active_client_follow_up": int(self.db.fetchone("SELECT COUNT(*) AS c FROM client_parts", ())["c"]),
            "active_parts": int(self.db.fetchone("SELECT COUNT(*) AS c FROM parts WHERE is_active=1", ())["c"]),
            "rtv_log_count": int(self.db.fetchone("SELECT COUNT(*) AS c FROM rtvs", ())["c"]),
            "client_jo_count": int(self.db.fetchone("SELECT COUNT(*) AS c FROM client_jo", ())["c"]),
        }

__all__ = [
    "DepotDB",
    "DepotRefreshCoordinator",
    "DepotRefreshViewState",
    "DepotSchema",
    "DepotTracker",
    "QA_FLAG_OPTIONS",
    "QA_FLAG_SEVERITY_OPTIONS",
    "TRACKER_DASHBOARD_TABLES",
]
