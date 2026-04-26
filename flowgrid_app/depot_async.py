from __future__ import annotations

import queue
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import QTimer

from flowgrid_app.runtime_logging import _json_safe, _runtime_log_event
from flowgrid_app.workflow_core import DepotDB, DepotTracker


DepotWorkerLoader = Callable[[DepotTracker], Any]
DepotLoadCallback = Callable[["DepotLoadResult"], None]


@dataclass(frozen=True, slots=True)
class DepotLoadRequest:
    request_id: str
    view_key: str
    state_key: str
    reason: str
    generation: int
    force: bool
    started_ms: float


@dataclass(slots=True)
class DepotLoadResult:
    request: DepotLoadRequest
    ok: bool
    payload: Any = None
    duration_ms: float = 0.0
    error_type: str = ""
    error_message: str = ""
    log_path: str = ""


def _now_ms() -> float:
    return time.monotonic() * 1000.0


def _plain_payload(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, sqlite3.Row):
        return {str(key): _plain_payload(value[key]) for key in value.keys()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _plain_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain_payload(item) for item in value]
    return _json_safe(value)


class DepotAsyncLoadCoordinator:
    """Run shared workflow read loads off the UI thread with per-thread SQLite connections."""

    def __init__(self, db_path: Path, *, parent: Any | None = None, poll_interval_ms: int = 80) -> None:
        self.db_path = Path(db_path)
        self._result_queue: queue.Queue[DepotLoadResult] = queue.Queue()
        self._callbacks: dict[str, tuple[DepotLoadCallback, DepotLoadCallback]] = {}
        self._requests: dict[str, DepotLoadRequest] = {}
        self._active_by_view: dict[str, DepotLoadRequest] = {}
        self._generation_by_view: dict[str, int] = {}
        self._cancelled_ids: set[str] = set()
        self._closed = False
        self._lock = threading.Lock()
        self._timer = QTimer(parent)
        self._timer.setInterval(int(max(25, poll_interval_ms)))
        self._timer.timeout.connect(self._drain_results)
        self._timer.start()

    @staticmethod
    def normalize_state_key(state_key: Any) -> str:
        if isinstance(state_key, str):
            return state_key
        try:
            import json

            return json.dumps(_json_safe(state_key), sort_keys=True, ensure_ascii=True, separators=(",", ":"))
        except Exception:
            return repr(state_key)

    @staticmethod
    def _normalize_view_key(view_key: str) -> str:
        return str(view_key or "").strip()

    def start_read(
        self,
        view_key: str,
        state_key: Any,
        *,
        reason: str = "",
        force: bool = False,
        loader: DepotWorkerLoader,
        on_success: DepotLoadCallback,
        on_error: DepotLoadCallback,
    ) -> DepotLoadRequest | None:
        normalized_view = self._normalize_view_key(view_key)
        if not normalized_view:
            raise ValueError("Async depot read requires a view key.")
        normalized_state = self.normalize_state_key(state_key)

        with self._lock:
            if self._closed:
                _runtime_log_event(
                    "depot.async_read_start_blocked_closed",
                    severity="warning",
                    summary="Blocked a shared workflow background read because the coordinator is closed.",
                    context={"view": normalized_view, "reason": str(reason or "")},
                )
                return None

            active = self._active_by_view.get(normalized_view)
            if (
                active is not None
                and not bool(force)
                and active.state_key == normalized_state
                and active.request_id not in self._cancelled_ids
            ):
                return active

            self._cancel_view_locked(normalized_view, bump_generation=False)
            generation = int(self._generation_by_view.get(normalized_view, 0)) + 1
            self._generation_by_view[normalized_view] = generation
            request = DepotLoadRequest(
                request_id=uuid.uuid4().hex,
                view_key=normalized_view,
                state_key=normalized_state,
                reason=str(reason or ""),
                generation=generation,
                force=bool(force),
                started_ms=_now_ms(),
            )
            self._requests[request.request_id] = request
            self._active_by_view[normalized_view] = request
            self._callbacks[request.request_id] = (on_success, on_error)

        thread = threading.Thread(
            target=self._run_worker,
            args=(request, loader),
            name=f"flowgrid-depot-read-{normalized_view}",
            daemon=True,
        )
        thread.start()
        return request

    def _run_worker(self, request: DepotLoadRequest, loader: DepotWorkerLoader) -> None:
        worker_db: DepotDB | None = None
        started = time.monotonic()
        try:
            worker_db = DepotDB(self.db_path, read_only=True, ensure_schema=False)
            tracker = DepotTracker(
                worker_db,
                startup_repairs_enabled=False,
                allow_metadata_repairs=False,
            )
            payload = _plain_payload(loader(tracker))
            result = DepotLoadResult(
                request=request,
                ok=True,
                payload=payload,
                duration_ms=(time.monotonic() - started) * 1000.0,
            )
        except Exception as exc:
            log_path = _runtime_log_event(
                "depot.async_read_failed",
                severity="warning",
                summary="A shared workflow background read failed.",
                exc=exc,
                context={
                    "db_path": str(self.db_path),
                    "view": request.view_key,
                    "reason": request.reason,
                    "state_key": request.state_key[:500],
                    "generation": int(request.generation),
                    "duration_ms": int(max(0.0, (time.monotonic() - started) * 1000.0)),
                },
            )
            result = DepotLoadResult(
                request=request,
                ok=False,
                duration_ms=(time.monotonic() - started) * 1000.0,
                error_type=type(exc).__name__,
                error_message=str(exc),
                log_path=str(log_path) if log_path is not None else "",
            )
        finally:
            if worker_db is not None:
                try:
                    worker_db.close(f"async_read:{request.view_key}")
                except Exception as exc:
                    _runtime_log_event(
                        "depot.async_read_close_failed",
                        severity="warning",
                        summary="Closing a shared workflow background read connection failed.",
                        exc=exc,
                        context={"db_path": str(self.db_path), "view": request.view_key},
                    )
        self._result_queue.put(result)

    def _is_current_locked(self, request: DepotLoadRequest) -> bool:
        if self._closed or request.request_id in self._cancelled_ids:
            return False
        active = self._active_by_view.get(request.view_key)
        if active is None:
            return False
        return bool(
            active.request_id == request.request_id
            and active.generation == request.generation
            and active.state_key == request.state_key
            and int(self._generation_by_view.get(request.view_key, 0)) == int(request.generation)
        )

    def _drain_results(self) -> None:
        while True:
            try:
                result = self._result_queue.get_nowait()
            except queue.Empty:
                break

            success_callback: DepotLoadCallback | None = None
            error_callback: DepotLoadCallback | None = None
            with self._lock:
                request = result.request
                callbacks = self._callbacks.pop(request.request_id, None)
                is_current = self._is_current_locked(request)
                self._requests.pop(request.request_id, None)
                active = self._active_by_view.get(request.view_key)
                if active is not None and active.request_id == request.request_id:
                    self._active_by_view.pop(request.view_key, None)
                if callbacks is None or not is_current:
                    self._cancelled_ids.discard(request.request_id)
                    continue
                success_callback, error_callback = callbacks
                self._cancelled_ids.discard(request.request_id)

            try:
                if result.ok:
                    success_callback(result)
                else:
                    error_callback(result)
            except Exception as exc:
                _runtime_log_event(
                    "depot.async_read_callback_failed",
                    severity="error",
                    summary="Applying a shared workflow background read result failed.",
                    exc=exc,
                    context={
                        "view": result.request.view_key,
                        "reason": result.request.reason,
                        "state_key": result.request.state_key[:500],
                        "generation": int(result.request.generation),
                    },
                )

    def cancel_view(self, view_key: str, *, reason: str = "") -> None:
        normalized_view = self._normalize_view_key(view_key)
        if not normalized_view:
            return
        with self._lock:
            self._cancel_view_locked(normalized_view, bump_generation=True)
        _runtime_log_event(
            "depot.async_read_view_cancelled",
            severity="info",
            summary="Cancelled pending shared workflow background reads for a view.",
            context={"view": normalized_view, "reason": str(reason or "")},
        )

    def _cancel_view_locked(self, normalized_view: str, *, bump_generation: bool) -> None:
        if bump_generation:
            self._generation_by_view[normalized_view] = int(self._generation_by_view.get(normalized_view, 0)) + 1
        for request_id, request in list(self._requests.items()):
            if request.view_key != normalized_view:
                continue
            self._cancelled_ids.add(request_id)
            self._callbacks.pop(request_id, None)
            self._requests.pop(request_id, None)
        self._active_by_view.pop(normalized_view, None)

    def cancel_all(self, *, reason: str = "") -> None:
        with self._lock:
            for request_id in list(self._requests.keys()):
                self._cancelled_ids.add(request_id)
            self._callbacks.clear()
            self._requests.clear()
            self._active_by_view.clear()
            self._closed = True
        try:
            self._timer.stop()
        except Exception:
            pass
        _runtime_log_event(
            "depot.async_reads_cancelled",
            severity="info",
            summary="Cancelled all pending shared workflow background reads.",
            context={"reason": str(reason or "")},
        )


__all__ = [
    "DepotAsyncLoadCoordinator",
    "DepotLoadRequest",
    "DepotLoadResult",
]
