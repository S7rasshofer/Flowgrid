from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable
import getpass
import json
import os
import re
import socket
import sys
import traceback


RUNTIME_LOG_FILENAME_PREFIX = "Flowgrid_runtime"
RUNTIME_LOG_FILENAME_SUFFIX = ".log.jsonl"
RUNTIME_LOG_MAX_BYTES = 10 * 1024 * 1024
RUNTIME_LOG_MAX_BACKUPS = 20
RUNTIME_LOG_DIRNAME = "Logs"

_RUNTIME_LOG_WRITE_IN_PROGRESS = False
_RUNTIME_LOG_DIR_PROVIDER: Callable[[], Path] | None = None
_LAUNCH_LOG_ERROR_CALLBACK: Callable[[str, str, str], None] | None = None
_SAFE_PRINT_CALLBACK: Callable[[str], None] | None = None
_DETECT_CURRENT_USER_ID_CALLBACK: Callable[[], str] | None = None


def configure_runtime_logging(
    *,
    log_dir_provider: Callable[[], Path] | None = None,
    launch_log_error_callback: Callable[[str, str, str], None] | None = None,
    safe_print_callback: Callable[[str], None] | None = None,
    detect_current_user_id_callback: Callable[[], str] | None = None,
) -> None:
    global _RUNTIME_LOG_DIR_PROVIDER, _LAUNCH_LOG_ERROR_CALLBACK, _SAFE_PRINT_CALLBACK, _DETECT_CURRENT_USER_ID_CALLBACK
    _RUNTIME_LOG_DIR_PROVIDER = log_dir_provider
    _LAUNCH_LOG_ERROR_CALLBACK = launch_log_error_callback
    _SAFE_PRINT_CALLBACK = safe_print_callback
    _DETECT_CURRENT_USER_ID_CALLBACK = detect_current_user_id_callback


def _emit_launch_log_error(code: str, summary: str, details: str = "") -> None:
    callback = _LAUNCH_LOG_ERROR_CALLBACK
    if callback is not None:
        try:
            callback(code, summary, details)
            return
        except Exception:
            pass
    try:
        sys.stderr.write(f"[{code}] {summary}\n")
        if details:
            sys.stderr.write(f"{details}\n")
    except Exception:
        pass


def _safe_print(message: str) -> None:
    callback = _SAFE_PRINT_CALLBACK
    if callback is not None:
        try:
            callback(str(message))
            return
        except Exception:
            pass
    try:
        print(message)
    except Exception:
        pass


def detect_current_user_id() -> str:
    callback = _DETECT_CURRENT_USER_ID_CALLBACK
    if callback is not None:
        try:
            value = str(callback() or "").strip()
            if value:
                return value.upper()
        except Exception as exc:
            _emit_launch_log_error(
                "TH-9810",
                "Custom user detection callback failed.",
                f"Reason: {type(exc).__name__}: {exc}",
            )

    candidates = [
        os.environ.get("USERNAME", ""),
        os.environ.get("USER", ""),
        os.environ.get("LOGNAME", ""),
    ]
    try:
        candidates.append(getpass.getuser() or "")
    except Exception as exc:
        _emit_launch_log_error(
            "TH-9811",
            "getpass.getuser() failed while detecting the current user ID.",
            f"Reason: {type(exc).__name__}: {exc}",
        )
    for raw in candidates:
        value = str(raw or "").strip()
        if value:
            return value.upper()
    return "UNKNOWN"


def _sanitize_log_filename_component(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return "UNKNOWN"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    cleaned = cleaned.strip("._-")
    return cleaned or "UNKNOWN"


def _runtime_log_dir() -> Path:
    if _RUNTIME_LOG_DIR_PROVIDER is not None:
        try:
            path = Path(_RUNTIME_LOG_DIR_PROVIDER())
            path.mkdir(parents=True, exist_ok=True)
            return path
        except Exception as exc:
            _emit_launch_log_error(
                "TH-9802",
                "Configured runtime log directory unavailable.",
                f"Reason: {type(exc).__name__}: {exc}",
            )
    fallback = Path.cwd() / RUNTIME_LOG_DIRNAME
    try:
        fallback.mkdir(parents=True, exist_ok=True)
    except Exception:
        return Path.cwd()
    return fallback


def _runtime_log_path(user_id: str | None = None) -> Path:
    normalized_user = _sanitize_log_filename_component(user_id or detect_current_user_id())
    filename = f"{RUNTIME_LOG_FILENAME_PREFIX}_{normalized_user}{RUNTIME_LOG_FILENAME_SUFFIX}"
    return _runtime_log_dir() / filename


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return str(value)


def _brief_runtime_context(context: dict[str, Any] | None) -> str:
    if not context:
        return ""
    parts: list[str] = []
    for key, value in list(context.items())[:6]:
        rendered = str(_json_safe(value))
        if len(rendered) > 120:
            rendered = rendered[:117] + "..."
        parts.append(f"{key}={rendered}")
    return "; ".join(parts)


def _rotate_runtime_log(path: Path) -> None:
    if not path.exists():
        return
    try:
        if path.stat().st_size < RUNTIME_LOG_MAX_BYTES:
            return
    except Exception as exc:
        _emit_launch_log_error(
            "TH-9804",
            "Runtime log rotation stat check failed.",
            f"Path: {path}\nReason: {type(exc).__name__}: {exc}",
        )
        return

    oldest = path.with_name(f"{path.name}.{RUNTIME_LOG_MAX_BACKUPS}")
    if oldest.exists():
        try:
            oldest.unlink()
        except Exception as exc:
            _emit_launch_log_error(
                "TH-9805",
                "Runtime log rotation failed deleting oldest backup.",
                f"Path: {oldest}\nReason: {type(exc).__name__}: {exc}",
            )

    for idx in range(RUNTIME_LOG_MAX_BACKUPS - 1, 0, -1):
        src = path.with_name(f"{path.name}.{idx}")
        if not src.exists():
            continue
        dst = path.with_name(f"{path.name}.{idx + 1}")
        try:
            src.replace(dst)
        except Exception as exc:
            _emit_launch_log_error(
                "TH-9806",
                "Runtime log rotation failed while shifting backups.",
                f"Source: {src}\nTarget: {dst}\nReason: {type(exc).__name__}: {exc}",
            )

    first = path.with_name(f"{path.name}.1")
    try:
        path.replace(first)
    except Exception as exc:
        _emit_launch_log_error(
            "TH-9807",
            "Runtime log rotation failed moving active log to first backup.",
            f"Source: {path}\nTarget: {first}\nReason: {type(exc).__name__}: {exc}",
        )


def _write_runtime_log_entry(path: Path, entry: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _rotate_runtime_log(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _runtime_log_event(
    event_key: str,
    severity: str = "warning",
    *,
    summary: str = "",
    exc: BaseException | None = None,
    context: dict[str, Any] | None = None,
) -> Path | None:
    global _RUNTIME_LOG_WRITE_IN_PROGRESS
    if _RUNTIME_LOG_WRITE_IN_PROGRESS:
        return None

    _RUNTIME_LOG_WRITE_IN_PROGRESS = True
    try:
        user_id = detect_current_user_id()
        payload: dict[str, Any] = {
            "timestamp_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "event_key": str(event_key),
            "severity": str(severity or "warning"),
            "user_id": str(user_id),
            "pid": int(os.getpid()),
            "host": str(socket.gethostname() or ""),
            "exception_type": "",
            "exception_message": "",
            "traceback": "",
        }
        if summary:
            payload["summary"] = str(summary)
        if exc is not None:
            payload["exception_type"] = type(exc).__name__
            payload["exception_message"] = str(exc)
            payload["traceback"] = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        if context:
            payload["context"] = _json_safe(context)
        log_path = _runtime_log_path(user_id)
        _write_runtime_log_entry(log_path, payload)
        return log_path
    except Exception as log_exc:
        try:
            _emit_launch_log_error(
                "TH-9801",
                "Runtime log write failed.",
                f"Event key: {event_key}\nReason: {type(log_exc).__name__}: {log_exc}",
            )
        except Exception:
            try:
                _safe_print(f"[Flowgrid] Runtime log write failed and launch log fallback also failed: {log_exc}")
            except Exception:
                return None
        return None
    finally:
        _RUNTIME_LOG_WRITE_IN_PROGRESS = False


__all__ = [
    "_brief_runtime_context",
    "_json_safe",
    "_runtime_log_event",
    "_runtime_log_path",
    "configure_runtime_logging",
    "detect_current_user_id",
]
