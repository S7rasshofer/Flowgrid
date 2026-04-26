#!/usr/bin/env python3
"""
Flowgrid Installer - Standalone GitHub bootstrap installer and updater.

Deployment contract:
- Shared drive bootstrap can start from only Flowgrid_installer.pyw
- Shared data lives in the shared root (Flowgrid_depot.db, Assets, Logs)
- App/runtime code comes from the configured GitHub repo/branch
- Shared Assets overlay onto the local runtime without deleting local-only files
"""

from __future__ import annotations

import ctypes
import getpass
import hashlib
import json
import os
import shutil
import ssl
import sqlite3
import struct
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
import urllib.request

APP_TITLE = "Flowgrid"
CONFIG_FILENAME = "Flowgrid_config.json"
CHANNEL_CONFIG_FILENAME = "Flowgrid_channel.json"
INSTALL_STATE_FILENAME = "Flowgrid_install_state.json"
LOCAL_INSTALLER_FILENAME = "Flowgrid_installer.pyw"
LOCAL_UPDATER_FILENAME = "Flowgrid_updater.pyw"
LOCAL_APP_FOLDER_NAME = APP_TITLE
LOGS_DIR_NAME = "Logs"
DEPOT_DB_FILENAME = "Flowgrid_depot.db"
MIN_PYTHON_VERSION = (3, 10, 0)
DEPENDENCY_SPECS: Tuple[Tuple[str, str, bool], ...] = (
    ("PySide6", "PySide6", True),
)
DEFAULT_REPO_URL = "https://github.com/S7rasshofer/Flowgrid.git"
DEFAULT_REPO_BRANCH = "main"
GITHUB_USER_AGENT = "Flowgrid-Installer/1.0"
GITHUB_API_ACCEPT = "application/vnd.github+json"
GITHUB_TIMEOUT_SECONDS = 20.0
GITHUB_RETRY_ATTEMPTS = 3
DESKTOP_SHORTCUT_FILENAME = f"{APP_TITLE}.lnk"
MANAGED_SHORTCUT_ICON_FILENAME = "Flowgrid_shortcut.ico"
WINDOWS_SHORTCUT_DESCRIPTION = "Launch Flowgrid"
REPO_MANAGED_ROOT_FILES = ("Flowgrid.pyw", LOCAL_UPDATER_FILENAME)
REPO_MANAGED_DIRS = ("flowgrid_app", "Assets")
REPO_MANAGED_HASHES_KEY = "repo_managed_files"
SHARED_ASSET_HASHES_KEY = "shared_asset_files"
DEFAULT_CHANNEL_ID = "main"
DEFAULT_CHANNEL_LABEL = "Main"
DEFAULT_CHANNEL_READ_ONLY_DB = False
LAST_SNAPSHOT_SYNC_AT_KEY = "last_snapshot_sync_at_utc"
LAST_SNAPSHOT_SYNC_STATUS_KEY = "last_snapshot_sync_status"
LAST_SNAPSHOT_SYNC_SUMMARY_KEY = "last_snapshot_sync_summary"
MANAGED_SHARED_ASSET_DIRS = (
    "agent_icons",
    "admin_icons",
    "qa_flag_icons",
    "part_flag_images",
    "ui_icons",
    "Flowgrid Icons",
)
_FLOWGRID_PATHS_CONFIG: Optional[Dict[str, Any]] = None
SSL_CA_FILE = ""
SSL_CA_SOURCE = "system"
SSL_CONTEXT: ssl.SSLContext | None = None


def _normalize_channel_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text or DEFAULT_CHANNEL_ID


def _normalize_channel_label(value: Any, *, channel_id: str | None = None) -> str:
    resolved_channel = _normalize_channel_id(channel_id or value)
    text = str(value or "").strip()
    if text:
        return text
    if resolved_channel == DEFAULT_CHANNEL_ID:
        return DEFAULT_CHANNEL_LABEL
    return resolved_channel.replace("_", " ").replace("-", " ").title()


def _channel_display_name(channel_id: Any = "", channel_label: Any = "") -> str:
    resolved_channel = _normalize_channel_id(channel_id)
    resolved_label = _normalize_channel_label(channel_label, channel_id=resolved_channel)
    if resolved_channel == DEFAULT_CHANNEL_ID:
        return APP_TITLE
    return f"{APP_TITLE} {resolved_label}".strip()


def _expand_path_text(value: Any) -> str:
    return os.path.expandvars(os.path.expanduser(str(value or "").strip()))


def _load_channel_config(shared_root: Path) -> Dict[str, Any]:
    config_path = shared_root / CHANNEL_CONFIG_FILENAME
    payload: Dict[str, Any] = {}
    if config_path.exists() and config_path.is_file():
        try:
            loaded = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"Failed parsing {config_path}: {type(exc).__name__}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise RuntimeError(f"{config_path} must contain a JSON object.")
        payload.update(loaded)

    channel_id = _normalize_channel_id(payload.get("channel_id", DEFAULT_CHANNEL_ID))
    channel_label = _normalize_channel_label(payload.get("channel_label", ""), channel_id=channel_id)
    repo_url = str(payload.get("repo_url") or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL
    branch = str(payload.get("branch") or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH
    read_only_db = bool(payload.get("read_only_db", DEFAULT_CHANNEL_READ_ONLY_DB))
    snapshot_source_root = _expand_path_text(payload.get("snapshot_source_root", ""))
    local_app_folder_name = APP_TITLE if channel_id == DEFAULT_CHANNEL_ID else _channel_display_name(channel_id, channel_label)
    shortcut_filename = f"{local_app_folder_name}.lnk"
    shortcut_description = f"Launch {local_app_folder_name}"
    normalized = {
        "channel_id": channel_id,
        "channel_label": channel_label,
        "channel_display_name": _channel_display_name(channel_id, channel_label),
        "repo_url": repo_url,
        "branch": branch,
        "read_only_db": read_only_db,
        "snapshot_source_root": snapshot_source_root,
        "local_app_folder_name": local_app_folder_name,
        "shortcut_filename": shortcut_filename,
        "shortcut_description": shortcut_description,
        "config_path": str(config_path),
    }
    return normalized


def _resolve_snapshot_source_root(channel: Dict[str, Any], shared_root: Path) -> Path | None:
    raw = str(channel.get("snapshot_source_root") or "").strip()
    if not raw:
        return None
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = shared_root / candidate
    return candidate.resolve()


def _record_step(
    steps: List[Dict[str, str]],
    label: str,
    status: str,
    detail: str = "",
    path: str = "",
) -> None:
    steps.append(
        {
            "label": str(label or "").strip(),
            "status": str(status or "").strip().lower(),
            "detail": str(detail or "").strip(),
            "path": str(path or "").strip(),
        }
    )


def _step_marker(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "ok":
        return "[OK]"
    if normalized == "warning":
        return "[WARN]"
    if normalized == "failed":
        return "[FAIL]"
    return "[INFO]"


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _short_sha(value: str, length: int = 12) -> str:
    text = str(value or "").strip()
    return text[:length] if text else ""


def _safe_print(message: str = "", end: str = "\n") -> None:
    try:
        print(message, end=end)
    except Exception:
        pass


def _find_local_paths_config() -> Optional[Path]:
    candidates: List[Path] = []
    env_override = str(os.environ.get("FLOWGRID_PATHS_CONFIG", "") or "").strip()
    if env_override:
        candidates.append(Path(env_override))
    try:
        candidates.append(Path(__file__).resolve().parent / "Flowgrid_paths.json")
    except Exception:
        pass
    candidates.append(Path.cwd() / "Flowgrid_paths.json")

    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()
        except Exception:
            pass
    return None


def load_paths_config() -> Dict[str, Any]:
    global _FLOWGRID_PATHS_CONFIG
    if _FLOWGRID_PATHS_CONFIG is not None:
        return _FLOWGRID_PATHS_CONFIG

    config_path = _find_local_paths_config()
    if config_path is None:
        _FLOWGRID_PATHS_CONFIG = {}
        return _FLOWGRID_PATHS_CONFIG

    try:
        loaded = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed parsing {config_path}: {type(exc).__name__}: {exc}") from exc

    if not isinstance(loaded, dict):
        raise RuntimeError(f"{config_path} must contain a JSON object.")

    _FLOWGRID_PATHS_CONFIG = loaded
    return _FLOWGRID_PATHS_CONFIG


def get_script_root() -> Path:
    try:
        return Path(__file__).resolve().parent
    except Exception:
        return Path.cwd()


def _resolve_windows_documents_directory() -> Path | None:
    if os.name != "nt":
        return None

    try:
        buffer = ctypes.create_unicode_buffer(260)
        result = ctypes.windll.shell32.SHGetFolderPathW(None, 0x0005, None, 0, buffer)
        if result == 0 and str(buffer.value).strip():
            path = Path(str(buffer.value).strip())
            if path.exists() and path.is_dir():
                return path
    except Exception:
        pass

    userprofile = str(os.environ.get("USERPROFILE", "") or "").strip()
    if userprofile:
        candidate = Path(userprofile) / "Documents"
        if candidate.exists() and candidate.is_dir():
            return candidate

    candidate = Path.home() / "Documents"
    if candidate.exists() and candidate.is_dir():
        return candidate

    return None


def substitute_path_variables(template: str, shared_root: Path) -> str:
    result = str(template or "")
    if "{DOCUMENTS}" in result:
        documents = _resolve_windows_documents_directory() or (Path.home() / "Documents")
        result = result.replace("{DOCUMENTS}", str(documents))
    if "{SHARED_ROOT}" in result:
        result = result.replace("{SHARED_ROOT}", str(shared_root))
    return result


def resolve_path_from_config(config_key: str, default: str, shared_root: Path) -> Path:
    config = load_paths_config()
    value: Any = config
    for part in str(config_key or "").split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = None
            break
    if value is None:
        value = default
    return Path(substitute_path_variables(str(value), shared_root))


def find_actual_shared_root() -> Path:
    env_root = str(os.environ.get("FLOWGRID_SHARED_ROOT", "") or os.environ.get("FLOWGRID_SOURCE_ROOT", "")).strip()
    if env_root:
        candidate = Path(env_root)
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()

    config = load_paths_config()
    shared_root_str = str(config.get("shared_drive_root") or "").strip()
    if shared_root_str:
        candidate = Path(shared_root_str)
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve()

    script_root = get_script_root()
    if (script_root / DEPOT_DB_FILENAME).exists():
        return script_root.resolve()
    if _find_local_paths_config() is None:
        return script_root.resolve()

    raise RuntimeError(
        "Installer could not determine the shared Flowgrid root. "
        "Run it from the shared Flowgrid folder or set FLOWGRID_SHARED_ROOT."
    )


def get_installation_paths() -> Dict[str, Any]:
    shared_root = find_actual_shared_root()
    channel = _load_channel_config(shared_root)
    documents_folder = _resolve_windows_documents_directory() or (Path.home() / "Documents")
    local_app_folder = documents_folder / str(channel.get("local_app_folder_name") or LOCAL_APP_FOLDER_NAME)
    local_config_folder = local_app_folder / "Config"
    local_data_folder = local_app_folder / "Data"
    local_queue_folder = local_app_folder / "Queue"
    shared_assets = resolve_path_from_config("shared_paths.assets_folder", "{SHARED_ROOT}\\Assets", shared_root)
    snapshot_source_root = _resolve_snapshot_source_root(channel, shared_root)
    return {
        "channel": channel,
        "channel_id": str(channel.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(channel.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(channel.get("channel_display_name") or APP_TITLE),
        "repo_url": str(channel.get("repo_url") or DEFAULT_REPO_URL),
        "branch": str(channel.get("branch") or DEFAULT_REPO_BRANCH),
        "read_only_db": bool(channel.get("read_only_db", False)),
        "snapshot_source_root": str(channel.get("snapshot_source_root") or ""),
        "snapshot_source_root_path": snapshot_source_root,
        "shortcut_filename": str(channel.get("shortcut_filename") or DESKTOP_SHORTCUT_FILENAME),
        "shortcut_description": str(channel.get("shortcut_description") or WINDOWS_SHORTCUT_DESCRIPTION),
        "shared_root": shared_root,
        "shared_logs": shared_root / LOGS_DIR_NAME,
        "source_root": get_script_root(),
        "shared_assets": shared_assets,
        "shared_db": shared_root / DEPOT_DB_FILENAME,
        "shared_config": shared_root / CONFIG_FILENAME,
        "shared_channel_config": shared_root / CHANNEL_CONFIG_FILENAME,
        "documents_folder": documents_folder,
        "local_app_folder": local_app_folder,
        "local_app": local_app_folder / "Flowgrid.pyw",
        "local_updater": local_app_folder / LOCAL_UPDATER_FILENAME,
        "local_package": local_app_folder / "flowgrid_app",
        "local_config_folder": local_config_folder,
        "local_config": local_config_folder / CONFIG_FILENAME,
        "local_data_folder": local_data_folder,
        "local_queue_folder": local_queue_folder,
        "local_assets": local_app_folder / "Assets",
        "local_paths_config": local_app_folder / "Flowgrid_paths.json",
        "install_state": local_config_folder / INSTALL_STATE_FILENAME,
    }


def get_installer_error_log_path() -> Path:
    candidates: List[Path] = []
    try:
        paths = get_installation_paths()
        candidates.append(paths["local_config_folder"] / LOGS_DIR_NAME / "Flowgrid_installer_errors.log")
    except Exception:
        pass
    try:
        shared_root = find_actual_shared_root()
        candidates.append(shared_root / LOGS_DIR_NAME / "Flowgrid_installer_errors.log")
    except Exception:
        pass
    candidates.append(get_script_root() / LOGS_DIR_NAME / "Flowgrid_installer_errors.log")
    candidates.append(Path(tempfile.gettempdir()) / "Flowgrid_installer_errors.log")

    unique_candidates: List[Path] = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)

    for log_path in unique_candidates:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            return log_path
        except Exception:
            continue

    fallback = Path(tempfile.gettempdir()) / "Flowgrid_installer_errors.log"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    return fallback


def log_installer_error(error_code: str, summary: str, details: str = "") -> None:
    try:
        now = _utc_now_iso()
        lines = [
            f"[{now}] [{error_code}] {summary}",
            f"Python: {sys.executable}",
            f"Version: {sys.version}",
            f"User: {os.environ.get('USERNAME', 'UNKNOWN')}",
            f"Computer: {os.environ.get('COMPUTERNAME', 'UNKNOWN')}",
        ]
        if details:
            lines.append(f"Details: {details}")
        lines.extend(
            [
                f"Traceback: {traceback.format_exc()}",
                "-" * 80,
                "",
            ]
        )
        with get_installer_error_log_path().open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
    except Exception:
        try:
            _safe_print(f"[INSTALLER ERROR] {error_code}: {summary}")
            if details:
                _safe_print(details)
        except Exception:
            pass


def log_installer_status(status_code: str, summary: str, details: str = "") -> None:
    try:
        now = _utc_now_iso()
        lines = [
            f"[{now}] [INFO:{status_code}] {summary}",
            f"Python: {sys.executable}",
            f"Version: {sys.version}",
            f"User: {os.environ.get('USERNAME', 'UNKNOWN')}",
            f"Computer: {os.environ.get('COMPUTERNAME', 'UNKNOWN')}",
        ]
        if details:
            lines.append(f"Details: {details}")
        lines.extend(["-" * 80, ""])
        with get_installer_error_log_path().open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
    except Exception:
        try:
            _safe_print(f"[INSTALLER STATUS] {status_code}: {summary}")
            if details:
                _safe_print(details)
        except Exception:
            pass


def configure_ssl() -> ssl.SSLContext:
    global SSL_CA_FILE, SSL_CA_SOURCE
    try:
        import certifi  # type: ignore[import-not-found]
        cafile = str(certifi.where())
        os.environ.setdefault("SSL_CERT_FILE", cafile)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", cafile)
        SSL_CA_FILE = cafile
        SSL_CA_SOURCE = "certifi"
        return ssl.create_default_context(cafile=cafile)
    except Exception as certifi_exc:
        SSL_CA_FILE = ""
        SSL_CA_SOURCE = "system"
        try:
            return ssl.create_default_context()
        except Exception as exc:
            log_installer_error(
                "SSL_CONTEXT_FAILED",
                "Failed creating SSL context for verified HTTPS downloads.",
                (
                    f"Reason: {type(exc).__name__}: {exc}\n"
                    f"Certifi fallback reason: {type(certifi_exc).__name__}: {certifi_exc}"
                ),
            )
            raise


def _ssl_context() -> ssl.SSLContext:
    global SSL_CONTEXT
    if SSL_CONTEXT is None:
        SSL_CONTEXT = configure_ssl()
    return SSL_CONTEXT


def _fetch_url_bytes(
    url: str,
    *,
    headers: Dict[str, str] | None = None,
    timeout_seconds: float = GITHUB_TIMEOUT_SECONDS,
) -> bytes:
    resolved_headers = headers or {}
    requests_error: Exception | None = None
    context = _ssl_context()
    try:
        import requests  # type: ignore[import-not-found]

        response = requests.get(
            url,
            headers=resolved_headers,
            timeout=timeout_seconds,
            verify=SSL_CA_FILE or True,
        )
        response.raise_for_status()
        return bytes(response.content)
    except Exception as exc:
        requests_error = exc

    try:
        request = urllib.request.Request(url, headers=resolved_headers)
        with urllib.request.urlopen(request, context=context, timeout=timeout_seconds) as response:
            return response.read()
    except Exception as exc:
        log_installer_error(
            "NETWORK_DOWNLOAD_FAILED",
            "Verified HTTPS request failed through requests and urllib.",
            (
                f"URL: {url}\n"
                f"Transport: requests, urllib\n"
                f"CA source: {SSL_CA_SOURCE}\n"
                f"CA file: {SSL_CA_FILE}\n"
                f"Requests error: {type(requests_error).__name__}: {requests_error}\n"
                f"Urllib error: {type(exc).__name__}: {exc}"
            ),
        )
        raise


def check_python_version() -> Tuple[bool, str]:
    current_version = sys.version_info[:3]
    if current_version < MIN_PYTHON_VERSION:
        version_str = ".".join(map(str, current_version))
        required_str = ".".join(map(str, MIN_PYTHON_VERSION))
        return False, f"Python {required_str}+ is required. Current: {version_str}"
    return True, ""


def check_package_import(module_name: str) -> Tuple[bool, str]:
    try:
        __import__(module_name)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def install_package(package_name: str) -> Tuple[bool, str]:
    _ = package_name
    return False, "Automatic package installation is disabled; only preinstalled standard-library/runtime dependencies are supported."


def ensure_dependencies() -> Tuple[bool, str]:
    warnings: List[str] = []
    for package_name, module_name, required in DEPENDENCY_SPECS:
        _ = required
        ok, reason = check_package_import(module_name)
        if ok:
            continue
        install_detail = (
            f"{package_name} is not importable ({reason}). "
            "The installer will continue, but launching Flowgrid on this machine will still require that runtime dependency."
        )
        log_installer_status(
            "DEPENDENCY_WARNING",
            "A runtime dependency is missing from the current Python environment.",
            install_detail,
        )
        warnings.append(f"{package_name}: {install_detail}")
    if warnings:
        return True, "; ".join(warnings)
    return True, ""


def _powershell_single_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _preferred_gui_python_executable() -> Path:
    candidates: List[Path] = []
    for raw in (getattr(sys, "_base_executable", ""), sys.executable):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text)
        candidates.append(path)
        candidates.append(path.parent / "pythonw.exe")
        if path.name.lower() == "python.exe":
            candidates.append(path.with_name("pythonw.exe"))
    unique: List[Path] = []
    for candidate in candidates:
        if candidate in unique:
            continue
        unique.append(candidate)
    for candidate in unique:
        if candidate.name.lower() == "pythonw.exe" and candidate.exists() and candidate.is_file():
            return candidate
    for candidate in unique:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(sys.executable)


def _preferred_cli_python_executable() -> Path:
    candidates: List[Path] = []
    for raw in (getattr(sys, "_base_executable", ""), sys.executable):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text)
        candidates.append(path)
        if path.name.lower() == "pythonw.exe":
            candidates.append(path.with_name("python.exe"))
    unique: List[Path] = []
    for candidate in candidates:
        if candidate in unique:
            continue
        unique.append(candidate)
    for candidate in unique:
        if candidate.name.lower() == "python.exe" and candidate.exists() and candidate.is_file():
            return candidate
    for candidate in unique:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(getattr(sys, "_base_executable", "") or sys.executable)


def _bootstrap_shared_runtime_storage(paths: Dict[str, Any]) -> Tuple[bool, str]:
    if not _ensure_shared_storage_contract(paths):
        return False, "Failed preparing the shared workflow location."

    if bool(paths.get("read_only_db", False)):
        if not _refresh_read_only_snapshot(paths):
            return False, "Failed refreshing the read-only shared snapshot."
        if not _verify_read_only_snapshot(paths):
            return False, "Failed verifying the read-only shared snapshot."
        snapshot_state = paths.get("_snapshot_sync_state")
        if isinstance(snapshot_state, dict):
            return True, str(snapshot_state.get(LAST_SNAPSHOT_SYNC_SUMMARY_KEY) or "").strip() or "Read-only shared snapshot refreshed."
        return True, "Read-only shared snapshot refreshed."

    launcher_path = _preferred_cli_python_executable()
    local_root = Path(paths["local_app_folder"])
    shared_db = Path(paths["shared_db"])
    shared_db_existed = shared_db.exists() and shared_db.is_file()
    if not launcher_path.exists() or not launcher_path.is_file():
        detail = f"Python launcher not found: {launcher_path}"
        log_installer_error("SHARED_DB_BOOTSTRAP_LAUNCHER_MISSING", "Failed locating the Python launcher for shared DB bootstrap.", detail)
        return False, detail
    if not local_root.exists() or not local_root.is_dir():
        detail = f"Local runtime root is missing: {local_root}"
        log_installer_error("SHARED_DB_BOOTSTRAP_RUNTIME_MISSING", "Failed locating the local runtime for shared DB bootstrap.", detail)
        return False, detail

    script = "\n".join(
        [
            "import sys",
            "from pathlib import Path",
            "from flowgrid_app.workflow_core import DepotDB",
            "db_path = Path(sys.argv[1])",
            "db = DepotDB(db_path, read_only=False, ensure_schema=True)",
            "try:",
            "    db.fetchone('SELECT 1')",
            "finally:",
            "    db.close('installer.shared_db_bootstrap')",
        ]
    )
    env = os.environ.copy()
    existing_pythonpath = str(env.get("PYTHONPATH", "") or "").strip()
    env["PYTHONPATH"] = str(local_root) if not existing_pythonpath else str(local_root) + os.pathsep + existing_pythonpath

    try:
        result = subprocess.run(
            [str(launcher_path), "-c", script, str(shared_db)],
            cwd=str(local_root),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        log_installer_error("SHARED_DB_BOOTSTRAP_RUN_FAILED", "Failed running shared DB bootstrap from the local runtime.", detail)
        return False, detail

    if result.returncode != 0:
        detail = (str(result.stderr or "").strip() or str(result.stdout or "").strip() or "Unknown shared DB bootstrap failure.")[-2000:]
        log_installer_error(
            "SHARED_DB_BOOTSTRAP_FAILED",
            "Failed creating or migrating the shared workflow DB from the local runtime.",
            f"DB path: {shared_db}\nRuntime root: {local_root}\nDetail: {detail}",
        )
        return False, detail

    if shared_db_existed:
        return True, f"Shared workflow DB already existed and was verified at {shared_db}."
    return True, f"Shared workflow DB created at {shared_db}."


def _create_or_update_windows_shortcut(
    shortcut_path: Path,
    target_path: Path,
    arguments: str,
    working_directory: Path,
    icon_path: Path,
    description: str,
) -> Tuple[bool, str]:
    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            "$shell = New-Object -ComObject WScript.Shell",
            f"$shortcut = $shell.CreateShortcut({_powershell_single_quote(str(shortcut_path))})",
            f"$shortcut.TargetPath = {_powershell_single_quote(str(target_path))}",
            f"$shortcut.Arguments = {_powershell_single_quote(arguments)}",
            f"$shortcut.WorkingDirectory = {_powershell_single_quote(str(working_directory))}",
            f"$shortcut.IconLocation = {_powershell_single_quote(str(icon_path) + ',0')}",
            f"$shortcut.Description = {_powershell_single_quote(description)}",
            "$shortcut.WindowStyle = 1",
            "$shortcut.Save()",
        ]
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if result.returncode == 0:
        return True, ""
    detail = (str(result.stderr or "").strip() or str(result.stdout or "").strip() or "Unknown shortcut save failure.")[-2000:]
    return False, detail


def _resolve_windows_desktop_directory() -> Path | None:
    if os.name != "nt":
        return None

    try:
        buffer = ctypes.create_unicode_buffer(260)
        result = ctypes.windll.shell32.SHGetFolderPathW(None, 0x0010, None, 0, buffer)
        if result == 0 and str(buffer.value).strip():
            path = Path(str(buffer.value).strip())
            if path.exists() and path.is_dir():
                return path
    except Exception:
        pass

    onedrive = str(os.environ.get("OneDrive", "") or "").strip()
    if onedrive:
        candidate = Path(onedrive) / "Desktop"
        if candidate.exists() and candidate.is_dir():
            return candidate

    candidate = Path.home() / "Desktop"
    if candidate.exists() and candidate.is_dir():
        return candidate

    return None


def _create_shortcut_icon(source_icon: Path, target_ico: Path) -> Path:
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QImage, QPainter, QPixmap
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance()
    if app is None:
        app = QApplication([])

    image = QImage(str(source_icon))
    if image.isNull():
        raise ValueError(f"Unable to load icon source: {source_icon}")

    max_dim = max(image.width(), image.height())
    if max_dim > 256:
        image = image.scaled(256, 256, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)

    canvas = QImage(256, 256, QImage.Format.Format_ARGB32)
    canvas.fill(Qt.GlobalColor.transparent)
    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
    pixmap = QPixmap.fromImage(image)
    x = max(0, (256 - pixmap.width()) // 2)
    y = max(0, (256 - pixmap.height()) // 2)
    painter.drawPixmap(x, y, pixmap)
    painter.end()

    target_ico.parent.mkdir(parents=True, exist_ok=True)
    if not canvas.save(str(target_ico), "ICO"):
        raise ValueError(f"Unable to save icon file: {target_ico}")
    return target_ico


def _managed_shortcut_icon_path(paths: Dict[str, Path]) -> Path:
    return paths["local_config_folder"] / MANAGED_SHORTCUT_ICON_FILENAME


def _find_default_shortcut_ico(paths: Dict[str, Path]) -> Path | None:
    candidates = [
        paths["local_assets"] / "Flowgrid Icons" / MANAGED_SHORTCUT_ICON_FILENAME,
        paths["shared_root"] / "Assets" / "Flowgrid Icons" / MANAGED_SHORTCUT_ICON_FILENAME,
        paths["source_root"] / "Assets" / "Flowgrid Icons" / MANAGED_SHORTCUT_ICON_FILENAME,
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _prepare_shortcut_icon(paths: Dict[str, Path], launcher_path: Path) -> Path:
    ico_source = _find_default_shortcut_ico(paths)
    if ico_source is not None:
        managed_icon_path = _managed_shortcut_icon_path(paths)
        try:
            managed_icon_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ico_source, managed_icon_path)
            return managed_icon_path
        except Exception as exc:
            log_installer_status(
                "SHORTCUT_ICON_COPY_FALLBACK",
                "Falling back to generated or launcher icon because the managed shortcut ICO could not be copied.",
                f"Source icon: {ico_source}\nTarget icon: {managed_icon_path}\nReason: {type(exc).__name__}: {exc}",
            )

    icon_source = _find_default_wrench_icon(paths)
    if icon_source is None:
        log_installer_status(
            "SHORTCUT_ICON_MISSING",
            "Falling back to the Python launcher icon because no Flowgrid shortcut icon source was found.",
            (
                f"Checked local Assets, shared Assets, and installer source Assets.\n"
                f"Launcher icon: {launcher_path}"
            ),
        )
        return launcher_path

    try:
        return _create_shortcut_icon(icon_source, _managed_shortcut_icon_path(paths))
    except Exception as exc:
        log_installer_status(
            "SHORTCUT_ICON_FALLBACK",
            "Falling back to the Python launcher icon for the desktop shortcut.",
            f"Source icon: {icon_source}\nReason: {type(exc).__name__}: {exc}",
        )
        return launcher_path


def _find_default_wrench_icon(paths: Dict[str, Path]) -> Path | None:
    candidates = [
        paths["local_assets"] / "Flowgrid Icons" / "wrench.png",
        paths["shared_root"] / "Assets" / "Flowgrid Icons" / "wrench.png",
        paths["source_root"] / "Assets" / "Flowgrid Icons" / "wrench.png",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def create_desktop_shortcut(paths: Dict[str, Path]) -> bool:
    try:
        desktop_path = _resolve_windows_desktop_directory()
        if desktop_path is None:
            raise RuntimeError("Unable to resolve desktop folder.")

        launcher_path = _preferred_gui_python_executable()
        script_path = paths["local_app"]
        if not launcher_path.exists() or not launcher_path.is_file():
            raise RuntimeError(f"Python launcher not found: {launcher_path}")
        if not script_path.exists() or not script_path.is_file():
            raise RuntimeError(f"Installed Flowgrid.pyw not found: {script_path}")
        managed_icon_path = _prepare_shortcut_icon(paths, launcher_path)

        shortcut_path = desktop_path / str(paths.get("shortcut_filename") or DESKTOP_SHORTCUT_FILENAME)
        ok, detail = _create_or_update_windows_shortcut(
            shortcut_path,
            launcher_path,
            f'"{script_path}"',
            script_path.parent,
            managed_icon_path,
            str(paths.get("shortcut_description") or WINDOWS_SHORTCUT_DESCRIPTION),
        )
        if not ok:
            raise RuntimeError(detail or "Unknown desktop shortcut save failure.")
        return True
    except Exception as exc:
        log_installer_error("SHORTCUT_FAILED", "Failed to create desktop shortcut.", str(exc))
        return False


def write_local_paths_config(paths: Dict[str, Path]) -> bool:
    target = paths["local_paths_config"]
    temp_path = target.with_name(f"{target.name}.tmp")
    payload = {
        "shared_drive_root": str(paths["shared_root"]),
        "shared_paths": {
            "assets_folder": str(paths["shared_assets"]),
        },
        "channel_id": str(paths.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(paths.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(paths.get("channel_display_name") or APP_TITLE),
        "read_only_db": bool(paths.get("read_only_db", False)),
        "repo_url": str(paths.get("repo_url") or DEFAULT_REPO_URL),
        "branch": str(paths.get("branch") or DEFAULT_REPO_BRANCH),
        "snapshot_source_root": str(paths.get("snapshot_source_root") or ""),
        "local_paths": {
            "app_folder": str(paths["local_app_folder"]),
            "config_folder": str(paths["local_config_folder"]),
            "database_folder": str(paths["local_data_folder"]),
            "queue_folder": str(paths["local_queue_folder"]),
            "assets_folder": str(paths["local_assets"]),
        },
    }
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temp_path, target)
        return True
    except Exception as exc:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        log_installer_error("WRITE_MANIFEST_FAILED", "Failed writing local Flowgrid_paths.json.", str(exc))
        return False


def verify_local_paths_config(paths: Dict[str, Path]) -> bool:
    target = paths["local_paths_config"]
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Manifest payload is not a JSON object.")
        actual_root = str(payload.get("shared_drive_root") or "").strip()
        expected_root = str(paths["shared_root"])
        if actual_root != expected_root:
            raise ValueError(f"Expected shared_drive_root={expected_root!r}, found {actual_root!r}.")
        actual_channel_id = _normalize_channel_id(payload.get("channel_id", DEFAULT_CHANNEL_ID))
        if actual_channel_id != str(paths.get("channel_id") or DEFAULT_CHANNEL_ID):
            raise ValueError(
                f"Expected channel_id={str(paths.get('channel_id') or DEFAULT_CHANNEL_ID)!r}, found {actual_channel_id!r}."
            )
        actual_read_only_db = bool(payload.get("read_only_db", False))
        if actual_read_only_db != bool(paths.get("read_only_db", False)):
            raise ValueError(
                f"Expected read_only_db={bool(paths.get('read_only_db', False))!r}, found {actual_read_only_db!r}."
            )
        return True
    except Exception as exc:
        log_installer_error(
            "VERIFY_MANIFEST_FAILED",
            "Failed verifying local Flowgrid_paths.json manifest.",
            f"Path: {target}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def create_local_folders(paths: Dict[str, Path]) -> bool:
    try:
        paths["local_app_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_config_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_data_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_queue_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_assets"].parent.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as exc:
        log_installer_error("CREATE_FOLDERS_FAILED", "Failed creating local folders.", str(exc))
        return False


def initialize_local_user_config(paths: Dict[str, Path]) -> bool:
    target = paths["local_config"]
    temp_path = target.with_name(f"{target.name}.tmp")
    shared_legacy = paths["shared_config"]
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and target.is_file():
            return True

        payload = "{}\n"
        if shared_legacy.exists() and shared_legacy.is_file():
            try:
                raw_text = shared_legacy.read_text(encoding="utf-8")
                loaded = json.loads(raw_text)
                if isinstance(loaded, dict):
                    payload = json.dumps(loaded, indent=2, ensure_ascii=False) + "\n"
            except Exception as exc:
                log_installer_error(
                    "LOCAL_CONFIG_SHARED_READ_FAILED",
                    "Failed reading shared Flowgrid config while bootstrapping the local config file.",
                    f"Path: {shared_legacy}\nReason: {type(exc).__name__}: {exc}",
                )

        temp_path.write_text(payload, encoding="utf-8")
        os.replace(temp_path, target)
        return True
    except Exception as exc:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        log_installer_error(
            "LOCAL_CONFIG_INIT_FAILED",
            "Failed creating the local Flowgrid_config.json file.",
            f"Path: {target}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def _detect_installer_user() -> str:
    candidates = [
        os.environ.get("USERNAME", ""),
        os.environ.get("USER", ""),
        os.environ.get("LOGNAME", ""),
    ]
    try:
        candidates.append(getpass.getuser() or "")
    except Exception:
        pass
    for raw in candidates:
        value = str(raw or "").strip()
        if value:
            return value
    return "UNKNOWN"


def _channel_metadata_for_state(paths: Dict[str, Any]) -> Dict[str, Any]:
    metadata = {
        "channel_id": str(paths.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(paths.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(paths.get("channel_display_name") or APP_TITLE),
        "read_only_db": bool(paths.get("read_only_db", False)),
        "repo_url": str(paths.get("repo_url") or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL,
        "branch": str(paths.get("branch") or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH,
        "snapshot_source_root": str(paths.get("snapshot_source_root") or "").strip(),
    }
    if not metadata["read_only_db"]:
        metadata.setdefault(LAST_SNAPSHOT_SYNC_AT_KEY, "")
        metadata.setdefault(LAST_SNAPSHOT_SYNC_STATUS_KEY, "not_applicable")
        metadata.setdefault(
            LAST_SNAPSHOT_SYNC_SUMMARY_KEY,
            "Main channel uses the production shared root directly.",
        )
    return metadata


def _channel_remote_label(paths: Dict[str, Any]) -> str:
    branch = str(paths.get("branch") or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH
    return f"GitHub {branch}"


def _ensure_shared_storage_contract(paths: Dict[str, Any]) -> bool:
    try:
        paths["shared_root"].mkdir(parents=True, exist_ok=True)
        Path(paths["shared_db"]).parent.mkdir(parents=True, exist_ok=True)
        paths["shared_assets"].mkdir(parents=True, exist_ok=True)
        for folder_name in MANAGED_SHARED_ASSET_DIRS:
            (paths["shared_assets"] / folder_name).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as exc:
        log_installer_error(
            "SHARED_STORAGE_CONTRACT_FAILED",
            "Failed preparing the shared workflow location.",
            f"Shared root: {paths['shared_root']}\nShared DB: {paths['shared_db']}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def _seed_missing_shared_assets_from_source(paths: Dict[str, Any], source_assets_root: Path | None) -> Dict[str, Any]:
    target_root = Path(paths["shared_assets"])
    if source_assets_root is None:
        return {
            "status": "warning",
            "summary": "No packaged Assets source was available to seed the shared Assets tree.",
            "added": 0,
            "skipped": 0,
            "errors": 0,
        }
    if not source_assets_root.exists() or not source_assets_root.is_dir():
        return {
            "status": "warning",
            "summary": f"Packaged Assets source was unavailable at {source_assets_root}; shared Assets seeding was skipped.",
            "added": 0,
            "skipped": 0,
            "errors": 0,
        }

    added = 0
    skipped = 0
    errors = 0
    try:
        target_root.mkdir(parents=True, exist_ok=True)
        for source_path in sorted(source_assets_root.rglob("*")):
            relative_path = source_path.relative_to(source_assets_root)
            target_path = target_root / relative_path
            if source_path.is_dir():
                target_path.mkdir(parents=True, exist_ok=True)
                continue
            if target_path.exists():
                skipped += 1
                continue
            try:
                _copy_file_atomic(source_path, target_path)
                added += 1
            except Exception as exc:
                errors += 1
                log_installer_error(
                    "SHARED_ASSET_SEED_COPY_FAILED",
                    "Failed copying a packaged asset into the shared Assets tree.",
                    f"Source: {source_path}\nTarget: {target_path}\nReason: {type(exc).__name__}: {exc}",
                )
    except Exception as exc:
        log_installer_error(
            "SHARED_ASSET_SEED_FAILED",
            "Failed seeding the shared Assets tree from packaged assets.",
            f"Source root: {source_assets_root}\nTarget root: {target_root}\nReason: {type(exc).__name__}: {exc}",
        )
        return {
            "status": "warning",
            "summary": f"Shared Assets seed failed: {type(exc).__name__}: {exc}",
            "added": added,
            "skipped": skipped,
            "errors": errors + 1,
        }

    status = "ok" if errors == 0 else "warning"
    summary = (
        f"Shared Assets seed complete: added {added}, existing {skipped}."
        if errors == 0
        else f"Shared Assets seeded with warnings: added {added}, existing {skipped}, errors {errors}."
    )
    return {
        "status": status,
        "summary": summary,
        "added": added,
        "skipped": skipped,
        "errors": errors,
    }


def _read_only_sqlite_uri(path: Path) -> str:
    raw_path = str(path)
    if raw_path.startswith("\\\\"):
        return f"file:{raw_path}?mode=ro"
    try:
        return f"{path.resolve().as_uri()}?mode=ro"
    except Exception:
        return f"file:{str(path).replace(os.sep, '/')}?mode=ro"


def _prune_snapshot_asset_tree(paths: Dict[str, Any], expected_files: set[str]) -> int:
    shared_assets = Path(paths["shared_assets"])
    shared_root = Path(paths["shared_root"])
    try:
        if not shared_assets.resolve().is_relative_to(shared_root.resolve()):
            return 0
    except Exception:
        return 0

    removed = 0
    for existing_file in sorted(shared_assets.rglob("*")):
        if not existing_file.is_file():
            continue
        relative = existing_file.relative_to(shared_assets).as_posix()
        if relative in expected_files:
            continue
        try:
            existing_file.unlink()
            removed += 1
        except Exception as exc:
            log_installer_error(
                "SNAPSHOT_ASSET_PRUNE_FAILED",
                "Failed pruning a stale beta snapshot asset.",
                f"Path: {existing_file}\nReason: {type(exc).__name__}: {exc}",
            )
    for existing_dir in sorted(shared_assets.rglob("*"), reverse=True):
        if existing_dir.is_dir():
            try:
                existing_dir.rmdir()
            except OSError:
                pass
    return removed


def _refresh_read_only_snapshot(paths: Dict[str, Any]) -> bool:
    snapshot_source_root = paths.get("snapshot_source_root_path")
    shared_root = Path(paths["shared_root"])
    if snapshot_source_root is None:
        log_installer_error(
            "SNAPSHOT_SOURCE_MISSING",
            "Read-only channel is missing snapshot_source_root.",
            f"Shared root: {shared_root}",
        )
        return False
    source_root = Path(snapshot_source_root)
    try:
        if source_root.resolve() == shared_root.resolve():
            raise RuntimeError("snapshot_source_root cannot point at the same folder as the target shared root.")
    except Exception as exc:
        log_installer_error(
            "SNAPSHOT_SOURCE_INVALID",
            "Read-only snapshot source root is invalid.",
            f"Source root: {source_root}\nTarget root: {shared_root}\nReason: {type(exc).__name__}: {exc}",
        )
        return False

    source_db = source_root / DEPOT_DB_FILENAME
    source_assets = source_root / "Assets"
    target_db = Path(paths["shared_db"])
    target_assets = Path(paths["shared_assets"])
    synced_at = _utc_now_iso()

    try:
        target_db.parent.mkdir(parents=True, exist_ok=True)
        _copy_file_atomic(source_db, target_db)
        target_assets.mkdir(parents=True, exist_ok=True)

        expected_files: set[str] = set()
        copied = 0
        updated = 0
        unchanged = 0
        for source_path in sorted(source_assets.rglob("*")):
            if not source_path.is_file():
                continue
            relative = source_path.relative_to(source_assets).as_posix()
            expected_files.add(relative)
            target_path = target_assets / Path(relative.replace("/", os.sep))
            if target_path.exists() and target_path.is_file():
                try:
                    if _file_sha256(source_path) == _file_sha256(target_path):
                        unchanged += 1
                        continue
                except Exception:
                    pass
                _copy_file_atomic(source_path, target_path)
                updated += 1
                continue
            _copy_file_atomic(source_path, target_path)
            copied += 1

        removed = _prune_snapshot_asset_tree(paths, expected_files)
        for folder_name in MANAGED_SHARED_ASSET_DIRS:
            (target_assets / folder_name).mkdir(parents=True, exist_ok=True)

        summary = (
            f"Read-only shared snapshot refreshed from {source_root}: "
            f"assets added {copied}, updated {updated}, unchanged {unchanged}, removed stale {removed}."
        )
        paths["_snapshot_sync_state"] = {
            LAST_SNAPSHOT_SYNC_AT_KEY: synced_at,
            LAST_SNAPSHOT_SYNC_STATUS_KEY: "ok",
            LAST_SNAPSHOT_SYNC_SUMMARY_KEY: summary,
            "snapshot_source_root": str(source_root),
        }
        log_installer_status("SNAPSHOT_SYNC_OK", "Read-only shared snapshot refreshed.", summary)
        return True
    except Exception as exc:
        summary = f"Failed refreshing the read-only shared snapshot: {type(exc).__name__}: {exc}"
        paths["_snapshot_sync_state"] = {
            LAST_SNAPSHOT_SYNC_AT_KEY: synced_at,
            LAST_SNAPSHOT_SYNC_STATUS_KEY: "failed",
            LAST_SNAPSHOT_SYNC_SUMMARY_KEY: summary,
            "snapshot_source_root": str(source_root),
        }
        log_installer_error(
            "SNAPSHOT_SYNC_FAILED",
            "Failed refreshing the read-only shared snapshot.",
            f"Source root: {source_root}\nTarget root: {shared_root}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def _verify_read_only_snapshot(paths: Dict[str, Any]) -> bool:
    shared_db = Path(paths["shared_db"])
    try:
        with sqlite3.connect(_read_only_sqlite_uri(shared_db), uri=True, timeout=30.0) as conn:
            conn.execute("SELECT 1").fetchone()
        return True
    except Exception as exc:
        log_installer_error(
            "VERIFY_SHARED_SNAPSHOT_FAILED",
            "Failed verifying the read-only shared snapshot database.",
            f"Shared DB path: {shared_db}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def initialize_database(paths: Dict[str, Any]) -> bool:
    return initialize_local_install_metadata(paths)


def initialize_local_install_metadata(paths: Dict[str, Any]) -> bool:
    if not write_local_paths_config(paths):
        return False
    if not verify_local_paths_config(paths):
        return False
    if not initialize_local_user_config(paths):
        return False

    if bool(paths.get("read_only_db", False)):
        paths["_snapshot_sync_state"] = {
            LAST_SNAPSHOT_SYNC_AT_KEY: "",
            LAST_SNAPSHOT_SYNC_STATUS_KEY: "skipped",
            LAST_SNAPSHOT_SYNC_SUMMARY_KEY: (
                "Installer skipped shared snapshot refresh by design; "
                "runtime will use the configured shared DB path directly."
            ),
        }
    else:
        paths["_snapshot_sync_state"] = {
            LAST_SNAPSHOT_SYNC_AT_KEY: "",
            LAST_SNAPSHOT_SYNC_STATUS_KEY: "not_applicable",
            LAST_SNAPSHOT_SYNC_SUMMARY_KEY: "Installer wrote only local bootstrap metadata; runtime will use the production shared root directly.",
        }
    return True


def _default_install_state(paths: Dict[str, Any] | None = None) -> Dict[str, Any]:
    metadata = _channel_metadata_for_state(paths or {})
    return {
        "repo_url": str(metadata.get("repo_url") or DEFAULT_REPO_URL),
        "branch": str(metadata.get("branch") or DEFAULT_REPO_BRANCH),
        "channel_id": str(metadata.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(metadata.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(metadata.get("channel_display_name") or APP_TITLE),
        "read_only_db": bool(metadata.get("read_only_db", False)),
        "snapshot_source_root": str(metadata.get("snapshot_source_root") or ""),
        "installed_commit_sha": "",
        "installed_at_utc": "",
        "last_check_at_utc": "",
        "last_check_status": "",
        "last_check_summary": "",
        "last_remote_commit_sha": "",
        "last_shared_asset_sync_at_utc": "",
        "last_shared_asset_sync_status": "",
        "last_shared_asset_sync_summary": "",
        LAST_SNAPSHOT_SYNC_AT_KEY: str(metadata.get(LAST_SNAPSHOT_SYNC_AT_KEY) or ""),
        LAST_SNAPSHOT_SYNC_STATUS_KEY: str(metadata.get(LAST_SNAPSHOT_SYNC_STATUS_KEY) or ""),
        LAST_SNAPSHOT_SYNC_SUMMARY_KEY: str(metadata.get(LAST_SNAPSHOT_SYNC_SUMMARY_KEY) or ""),
        REPO_MANAGED_HASHES_KEY: {},
        SHARED_ASSET_HASHES_KEY: {},
    }


def _normalize_hash_mapping(raw_value: Any) -> Dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    normalized: Dict[str, str] = {}
    for raw_key, raw_hash in raw_value.items():
        key = str(raw_key or "").replace("\\", "/").strip("/")
        value = str(raw_hash or "").strip().lower()
        if key and value:
            normalized[key] = value
    return normalized


def load_install_state(paths: Dict[str, Path]) -> Dict[str, Any]:
    target = paths["install_state"]
    state = _default_install_state(paths)
    if not target.exists() or not target.is_file():
        return state
    try:
        loaded = json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:
        log_installer_error(
            "INSTALL_STATE_PARSE_FAILED",
            "Failed parsing Flowgrid_install_state.json; installer will continue with defaults.",
            f"Path: {target}\nReason: {type(exc).__name__}: {exc}",
        )
        return state
    if not isinstance(loaded, dict):
        log_installer_error(
            "INSTALL_STATE_INVALID",
            "Flowgrid_install_state.json was not a JSON object; installer will continue with defaults.",
            f"Path: {target}\nValue type: {type(loaded).__name__}",
        )
        return state
    merged = dict(state)
    merged.update(loaded)
    merged["channel_id"] = _normalize_channel_id(merged.get("channel_id", state.get("channel_id", DEFAULT_CHANNEL_ID)))
    merged["channel_label"] = _normalize_channel_label(
        merged.get("channel_label", state.get("channel_label", DEFAULT_CHANNEL_LABEL)),
        channel_id=merged["channel_id"],
    )
    merged["channel_display_name"] = str(merged.get("channel_display_name") or state.get("channel_display_name") or APP_TITLE).strip() or APP_TITLE
    merged["read_only_db"] = bool(merged.get("read_only_db", state.get("read_only_db", False)))
    merged["snapshot_source_root"] = str(merged.get("snapshot_source_root") or state.get("snapshot_source_root") or "").strip()
    merged["repo_url"] = str(merged.get("repo_url", DEFAULT_REPO_URL) or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL
    merged["branch"] = str(merged.get("branch", DEFAULT_REPO_BRANCH) or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH
    merged[REPO_MANAGED_HASHES_KEY] = _normalize_hash_mapping(merged.get(REPO_MANAGED_HASHES_KEY))
    merged[SHARED_ASSET_HASHES_KEY] = _normalize_hash_mapping(merged.get(SHARED_ASSET_HASHES_KEY))
    return merged


def save_install_state(paths: Dict[str, Path], state: Dict[str, Any]) -> bool:
    target = paths["install_state"]
    temp_path = target.with_name(f"{target.name}.tmp")
    payload = dict(_default_install_state(paths))
    payload.update(state if isinstance(state, dict) else {})
    payload["channel_id"] = _normalize_channel_id(payload.get("channel_id", DEFAULT_CHANNEL_ID))
    payload["channel_label"] = _normalize_channel_label(payload.get("channel_label", ""), channel_id=payload["channel_id"])
    payload["channel_display_name"] = str(payload.get("channel_display_name") or paths.get("channel_display_name") or APP_TITLE).strip() or APP_TITLE
    payload["read_only_db"] = bool(payload.get("read_only_db", False))
    payload["snapshot_source_root"] = str(payload.get("snapshot_source_root") or "").strip()
    payload["repo_url"] = str(payload.get("repo_url", DEFAULT_REPO_URL) or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL
    payload["branch"] = str(payload.get("branch", DEFAULT_REPO_BRANCH) or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH
    payload[REPO_MANAGED_HASHES_KEY] = _normalize_hash_mapping(payload.get(REPO_MANAGED_HASHES_KEY))
    payload[SHARED_ASSET_HASHES_KEY] = _normalize_hash_mapping(payload.get(SHARED_ASSET_HASHES_KEY))
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temp_path, target)
        return True
    except Exception as exc:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        log_installer_error(
            "INSTALL_STATE_SAVE_FAILED",
            "Failed writing Flowgrid_install_state.json.",
            f"Path: {target}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def _split_github_repo_parts(repo_url: str) -> Tuple[str, str]:
    parsed = urlparse(str(repo_url or "").strip() or DEFAULT_REPO_URL)
    if "github.com" not in parsed.netloc.lower():
        raise RuntimeError(f"Unsupported repository host for update checks: {repo_url}")
    path = str(parsed.path or "").strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    parts = [piece for piece in path.split("/") if piece]
    if len(parts) < 2:
        raise RuntimeError(f"Unable to derive GitHub owner/repo from: {repo_url}")
    return parts[0], parts[1]


def safe_request(
    url: str,
    *,
    timeout_seconds: float = GITHUB_TIMEOUT_SECONDS,
    accept: str = GITHUB_API_ACCEPT,
) -> bytes:
    headers = {
        "User-Agent": GITHUB_USER_AGENT,
        "Accept": str(accept or "*/*"),
    }
    last_error: Exception | None = None
    for attempt in range(1, GITHUB_RETRY_ATTEMPTS + 1):
        try:
            return _fetch_url_bytes(url, headers=headers, timeout_seconds=timeout_seconds)
        except HTTPError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt >= GITHUB_RETRY_ATTEMPTS:
                break
            time.sleep(min(1.0, 0.25 * attempt))
    if last_error is None:
        raise RuntimeError(f"Request to {url} failed without an exception payload.")
    raise last_error


def _json_request(url: str, *, timeout_seconds: float = GITHUB_TIMEOUT_SECONDS) -> Any:
    payload = safe_request(url, timeout_seconds=timeout_seconds, accept=GITHUB_API_ACCEPT)
    try:
        return json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Invalid JSON payload from {url}: {type(exc).__name__}: {exc}") from exc


def _normalize_repo_relative_path(raw_path: Any) -> str:
    return str(raw_path or "").replace("\\", "/").strip().strip("/")


def _is_repo_managed_path(relative_path: str) -> bool:
    normalized = _normalize_repo_relative_path(relative_path)
    if not normalized:
        return False
    if normalized in REPO_MANAGED_ROOT_FILES:
        return True
    return any(normalized.startswith(f"{dirname}/") for dirname in REPO_MANAGED_DIRS)


def _build_github_contents_url(owner: str, repo_name: str, relative_path: str, branch: str) -> str:
    normalized_path = _normalize_repo_relative_path(relative_path)
    encoded_branch = quote(str(branch or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH, safe="")
    if normalized_path:
        encoded_path = quote(normalized_path, safe="/")
        return f"https://api.github.com/repos/{owner}/{repo_name}/contents/{encoded_path}?ref={encoded_branch}"
    return f"https://api.github.com/repos/{owner}/{repo_name}/contents?ref={encoded_branch}"


def fetch_repo_tree(repo_url: str, branch: str) -> List[Dict[str, Any]]:
    owner, repo_name = _split_github_repo_parts(repo_url)
    pending_dirs: List[str] = [""]
    visited_dirs: set[str] = set()
    files: List[Dict[str, Any]] = []

    while pending_dirs:
        current_dir = pending_dirs.pop()
        api_url = _build_github_contents_url(owner, repo_name, current_dir, branch)
        try:
            payload = _json_request(api_url)
        except HTTPError as exc:
            raise RuntimeError(
                f"Repository listing failed for {current_dir or '/'}: HTTP {exc.code}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                f"Repository listing failed for {current_dir or '/'}: {exc.reason}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"Repository listing failed for {current_dir or '/'}: {type(exc).__name__}: {exc}"
            ) from exc

        entries = payload if isinstance(payload, list) else [payload]
        if not isinstance(entries, list):
            raise RuntimeError(f"Repository listing returned an unexpected payload for {current_dir or '/'}")

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            entry_type = str(entry.get("type") or "").strip().lower()
            relative_path = _normalize_repo_relative_path(entry.get("path") or current_dir)
            if entry_type == "dir":
                if relative_path and relative_path not in visited_dirs:
                    visited_dirs.add(relative_path)
                    pending_dirs.append(relative_path)
                continue
            if entry_type != "file":
                log_installer_status(
                    "REPO_ENTRY_SKIPPED",
                    "Skipped a non-file repository entry during sync discovery.",
                    f"Type: {entry_type or 'unknown'}\nPath: {relative_path or '/'}",
                )
                continue
            download_url = str(entry.get("download_url") or "").strip()
            if not relative_path or not download_url:
                log_installer_error(
                    "REPO_ENTRY_INVALID",
                    "Repository listing returned an invalid file entry.",
                    f"Path: {relative_path or '/'}\nDownload URL: {download_url or '<missing>'}",
                )
                continue
            files.append(
                {
                    "path": relative_path,
                    "download_url": download_url,
                    "sha": str(entry.get("sha") or "").strip().lower(),
                    "size": int(entry.get("size") or 0),
                }
            )

    return sorted(files, key=lambda item: str(item.get("path") or ""))


def _calculate_repo_revision(files: List[Dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for entry in sorted(files, key=lambda item: str(item.get("path") or "")):
        digest.update(str(entry.get("path") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(entry.get("sha") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(entry.get("size") or 0).encode("ascii", "ignore"))
        digest.update(b"\n")
    return digest.hexdigest()


def fetch_remote_commit_info(repo_url: str, branch: str) -> Dict[str, Any]:
    resolved_repo_url = str(repo_url or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL
    resolved_branch = str(branch or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH
    repo_files = fetch_repo_tree(resolved_repo_url, resolved_branch)
    managed_files = [entry for entry in repo_files if _is_repo_managed_path(str(entry.get("path") or ""))]
    if not managed_files:
        raise RuntimeError("Repository listing returned no managed runtime files.")
    sha = _calculate_repo_revision(managed_files)
    return {
        "repo_url": resolved_repo_url,
        "branch": resolved_branch,
        "sha": sha,
        "short_sha": _short_sha(sha),
        "files": managed_files,
        "file_count": len(managed_files),
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_file_atomic(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    shutil.copy2(source_path, temp_path)
    os.replace(temp_path, target_path)


def _write_bytes_atomic(target_path: Path, payload: bytes) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    try:
        with temp_path.open("wb") as handle:
            handle.write(payload)
        os.replace(temp_path, target_path)
        return target_path
    except Exception:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        raise


def safe_download(
    url: str,
    dest_path: str | Path,
    headers: Dict[str, str] | None = None,
    timeout: int | float = GITHUB_TIMEOUT_SECONDS,
) -> Path:
    accept = "application/octet-stream"
    if isinstance(headers, dict) and str(headers.get("Accept") or "").strip():
        accept = str(headers.get("Accept") or "").strip()
    payload = safe_request(url, timeout_seconds=float(timeout), accept=accept)
    return _write_bytes_atomic(Path(dest_path), payload)


def download_file(
    remote_url: str,
    local_path: Path,
    *,
    timeout_seconds: float = GITHUB_TIMEOUT_SECONDS,
    payload: bytes | None = None,
) -> Path:
    try:
        resolved_payload = payload
        if resolved_payload is None:
            written_path = safe_download(
                remote_url,
                local_path,
                headers={
                    "User-Agent": GITHUB_USER_AGENT,
                    "Accept": "application/octet-stream",
                },
                timeout=timeout_seconds,
            )
            try:
                byte_count = written_path.stat().st_size
            except Exception:
                byte_count = 0
            log_installer_status(
                "DOWNLOAD_OK",
                "Downloaded a repository file successfully.",
                f"URL: {remote_url}\nPath: {written_path}\nBytes: {byte_count}",
            )
            return written_path
        written_path = _write_bytes_atomic(local_path, resolved_payload)
        log_installer_status(
            "DOWNLOAD_OK",
            "Downloaded a repository file successfully.",
            f"URL: {remote_url}\nPath: {written_path}\nBytes: {len(resolved_payload)}",
        )
        return written_path
    except Exception as exc:
        log_installer_error(
            "DOWNLOAD_FAILED",
            "Failed downloading a repository file.",
            f"URL: {remote_url}\nPath: {local_path}\nReason: {type(exc).__name__}: {exc}",
        )
        raise


def _iter_repo_source_files(snapshot_root: Path) -> Iterator[Tuple[str, Path]]:
    for filename in REPO_MANAGED_ROOT_FILES:
        source_path = snapshot_root / filename
        if source_path.exists() and source_path.is_file():
            yield filename, source_path
    for dirname in REPO_MANAGED_DIRS:
        root = snapshot_root / dirname
        if not root.exists() or not root.is_dir():
            continue
        for source_path in sorted(root.rglob("*")):
            if not source_path.is_file():
                continue
            if source_path.suffix.lower() in {".pyc", ".pyo"} or "__pycache__" in source_path.parts:
                continue
            yield source_path.relative_to(snapshot_root).as_posix(), source_path


def _stage_repo_snapshot(remote_info: Dict[str, Any]) -> Tuple[Any, Path]:
    temp_dir = tempfile.TemporaryDirectory(prefix="flowgrid_repo_sync_")
    staging_root = Path(temp_dir.name) / "snapshot"
    staging_root.mkdir(parents=True, exist_ok=True)

    files = remote_info.get("files")
    if not isinstance(files, list) or not files:
        raise RuntimeError("Remote repository metadata did not include any files to stage.")

    for entry in files:
        if not isinstance(entry, dict):
            continue
        relative_path = _normalize_repo_relative_path(entry.get("path"))
        download_url = str(entry.get("download_url") or "").strip()
        if not relative_path or not download_url:
            raise RuntimeError(f"Remote repository entry is missing path or download URL: {entry!r}")
        target_path = staging_root / Path(relative_path.replace("/", os.sep))
        download_file(download_url, target_path, timeout_seconds=GITHUB_TIMEOUT_SECONDS)

    return temp_dir, staging_root


def _apply_repo_manifest(
    paths: Dict[str, Path],
    snapshot_root: Path,
    previous_state: Dict[str, Any],
    remote_info: Dict[str, str],
) -> Dict[str, Any]:
    local_root = paths["local_app_folder"]
    source_map: Dict[str, Path] = {}
    new_manifest: Dict[str, str] = {}

    for relative_path, source_path in _iter_repo_source_files(snapshot_root):
        source_map[relative_path] = source_path
        new_manifest[relative_path] = _file_sha256(source_path)

    copied = 0
    unchanged = 0
    removed = 0

    for relative_path, source_path in source_map.items():
        target_path = local_root / Path(relative_path.replace("/", os.sep))
        target_hash = ""
        if target_path.exists() and target_path.is_file():
            try:
                target_hash = _file_sha256(target_path)
            except Exception:
                target_hash = ""
        if target_hash == new_manifest[relative_path]:
            unchanged += 1
            continue
        _copy_file_atomic(source_path, target_path)
        copied += 1

    previous_manifest = _normalize_hash_mapping(previous_state.get(REPO_MANAGED_HASHES_KEY))
    for relative_path in sorted(set(previous_manifest) - set(new_manifest)):
        relative_obj = Path(relative_path)
        if relative_obj.is_absolute() or ".." in relative_obj.parts:
            continue
        if relative_path not in {"Flowgrid.pyw", LOCAL_INSTALLER_FILENAME, LOCAL_UPDATER_FILENAME} and not (
            relative_path.startswith("flowgrid_app/") or relative_path.startswith("Assets/")
        ):
            continue
        target_path = local_root / Path(relative_path.replace("/", os.sep))
        if not target_path.exists() or not target_path.is_file():
            continue
        try:
            target_path.unlink()
            removed += 1
            parent = target_path.parent
            while parent != local_root and parent.exists():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
        except Exception as exc:
            log_installer_error(
                "STALE_MANAGED_FILE_REMOVE_FAILED",
                "Failed removing a stale repo-managed file during update.",
                f"Relative path: {relative_path}\nReason: {type(exc).__name__}: {exc}",
            )

    if not previous_manifest:
        expected_package_files = {path for path in new_manifest if path.startswith("flowgrid_app/")}
        local_package = paths["local_package"]
        if local_package.exists() and local_package.is_dir():
            for existing_file in sorted(local_package.rglob("*")):
                if not existing_file.is_file():
                    continue
                relative_path = existing_file.relative_to(local_root).as_posix()
                if relative_path in expected_package_files:
                    continue
                try:
                    existing_file.unlink()
                    removed += 1
                except Exception as exc:
                    log_installer_error(
                        "LEGACY_PACKAGE_PRUNE_FAILED",
                        "Failed pruning a stale file from the local flowgrid_app package.",
                        f"Path: {existing_file}\nReason: {type(exc).__name__}: {exc}",
                    )

    updated_state = dict(previous_state)
    updated_state.update(
        {
            "repo_url": str(remote_info.get("repo_url") or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL,
            "branch": str(remote_info.get("branch") or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH,
            "installed_commit_sha": str(remote_info.get("sha") or "").strip(),
            "installed_at_utc": _utc_now_iso(),
            "last_check_at_utc": _utc_now_iso(),
            "last_check_status": "up_to_date",
            "last_check_summary": (
                f"{str(paths.get('channel_display_name') or APP_TITLE)} installed from "
                f"{_channel_remote_label(paths)} at {remote_info.get('short_sha', '')}."
            ),
            "last_remote_commit_sha": str(remote_info.get("sha") or "").strip(),
            REPO_MANAGED_HASHES_KEY: new_manifest,
        }
    )
    return {
        "copied": copied,
        "unchanged": unchanged,
        "removed": removed,
        "state": updated_state,
    }


def sync_repo_to_local(
    paths: Dict[str, Path],
    state: Dict[str, Any],
    repo_url: str,
    branch: str,
    *,
    remote_info: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    resolved_remote = remote_info if isinstance(remote_info, dict) else fetch_remote_commit_info(repo_url, branch)
    temp_dir = None
    try:
        temp_dir, snapshot_root = _stage_repo_snapshot(resolved_remote)
        result = _apply_repo_manifest(paths, snapshot_root, state, resolved_remote)
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()

    log_installer_status(
        "REPO_SYNC_OK",
        "Repository runtime sync completed.",
        (
            f"Source: {resolved_remote.get('repo_url', repo_url)}\n"
            f"Branch: {resolved_remote.get('branch', branch)}\n"
            f"Revision: {resolved_remote.get('short_sha', '')}\n"
            f"Files staged: {resolved_remote.get('file_count', 0)}\n"
            f"Copied: {result.get('copied', 0)}\n"
            f"Unchanged: {result.get('unchanged', 0)}\n"
            f"Removed: {result.get('removed', 0)}"
        ),
    )
    return result


def sync_shared_assets(paths: Dict[str, Path], state: Dict[str, Any]) -> Dict[str, Any]:
    shared_assets_root = paths["shared_assets"]
    local_assets_root = paths["local_assets"]
    local_assets_root.mkdir(parents=True, exist_ok=True)
    synced_at = _utc_now_iso()

    if not shared_assets_root.exists() or not shared_assets_root.is_dir():
        summary = f"Shared Assets folder is unavailable at {shared_assets_root}. Local packaged assets remain in use."
        updated_state = dict(state)
        updated_state.update(
            {
                "last_shared_asset_sync_at_utc": synced_at,
                "last_shared_asset_sync_status": "warning",
                "last_shared_asset_sync_summary": summary,
            }
        )
        log_installer_status("SHARED_ASSETS_WARNING", "Shared asset sync skipped.", summary)
        return {
            "status": "warning",
            "summary": summary,
            "added": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
            "state": updated_state,
        }

    added = 0
    updated = 0
    unchanged = 0
    errors = 0
    manifest: Dict[str, str] = {}

    for source_path in sorted(shared_assets_root.rglob("*")):
        if not source_path.is_file():
            continue
        relative_path = source_path.relative_to(shared_assets_root).as_posix()
        target_path = local_assets_root / Path(relative_path.replace("/", os.sep))
        try:
            shared_hash = _file_sha256(source_path)
            manifest[relative_path] = shared_hash
            if not target_path.exists():
                _copy_file_atomic(source_path, target_path)
                added += 1
                continue
            local_hash = _file_sha256(target_path)
            if local_hash == shared_hash:
                unchanged += 1
                continue
            _copy_file_atomic(source_path, target_path)
            updated += 1
        except Exception as exc:
            errors += 1
            log_installer_error(
                "SHARED_ASSET_COPY_FAILED",
                "Failed copying a shared asset into the local runtime.",
                f"Source: {source_path}\nTarget: {target_path}\nReason: {type(exc).__name__}: {exc}",
            )

    status = "ok" if errors == 0 else "warning"
    summary = (
        f"Shared assets synced: added {added}, updated {updated}, unchanged {unchanged}."
        if errors == 0
        else f"Shared assets synced with warnings: added {added}, updated {updated}, unchanged {unchanged}, errors {errors}."
    )
    updated_state = dict(state)
    updated_state.update(
        {
            SHARED_ASSET_HASHES_KEY: manifest,
            "last_shared_asset_sync_at_utc": synced_at,
            "last_shared_asset_sync_status": status,
            "last_shared_asset_sync_summary": summary,
        }
    )
    log_installer_status("SHARED_ASSETS_SYNC", "Shared asset sync completed.", summary)
    return {
        "status": status,
        "summary": summary,
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "errors": errors,
        "state": updated_state,
    }


def assess_source_materials(paths: Dict[str, Path]) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    shared_root = paths["shared_root"]
    if not shared_root.exists():
        errors.append(f"Expected shared drive root does not exist: {shared_root}")
    elif not shared_root.is_dir():
        errors.append(f"Expected shared drive root is not a directory: {shared_root}")

    shared_db = paths["shared_db"]
    if bool(paths.get("read_only_db", False)):
        if not shared_db.exists() or not shared_db.is_file():
            warnings.append(
                "Read-only channel expects an existing readable shared DB snapshot at "
                f"{shared_db}; installer will attempt to prepare it during install."
            )
    elif not shared_db.exists() or not shared_db.is_file():
        warnings.append(
            "Shared workflow DB is not present yet; the installer will create or connect to "
            f"{shared_db} before launch if the user has shared write access."
        )

    shared_assets = paths["shared_assets"]
    if not shared_assets.exists() or not shared_assets.is_dir():
        warnings.append(
            "Shared Assets folder is not present at "
            f"{shared_assets}; installer will seed the baseline packaged assets into the shared drive without overwriting existing files."
        )

    return warnings, errors


def detect_existing_local_install(paths: Dict[str, Path]) -> bool:
    targets = (
        paths["local_app"],
        paths["local_updater"],
        paths["local_package"],
        paths["local_paths_config"],
        paths["install_state"],
        paths["local_assets"],
    )
    return any(path.exists() for path in targets)


def build_installation_report(paths: Dict[str, Path], steps: List[Dict[str, str]], is_update_install: bool = False) -> str:
    state = load_install_state(paths)
    channel_display_name = str(paths.get("channel_display_name") or APP_TITLE)
    headline = f"{channel_display_name} has been updated." if is_update_install else f"{channel_display_name} Installation Complete"
    lines: List[str] = [
        headline,
        "",
        f"Channel: {channel_display_name}",
        f"Local install: {paths['local_app_folder']}",
        f"Local package: {paths['local_package']}",
        f"Local updater: {paths['local_updater']}",
        f"Shared root: {paths['shared_root']}",
        f"Shared DB: {paths['shared_db']}",
        f"Installed commit: {_short_sha(state.get('installed_commit_sha', '')) or '-'}",
        "",
        "Verification checks:",
    ]
    for step in steps:
        label = str(step.get("label", "") or "").strip()
        status = str(step.get("status", "") or "").strip().lower()
        detail = str(step.get("detail", "") or "").strip()
        path = str(step.get("path", "") or "").strip()
        line = f"{_step_marker(status)} {label}"
        if detail:
            line += f" - {detail}"
        lines.append(line)
        if path:
            lines.append(f"      {path}")
    lines.extend(
        [
            "",
            "Authoritative data source:",
            (
                f"- Shared workflow DB: {paths['shared_root']}\\{DEPOT_DB_FILENAME}"
                if not bool(paths.get("read_only_db", False))
                else f"- Shared workflow DB: read-only snapshot at {paths['shared_db']}"
            ),
            f"- Shared Assets source of truth: {paths['shared_assets']}",
            f"- Local packaged Assets cache: {paths['local_assets']}",
            f"- Code/runtime source: {_channel_remote_label(paths)}",
        ]
    )
    if bool(paths.get("read_only_db", False)):
        lines.append(f"- Snapshot source: {str(paths.get('snapshot_source_root') or '-').strip() or '-'}")
    return "\n".join(lines)


def verify_installed_state(paths: Dict[str, Path]) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    _record_step(
        results,
        "Flowgrid.pyw installed",
        "ok" if paths["local_app"].exists() and paths["local_app"].is_file() else "failed",
        "Local runtime entrypoint present." if paths["local_app"].exists() and paths["local_app"].is_file() else "Local Flowgrid.pyw missing after install.",
        str(paths["local_app"]),
    )
    package_init = paths["local_package"] / "__init__.py"
    _record_step(
        results,
        "flowgrid_app package installed",
        "ok" if paths["local_package"].exists() and package_init.exists() and package_init.is_file() else "failed",
        "Local runtime support package present." if paths["local_package"].exists() and package_init.exists() else "Local flowgrid_app package missing after install.",
        str(paths["local_package"]),
    )
    _record_step(
        results,
        "Local standalone updater installed",
        "ok" if paths["local_updater"].exists() and paths["local_updater"].is_file() else "failed",
        "Local Flowgrid updater copy present." if paths["local_updater"].exists() and paths["local_updater"].is_file() else "Local Flowgrid updater copy missing.",
        str(paths["local_updater"]),
    )
    _record_step(
        results,
        "Packaged Assets installed",
        "ok" if paths["local_assets"].exists() and paths["local_assets"].is_dir() else "failed",
        "Local packaged Assets folder present." if paths["local_assets"].exists() and paths["local_assets"].is_dir() else "Local Assets folder missing after install.",
        str(paths["local_assets"]),
    )
    _record_step(
        results,
        "Shared Assets tree ready",
        "ok" if paths["shared_assets"].exists() and paths["shared_assets"].is_dir() else "failed",
        "Shared Assets folder present for app-wide icons and uploads." if paths["shared_assets"].exists() and paths["shared_assets"].is_dir() else "Shared Assets folder missing after install.",
        str(paths["shared_assets"]),
    )
    manifest_ok = paths["local_paths_config"].exists() and verify_local_paths_config(paths)
    _record_step(
        results,
        "Local Flowgrid_paths.json written and verified",
        "ok" if manifest_ok else "failed",
        "Local manifest points to the expected shared root." if manifest_ok else "Local Flowgrid_paths.json missing or invalid.",
        str(paths["local_paths_config"]),
    )
    _record_step(
        results,
        "Local user config ready",
        "ok" if paths["local_config"].exists() and paths["local_config"].is_file() else "failed",
        "Per-user local config file present." if paths["local_config"].exists() and paths["local_config"].is_file() else "Per-user local config file missing.",
        str(paths["local_config"]),
    )
    _record_step(
        results,
        "Install state manifest ready",
        "ok" if paths["install_state"].exists() and paths["install_state"].is_file() else "failed",
        "Flowgrid_install_state.json present." if paths["install_state"].exists() and paths["install_state"].is_file() else "Flowgrid_install_state.json missing.",
        str(paths["install_state"]),
    )

    shared_db_status = "failed"
    shared_db_detail = (
        "Shared workflow DB missing or not reachable."
        if not bool(paths.get("read_only_db", False))
        else "Shared read-only workflow DB snapshot missing or not reachable."
    )
    if paths["shared_db"].exists() and paths["shared_db"].is_file():
        try:
            connect_target = _read_only_sqlite_uri(paths["shared_db"]) if bool(paths.get("read_only_db", False)) else str(paths["shared_db"])
            with sqlite3.connect(connect_target, uri=bool(paths.get("read_only_db", False)), timeout=10.0) as conn:
                conn.execute("SELECT 1").fetchone()
            shared_db_status = "ok"
            shared_db_detail = (
                "Shared workflow DB exists and opened successfully."
                if not bool(paths.get("read_only_db", False))
                else "Shared read-only workflow DB snapshot exists and opened successfully."
            )
        except Exception as exc:
            log_installer_error(
                "VERIFY_SHARED_DB_POST_INSTALL_FAILED",
                "Shared DB exists but could not be opened during final installer verification.",
                f"Path: {paths['shared_db']}\nReason: {type(exc).__name__}: {exc}",
            )
    _record_step(
        results,
        "Shared DB reachable",
        shared_db_status,
        shared_db_detail,
        str(paths["shared_db"]),
    )
    return results


def show_installation_dialog(title: str, message: str, is_error: bool = False, paths: Dict[str, Path] | None = None) -> None:
    try:
        from PySide6.QtWidgets import QApplication, QDialog, QLabel, QMessageBox, QPushButton, QPlainTextEdit, QVBoxLayout

        app = QApplication.instance()
        if app is None:
            app = QApplication([])

        if is_error:
            msg_box = QMessageBox()
            msg_box.setIcon(QMessageBox.Critical)
            msg_box.setWindowTitle(title)
            msg_box.setText(message)
            msg_box.setStandardButtons(QMessageBox.Ok)
            msg_box.exec()
            return

        dialog = QDialog()
        dialog.setWindowTitle(title)
        dialog.setModal(True)
        dialog.resize(760, 520)
        layout = QVBoxLayout()
        summary_label = QLabel(title)
        summary_label.setWordWrap(True)
        layout.addWidget(summary_label)
        status_block = QPlainTextEdit()
        status_block.setReadOnly(True)
        status_block.setPlainText(message)
        status_block.setMinimumSize(700, 360)
        layout.addWidget(status_block, 1)
        launch_button = QPushButton("Launch Flowgrid")
        close_button = QPushButton("Close")
        launch_button.clicked.connect(dialog.accept)
        close_button.clicked.connect(dialog.reject)
        layout.addWidget(launch_button)
        layout.addWidget(close_button)
        dialog.setLayout(layout)
        result = dialog.exec()
        if result == QDialog.Accepted and paths is not None:
            _launch_local_flowgrid(paths)
    except Exception as exc:
        log_installer_error("DIALOG_FAILED", "Failed showing the installer dialog.", str(exc))
        if os.name == "nt":
            try:
                ctypes.windll.user32.MessageBoxW(None, str(message), str(title), 0x10 | 0x1000 if is_error else 0x40 | 0x1000)
                return
            except Exception:
                pass
        _safe_print(f"{title}: {message}")


def _launch_local_flowgrid(paths: Dict[str, Path]) -> bool:
    try:
        launcher_path = _preferred_gui_python_executable()
        script_path = paths["local_app"]
        if not launcher_path.exists() or not launcher_path.is_file():
            raise RuntimeError(f"Python launcher not found: {launcher_path}")
        if not script_path.exists() or not script_path.is_file():
            raise RuntimeError(f"Flowgrid script not found: {script_path}")
        subprocess.Popen([str(launcher_path), str(script_path)], cwd=str(script_path.parent))
        return True
    except Exception as exc:
        log_installer_error("POST_INSTALL_LAUNCH_FAILED", "Failed launching Flowgrid after install/update.", str(exc))
        return False


def _wait_for_parent_exit(parent_pid: int) -> None:
    if parent_pid <= 0 or os.name != "nt":
        return
    synchronize = 0x00100000
    wait_timeout = 0x00000102
    handle = None
    try:
        handle = ctypes.windll.kernel32.OpenProcess(synchronize, False, int(parent_pid))
        if not handle:
            return
        result = ctypes.windll.kernel32.WaitForSingleObject(handle, 30000)
        if result == wait_timeout:
            log_installer_error(
                "PARENT_WAIT_TIMEOUT",
                "Timed out waiting for the parent Flowgrid process to exit before update.",
                f"Parent PID: {parent_pid}",
            )
    except Exception as exc:
        log_installer_error(
            "PARENT_WAIT_FAILED",
            "Failed waiting for the parent Flowgrid process before update.",
            f"Parent PID: {parent_pid}\nReason: {type(exc).__name__}: {exc}",
        )
    finally:
        if handle:
            try:
                ctypes.windll.kernel32.CloseHandle(handle)
            except Exception:
                pass


def _parse_cli_options() -> Dict[str, Any]:
    options: Dict[str, Any] = {
        "apply_update": False,
        "sync_assets_only": False,
        "launch_after_install": True,
        "relaunch_after_update": False,
        "parent_pid": 0,
    }
    argv = list(sys.argv[1:])
    idx = 0
    while idx < len(argv):
        raw = str(argv[idx] or "").strip()
        lowered = raw.lower()
        if lowered == "--apply-update":
            options["apply_update"] = True
        elif lowered == "--sync-assets-only":
            options["sync_assets_only"] = True
        elif lowered == "--no-launch":
            options["launch_after_install"] = False
        elif lowered == "--relaunch":
            options["relaunch_after_update"] = True
        elif lowered == "--parent-pid" and idx + 1 < len(argv):
            idx += 1
            try:
                options["parent_pid"] = int(str(argv[idx] or "").strip())
            except Exception:
                options["parent_pid"] = 0
        elif lowered.startswith("--parent-pid="):
            try:
                options["parent_pid"] = int(lowered.split("=", 1)[1].strip())
            except Exception:
                options["parent_pid"] = 0
        idx += 1
    return options


def run_installer(
    *,
    apply_update: bool = False,
    sync_assets_only: bool = False,
    launch_after_install: bool = True,
    relaunch_after_update: bool = False,
    parent_pid: int = 0,
) -> int:
    install_steps: List[Dict[str, str]] = []
    _safe_print("Flowgrid Installer")
    _safe_print(f"Python: {sys.executable}")
    _safe_print(f"Version: {sys.version}")
    _safe_print()

    if apply_update:
        _wait_for_parent_exit(int(parent_pid))

    _safe_print("Step 1: Checking Python version...")
    version_ok, version_error = check_python_version()
    if not version_ok:
        error_msg = f"Python version check failed: {version_error}"
        _record_step(install_steps, "Python version", "failed", error_msg)
        log_installer_error("PYTHON_VERSION", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Python version", "ok", f"Python {'.'.join(map(str, MIN_PYTHON_VERSION))}+ detected.", sys.executable)
    _safe_print(f"[OK] Python {'.'.join(map(str, MIN_PYTHON_VERSION))}+ detected")
    _safe_print()

    if sync_assets_only:
        _record_step(install_steps, "Dependencies", "ok", "Dependency bootstrap skipped for asset-only sync.")
    else:
        _safe_print("Step 2: Checking dependencies...")
        deps_ok, deps_error = ensure_dependencies()
        if not deps_ok:
            error_msg = f"Dependency installation failed: {deps_error}"
            _record_step(install_steps, "Dependencies", "failed", deps_error)
            log_installer_error("DEPENDENCIES", error_msg)
            show_installation_dialog("Installation Failed", error_msg, is_error=True)
            return 1
        deps_status = "warning" if deps_error else "ok"
        deps_detail = (
            deps_error
            if deps_error
            else "Installer prerequisites are satisfied with standard-library networking and local file writes."
        )
        _record_step(install_steps, "Dependencies", deps_status, deps_detail)
        _safe_print(f"{_step_marker(deps_status)} {deps_detail}")
        _safe_print()

    _safe_print("Step 3: Preparing installation paths...")
    try:
        paths = get_installation_paths()
    except Exception as exc:
        error_msg = f"Failed resolving installer paths: {type(exc).__name__}: {exc}"
        _record_step(install_steps, "Installation paths", "failed", error_msg)
        log_installer_error("INSTALL_PATHS_FAILED", "Failed resolving installer path/channel context.", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _safe_print(f"Source root: {paths['source_root']}")
    _safe_print(f"Shared root: {paths['shared_root']}")
    _safe_print(f"Channel: {paths['channel_display_name']}")
    _safe_print(f"Local app folder: {paths['local_app_folder']}")
    _safe_print(f"Local package folder: {paths['local_package']}")
    _safe_print(f"Local assets folder: {paths['local_assets']}")
    is_update_install = detect_existing_local_install(paths)
    log_installer_status(
        "INSTALL_CONTEXT",
        "Resolved installer path context.",
        "\n".join(
            [
                f"source_root={paths['source_root']}",
                f"shared_root={paths['shared_root']}",
                f"channel_id={paths['channel_id']}",
                f"channel_label={paths['channel_label']}",
                f"channel_display_name={paths['channel_display_name']}",
                f"read_only_db={bool(paths.get('read_only_db', False))}",
                f"repo_url={paths['repo_url']}",
                f"branch={paths['branch']}",
                f"snapshot_source_root={paths.get('snapshot_source_root') or ''}",
                f"shared_db={paths['shared_db']}",
                f"local_app={paths['local_app']}",
                f"local_updater={paths['local_updater']}",
                f"local_package={paths['local_package']}",
                f"local_manifest={paths['local_paths_config']}",
                f"install_state={paths['install_state']}",
                "workflow_db_source_of_truth=shared_root/Flowgrid_depot.db",
            ]
        ),
    )
    _safe_print()

    _safe_print(f"Step 4: Verifying shared bootstrap source at {paths['shared_root']}...")
    source_warnings, source_errors = assess_source_materials(paths)
    for warning in source_warnings:
        _safe_print(f"[WARN] {warning}")
    if source_warnings:
        log_installer_error("SHARED_ROOT_WARNING", "Shared drive visibility issue", "\n".join(source_warnings))
    if source_errors:
        error_msg = "Installer cannot continue because the shared Flowgrid location is missing required channel/bootstrap inputs."
        details = "\n".join(source_errors)
        _record_step(install_steps, "Shared bootstrap source", "failed", details, str(paths["shared_root"]))
        log_installer_error("SOURCE_MATERIALS_UNAVAILABLE", error_msg, details)
        show_installation_dialog("Installation Failed", f"{error_msg}\n\n{details}", is_error=True)
        return 1
    _record_step(
        install_steps,
        "Shared bootstrap source",
        "warning" if source_warnings else "ok",
        "Required shared bootstrap inputs are available."
        if not source_warnings
        else f"Shared bootstrap inputs are available with warnings: {'; '.join(source_warnings)}",
        str(paths["shared_root"]),
    )
    _safe_print("[OK] Shared bootstrap source verified")
    _safe_print()

    _safe_print("Step 5: Creating local folders...")
    if not create_local_folders(paths):
        error_msg = "Failed to create local installation folders"
        _record_step(install_steps, "Local folders", "failed", error_msg, str(paths["local_app_folder"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Local folders", "ok", "Local app/config/data/queue folders prepared.", str(paths["local_app_folder"]))
    _safe_print("[OK] Local folders created")
    _safe_print()

    _safe_print("Step 6: Writing local install metadata...")
    if not initialize_local_install_metadata(paths):
        error_msg = "Failed to initialize local install metadata."
        _record_step(install_steps, "Local install metadata", "failed", error_msg, str(paths["local_paths_config"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Local Flowgrid_paths.json", "ok", "Local shared-root manifest written and verified.", str(paths["local_paths_config"]))
    _record_step(install_steps, "Local user config", "ok", "Per-user local config file is ready.", str(paths["local_config"]))
    _record_step(
        install_steps,
        "Shared DB reference",
        "ok",
        "Installer wrote only the local shared-root reference."
        if not bool(paths.get("read_only_db", False))
        else "Installer wrote only the local shared DB reference for the read-only channel.",
        str(paths["shared_db"]),
    )
    _safe_print("[OK] Local install metadata written")
    _safe_print()

    state = load_install_state(paths)
    state.update(_channel_metadata_for_state(paths))
    snapshot_state = paths.get("_snapshot_sync_state")
    if isinstance(snapshot_state, dict):
        state.update(snapshot_state)

    if sync_assets_only:
        asset_result = sync_shared_assets(paths, state)
        save_install_state(paths, asset_result["state"])
        _record_step(install_steps, "Shared assets pull", asset_result["status"], asset_result["summary"], str(paths["local_assets"]))
        _safe_print(f"{_step_marker(asset_result['status'])} {asset_result['summary']}")
        return 0

    repo_url = str(state.get("repo_url", DEFAULT_REPO_URL) or DEFAULT_REPO_URL).strip() or DEFAULT_REPO_URL
    branch = str(state.get("branch", DEFAULT_REPO_BRANCH) or DEFAULT_REPO_BRANCH).strip() or DEFAULT_REPO_BRANCH

    remote_label = _channel_remote_label(paths)
    _safe_print(f"Step 7: Checking {remote_label}...")
    try:
        remote_info = fetch_remote_commit_info(repo_url, branch)
    except Exception as exc:
        error_msg = str(exc) or "GitHub update check failed."
        log_installer_error("REMOTE_CHECK_FAILED", f"Failed checking {remote_label}.", error_msg)
        _record_step(install_steps, f"{remote_label} check", "failed", error_msg, repo_url)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(
        install_steps,
        f"{remote_label} check",
        "ok",
        f"Remote revision {_short_sha(remote_info['sha'])} resolved successfully across {remote_info.get('file_count', 0)} files.",
        remote_info["repo_url"],
    )
    _safe_print(f"[OK] {remote_label} resolved to {remote_info['short_sha']}")
    _safe_print()

    installed_sha = str(state.get("installed_commit_sha") or "").strip()
    runtime_bootstrap_needed = (
        not paths["local_app"].exists()
        or not paths["local_updater"].exists()
        or not paths["local_package"].exists()
        or not paths["install_state"].exists()
        or not _normalize_hash_mapping(state.get(REPO_MANAGED_HASHES_KEY))
    )
    needs_code_refresh = runtime_bootstrap_needed or installed_sha != remote_info["sha"]

    if needs_code_refresh:
        _safe_print(f"Step 8: Syncing the {remote_label} runtime into the local user-space install...")
        try:
            apply_result = sync_repo_to_local(paths, state, repo_url, branch, remote_info=remote_info)
        except Exception as exc:
            error_msg = f"Failed syncing repository files: {type(exc).__name__}: {exc}"
            log_installer_error("GITHUB_SNAPSHOT_APPLY_FAILED", f"Failed installing Flowgrid from {remote_label}.", error_msg)
            _record_step(install_steps, f"{remote_label} runtime install", "failed", error_msg, remote_info["repo_url"])
            show_installation_dialog("Installation Failed", error_msg, is_error=True)
            return 1
        state = apply_result["state"]
        _record_step(
            install_steps,
            f"{remote_label} runtime install",
            "ok",
            f"Copied {apply_result['copied']}, unchanged {apply_result['unchanged']}, removed stale {apply_result['removed']}.",
            str(paths["local_app_folder"]),
        )
        _safe_print(
            f"[OK] Runtime synced from GitHub: copied {apply_result['copied']}, "
            f"unchanged {apply_result['unchanged']}, removed {apply_result['removed']}"
        )
        _safe_print()
    else:
        state.update(
            {
                "last_check_at_utc": _utc_now_iso(),
                "last_check_status": "up_to_date",
                "last_check_summary": (
                    f"{str(paths.get('channel_display_name') or APP_TITLE)} already matched "
                    f"{remote_label} at {remote_info['short_sha']}."
                ),
                "last_remote_commit_sha": remote_info["sha"],
            }
        )
        _record_step(
            install_steps,
            f"{remote_label} runtime install",
            "ok",
            f"Local runtime already matched {remote_label} at {remote_info['short_sha']}; no code files changed.",
            str(paths["local_app_folder"]),
        )
        _safe_print(f"[OK] Local runtime already matched {remote_label} at {remote_info['short_sha']}")
        _safe_print()

    _safe_print("Step 9: Preparing shared workflow storage...")
    shared_bootstrap_ok, shared_bootstrap_summary = _bootstrap_shared_runtime_storage(paths)
    if not shared_bootstrap_ok:
        error_msg = f"Failed preparing shared workflow storage: {shared_bootstrap_summary}"
        _record_step(install_steps, "Shared workflow storage", "failed", shared_bootstrap_summary, str(paths["shared_db"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Shared workflow storage", "ok", shared_bootstrap_summary, str(paths["shared_db"]))
    _safe_print(f"[OK] {shared_bootstrap_summary}")
    _safe_print()

    _safe_print("Step 10: Seeding baseline shared assets...")
    asset_seed_source = paths["local_assets"] if paths["local_assets"].exists() and paths["local_assets"].is_dir() else (paths["source_root"] / "Assets")
    asset_seed_result = _seed_missing_shared_assets_from_source(paths, asset_seed_source)
    shared_seed_summary = (
        f"{asset_seed_result['summary']} The standalone updater will still mirror shared assets into the local install on launch."
    )
    state.update(
        {
            "last_shared_asset_sync_at_utc": "",
            "last_shared_asset_sync_status": "pending",
            "last_shared_asset_sync_summary": shared_seed_summary,
        }
    )
    _record_step(
        install_steps,
        "Shared assets seed",
        asset_seed_result["status"],
        shared_seed_summary,
        str(paths["shared_assets"]),
    )
    _safe_print(f"{_step_marker(asset_seed_result['status'])} {shared_seed_summary}")
    _safe_print()

    if not save_install_state(paths, state):
        _record_step(install_steps, "Install state manifest", "warning", "Flowgrid_install_state.json could not be written.", str(paths["install_state"]))
    else:
        _record_step(install_steps, "Install state manifest", "ok", "Flowgrid_install_state.json updated successfully.", str(paths["install_state"]))

    _safe_print("Step 11: Creating desktop shortcut...")
    if not create_desktop_shortcut(paths):
        _record_step(install_steps, "Desktop shortcut", "warning", "Desktop shortcut could not be created.", str(paths["local_app_folder"]))
        _safe_print("[WARN] Desktop shortcut creation failed, but installation continues")
    else:
        _record_step(install_steps, "Desktop shortcut", "ok", "Desktop shortcut created.", str(paths["local_app_folder"]))
        _safe_print("[OK] Desktop shortcut created")
    _safe_print()

    completion_msg = build_installation_report(
        paths,
        [*install_steps, *verify_installed_state(paths)],
        is_update_install=is_update_install or apply_update,
    )
    completion_title = (
        f"{str(paths.get('channel_display_name') or APP_TITLE)} Updated"
        if (is_update_install or apply_update)
        else f"{str(paths.get('channel_display_name') or APP_TITLE)} Installed Successfully"
    )

    if apply_update:
        if relaunch_after_update or launch_after_install:
            _launch_local_flowgrid(paths)
    else:
        show_installation_dialog(completion_title, completion_msg, False, paths if launch_after_install else None)

    _safe_print("Update completed successfully!" if (is_update_install or apply_update) else "Installation completed successfully!")
    return 0


if __name__ == "__main__":
    try:
        cli_options = _parse_cli_options()
        exit_code = run_installer(
            apply_update=bool(cli_options.get("apply_update", False)),
            sync_assets_only=bool(cli_options.get("sync_assets_only", False)),
            launch_after_install=bool(cli_options.get("launch_after_install", True)),
            relaunch_after_update=bool(cli_options.get("relaunch_after_update", False)),
            parent_pid=int(cli_options.get("parent_pid", 0) or 0),
        )
        sys.exit(exit_code)
    except Exception as exc:
        error_msg = f"Installer crashed: {exc}"
        log_installer_error("INSTALLER_CRASH", error_msg, traceback.format_exc())
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        sys.exit(1)
