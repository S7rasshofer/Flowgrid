from __future__ import annotations

import ctypes
import json
import os
import shutil
from pathlib import Path
from typing import Any

from flowgrid_app.runtime_logging import _runtime_log_event

FLOWGRID_PACKAGE_DIR = Path(__file__).resolve().parent
FLOWGRID_PROJECT_ROOT = FLOWGRID_PACKAGE_DIR.parent
FLOWGRID_SCRIPT_PATH = FLOWGRID_PROJECT_ROOT / "Flowgrid.pyw"

DATA_ROOT_ENV_VAR = "FLOWGRID_DATA_ROOT"

DEFAULT_SHARED_DATA_ROOT = Path(r"Z:\DATA\Flowgrid")

LAUNCH_LOG_FILENAME = "Flowgrid_launch_errors.log"
LOGS_DIR_NAME = "Logs"

RUNTIME_LOG_FILENAME_PREFIX = "Flowgrid_runtime"

RUNTIME_LOG_FILENAME_SUFFIX = ".log.jsonl"

RUNTIME_LOG_MAX_BYTES = 10 * 1024 * 1024

RUNTIME_LOG_MAX_BACKUPS = 20

RUNTIME_PROMPT_TITLE = "Flowgrid Runtime Issue"

_RESOLVED_DATA_ROOT: Path | None = None

_DATA_ROOT_FALLBACK_DETAILS = ""

_DATA_ROOT_FALLBACK_NOTIFIED = False

ASSETS_DIR_NAME = "Assets"

FLOWGRID_ICON_PACK_DIR_NAME = "Flowgrid Icons"

ASSET_AGENT_ICON_DIR_NAME = "agent_icons"

ASSET_ADMIN_ICON_DIR_NAME = "admin_icons"

ASSET_QA_FLAG_ICON_DIR_NAME = "qa_flag_icons"

ASSET_PART_FLAG_IMAGE_DIR_NAME = "part_flag_images"

ASSET_UI_ICON_COMPAT_DIR_NAME = "ui_icons"

APP_TITLE = "Flowgrid"

CONFIG_FILENAME = "Flowgrid_config.json"

DEPOT_DB_FILENAME = "Flowgrid_depot.db"

SHARED_SYNC_REFRESH_INTERVAL_MS = 15000

DEPOT_SEARCH_REFRESH_DEBOUNCE_MS = 250

DEPOT_VIEW_TTL_MS = 4000

DEPOT_RECENT_VIEW_TTL_MS = 5000

DEPOT_BACKGROUND_AUTO_REFRESH_MS = 30000

DEPOT_DB_REOPEN_COOLDOWN_MS = 5000

MIN_PYTHON_VERSION = (3, 10, 0)

_FLOWGRID_PATHS_CONFIG: dict[str, Any] | None = None

_FLOWGRID_PATHS_CONFIG_ERROR: str = ""

def _reset_path_runtime_cache() -> None:
    global _FLOWGRID_PATHS_CONFIG, _FLOWGRID_PATHS_CONFIG_ERROR, _RESOLVED_DATA_ROOT
    _FLOWGRID_PATHS_CONFIG = None
    _FLOWGRID_PATHS_CONFIG_ERROR = ""
    _RESOLVED_DATA_ROOT = None

def _find_local_paths_config() -> Path | None:
    """Locate the local Flowgrid_paths.json beside the installed runtime."""
    candidates: list[Path] = []

    env_override = str(os.environ.get("FLOWGRID_PATHS_CONFIG", "") or "").strip()
    if env_override:
        candidates.append(Path(env_override))

    try:
        script_dir = FLOWGRID_PROJECT_ROOT
        candidates.append(script_dir / "Flowgrid_paths.json")
    except Exception:
        pass

    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate.resolve()
        except Exception:
            pass

    return None

def _load_paths_config() -> dict[str, Any]:
    """Load and cache Flowgrid_paths.json from the local install root."""
    global _FLOWGRID_PATHS_CONFIG, _FLOWGRID_PATHS_CONFIG_ERROR

    if _FLOWGRID_PATHS_CONFIG is not None:
        return _FLOWGRID_PATHS_CONFIG

    config_path = _find_local_paths_config()
    if config_path is None:
        _FLOWGRID_PATHS_CONFIG_ERROR = (
            "Flowgrid_paths.json is missing from the installed runtime folder. "
            "The app cannot determine the shared data root."
        )
        raise RuntimeError(_FLOWGRID_PATHS_CONFIG_ERROR)

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception as exc:
        _FLOWGRID_PATHS_CONFIG_ERROR = f"Failed to parse {config_path}: {type(exc).__name__}: {exc}"
        raise RuntimeError(_FLOWGRID_PATHS_CONFIG_ERROR)

    if not isinstance(loaded, dict):
        _FLOWGRID_PATHS_CONFIG_ERROR = f"Flowgrid_paths.json must contain a JSON object. Found {type(loaded).__name__}."
        raise RuntimeError(_FLOWGRID_PATHS_CONFIG_ERROR)

    _FLOWGRID_PATHS_CONFIG = loaded
    return _FLOWGRID_PATHS_CONFIG

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

def _substitute_path_variables(template: str, shared_root: Path | None = None) -> str:
    """Substitute {DOCUMENTS}, {SHARED_ROOT}, etc. in path strings."""
    if not template:
        return ""

    result = str(template)

    if "{DOCUMENTS}" in result:
        documents = _resolve_windows_documents_directory() or (Path.home() / "Documents")
        result = result.replace("{DOCUMENTS}", str(documents))

    if "{SHARED_ROOT}" in result:
        if shared_root is None:
            raise RuntimeError("Shared root is required to substitute {SHARED_ROOT}.")
        result = result.replace("{SHARED_ROOT}", str(shared_root))

    return result

def _resolve_path_from_config(config_key: str, default: str | Path | None = None, shared_root: Path | None = None) -> Path:
    """Retrieve a path from the config, with substitution and fallback."""
    config = _load_paths_config()
    
    # Navigate nested keys (e.g., "local_paths.database_folder")
    parts = str(config_key).split(".")
    value = config
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = None
            break
    
    if value is None:
        if default is None:
            return Path.cwd()
        return Path(_substitute_path_variables(str(default), shared_root))
    
    return Path(_substitute_path_variables(str(value), shared_root))

def _get_shared_root_from_config() -> Path:
    """Get the configured shared drive root from Flowgrid_paths.json."""
    config = _load_paths_config()
    shared_root_str = str(config.get("shared_drive_root") or "").strip()
    if not shared_root_str:
        raise RuntimeError("Flowgrid_paths.json does not define shared_drive_root.")

    shared_root = Path(shared_root_str)
    if not shared_root.exists() or not shared_root.is_dir():
        raise RuntimeError(f"Configured shared drive root is not accessible: {shared_root}")

    return shared_root.resolve()

def _get_local_config_folder() -> Path:
    r"""Get the local config folder (e.g., Documents\Flowgrid\Config)."""
    shared_root = _get_shared_root_from_config()
    config_folder = _resolve_path_from_config("local_paths.config_folder", "{DOCUMENTS}\\Flowgrid\\Config", shared_root)
    config_folder.mkdir(parents=True, exist_ok=True)
    return config_folder

def _get_local_config_path() -> Path:
    """Get the local configuration file path for the current user."""
    folder = _get_local_config_folder()
    folder.mkdir(parents=True, exist_ok=True)
    return folder / CONFIG_FILENAME

def _shared_workflow_db_path() -> Path:
    """Return the authoritative shared workflow database path."""
    shared_root = _resolve_data_root()
    return shared_root / DEPOT_DB_FILENAME

def _local_data_root() -> Path:
    try:
        return FLOWGRID_PROJECT_ROOT
    except Exception as exc:
        fallback = Path.cwd()
        _runtime_log_event(
            "bootstrap.local_data_root_fallback",
            severity="warning",
            summary="Fell back to current working directory for local data root.",
            exc=exc,
            context={"fallback_path": str(fallback)},
        )
        return fallback

def _configured_data_root() -> Path:
    """Return the canonical shared data root from the install manifest."""
    return _get_shared_root_from_config()

def _paths_equal(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.paths_equal_resolve_failed",
            severity="warning",
            summary="Path resolution failed while comparing paths; using string comparison fallback.",
            exc=exc,
            context={"left": str(left), "right": str(right)},
        )
        return str(left) == str(right)

def _legacy_data_candidates(filename: str) -> list[Path]:
    candidates: list[Path] = []
    try:
        candidates.append(FLOWGRID_PROJECT_ROOT / filename)
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.legacy_candidate_file_dir_failed",
            severity="warning",
            summary="Unable to derive __file__-relative legacy data candidate.",
            exc=exc,
            context={"filename": filename},
        )
    candidates.append(Path.cwd() / filename)

    unique: list[Path] = []
    for path in candidates:
        if any(_paths_equal(path, existing) for existing in unique):
            continue
        unique.append(path)
    return unique

def _resolve_data_root() -> Path:
    global _RESOLVED_DATA_ROOT
    if _RESOLVED_DATA_ROOT is not None:
        return _RESOLVED_DATA_ROOT

    target = _configured_data_root()
    try:
        target.mkdir(parents=True, exist_ok=True)
        _RESOLVED_DATA_ROOT = target
        return _RESOLVED_DATA_ROOT
    except Exception as exc:
        raise RuntimeError(
            f"Unable to access shared data root {target}: {type(exc).__name__}: {exc}"
        )

def _data_file_path(filename: str, migrate_legacy: bool = True) -> Path:
    """
    Resolve path for a data file.

    Shared data files use the shared root as the canonical source of truth.
    User-specific settings are stored locally in Documents to avoid cross-user conflicts.
    """
    if filename == CONFIG_FILENAME:
        target = _get_local_config_path()
        if target.exists() or not migrate_legacy:
            return target

        legacy_shared = _resolve_data_root() / filename
        if legacy_shared.exists() and legacy_shared.is_file():
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(legacy_shared, target)
            except Exception as exc:
                _runtime_log_event(
                    "bootstrap.config_migration_failed",
                    severity="warning",
                    summary="Failed to migrate shared root config to local per-user config.",
                    exc=exc,
                    context={"shared_config": str(legacy_shared), "local_config": str(target)},
                )
        return target

    # All other workflow data files go to shared root for centralized reading
    target = _resolve_data_root() / filename
    
    if not migrate_legacy or target.exists():
        return target

    for legacy in _legacy_data_candidates(filename):
        if _paths_equal(legacy, target):
            continue
        if not legacy.exists() or not legacy.is_file():
            continue
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, target)
            break
        except Exception as exc:
            _runtime_log_event(
                "bootstrap.data_file_legacy_copy_failed",
                severity="warning",
                summary="Legacy data file copy failed; continuing without migration for this source.",
                exc=exc,
                context={
                    "filename": filename,
                    "legacy_path": str(legacy),
                    "target_path": str(target),
                },
            )
            continue
    return target

def _migrate_legacy_agent_icons(target_db_path: Path) -> None:
    data_root = target_db_path.parent
    assets_root = data_root / ASSETS_DIR_NAME
    managed_folders = (
        ASSET_AGENT_ICON_DIR_NAME,
        ASSET_ADMIN_ICON_DIR_NAME,
        ASSET_QA_FLAG_ICON_DIR_NAME,
        ASSET_PART_FLAG_IMAGE_DIR_NAME,
        ASSET_UI_ICON_COMPAT_DIR_NAME,
        FLOWGRID_ICON_PACK_DIR_NAME,
    )
    try:
        assets_root.mkdir(parents=True, exist_ok=True)
        for folder in managed_folders:
            (assets_root / folder).mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.assets_dir_create_failed",
            severity="warning",
            summary="Unable to create Assets folders; skipping legacy asset migration.",
            exc=exc,
            context={"assets_root": str(assets_root)},
        )
        return

    candidate_roots: list[Path] = []
    try:
        candidate_roots.append(FLOWGRID_PROJECT_ROOT)
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.legacy_asset_root_resolve_failed",
            severity="warning",
            summary="Unable to resolve module directory while gathering legacy asset roots.",
            exc=exc,
            context={"target_db_path": str(target_db_path)},
        )
    candidate_roots.append(Path.cwd())
    candidate_roots.append(data_root)

    unique_roots: list[Path] = []
    for root in candidate_roots:
        if any(_paths_equal(root, existing) for existing in unique_roots):
            continue
        unique_roots.append(root)

    legacy_to_assets = {
        ASSET_AGENT_ICON_DIR_NAME: ASSET_AGENT_ICON_DIR_NAME,
        ASSET_ADMIN_ICON_DIR_NAME: ASSET_ADMIN_ICON_DIR_NAME,
        ASSET_QA_FLAG_ICON_DIR_NAME: ASSET_QA_FLAG_ICON_DIR_NAME,
        ASSET_PART_FLAG_IMAGE_DIR_NAME: ASSET_PART_FLAG_IMAGE_DIR_NAME,
        "ui_icons": ASSET_UI_ICON_COMPAT_DIR_NAME,
    }

    for root in unique_roots:
        for legacy_folder, asset_folder in legacy_to_assets.items():
            source_dir = root / legacy_folder
            target_dir = assets_root / asset_folder
            if not source_dir.exists() or not source_dir.is_dir():
                continue
            try:
                if _paths_equal(source_dir, target_dir):
                    continue
            except Exception as exc:
                _runtime_log_event(
                    "bootstrap.legacy_asset_path_compare_failed",
                    severity="warning",
                    summary="Failed comparing legacy and target asset directories; continuing migration scan.",
                    exc=exc,
                    context={"source_dir": str(source_dir), "target_dir": str(target_dir)},
                )
            try:
                for source_file in source_dir.rglob("*"):
                    if not source_file.is_file():
                        continue
                    rel = source_file.relative_to(source_dir)
                    target_file = target_dir / rel
                    if target_file.exists():
                        continue
                    target_file.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.copy2(source_file, target_file)
                    except Exception as exc:
                        _runtime_log_event(
                            "bootstrap.legacy_asset_copy_failed",
                            severity="warning",
                            summary="Failed copying a legacy asset file into Assets.",
                            exc=exc,
                            context={
                                "source_file": str(source_file),
                                "target_file": str(target_file),
                            },
                        )
                        continue
            except Exception as exc:
                _runtime_log_event(
                    "bootstrap.legacy_asset_root_scan_failed",
                    severity="warning",
                    summary="Failed scanning a legacy asset source directory.",
                    exc=exc,
                    context={"source_dir": str(source_dir), "target_dir": str(target_dir)},
                )
                continue

__all__ = [
    "APP_TITLE",
    "ASSETS_DIR_NAME",
    "ASSET_ADMIN_ICON_DIR_NAME",
    "ASSET_AGENT_ICON_DIR_NAME",
    "ASSET_PART_FLAG_IMAGE_DIR_NAME",
    "ASSET_QA_FLAG_ICON_DIR_NAME",
    "ASSET_UI_ICON_COMPAT_DIR_NAME",
    "CONFIG_FILENAME",
    "DATA_ROOT_ENV_VAR",
    "DEFAULT_SHARED_DATA_ROOT",
    "DEPOT_BACKGROUND_AUTO_REFRESH_MS",
    "DEPOT_DB_FILENAME",
    "DEPOT_DB_REOPEN_COOLDOWN_MS",
    "DEPOT_RECENT_VIEW_TTL_MS",
    "DEPOT_SEARCH_REFRESH_DEBOUNCE_MS",
    "DEPOT_VIEW_TTL_MS",
    "FLOWGRID_ICON_PACK_DIR_NAME",
    "FLOWGRID_PACKAGE_DIR",
    "FLOWGRID_PROJECT_ROOT",
    "FLOWGRID_SCRIPT_PATH",
    "LOGS_DIR_NAME",
    "MIN_PYTHON_VERSION",
    "SHARED_SYNC_REFRESH_INTERVAL_MS",
    "_configured_data_root",
    "_data_file_path",
    "_find_local_paths_config",
    "_get_local_config_folder",
    "_get_local_config_path",
    "_get_shared_root_from_config",
    "_legacy_data_candidates",
    "_load_paths_config",
    "_local_data_root",
    "_migrate_legacy_agent_icons",
    "_paths_equal",
    "_resolve_data_root",
    "_resolve_path_from_config",
    "_resolve_windows_documents_directory",
    "_reset_path_runtime_cache",
    "_shared_workflow_db_path",
    "_substitute_path_variables",
]
