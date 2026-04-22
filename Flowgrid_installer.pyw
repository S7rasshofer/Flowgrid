#!/usr/bin/env python3
"""
Flowgrid Installer - Pure Python Installation Script

This installer runs from the shared drive and installs Flowgrid locally.
No .CMD files, no PowerShell, no external scripts.

Requirements:
- Python 3.10+
- PySide6 (required, will be auto-installed if missing)
- Run from shared drive by double-clicking this .pyw file

Installation steps:
1. Verify Python version
2. Check/install dependencies
3. Copy app and assets to local Documents\\Flowgrid
4. Write local shared-root manifest
5. Verify shared database path
6. Create desktop shortcut
7. Show completion dialog
"""

import ctypes
import getpass
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flowgrid_app.installer import (
    DESKTOP_SHORTCUT_FILENAME,
    MANAGED_SHORTCUT_ICON_FILENAME,
    WINDOWS_SHORTCUT_DESCRIPTION,
    _create_or_update_windows_shortcut,
    _preferred_gui_python_executable,
)

APP_TITLE = "Flowgrid"
CONFIG_FILENAME = "Flowgrid_config.json"
LOCAL_APP_FOLDER_NAME = APP_TITLE
LOGS_DIR_NAME = "Logs"
MIN_PYTHON_VERSION = (3, 8, 0)
DEPENDENCY_SPECS: Tuple[Tuple[str, str, bool], ...] = (
    ("PySide6", "PySide6", True),
)
_FLOWGRID_PATHS_CONFIG: Optional[Dict[str, Any]] = None


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

# ============================================================================
# CONFIGURATION LOADING (same as main app)
# ============================================================================

def _find_local_paths_config() -> Optional[Path]:
    """Locate Flowgrid_paths.json in the local installer runtime folder."""
    candidates: List[Path] = []

    env_override = str(os.environ.get("FLOWGRID_PATHS_CONFIG", "") or "").strip()
    if env_override:
        candidates.append(Path(env_override))

    try:
        script_dir = Path(__file__).resolve().parent
        candidates.append(script_dir / "Flowgrid_paths.json")
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
    """Load and cache Flowgrid_paths.json configuration."""
    global _FLOWGRID_PATHS_CONFIG

    if _FLOWGRID_PATHS_CONFIG is not None:
        return _FLOWGRID_PATHS_CONFIG

    config_path = _find_local_paths_config()
    if config_path is None:
        return {}

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except Exception as exc:
        details = f"Config path: {config_path}\nReason: {type(exc).__name__}: {exc}"
        log_installer_error("PATHS_CONFIG_PARSE_FAILED", "Failed to parse Flowgrid_paths.json.", details)
        raise RuntimeError(details)

    if not isinstance(loaded, dict):
        details = f"Config path: {config_path}\nValue type: {type(loaded).__name__}"
        log_installer_error("PATHS_CONFIG_INVALID", "Flowgrid_paths.json must contain a JSON object.", details)
        raise RuntimeError(details)

    _FLOWGRID_PATHS_CONFIG = loaded
    return _FLOWGRID_PATHS_CONFIG


def get_script_root() -> Path:
    """Get the directory where this installer script is located."""
    try:
        return Path(__file__).resolve().parent
    except Exception:
        return Path.cwd()


def get_env_source_root() -> Optional[Path]:
    """Get source root from an environment override, if present."""
    override = str(os.environ.get("FLOWGRID_SOURCE_ROOT", "") or "").strip()
    if not override:
        return None
    candidate = Path(override)
    if candidate.exists() and (candidate / "Flowgrid.pyw").exists():
        return candidate.resolve()
    return None


def get_config_source_root(shared_root: Path) -> Optional[Path]:
    """Get an explicit source root from configuration, if present."""
    config = load_paths_config()
    source_root = config.get("source_root")
    if not source_root and isinstance(config.get("shared_paths"), dict):
        source_root = config["shared_paths"].get("source_root")
    if not source_root:
        return None
    candidate = Path(substitute_path_variables(str(source_root), shared_root))
    if candidate.exists() and (candidate / "Flowgrid.pyw").exists():
        return candidate.resolve()
    return None


def find_nearby_source_root() -> Optional[Path]:
    """Search the script and working-directory ancestry for Flowgrid.pyw."""
    checked = set()
    for base in (get_script_root(), Path.cwd()):
        path = base
        for _ in range(6):
            if path in checked:
                break
            checked.add(path)
            if (path / "Flowgrid.pyw").exists():
                return path.resolve()
            path = path.parent
    return None


def find_actual_shared_root() -> Path:
    """Resolve the shared root from the installer environment."""
    env_root = get_env_source_root()
    if env_root is not None:
        return env_root

    script_root = get_script_root()
    if (script_root / "Flowgrid.pyw").exists() and (script_root / "Assets").exists():
        return script_root.resolve()

    raise RuntimeError(
        "Installer must be run from the shared drive source root or "
        "FLOWGRID_SOURCE_ROOT must point to a valid shared install path."
    )


def substitute_path_variables(template: str, shared_root: Path) -> str:
    """Substitute {DOCUMENTS}, {SHARED_ROOT}, etc. in path strings."""
    if not template:
        return ""

    result = str(template)

    # Substitute {DOCUMENTS}
    if "{DOCUMENTS}" in result:
        try:
            documents = _resolve_windows_documents_directory() or (Path.home() / "Documents")
            result = result.replace("{DOCUMENTS}", str(documents))
        except Exception:
            pass

    # Substitute {SHARED_ROOT}
    if "{SHARED_ROOT}" in result:
        result = result.replace("{SHARED_ROOT}", str(shared_root))

    return result


def resolve_path_from_config(config_key: str, default: str, shared_root: Path) -> Path:
    """Retrieve a path from the config, with substitution and fallback."""
    config = load_paths_config()

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
        return Path(substitute_path_variables(default, shared_root))

    return Path(substitute_path_variables(str(value), shared_root))


DEFAULT_SHARED_ROOT = Path(r"Z:\DATA\Flowgrid")


def get_shared_root_from_config() -> Path:
    """Get the configured shared drive root from the actual installer location."""
    return find_actual_shared_root()


def get_source_root() -> Path:
    """Return the actual source root used for installation files."""
    return find_actual_shared_root()


# ============================================================================
# ERROR LOGGING TO SHARED DRIVE
# ============================================================================

def get_installer_error_log_path() -> Path:
    """Get path for installer error log on shared drive."""
    shared_root = find_actual_shared_root()
    log_path = shared_root / LOGS_DIR_NAME / "Flowgrid_installer_errors.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return log_path


def log_installer_error(error_code: str, summary: str, details: str = "") -> None:
    """Log installer errors to shared drive."""
    try:
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_path = get_installer_error_log_path()

        lines = [
            f"[{now}] [{error_code}] {summary}",
            f"Python: {sys.executable}",
            f"Version: {sys.version}",
            f"User: {os.environ.get('USERNAME', 'UNKNOWN')}",
            f"Computer: {os.environ.get('COMPUTERNAME', 'UNKNOWN')}",
        ]

        if details:
            lines.append(f"Details: {details}")

        lines.extend([
            f"Traceback: {traceback.format_exc()}",
            "-" * 80,
            ""
        ])

        try:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write("\n".join(lines))
        except Exception as write_exc:
            fallback_path = get_script_root() / LOGS_DIR_NAME / "Flowgrid_installer_errors.log"
            try:
                fallback_path.parent.mkdir(parents=True, exist_ok=True)
                with fallback_path.open("a", encoding="utf-8") as handle:
                    handle.write("\n".join(lines))
                    handle.write("\n[FAILOVER] Failed to write to shared log path: " + str(write_exc) + "\n")
            except Exception:
                # If fallback also fails, print to console
                print(f"[INSTALLER ERROR] {error_code}: {summary}")
                if details:
                    print(f"Details: {details}")
                print(f"Failed to write installer log to both {log_path} and {fallback_path}")
    except Exception as log_exc:
        # If logging fails, try to print to console as last resort
        try:
            print(f"[INSTALLER ERROR] {error_code}: {summary}")
            if details:
                print(f"Details: {details}")
        except Exception:
            pass


def log_installer_status(status_code: str, summary: str, details: str = "") -> None:
    """Write informational installer diagnostics to the shared installer log."""
    try:
        from datetime import datetime
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_path = get_installer_error_log_path()

        lines = [
            f"[{now}] [INFO:{status_code}] {summary}",
            f"Python: {sys.executable}",
            f"Version: {sys.version}",
            f"User: {os.environ.get('USERNAME', 'UNKNOWN')}",
            f"Computer: {os.environ.get('COMPUTERNAME', 'UNKNOWN')}",
        ]

        if details:
            lines.append(f"Details: {details}")

        lines.extend([
            "-" * 80,
            ""
        ])

        with log_path.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
    except Exception:
        pass


# ============================================================================
# PYTHON VERSION CHECK
# ============================================================================

def check_python_version() -> Tuple[bool, str]:
    """Verify Python version meets the shared Flowgrid runtime minimum."""
    required_version = MIN_PYTHON_VERSION
    current_version = sys.version_info[:3]

    if current_version < required_version:
        version_str = ".".join(map(str, current_version))
        required_str = ".".join(map(str, required_version))
        return False, f"Python {required_str}+ required. Current: {version_str}"

    return True, ""


# ============================================================================
# DEPENDENCY MANAGEMENT
# ============================================================================

def get_dependency_specs() -> List[Tuple[str, str, bool]]:
    """Get dependency specs aligned with the runtime contract."""
    specs = list(DEPENDENCY_SPECS)
    config = load_paths_config()
    app_settings = config.get("app_settings", {})
    required_raw = app_settings.get("required_packages")
    optional_raw = app_settings.get("optional_packages")

    if isinstance(required_raw, list):
        specs = [(str(name), str(name).replace("-", "_"), True) for name in required_raw if str(name).strip()]
        if isinstance(optional_raw, list):
            specs.extend(
                (str(name), str(name).replace("-", "_"), False)
                for name in optional_raw
                if str(name).strip()
            )

    return specs


def get_required_packages() -> List[str]:
    """Compatibility helper returning required package names only."""
    return [package_name for package_name, _module_name, required in get_dependency_specs() if required]


def check_package_import(module_name: str) -> Tuple[bool, str]:
    """Check if a package/module can be imported."""
    try:
        __import__(module_name)
        return True, ""
    except ImportError as e:
        return False, str(e)


def install_package(package_name: str) -> Tuple[bool, str]:
    """Install a Python package using pip."""
    try:
        print(f"Installing {package_name}...")

        result = subprocess.run(
            [sys.executable, "-m", "pip", "--disable-pip-version-check", "install", package_name],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            print(f"✓ Successfully installed {package_name}")
            return True, ""
        else:
            error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown pip error"
            return False, f"pip install failed: {error_msg}"

    except subprocess.TimeoutExpired:
        return False, "Installation timed out after 5 minutes"
    except Exception as e:
        return False, f"Installation error: {e}"


def ensure_dependencies() -> Tuple[bool, str]:
    """Check/install required dependencies and report optional-package state."""
    dependency_specs = get_dependency_specs()
    failed_packages: List[str] = []
    optional_warnings: List[str] = []

    for package_name, module_name, required in dependency_specs:
        print(f"Checking {package_name}...")

        installed, error = check_package_import(module_name)
        if installed:
            print(f"[OK] {package_name} is already installed")
            continue

        success, install_error = install_package(package_name)
        if not success:
            if required:
                failed_packages.append(f"{package_name}: {install_error}")
            else:
                optional_warnings.append(f"{package_name}: {install_error}")
            continue

        installed, error = check_package_import(module_name)
        if not installed:
            if required:
                failed_packages.append(f"{package_name}: Import failed after install - {error}")
            else:
                optional_warnings.append(f"{package_name}: Import failed after install - {error}")

    if failed_packages:
        return False, f"Failed to install: {', '.join(failed_packages)}"

    if optional_warnings:
        warning_text = "; ".join(optional_warnings)
        print(f"[WARN] Optional packages unavailable: {warning_text}")
        log_installer_status("OPTIONAL_DEPENDENCIES", "Optional dependencies unavailable.", warning_text)

    return True, ""


# ============================================================================
# FILE COPYING UTILITIES
# ============================================================================

def copy_file_with_progress(src: Path, dst: Path, description: str) -> bool:
    """Copy a file using temp+replace for deterministic reinstall behavior."""
    temp_path = dst.with_name(f"{dst.name}.tmp")
    try:
        print(f"Copying {description}...")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if temp_path.exists():
            temp_path.unlink()
        shutil.copy2(src, temp_path)
        os.replace(temp_path, dst)
        return True
    except Exception as e:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        log_installer_error("COPY_FILE_FAILED", f"Failed to copy {description}", str(e))
        return False


def copy_directory_recursive(src: Path, dst: Path, description: str) -> bool:
    """Copy an entire directory recursively."""
    try:
        print(f"Copying {description}...")
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(
            src,
            dst,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
        )
        return True
    except Exception as e:
        log_installer_error("COPY_DIR_FAILED", f"Failed to copy {description}", str(e))
        return False


def _remove_managed_path(path: Path, description: str) -> bool:
    """Delete an installer-managed file or directory before reinstall."""
    try:
        if not path.exists():
            return True
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return True
    except Exception as exc:
        log_installer_error(
            "MANAGED_PATH_REMOVE_FAILED",
            f"Failed removing managed runtime path: {description}",
            f"Path: {path}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def _managed_local_runtime_targets(paths: Dict[str, Path]) -> List[Tuple[Path, str]]:
    """Return installer-managed local runtime artifacts used for reinstall/update detection."""
    return [
        (paths["local_app"], "local Flowgrid application"),
        (paths["local_package"], "local Flowgrid package folder"),
        (paths["local_paths_config"], "local shared-root manifest"),
        (paths["local_assets"], "local packaged Assets folder"),
    ]


def detect_existing_local_install(paths: Dict[str, Path]) -> bool:
    """Treat the run as an update when an installer-managed local runtime already exists."""
    local_app = paths["local_app"]
    if local_app.exists() and local_app.is_file():
        return True

    for target, _description in _managed_local_runtime_targets(paths)[1:]:
        if target.exists():
            return True
    return False


def purge_managed_local_runtime(paths: Dict[str, Path]) -> bool:
    """Remove installer-owned local runtime artifacts before reinstalling."""
    for target, description in _managed_local_runtime_targets(paths):
        if not _remove_managed_path(target, description):
            return False
    return True


def _find_default_wrench_icon(source_root: Path, shared_root: Path) -> Path | None:
    candidates = [
        source_root / "Assets" / "Flowgrid Icons" / "wrench.png",
        source_root / "Assets" / "Flowgrid Icons" / "wrench.png",
        Path(str(shared_root)) / "Assets" / "Flowgrid Icons" / "wrench.png",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _create_shortcut_icon(source_icon: Path, target_ico: Path) -> Path:
    from PySide6.QtWidgets import QApplication
    from PySide6.QtGui import QImage, QPainter, QPixmap
    from PySide6.QtCore import Qt

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


# ============================================================================
# INSTALLATION STEPS
# ============================================================================

def _assets_source_candidates(source_root: Path, shared_root: Path) -> Path:
    """Choose the best source path for assets."""
    default_shared_assets = resolve_path_from_config("shared_paths.assets_folder", "{SHARED_ROOT}\\Assets", shared_root)
    local_installer_assets = source_root / "Assets"

    def is_valid_asset_folder(path: Path) -> bool:
        if not path.exists() or not path.is_dir():
            return False
        try:
            names = {child.name for child in path.iterdir() if child.is_dir()}
            expected = {
                "admin_icons",
                "agent_icons",
                "Flowgrid Icons",
                "part_flag_images",
                "qa_flag_icons",
                "ui_icons",
            }
            return bool(names & expected)
        except Exception:
            return False

    if is_valid_asset_folder(local_installer_assets):
        return local_installer_assets
    if is_valid_asset_folder(default_shared_assets):
        return default_shared_assets
    return local_installer_assets if local_installer_assets.exists() else default_shared_assets


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


def get_installation_paths() -> Dict[str, Path]:
    """Get all paths needed for installation."""
    source_root = get_source_root()
    shared_root = find_actual_shared_root()

    shared_assets = _assets_source_candidates(source_root, shared_root)
    documents_folder = _resolve_windows_documents_directory() or (Path.home() / "Documents")
    local_app_folder = documents_folder / LOCAL_APP_FOLDER_NAME
    local_config_folder = local_app_folder / "Config"
    local_data_folder = local_app_folder / "Data"
    local_queue_folder = local_app_folder / "Queue"

    return {
        "shared_root": shared_root,
        "source_root": source_root,
        "shared_app": source_root / "Flowgrid.pyw",
        "shared_package": source_root / "flowgrid_app",
        "shared_assets": shared_assets,
        "shared_db": shared_root / "Flowgrid_depot.db",
        "shared_config": shared_root / CONFIG_FILENAME,
        "documents_folder": documents_folder,
        "local_app_folder": local_app_folder,
        "local_app": local_app_folder / "Flowgrid.pyw",
        "local_package": local_app_folder / "flowgrid_app",
        "local_config_folder": local_config_folder,
        "local_config": local_config_folder / CONFIG_FILENAME,
        "local_data_folder": local_data_folder,
        "local_queue_folder": local_queue_folder,
        "local_assets": local_app_folder / "Assets",
        "local_paths_config": local_app_folder / "Flowgrid_paths.json",
    }


def write_local_paths_config(paths: Dict[str, Path]) -> bool:
    """Write the local install manifest that pins the shared root."""
    target = paths["local_paths_config"]
    temp_path = target.with_name(f"{target.name}.tmp")
    payload = {
        "shared_drive_root": str(paths["shared_root"]),
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
        temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temp_path, target)
        return True
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        log_installer_error("WRITE_MANIFEST_FAILED", "Failed to write local Flowgrid_paths.json manifest", str(exc))
        return False


def verify_local_paths_config(paths: Dict[str, Path]) -> bool:
    """Read back the local manifest and verify the shared root contract."""
    target = paths["local_paths_config"]
    expected_root = str(paths["shared_root"])
    try:
        with target.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("Manifest payload is not a JSON object.")
        actual_root = str(payload.get("shared_drive_root") or "").strip()
        if actual_root != expected_root:
            raise ValueError(f"Expected shared_drive_root={expected_root!r}, found {actual_root!r}.")
        return True
    except Exception as exc:
        details = (
            f"Manifest path: {target}\n"
            f"Expected shared root: {expected_root}\n"
            f"Reason: {type(exc).__name__}: {exc}"
        )
        log_installer_error("VERIFY_MANIFEST_FAILED", "Failed verifying local Flowgrid_paths.json manifest.", details)
        return False


def create_local_folders(paths: Dict[str, Path]) -> bool:
    """Create all necessary local folders."""
    try:
        paths["local_app_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_config_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_data_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_queue_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_assets"].parent.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        log_installer_error("CREATE_FOLDERS_FAILED", "Failed to create local folders", str(e))
        return False


def initialize_local_user_config(paths: Dict[str, Path]) -> bool:
    """Ensure each user has a local config file under Documents."""
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
                else:
                    log_installer_error(
                        "LOCAL_CONFIG_SHARED_INVALID",
                        "Shared Flowgrid config was not a JSON object; creating a blank local config instead.",
                        f"Path: {shared_legacy}",
                    )
            except Exception as exc:
                log_installer_error(
                    "LOCAL_CONFIG_SHARED_READ_FAILED",
                    "Failed reading shared Flowgrid config during local config bootstrap; creating a blank local config instead.",
                    f"Path: {shared_legacy}\nReason: {type(exc).__name__}: {exc}",
                )

        temp_path.write_text(payload, encoding="utf-8")
        os.replace(temp_path, target)
        return True
    except Exception as exc:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass
        log_installer_error(
            "LOCAL_CONFIG_INIT_FAILED",
            "Failed to create the per-user local Flowgrid config file.",
            f"Path: {target}\nReason: {type(exc).__name__}: {exc}",
        )
        return False


def assess_source_materials(paths: Dict[str, Path]) -> Tuple[List[str], List[str]]:
    """Return warnings and errors for source material availability."""
    warnings: List[str] = []
    errors: List[str] = []

    expected_shared_root = paths["shared_root"]
    if not expected_shared_root.exists():
        errors.append(f"Expected shared drive root does not exist: {expected_shared_root}")
    elif not expected_shared_root.is_dir():
        errors.append(f"Expected shared drive root is not a directory: {expected_shared_root}")
    else:
        # Check for required files at the configured shared root
        required_files = ["Flowgrid.pyw", "flowgrid_app", "Assets"]
        for req in required_files:
            req_path = expected_shared_root / req
            if not req_path.exists():
                errors.append(f"Required source file/folder missing: {req_path}")

    if not paths["shared_app"].exists():
        errors.append(f"Flowgrid.pyw not found at source location: {paths['shared_app']}")

    if not paths["shared_package"].exists() or not paths["shared_package"].is_dir():
        errors.append(f"flowgrid_app package not found at source location: {paths['shared_package']}")

    if not paths["shared_assets"].exists():
        errors.append(f"Assets folder not found at source location: {paths['shared_assets']}")

    return warnings, errors


def copy_app_files(paths: Dict[str, Path]) -> bool:
    """Copy Flowgrid.pyw to local folder."""
    if not paths["shared_app"].exists():
        log_installer_error("APP_NOT_FOUND", "Flowgrid.pyw not found on shared drive", str(paths["shared_app"]))
        return False

    return copy_file_with_progress(paths["shared_app"], paths["local_app"], "Flowgrid application")


def copy_app_package(paths: Dict[str, Path]) -> bool:
    """Copy flowgrid_app package to local folder."""
    if not paths["shared_package"].exists() or not paths["shared_package"].is_dir():
        log_installer_error("PACKAGE_NOT_FOUND", "flowgrid_app package not found on shared drive", str(paths["shared_package"]))
        return False

    return copy_directory_recursive(paths["shared_package"], paths["local_package"], "flowgrid_app package")


def copy_assets(paths: Dict[str, Path]) -> bool:
    """Copy Assets folder to local folder."""
    if not paths["shared_assets"].exists():
        log_installer_error("ASSETS_NOT_FOUND", "Assets folder not found on shared drive", str(paths["shared_assets"]))
        return False

    return copy_directory_recursive(paths["shared_assets"], paths["local_assets"], "Assets folder")


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


def initialize_database(paths: Dict[str, Path]) -> bool:
    """Write the local install manifest and bootstrap shared DB admin users."""
    if not write_local_paths_config(paths):
        return False
    if not verify_local_paths_config(paths):
        return False
    if not initialize_local_user_config(paths):
        return False

    current_user = _detect_installer_user()
    shared_db = paths["shared_db"]

    try:
        shared_db.parent.mkdir(parents=True, exist_ok=True)
        shared_assets_root = shared_db.parent / "Assets"
        for folder in ("agent_icons", "admin_icons", "qa_flag_icons", "part_flag_images"):
            (shared_assets_root / folder).mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(shared_db), timeout=30.0) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA busy_timeout = 20000")
            conn.execute("PRAGMA journal_mode = DELETE")
            conn.execute(
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS role_definitions (
                    id INTEGER PRIMARY KEY,
                    role_name TEXT NOT NULL UNIQUE,
                    role_slot TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            default_roles = (
                ("Tech 1", "tech1", 1),
                ("Tech 2", "tech2", 2),
                ("Tech 3", "tech3", 3),
                ("MP", "mp", 4),
                ("QA", "qa", 5),
                ("Other", "none", 6),
            )
            for role_name, role_slot, sort_order in default_roles:
                conn.execute(
                    "INSERT OR IGNORE INTO role_definitions (role_name, role_slot, sort_order) VALUES (?, ?, ?)",
                    (role_name, role_slot, int(sort_order)),
                )
            row = conn.execute("SELECT COUNT(*) FROM admin_users").fetchone()
            admin_count = int(row[0] if row is not None else 0)
            if admin_count == 0:
                conn.execute(
                    "INSERT INTO admin_users (user_id, admin_name, position, location, icon_path, access_level) VALUES (?, ?, 'Other', '', '', 'admin')",
                    (current_user, current_user),
                )
            conn.commit()
        return True
    except Exception as e:
        log_installer_error(
            "VERIFY_SHARED_DB_FAILED",
            "Failed to verify shared database access on the network drive",
            f"Shared DB path: {shared_db}\nReason: {type(e).__name__}: {e}",
        )
        return False


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


def _managed_shortcut_icon_path(paths: Dict[str, Path]) -> Path:
    return paths["shared_root"] / "Assets" / "Flowgrid Icons" / MANAGED_SHORTCUT_ICON_FILENAME


def create_desktop_shortcut(paths: Dict[str, Path]) -> bool:
    """Create desktop shortcut using the shared .lnk contract."""
    try:
        print("Creating desktop shortcut...")

        desktop_path = _resolve_windows_desktop_directory()
        if desktop_path is None:
            raise RuntimeError("Unable to resolve desktop folder")

        icon_source = _find_default_wrench_icon(paths["source_root"], paths["shared_root"])
        if icon_source is None:
            raise RuntimeError("Unable to locate the default wrench icon for desktop shortcut creation.")

        from flowgrid_app.icon_io import _write_managed_shortcut_icon

        managed_icon_path = _managed_shortcut_icon_path(paths)
        managed_icon_path.parent.mkdir(parents=True, exist_ok=True)
        _write_managed_shortcut_icon(icon_source, managed_icon_path)

        launcher_path = _preferred_gui_python_executable()
        script_path = paths["local_app"]
        if not launcher_path.exists() or not launcher_path.is_file():
            raise RuntimeError(f"Python launcher not found: {launcher_path}")
        if not script_path.exists() or not script_path.is_file():
            raise RuntimeError(f"Installed Flowgrid.pyw not found: {script_path}")

        shortcut_path = desktop_path / DESKTOP_SHORTCUT_FILENAME
        arguments = f'"{script_path}"'
        ok, detail = _create_or_update_windows_shortcut(
            shortcut_path,
            launcher_path,
            arguments,
            script_path.parent,
            managed_icon_path,
            WINDOWS_SHORTCUT_DESCRIPTION,
        )
        if not ok:
            raise RuntimeError(detail or "Unknown desktop shortcut save failure.")
        return True

    except Exception as e:
        log_installer_error("SHORTCUT_FAILED", "Failed to create desktop shortcut", str(e))
        return False


def build_installation_report(paths: Dict[str, Path], steps: List[Dict[str, str]], is_update_install: bool = False) -> str:
    """Build a human-readable installer verification summary."""
    headline = "Flowgrid has been updated." if is_update_install else "Flowgrid Installation Complete"
    lines: List[str] = [
        headline,
        "",
        f"Local install: {paths['local_app_folder']}",
        f"Local package: {paths['local_package']}",
        f"Shared root: {paths['shared_root']}",
        f"Shared DB: {paths['shared_db']}",
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

    warnings = [step for step in steps if str(step.get("status", "")).strip().lower() == "warning"]
    if warnings:
        lines.extend(["", "Warnings:"])
        for step in warnings:
            label = str(step.get("label", "") or "").strip()
            detail = str(step.get("detail", "") or "").strip()
            lines.append(f"- {label}{(': ' + detail) if detail else ''}")

    lines.extend(
        [
            "",
            "Authoritative data source:",
            "- Shared workflow DB: shared_root\\Flowgrid_depot.db",
            "- Editable icons: shared_root\\Assets\\agent_icons, admin_icons, qa_flag_icons",
        ]
    )
    return "\n".join(lines)


def verify_installed_state(paths: Dict[str, Path]) -> List[Dict[str, str]]:
    """Verify final installed runtime state for the completion popup."""
    results: List[Dict[str, str]] = []

    local_app = paths["local_app"]
    _record_step(
        results,
        "Flowgrid.pyw installed",
        "ok" if local_app.exists() and local_app.is_file() else "failed",
        "Local runtime entrypoint present." if local_app.exists() and local_app.is_file() else "Local Flowgrid.pyw missing after install.",
        str(local_app),
    )

    local_package = paths["local_package"]
    package_init = local_package / "__init__.py"
    _record_step(
        results,
        "flowgrid_app package installed",
        "ok" if local_package.exists() and local_package.is_dir() and package_init.exists() and package_init.is_file() else "failed",
        (
            "Local runtime support package present."
            if local_package.exists() and local_package.is_dir() and package_init.exists() and package_init.is_file()
            else "Local flowgrid_app package missing after install."
        ),
        str(local_package),
    )

    local_assets = paths["local_assets"]
    _record_step(
        results,
        "Packaged Assets installed",
        "ok" if local_assets.exists() and local_assets.is_dir() else "failed",
        "Local packaged Assets folder present." if local_assets.exists() and local_assets.is_dir() else "Local Assets folder missing after install.",
        str(local_assets),
    )

    local_manifest = paths["local_paths_config"]
    manifest_ok = local_manifest.exists() and local_manifest.is_file() and verify_local_paths_config(paths)
    _record_step(
        results,
        "Local Flowgrid_paths.json written and verified",
        "ok" if manifest_ok else "failed",
        "Local manifest points to the expected shared root." if manifest_ok else "Local Flowgrid_paths.json missing or invalid.",
        str(local_manifest),
    )

    local_config = paths["local_config"]
    _record_step(
        results,
        "Local user config ready",
        "ok" if local_config.exists() and local_config.is_file() else "failed",
        "Per-user local config file present." if local_config.exists() and local_config.is_file() else "Per-user local config file missing.",
        str(local_config),
    )

    shared_db = paths["shared_db"]
    shared_db_ok = False
    if shared_db.exists() and shared_db.is_file():
        try:
            with sqlite3.connect(str(shared_db), timeout=10.0) as conn:
                conn.execute("SELECT 1")
            shared_db_ok = True
        except Exception as exc:
            log_installer_error(
                "VERIFY_SHARED_DB_POST_INSTALL_FAILED",
                "Shared DB exists but could not be opened during final installer verification.",
                f"Path: {shared_db}\nReason: {type(exc).__name__}: {exc}",
            )
    _record_step(
        results,
        "Shared DB reachable",
        "ok" if shared_db_ok else "failed",
        "Shared workflow DB exists and opened successfully." if shared_db_ok else "Shared workflow DB missing or not reachable.",
        str(shared_db),
    )

    shared_assets_root = shared_db.parent / "Assets"
    editable_dirs = [
        shared_assets_root / "agent_icons",
        shared_assets_root / "admin_icons",
        shared_assets_root / "qa_flag_icons",
    ]
    editable_ok = all(path.exists() and path.is_dir() for path in editable_dirs)
    _record_step(
        results,
        "Shared editable icon folders ready",
        "ok" if editable_ok else "failed",
        "Shared editable icon directories are present." if editable_ok else "One or more shared editable icon folders are missing.",
        str(shared_assets_root),
    )

    return results


# ============================================================================
# GUI DIALOGS
# ============================================================================

def show_installation_dialog(title: str, message: str, is_error: bool = False, paths: Dict[str, Path] | None = None) -> None:
    """Show installation status dialog using PySide6."""
    try:
        from PySide6.QtWidgets import QApplication, QMessageBox, QDialog, QVBoxLayout, QLabel, QPushButton, QPlainTextEdit
        from PySide6.QtCore import Qt

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
        else:
            # Custom dialog for installation complete
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

            button = QPushButton("Launch Flowgrid")
            button.clicked.connect(dialog.accept)
            layout.addWidget(button)

            close_button = QPushButton("Close")
            close_button.clicked.connect(dialog.reject)
            layout.addWidget(close_button)

            dialog.setLayout(layout)
            result = dialog.exec()

            if result == QDialog.Accepted and paths is not None:
                # Launch Flowgrid
                try:
                    import subprocess
                    subprocess.Popen([sys.executable, str(paths["local_app"])], cwd=str(paths["local_app"].parent))
                except Exception as e:
                    log_installer_error("LAUNCH_FAILED", "Failed to launch Flowgrid after install", str(e))

    except Exception as e:
        log_installer_error("DIALOG_FAILED", "Failed to show installation dialog", str(e))
        # Fallback to console
        print(f"{title}: {message}")


# ============================================================================
# MAIN INSTALLATION FUNCTION
# ============================================================================

def run_installer() -> int:
    """Main installer function."""
    install_steps: List[Dict[str, str]] = []
    print("Flowgrid Installer")
    print(f"Python: {sys.executable}")
    print(f"Version: {sys.version}")
    print()

    # Step 1: Check Python version
    print("Step 1: Checking Python version...")
    version_ok, version_error = check_python_version()
    if not version_ok:
        error_msg = f"Python version check failed: {version_error}"
        _record_step(install_steps, "Python version", "failed", error_msg)
        log_installer_error("PYTHON_VERSION", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Python version", "ok", f"Python {'.'.join(map(str, MIN_PYTHON_VERSION))}+ detected.", sys.executable)
    print(f"✓ Python {'.'.join(map(str, MIN_PYTHON_VERSION))}+ detected")
    print()

    # Step 2: Ensure dependencies
    print("Step 2: Checking dependencies...")
    deps_ok, deps_error = ensure_dependencies()
    if not deps_ok:
        error_msg = f"Dependency installation failed: {deps_error}"
        _record_step(install_steps, "Dependencies", "failed", deps_error)
        log_installer_error("DEPENDENCIES", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Dependencies", "ok", "Required dependencies are installed and importable.")
    print("✓ All dependencies installed")
    print()

    # Step 3: Get installation paths
    print("Step 3: Preparing installation paths...")
    paths = get_installation_paths()
    print(f"Source root: {paths['source_root']}")
    print(f"Shared root: {paths['shared_root']}")
    print(f"Local app folder: {paths['local_app_folder']}")
    print(f"Local package folder: {paths['local_package']}")
    print(f"Local assets folder: {paths['local_assets']}")
    is_update_install = detect_existing_local_install(paths)
    log_installer_status(
        "INSTALL_CONTEXT",
        "Resolved installer path context.",
        "\n".join(
            [
                f"source_root={paths['source_root']}",
                f"shared_root={paths['shared_root']}",
                f"shared_db={paths['shared_db']}",
                f"local_app={paths['local_app']}",
                f"local_package={paths['local_package']}",
                f"local_manifest={paths['local_paths_config']}",
                "workflow_db_source_of_truth=shared_root/Flowgrid_depot.db",
            ]
        ),
    )
    print()

    # Step 4: Verify source materials are available
    print(f"Step 4: Verifying source materials exist at {paths['shared_root']}...")
    source_warnings, source_errors = assess_source_materials(paths)
    for warning in source_warnings:
        print(f"⚠ {warning}")
    if source_warnings:
        log_installer_error("SHARED_ROOT_WARNING", "Shared drive visibility issue", "\n".join(source_warnings))
    if source_errors:
        error_msg = (
            "Installer cannot continue because source materials are unavailable. "
            "Please ensure Flowgrid.pyw, flowgrid_app, and the Assets folder are visible from the shared drive or the downloaded package."
        )
        details = "\n".join(source_errors)
        _record_step(install_steps, "Source materials", "failed", details, str(paths["shared_root"]))
        log_installer_error("SOURCE_MATERIALS_UNAVAILABLE", error_msg, details)
        show_installation_dialog("Installation Failed", f"{error_msg}\n\n{details}", is_error=True)
        return 1
    _record_step(
        install_steps,
        "Source materials",
        "warning" if source_warnings else "ok",
        "Required source files are available." if not source_warnings else f"Required source files are available with warnings: {'; '.join(source_warnings)}",
        str(paths["shared_root"]),
    )
    print("✓ Source materials verified")
    print()

    # Step 5: Create local folders
    print("Step 5: Creating local folders...")
    if not create_local_folders(paths):
        error_msg = "Failed to create local installation folders"
        _record_step(install_steps, "Local folders", "failed", error_msg, str(paths["local_app_folder"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Local folders", "ok", "Local app/config/data/queue folders prepared.", str(paths["local_app_folder"]))
    print("✓ Local folders created")
    print()

    # Step 6: Remove managed runtime artifacts from prior installs
    print("Step 6: Removing stale managed runtime files...")
    if not purge_managed_local_runtime(paths):
        error_msg = "Failed to remove stale managed local runtime files"
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("âœ“ Managed local runtime cleaned")
    print()

    # Step 7: Copy application
    print("Step 7: Installing Flowgrid application...")
    if not copy_app_files(paths):
        error_msg = "Failed to copy Flowgrid application"
        _record_step(install_steps, "Flowgrid.pyw install", "failed", error_msg, str(paths["local_app"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Flowgrid.pyw install", "ok", "Local Flowgrid.pyw copied successfully.", str(paths["local_app"]))
    print("[OK] Application installed")
    print()

    # Step 8: Copy support package
    print("Step 8: Installing support package...")
    if not copy_app_package(paths):
        error_msg = "Failed to copy flowgrid_app package"
        _record_step(install_steps, "flowgrid_app install", "failed", error_msg, str(paths["local_package"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "flowgrid_app install", "ok", "Local flowgrid_app package copied successfully.", str(paths["local_package"]))
    print("[OK] Support package installed")
    print()

    # Step 9: Copy assets
    print("Step 9: Installing assets...")
    if not copy_assets(paths):
        error_msg = "Failed to copy assets folder"
        _record_step(install_steps, "Packaged Assets install", "failed", error_msg, str(paths["local_assets"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Packaged Assets install", "ok", "Local packaged Assets folder copied successfully.", str(paths["local_assets"]))
    print("[OK] Assets installed")
    print()

    # Step 10: Initialize database
    print("Step 10: Initializing database...")
    if not initialize_database(paths):
        error_msg = "Failed to initialize database"
        _record_step(install_steps, "Shared DB initialization", "failed", error_msg, str(paths["shared_db"]))
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    _record_step(install_steps, "Local Flowgrid_paths.json", "ok", "Local shared-root manifest written and verified.", str(paths["local_paths_config"]))
    _record_step(install_steps, "Local user config", "ok", "Per-user local config file is ready.", str(paths["local_config"]))
    _record_step(install_steps, "Shared DB initialization", "ok", "Shared DB validated and bootstrap completed.", str(paths["shared_db"]))
    _record_step(install_steps, "Shared editable icon folders", "ok", "Shared editable icon folders were prepared.", str(paths["shared_db"].parent / "Assets"))
    print("[OK] Database initialized")
    print()

    # Step 11: Create desktop shortcut
    print("Step 11: Creating desktop shortcut...")
    if not create_desktop_shortcut(paths):
        _record_step(install_steps, "Desktop shortcut", "warning", "Desktop shortcut could not be created.", str(paths["local_app_folder"]))
        print("[WARN] Desktop shortcut creation failed, but installation continues")
    else:
        _record_step(install_steps, "Desktop shortcut", "ok", "Desktop shortcut created.", str(paths["local_app_folder"]))
        print("[OK] Desktop shortcut created")
    print()

    # Step 12: Show completion dialog
    completion_msg = build_installation_report(
        paths,
        [*install_steps, *verify_installed_state(paths)],
        is_update_install=is_update_install,
    )
    completion_title = "Flowgrid Updated" if is_update_install else "Flowgrid Installed Successfully"

    show_installation_dialog(completion_title, completion_msg, False, paths)

    print("Update completed successfully!" if is_update_install else "Installation completed successfully!")
    return 0


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    try:
        exit_code = run_installer()
        sys.exit(exit_code)
    except Exception as e:
        error_msg = f"Installer crashed: {e}"
        log_installer_error("INSTALLER_CRASH", error_msg, traceback.format_exc())
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        sys.exit(1)
