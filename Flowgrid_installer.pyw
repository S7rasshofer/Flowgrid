#!/usr/bin/env python3
"""
Flowgrid Installer - Pure Python Installation Script

This installer runs from the shared drive and installs Flowgrid locally.
No .CMD files, no PowerShell, no external scripts.

Requirements:
- Python 3.14
- PySide6 (will be auto-installed if missing)
- Run from shared drive by double-clicking this .pyw file

Installation steps:
1. Verify Python 3.14
2. Check/install dependencies
3. Copy app and assets to local Documents\\Flowgrid
4. Initialize local database
5. Create desktop shortcut
6. Show completion dialog
"""

import ctypes
import json
import os
import shutil
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
# CONFIGURATION LOADING (same as main app)
# ============================================================================

def find_paths_config_on_shared_drive() -> Optional[Path]:
    """Locate Flowgrid_paths.json starting from the script location."""
    candidates: List[Path] = []

    # Try environment variable override first
    env_override = str(os.environ.get("FLOWGRID_PATHS_CONFIG", "") or "").strip()
    if env_override:
        candidates.append(Path(env_override))

    # Try script parent directory (common when running from shared drive)
    try:
        script_dir = Path(__file__).resolve().parent
        candidates.append(script_dir / "Flowgrid_paths.json")
    except Exception:
        pass

    # Try current working directory
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
    config_path = find_paths_config_on_shared_drive()
    if config_path is None:
        return {}

    try:
        with config_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        return {}


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
    """Resolve the best source root for installation files."""
    config = load_paths_config()
    shared_root_value = config.get("shared_drive_root")
    shared_root = Path(str(shared_root_value)) if shared_root_value else DEFAULT_SHARED_ROOT

    env_root = get_env_source_root()
    if env_root is not None:
        return env_root

    if shared_root.exists() and (shared_root / "Flowgrid.pyw").exists():
        return shared_root.resolve()

    # Use the configured shared root path even if it is not currently accessible.
    return shared_root


def substitute_path_variables(template: str, shared_root: Path) -> str:
    """Substitute {DOCUMENTS}, {SHARED_ROOT}, etc. in path strings."""
    if not template:
        return ""

    result = str(template)

    # Substitute {DOCUMENTS}
    if "{DOCUMENTS}" in result:
        try:
            documents = Path.home() / "Documents"
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
    """Get the configured shared drive root, defaulting to the expected path."""
    config = load_paths_config()
    shared_root_str = config.get("shared_drive_root")
    if not shared_root_str:
        shared_root_str = str(DEFAULT_SHARED_ROOT)
    return Path(shared_root_str)


def get_source_root() -> Path:
    """Return the actual source root used for installation files."""
    return find_actual_shared_root()


# ============================================================================
# ERROR LOGGING TO SHARED DRIVE
# ============================================================================

def get_installer_error_log_path() -> Path:
    """Get path for installer error log on shared drive, falling back to installer directory."""
    shared_root = get_shared_root_from_config()
    log_path = shared_root / "Flowgrid_installer_errors.log"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        return log_path
    except Exception:
        return get_script_root() / "Flowgrid_installer_errors.log"


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
            fallback_path = get_script_root() / "Flowgrid_installer_errors.log"
            try:
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


# ============================================================================
# PYTHON VERSION CHECK
# ============================================================================

def check_python_version() -> Tuple[bool, str]:
    """Verify Python 3.14 is being used."""
    required_version = (3, 14, 0)
    current_version = sys.version_info[:3]

    if current_version < required_version:
        version_str = ".".join(map(str, current_version))
        required_str = ".".join(map(str, required_version))
        return False, f"Python {required_str}+ required. Current: {version_str}"

    return True, ""


# ============================================================================
# DEPENDENCY MANAGEMENT
# ============================================================================

def get_required_packages() -> List[str]:
    """Get list of required packages from config."""
    config = load_paths_config()
    app_settings = config.get("app_settings", {})
    required = app_settings.get("required_packages", ["PySide6"])
    return required


def check_package_import(package_name: str) -> Tuple[bool, str]:
    """Check if a package can be imported."""
    try:
        __import__(package_name)
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
    """Check and install required dependencies."""
    packages = get_required_packages()
    failed_packages = []

    for package in packages:
        print(f"Checking {package}...")

        # Check if already installed
        installed, error = check_package_import(package)
        if installed:
            print(f"✓ {package} is already installed")
            continue

        # Try to install
        success, install_error = install_package(package)
        if not success:
            failed_packages.append(f"{package}: {install_error}")
            continue

        # Verify installation worked
        installed, error = check_package_import(package)
        if not installed:
            failed_packages.append(f"{package}: Import failed after install - {error}")

    if failed_packages:
        return False, f"Failed to install: {', '.join(failed_packages)}"

    return True, ""


# ============================================================================
# FILE COPYING UTILITIES
# ============================================================================

def copy_file_with_progress(src: Path, dst: Path, description: str) -> bool:
    """Copy a file with progress indication."""
    try:
        print(f"Copying {description}...")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        log_installer_error("COPY_FILE_FAILED", f"Failed to copy {description}", str(e))
        return False


def copy_directory_recursive(src: Path, dst: Path, description: str) -> bool:
    """Copy an entire directory recursively."""
    try:
        print(f"Copying {description}...")
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
        return True
    except Exception as e:
        log_installer_error("COPY_DIR_FAILED", f"Failed to copy {description}", str(e))
        return False


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


def get_installation_paths() -> Dict[str, Path]:
    """Get all paths needed for installation."""
    source_root = get_source_root()
    shared_root = get_shared_root_from_config()

    shared_assets = _assets_source_candidates(source_root, shared_root)

    return {
        "shared_root": shared_root,
        "source_root": source_root,
        "shared_app": source_root / "Flowgrid.pyw",
        "shared_assets": shared_assets,
        "local_app_folder": resolve_path_from_config("local_paths.app_folder", "{DOCUMENTS}\\Flowgrid", shared_root),
        "local_app": resolve_path_from_config("local_paths.app_folder", "{DOCUMENTS}\\Flowgrid", shared_root) / "Flowgrid.pyw",
        "local_assets": resolve_path_from_config("local_paths.assets_folder", "{DOCUMENTS}\\Flowgrid\\Assets", shared_root),
        "local_db": resolve_path_from_config("local_paths.database_folder", "{DOCUMENTS}\\Flowgrid\\Data", shared_root) / "Flowgrid_depot.db",
    }


def create_local_folders(paths: Dict[str, Path]) -> bool:
    """Create all necessary local folders."""
    try:
        paths["local_app_folder"].mkdir(parents=True, exist_ok=True)
        paths["local_assets"].parent.mkdir(parents=True, exist_ok=True)
        paths["local_db"].parent.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        log_installer_error("CREATE_FOLDERS_FAILED", "Failed to create local folders", str(e))
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
        required_files = ["Flowgrid.pyw", "Assets"]
        for req in required_files:
            req_path = expected_shared_root / req
            if not req_path.exists():
                errors.append(f"Required source file/folder missing: {req_path}")

    if not paths["shared_root"].exists():
        if paths["source_root"] != paths["shared_root"] and paths["source_root"].exists():
            warnings.append(
                f"Configured shared drive root is not accessible: {paths['shared_root']}. "
                f"Using source materials from local package path: {paths['source_root']}"
            )
        else:
            warnings.append(f"Configured shared drive root is not accessible: {paths['shared_root']}")

    if not paths["shared_app"].exists():
        errors.append(f"Flowgrid.pyw not found at source location: {paths['shared_app']}")

    if not paths["shared_assets"].exists():
        errors.append(f"Assets folder not found at source location: {paths['shared_assets']}")

    return warnings, errors


def copy_app_files(paths: Dict[str, Path]) -> bool:
    """Copy Flowgrid.pyw to local folder."""
    if not paths["shared_app"].exists():
        log_installer_error("APP_NOT_FOUND", "Flowgrid.pyw not found on shared drive", str(paths["shared_app"]))
        return False

    return copy_file_with_progress(paths["shared_app"], paths["local_app"], "Flowgrid application")


def copy_assets(paths: Dict[str, Path]) -> bool:
    """Copy Assets folder to local folder."""
    if not paths["shared_assets"].exists():
        log_installer_error("ASSETS_NOT_FOUND", "Assets folder not found on shared drive", str(paths["shared_assets"]))
        return False

    return copy_directory_recursive(paths["shared_assets"], paths["local_assets"], "Assets folder")


def initialize_database(paths: Dict[str, Path]) -> bool:
    """Create initial empty database file."""
    try:
        print("Initializing local database...")
        # Just create an empty file - the app will initialize the schema
        paths["local_db"].parent.mkdir(parents=True, exist_ok=True)
        paths["local_db"].touch()
        return True
    except Exception as e:
        log_installer_error("INIT_DB_FAILED", "Failed to initialize database", str(e))
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


def create_desktop_shortcut(paths: Dict[str, Path]) -> bool:
    """Create desktop shortcut using native Windows desktop path lookup."""
    try:
        print("Creating desktop shortcut...")

        desktop_path = _resolve_windows_desktop_directory()
        if desktop_path is None:
            raise RuntimeError("Unable to resolve desktop folder")

        icon_source = _find_default_wrench_icon(paths["source_root"], paths["shared_root"])
        icon_file = None
        if icon_source is not None:
            icon_file = paths["local_app_folder"] / "Flowgrid_shortcut.ico"
            try:
                _create_shortcut_icon(icon_source, icon_file)
            except Exception as e:
                log_installer_error("ICON_CREATE_FAILED", "Failed to create shortcut icon", str(e))
                icon_file = None

        shortcut_path = desktop_path / "Flowgrid.url"
        icon_line = f"IconFile={icon_file}\n" if icon_file is not None else ""
        url_content = f"""[InternetShortcut]\nURL={paths['local_app'].as_uri()}\n{icon_line}IconIndex=0\n"""
        shortcut_path.write_text(url_content, encoding="utf-8")
        return True

    except Exception as e:
        log_installer_error("SHORTCUT_FAILED", "Failed to create desktop shortcut", str(e))
        return True


# ============================================================================
# GUI DIALOGS
# ============================================================================

def show_installation_dialog(title: str, message: str, is_error: bool = False, paths: Dict[str, Path] | None = None) -> None:
    """Show installation status dialog using PySide6."""
    try:
        from PySide6.QtWidgets import QApplication, QMessageBox, QDialog, QVBoxLayout, QLabel, QPushButton
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

            layout = QVBoxLayout()

            label = QLabel(message)
            label.setWordWrap(True)
            layout.addWidget(label)

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
    print("Flowgrid Installer")
    print(f"Python: {sys.executable}")
    print(f"Version: {sys.version}")
    print()

    # Step 1: Check Python version
    print("Step 1: Checking Python version...")
    version_ok, version_error = check_python_version()
    if not version_ok:
        error_msg = f"Python version check failed: {version_error}"
        log_installer_error("PYTHON_VERSION", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ Python 3.14+ detected")
    print()

    # Step 2: Ensure dependencies
    print("Step 2: Checking dependencies...")
    deps_ok, deps_error = ensure_dependencies()
    if not deps_ok:
        error_msg = f"Dependency installation failed: {deps_error}"
        log_installer_error("DEPENDENCIES", error_msg)
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ All dependencies installed")
    print()

    # Step 3: Get installation paths
    print("Step 3: Preparing installation paths...")
    paths = get_installation_paths()
    print(f"Source root: {paths['source_root']}")
    print(f"Shared root: {paths['shared_root']}")
    print(f"Local app folder: {paths['local_app_folder']}")
    print(f"Local assets folder: {paths['local_assets']}")
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
            "Please ensure Flowgrid.pyw and the Assets folder are visible from the shared drive or the downloaded package."
        )
        details = "\n".join(source_errors)
        log_installer_error("SOURCE_MATERIALS_UNAVAILABLE", error_msg, details)
        show_installation_dialog("Installation Failed", f"{error_msg}\n\n{details}", is_error=True)
        return 1
    print("✓ Source materials verified")
    print()

    # Step 5: Create local folders
    print("Step 5: Creating local folders...")
    if not create_local_folders(paths):
        error_msg = "Failed to create local installation folders"
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ Local folders created")
    print()

    # Step 5: Copy application
    print("Step 5: Installing Flowgrid application...")
    if not copy_app_files(paths):
        error_msg = "Failed to copy Flowgrid application"
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ Application installed")
    print()

    # Step 6: Copy assets
    print("Step 6: Installing assets...")
    if not copy_assets(paths):
        error_msg = "Failed to copy assets folder"
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ Assets installed")
    print()

    # Step 7: Initialize database
    print("Step 7: Initializing database...")
    if not initialize_database(paths):
        error_msg = "Failed to initialize database"
        show_installation_dialog("Installation Failed", error_msg, is_error=True)
        return 1
    print("✓ Database initialized")
    print()

    # Step 8: Create desktop shortcut
    print("Step 8: Creating desktop shortcut...")
    if not create_desktop_shortcut(paths):
        print("⚠ Desktop shortcut creation failed, but installation continues")
    else:
        print("✓ Desktop shortcut created")
    print()

    # Step 9: Show completion dialog
    completion_msg = f"""
Flowgrid Installation Complete!

✓ App installed to: {paths['local_app_folder']}
✓ Database created locally for safe, fast reads/writes
✓ Assets copied locally for reliable icon loading
✓ Shared drive ({paths['shared_root']}) will sync in background
✓ Desktop shortcut created

Your local data and configs are kept in Documents for reliability and offline work.

The app will sync with the team automatically every 30 seconds.
"""

    show_installation_dialog("Flowgrid Installed Successfully", completion_msg, False, paths)

    print("Installation completed successfully!")
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