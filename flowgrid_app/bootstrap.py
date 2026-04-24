from __future__ import annotations

import ctypes
import importlib
import os
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any

from flowgrid_app import RuntimeOptions
from flowgrid_app.paths import (
    APP_TITLE,
    CONFIG_FILENAME,
    DEPOT_DB_FILENAME,
    LOGS_DIR_NAME,
    MIN_PYTHON_VERSION,
    _current_channel_display_name,
    _current_channel_id,
    _current_channel_label,
    _current_channel_settings,
    _find_local_paths_config,
    _get_local_config_folder,
    _get_local_updater_path,
    _get_shared_root_from_config,
    _local_data_root,
    _paths_equal,
    _resolve_data_root,
    _resolve_path_from_config,
)
from flowgrid_app.runtime_logging import _runtime_log_event, configure_runtime_logging


DEPENDENCY_SPECS: tuple[tuple[str, str, str, bool], ...] = (
    ("PySide6", "PySide6", "Qt GUI framework", True),
)
LAUNCH_LOG_FILENAME = "Flowgrid_launch_errors.log"
_CLI_FLAGS = {str(arg or "").strip().lower() for arg in sys.argv[1:] if str(arg or "").strip()}
_SHORTCUT_MODE_FLAGS = {"--install", "--create-shortcut"}
_DIAGNOSTIC_MODE_FLAGS = {"--diagnose-install", "--smoke-ui"}
_STARTUP_UPDATE_SKIP_FLAGS = {"--skip-startup-update"}
_COMMAND_LINE_FLAGS_ACTIVE = bool((_SHORTCUT_MODE_FLAGS | _DIAGNOSTIC_MODE_FLAGS) & _CLI_FLAGS)
_BASE_STARTUP_INITIALIZED = False
_DEPENDENCIES_INITIALIZED = False
_PYSIDE6_INITIALIZED = False


def _error_log_path() -> Path:
    try:
        target = _get_local_config_folder() / LOGS_DIR_NAME
        target.mkdir(parents=True, exist_ok=True)
        return target / LAUNCH_LOG_FILENAME
    except Exception:
        try:
            fallback = _local_data_root() / LOGS_DIR_NAME
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback / LAUNCH_LOG_FILENAME
        except Exception:
            fallback = Path.cwd() / LOGS_DIR_NAME
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback / LAUNCH_LOG_FILENAME


def _log_launch_error(code: str, summary: str, details: str = "") -> None:
    try:
        lines = [f"[{code}] {summary}"]
        if details:
            lines.append(details)
        lines.append("-" * 72)
        with _error_log_path().open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.launch_log_write_failed",
            severity="error",
            summary="Failed to append launch error log entry.",
            exc=exc,
            context={"code": code, "summary": summary},
        )


def _show_error_dialog(title: str, message: str) -> None:
    if os.name == "nt":
        try:
            ctypes.windll.user32.MessageBoxW(None, str(message), str(title), 0x10 | 0x1000)
            return
        except Exception as exc:
            _runtime_log_event(
                "bootstrap.error_dialog_native_failed",
                severity="warning",
                summary="Native error dialog failed; falling back to console print.",
                exc=exc,
                context={"title": str(title)},
            )
    try:
        print(f"{title}\n{message}")
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.error_dialog_console_print_failed",
            severity="error",
            summary="Fallback console error dialog print failed.",
            exc=exc,
            context={"title": str(title)},
        )


def _notify_launch_error(code: str, summary: str, details: str = "") -> None:
    _log_launch_error(code, summary, details)
    body = f"[{code}] {summary}"[:240]
    if details:
        body = f"{body}\n\nDetails:\n{details}"
    body = f"{body}\n\nLog: {_error_log_path()}"
    _show_error_dialog("Flowgrid Launch Error", body)


def _fatal_launch_error(code: str, summary: str, details: str = "") -> None:
    _notify_launch_error(code, summary, details)
    raise SystemExit(f"[{code}] {summary}")


def _safe_print(message: str = "", end: str = "\n") -> None:
    try:
        print(message, end=end)
    except Exception:
        pass


def _runtime_log_dir() -> Path:
    try:
        target = _get_local_config_folder() / LOGS_DIR_NAME
        target.mkdir(parents=True, exist_ok=True)
        return target
    except Exception as exc:
        _log_launch_error(
            "TH-9802",
            "Primary runtime log directory unavailable.",
            f"Reason: {type(exc).__name__}: {exc}",
        )
        try:
            fallback = _local_data_root() / LOGS_DIR_NAME
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback
        except Exception:
            fallback = Path.cwd() / LOGS_DIR_NAME
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback


def _unhandled_exception_hook(exc_type, exc_value, exc_tb) -> None:
    details = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    _runtime_log_event(
        "bootstrap.unhandled_exception",
        severity="error",
        summary=f"Unhandled exception: {getattr(exc_type, '__name__', 'Exception')}",
        exc=exc_value,
        context={},
    )
    _notify_launch_error(
        "TH-9001",
        f"Unhandled exception: {getattr(exc_type, '__name__', 'Exception')}",
        details,
    )
    try:
        sys.__excepthook__(exc_type, exc_value, exc_tb)
    except Exception:
        pass


def _validate_runtime_storage_contract(shared_root: Path | None = None) -> Path:
    config_path = _find_local_paths_config()
    if config_path is None:
        raise RuntimeError("Flowgrid_paths.json is missing from the installed runtime folder.")

    resolved_shared_root = shared_root if shared_root is not None else _get_shared_root_from_config()
    runtime_dir = _local_data_root()

    if _paths_equal(config_path.parent, resolved_shared_root):
        raise RuntimeError(
            "Flowgrid_paths.json must remain in the local installed runtime folder, not in the shared data root."
        )

    if _paths_equal(runtime_dir, resolved_shared_root):
        _runtime_log_event(
            "bootstrap.runtime_on_shared_root",
            severity="warning",
            summary="Flowgrid appears to be running directly from the shared root; local install is recommended.",
            context={"runtime_dir": str(runtime_dir), "shared_root": str(resolved_shared_root)},
        )
    return resolved_shared_root


def _log_runtime_storage_contract(shared_root: Path | None = None) -> None:
    config_path = _find_local_paths_config()
    resolved_shared_root = shared_root if shared_root is not None else _get_shared_root_from_config()
    runtime_dir = _local_data_root()
    channel_settings = _current_channel_settings()
    local_config_path = _resolve_path_from_config(
        "local_paths.config_folder",
        "{DOCUMENTS}\\Flowgrid\\Config",
        resolved_shared_root,
    ) / CONFIG_FILENAME
    reserved_local_db = _resolve_path_from_config(
        "local_paths.database_folder",
        "{DOCUMENTS}\\Flowgrid\\Data",
        resolved_shared_root,
    ) / DEPOT_DB_FILENAME
    reserved_local_queue = _resolve_path_from_config(
        "local_paths.queue_folder",
        "{DOCUMENTS}\\Flowgrid\\Queue",
        resolved_shared_root,
    )
    _runtime_log_event(
        "bootstrap.storage_contract",
        severity="info",
        summary="Resolved Flowgrid storage contract.",
        context={
            "paths_config": str(config_path) if config_path is not None else "",
            "runtime_dir": str(runtime_dir),
            "channel_id": str(channel_settings.get("channel_id") or ""),
            "channel_label": str(channel_settings.get("channel_label") or ""),
            "channel_display_name": str(channel_settings.get("channel_display_name") or ""),
            "read_only_db": bool(channel_settings.get("read_only_db", False)),
            "repo_url": str(channel_settings.get("repo_url") or ""),
            "branch": str(channel_settings.get("branch") or ""),
            "snapshot_source_root": str(channel_settings.get("snapshot_source_root") or ""),
            "shared_root": str(resolved_shared_root),
            "shared_workflow_db": str(resolved_shared_root / DEPOT_DB_FILENAME),
            "local_user_config": str(local_config_path),
            "reserved_local_db_path": str(reserved_local_db),
            "reserved_local_queue_folder": str(reserved_local_queue),
            "workflow_db_source_of_truth": "shared_root/Flowgrid_depot.db",
        },
    )


def _check_python_version() -> None:
    current_version = sys.version_info[:3]
    if current_version < MIN_PYTHON_VERSION:
        version_str = ".".join(map(str, current_version))
        min_version_str = ".".join(map(str, MIN_PYTHON_VERSION))
        _fatal_launch_error(
            "TH-1001",
            f"Python {min_version_str}+ is required.",
            f"Python {min_version_str} or higher is required.\nCurrent version: {version_str}\n"
            "Please upgrade Python from https://python.org",
        )


def _dependency_specs_from_env() -> list[tuple[str, str, str, bool]]:
    specs: list[tuple[str, str, str, bool]] = []

    def parse_list(raw: str, required: bool) -> None:
        for token in str(raw or "").replace(";", ",").split(","):
            name = token.strip()
            if not name:
                continue
            module_name = name.replace("-", "_")
            desc = f"Extra {'required' if required else 'optional'} dependency"
            specs.append((name, module_name, desc, required))

    parse_list(os.environ.get("QI_EXTRA_PACKAGES", ""), False)
    parse_list(os.environ.get("QI_EXTRA_REQUIRED_PACKAGES", ""), True)
    return specs


def _module_import_status(module_name: str) -> tuple[bool, str]:
    try:
        importlib.import_module(module_name)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _format_pip_failure(stderr_text: str, stdout_text: str) -> str:
    stderr_clean = (stderr_text or "").strip()
    stdout_clean = (stdout_text or "").strip()
    if stderr_clean:
        return stderr_clean[-2000:]
    if stdout_clean:
        return stdout_clean[-2000:]
    return "No pip output captured."


def _install_package(package_name: str, description: str = "") -> tuple[bool, str]:
    try:
        _safe_print(f"Installing {package_name}...{f' ({description})' if description else ''}")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "--disable-pip-version-check", "install", package_name],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            _safe_print(f"[OK] Successfully installed {package_name}")
            return True, ""
        detail = _format_pip_failure(result.stderr, result.stdout)
        _safe_print(f"[FAIL] Failed to install {package_name}")
        _safe_print(f"Error: {detail}")
        return False, detail
    except subprocess.TimeoutExpired:
        return False, "pip install timed out after 300 seconds."
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _ensure_dependencies() -> None:
    missing_specs: list[dict[str, Any]] = []
    all_specs = list(DEPENDENCY_SPECS) + _dependency_specs_from_env()
    for package_name, module_name, description, required in all_specs:
        ok, reason = _module_import_status(module_name)
        if ok:
            continue
        missing_specs.append(
            {
                "package_name": package_name,
                "module_name": module_name,
                "description": description,
                "required": required,
                "reason": reason,
            }
        )

    if not missing_specs:
        return

    auto_install_disabled = os.environ.get("QI_AUTO_INSTALL") == "0"
    if auto_install_disabled:
        required_missing = [item for item in missing_specs if bool(item["required"])]
        if required_missing:
            details = [
                f"Interpreter: {sys.executable}",
                "Required dependencies missing and QI_AUTO_INSTALL=0.",
            ]
            for item in required_missing:
                details.append(f"- {item['package_name']} ({item['module_name']}): {item['reason']}")
            _fatal_launch_error(
                "TH-1102",
                "Required dependencies missing and automatic installation is disabled.",
                "\n".join(details),
            )
        return

    for item in missing_specs:
        installed, install_detail = _install_package(str(item["package_name"]), str(item["description"]))
        if installed:
            import_ok, import_reason = _module_import_status(str(item["module_name"]))
            if import_ok:
                continue
            install_detail = import_reason
        if bool(item["required"]):
            details = (
                f"Interpreter: {sys.executable}\n"
                f"Package: {item['package_name']}\n"
                f"Module: {item['module_name']}\n"
                f"Failure: {install_detail}\n"
                "See the launch/runtime log files for diagnostics."
            )
            _fatal_launch_error(
                "TH-1101",
                f"Required dependency installation failed: {item['package_name']}.",
                details,
            )


def _hide_console_window() -> None:
    if os.name != "nt" or os.environ.get("QI_KEEP_CONSOLE") == "1":
        return
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        user32_local = ctypes.WinDLL("user32", use_last_error=True)
        hwnd = kernel32.GetConsoleWindow()
        if hwnd:
            user32_local.ShowWindow(hwnd, 0)
            kernel32.FreeConsole()
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.hide_console_failed",
            severity="warning",
            summary="Unable to hide or detach console window.",
            exc=exc,
        )


def _ensure_pyside6() -> None:
    try:
        importlib.import_module("PySide6")
    except Exception as exc:
        _fatal_launch_error(
            "TH-1106",
            "PySide6 import failed.",
            f"Interpreter: {sys.executable}\n"
            f"Import error: {type(exc).__name__}: {exc}\n"
            "See the launch/runtime log files for diagnostics.",
        )


def _preferred_gui_python_executable() -> Path:
    candidates: list[Path] = []
    for raw in (getattr(sys, "_base_executable", ""), sys.executable):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text)
        candidates.append(path)
        candidates.append(path.parent / "pythonw.exe")
        if path.name.lower() == "python.exe":
            candidates.append(path.with_name("pythonw.exe"))
    unique: list[Path] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    for candidate in unique:
        if candidate.name.lower() == "pythonw.exe" and candidate.exists() and candidate.is_file():
            return candidate
    for candidate in unique:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(sys.executable)


def _handoff_to_startup_updater() -> bool:
    if _CLI_FLAGS & _STARTUP_UPDATE_SKIP_FLAGS:
        return False

    config_path = _find_local_paths_config()
    if config_path is None or not _paths_equal(config_path.parent, _local_data_root()):
        return False

    updater_path = _get_local_updater_path()
    if not updater_path.exists() or not updater_path.is_file():
        return False

    launcher_path = _preferred_gui_python_executable()
    if not launcher_path.exists() or not launcher_path.is_file():
        _runtime_log_event(
            "bootstrap.startup_update_launcher_missing",
            severity="warning",
            summary="Startup updater handoff was skipped because the Python launcher could not be resolved.",
            context={"launcher_path": str(launcher_path), "updater_path": str(updater_path)},
        )
        return False

    try:
        subprocess.Popen(
            [
                str(launcher_path),
                str(updater_path),
                "--parent-pid",
                str(os.getpid()),
                "--relaunch",
                "--launch-on-failure",
            ],
            cwd=str(updater_path.parent),
        )
    except Exception as exc:
        _runtime_log_event(
            "bootstrap.startup_update_handoff_failed",
            severity="warning",
            summary="Startup updater handoff failed; Flowgrid will continue launching without the pre-launch updater pass.",
            exc=exc,
            context={"launcher_path": str(launcher_path), "updater_path": str(updater_path)},
        )
        return False

    _runtime_log_event(
        "bootstrap.startup_update_handoff_started",
        severity="info",
        summary="Flowgrid launch was handed off to the standalone updater before opening the main window.",
        context={"launcher_path": str(launcher_path), "updater_path": str(updater_path), "parent_pid": int(os.getpid())},
    )
    return True


def _run_base_startup_initialization(
    *,
    create_shared_root: bool,
    require_dependencies: bool,
    require_pyside6: bool,
    allow_console_hide: bool,
) -> None:
    global _BASE_STARTUP_INITIALIZED, _DEPENDENCIES_INITIALIZED, _PYSIDE6_INITIALIZED
    try:
        if not _BASE_STARTUP_INITIALIZED:
            sys.excepthook = _unhandled_exception_hook
            resolved_shared_root = _validate_runtime_storage_contract()
            if create_shared_root:
                _resolve_data_root()
            _log_runtime_storage_contract(resolved_shared_root)
            _check_python_version()
            _BASE_STARTUP_INITIALIZED = True
        if allow_console_hide and not _COMMAND_LINE_FLAGS_ACTIVE:
            _hide_console_window()
        if require_dependencies and not _DEPENDENCIES_INITIALIZED:
            _ensure_dependencies()
            _DEPENDENCIES_INITIALIZED = True
        if require_pyside6 and not _PYSIDE6_INITIALIZED:
            _ensure_pyside6()
            _PYSIDE6_INITIALIZED = True
    except SystemExit:
        raise
    except Exception as exc:
        _fatal_launch_error("TH-1900", "Unexpected startup initialization failure.", repr(exc))


def _runtime_options_from_install_manifest() -> RuntimeOptions:
    channel_settings = _current_channel_settings()
    read_only_db = bool(channel_settings.get("read_only_db", False))
    channel_id = str(channel_settings.get("channel_id") or _current_channel_id())
    channel_label = str(channel_settings.get("channel_label") or _current_channel_label())
    channel_display_name = str(channel_settings.get("channel_display_name") or _current_channel_display_name() or APP_TITLE)
    return RuntimeOptions(
        read_only_db=read_only_db,
        skip_shortcut_sync=read_only_db,
        skip_startup_repairs=read_only_db,
        skip_shared_icon_reconcile=read_only_db,
        channel_id=channel_id,
        channel_label=channel_label,
        channel_display_name=channel_display_name,
        repo_url=str(channel_settings.get("repo_url") or ""),
        branch=str(channel_settings.get("branch") or ""),
        snapshot_source_root=str(channel_settings.get("snapshot_source_root") or ""),
    )


def main() -> int:
    from PySide6.QtWidgets import QApplication
    from .window.shell import QuickInputsWindow

    if os.name == "nt":
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("QuickInputs.QtApp")
        except Exception as exc:
            _runtime_log_event(
                "bootstrap.app_user_model_id_set_failed",
                severity="warning",
                summary="Failed setting explicit AppUserModelID for Windows shell integration.",
                exc=exc,
            )

    app = QApplication(sys.argv)
    runtime_options = _runtime_options_from_install_manifest()
    app.setApplicationName(str(runtime_options.channel_display_name or APP_TITLE))
    app.setQuitOnLastWindowClosed(True)
    window = QuickInputsWindow(runtime_options=runtime_options)
    app.aboutToQuit.connect(lambda: window._shutdown_application("app.aboutToQuit"))
    window.show()
    return app.exec()


def _run_command_line_mode() -> int | None:
    if "--diagnose-install" in _CLI_FLAGS:
        _run_base_startup_initialization(
            create_shared_root=False,
            require_dependencies=False,
            require_pyside6=False,
            allow_console_hide=False,
        )
        from flowgrid_app.diagnostics import run_install_diagnostics

        return run_install_diagnostics()
    if "--smoke-ui" in _CLI_FLAGS:
        _run_base_startup_initialization(
            create_shared_root=False,
            require_dependencies=True,
            require_pyside6=True,
            allow_console_hide=False,
        )
        from flowgrid_app.diagnostics import run_ui_smoke_diagnostics

        return run_ui_smoke_diagnostics()
    if "--install" in _CLI_FLAGS:
        _run_base_startup_initialization(
            create_shared_root=False,
            require_dependencies=True,
            require_pyside6=True,
            allow_console_hide=False,
        )
        from flowgrid_app.installer import _run_installer_mode

        return _run_installer_mode(launch_after_install="--no-launch" not in _CLI_FLAGS)
    if "--create-shortcut" in _CLI_FLAGS:
        _run_base_startup_initialization(
            create_shared_root=False,
            require_dependencies=True,
            require_pyside6=True,
            allow_console_hide=False,
        )
        from flowgrid_app.installer import _run_installer_mode

        return _run_installer_mode(launch_after_install=False)
    return None


def run_entrypoint() -> None:
    try:
        cli_result = _run_command_line_mode()
        if cli_result is not None:
            raise SystemExit(cli_result)
        if _handoff_to_startup_updater():
            raise SystemExit(0)
        _run_base_startup_initialization(
            create_shared_root=True,
            require_dependencies=True,
            require_pyside6=True,
            allow_console_hide=True,
        )
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        details = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        _notify_launch_error("TH-9000", "Fatal runtime crash during launch.", details)
        raise SystemExit(1)


configure_runtime_logging(
    log_dir_provider=_runtime_log_dir,
    launch_log_error_callback=_log_launch_error,
    safe_print_callback=lambda message: _safe_print(message),
)


__all__ = ["main", "run_entrypoint"]
