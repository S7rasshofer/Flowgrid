from __future__ import annotations

import ctypes
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from flowgrid_app.paths import (
    APP_TITLE,
    ASSETS_DIR_NAME,
    FLOWGRID_ICON_PACK_DIR_NAME,
    FLOWGRID_PROJECT_ROOT,
    _get_shared_root_from_config,
    _paths_equal,
)
from flowgrid_app.runtime_logging import _runtime_log_event

def _safe_print(message: str = "", end: str = "\n") -> None:
    try:
        print(message, end=end)
    except Exception:
        pass


def _show_error_dialog(title: str, message: str) -> None:
    if os.name == "nt":
        try:
            ctypes.windll.user32.MessageBoxW(None, str(message), str(title), 0x10 | 0x1000)
            return
        except Exception:
            pass
    try:
        print(f"{title}\n{message}")
    except Exception:
        pass


def _notify_launch_error(code: str, summary: str, details: str = "") -> None:
    _runtime_log_event(
        "installer.runtime_mode_error",
        severity="error",
        summary=str(summary or ""),
        context={
            "code": str(code or ""),
            "details": str(details or ""),
        },
    )
    short_msg = f"[{code}] {summary}"[:240]
    body = short_msg
    if details:
        body = f"{body}\n\nDetails:\n{details}"
    _show_error_dialog("Flowgrid Launch Error", body)


DESKTOP_SHORTCUT_FILENAME = f"{APP_TITLE}.lnk"
MANAGED_SHORTCUT_ICON_FILENAME = "Flowgrid_shortcut.ico"

WINDOWS_SHORTCUT_DESCRIPTION = "Launch Flowgrid"

def _format_pip_failure(stderr_text: str, stdout_text: str) -> str:
    stderr_clean = (stderr_text or "").strip()
    stdout_clean = (stdout_text or "").strip()
    if stderr_clean:
        return stderr_clean[-2000:]
    if stdout_clean:
        return stdout_clean[-2000:]
    return "No pip output captured."

def _flowgrid_script_path() -> Path:
    try:
        return FLOWGRID_PROJECT_ROOT / "Flowgrid.pyw"
    except Exception as exc:
        fallback = Path.cwd() / "Flowgrid.pyw"
        _runtime_log_event(
            "installer.script_path_resolve_failed",
            severity="warning",
            summary="Failed resolving Flowgrid script path; using current working directory fallback.",
            exc=exc,
            context={"fallback_path": str(fallback)},
        )
        return fallback

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
        if any(_paths_equal(candidate, existing) for existing in unique):
            continue
        unique.append(candidate)

    for candidate in unique:
        if candidate.name.lower() == "pythonw.exe" and candidate.exists() and candidate.is_file():
            return candidate
    for candidate in unique:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(sys.executable)

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
    except Exception as exc:
        _runtime_log_event(
            "installer.desktop_path_native_resolve_failed",
            severity="warning",
            summary="Native desktop path lookup failed; falling back to environment-based detection.",
            exc=exc,
        )

    fallback_candidates = []
    onedrive = str(os.environ.get("OneDrive", "") or "").strip()
    if onedrive:
        fallback_candidates.append(Path(onedrive) / "Desktop")
    fallback_candidates.append(Path.home() / "Desktop")

    for candidate in fallback_candidates:
        try:
            if candidate.exists() and candidate.is_dir():
                return candidate
        except Exception as exc:
            _runtime_log_event(
                "installer.desktop_path_fallback_stat_failed",
                severity="warning",
                summary="Desktop fallback path check failed during installer preparation.",
                exc=exc,
                context={"candidate": str(candidate)},
            )
    return None

def _powershell_single_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"

def _managed_shortcut_icon_path(*, create_dir: bool = True) -> Path:
    icon_dir = _get_shared_root_from_config() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME
    if create_dir:
        icon_dir.mkdir(parents=True, exist_ok=True)
    return icon_dir / MANAGED_SHORTCUT_ICON_FILENAME

def _shortcut_contract() -> dict[str, str]:
    desktop_dir = _resolve_windows_desktop_directory()
    shortcut_path = desktop_dir / DESKTOP_SHORTCUT_FILENAME if desktop_dir is not None else Path(DESKTOP_SHORTCUT_FILENAME)
    launcher_path = _preferred_gui_python_executable()
    script_path = _flowgrid_script_path()
    managed_icon_path = _managed_shortcut_icon_path(create_dir=False)
    arguments = f'"{script_path}"'
    return {
        "desktop_dir": str(desktop_dir) if desktop_dir is not None else "",
        "shortcut_path": str(shortcut_path),
        "launcher_path": str(launcher_path),
        "script_path": str(script_path),
        "arguments": arguments,
        "working_directory": str(script_path.parent),
        "managed_icon_path": str(managed_icon_path),
        "shortcut_description": WINDOWS_SHORTCUT_DESCRIPTION,
    }

def _inspect_windows_shortcut(shortcut_path: Path) -> dict[str, str] | None:
    if os.name != "nt":
        return None
    if not shortcut_path.exists() or not shortcut_path.is_file():
        return None

    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            "$shell = New-Object -ComObject WScript.Shell",
            f"$shortcut = $shell.CreateShortcut({_powershell_single_quote(str(shortcut_path))})",
            "$payload = [ordered]@{",
            "  TargetPath = [string]$shortcut.TargetPath",
            "  Arguments = [string]$shortcut.Arguments",
            "  WorkingDirectory = [string]$shortcut.WorkingDirectory",
            "  IconLocation = [string]$shortcut.IconLocation",
            "  Description = [string]$shortcut.Description",
            "}",
            "$payload | ConvertTo-Json -Compress",
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
        _runtime_log_event(
            "installer.shortcut_inspect_failed",
            severity="warning",
            summary="Desktop shortcut inspection failed before PowerShell could run.",
            exc=exc,
            context={"shortcut_path": str(shortcut_path)},
        )
        return None

    if result.returncode != 0:
        _runtime_log_event(
            "installer.shortcut_inspect_failed",
            severity="warning",
            summary="Desktop shortcut inspection PowerShell command failed.",
            context={
                "shortcut_path": str(shortcut_path),
                "detail": _format_pip_failure(result.stderr, result.stdout),
            },
        )
        return None

    try:
        payload = json.loads(str(result.stdout or "").strip() or "{}")
    except Exception as exc:
        _runtime_log_event(
            "installer.shortcut_inspect_parse_failed",
            severity="warning",
            summary="Desktop shortcut inspection returned invalid JSON.",
            exc=exc,
            context={"shortcut_path": str(shortcut_path), "stdout": str(result.stdout or "")[:500]},
        )
        return None

    if not isinstance(payload, dict):
        return None
    return {str(key): str(value or "") for key, value in payload.items()}

def _create_or_update_windows_shortcut(
    shortcut_path: Path,
    target_path: Path,
    arguments: str,
    working_directory: Path,
    icon_path: Path,
    description: str,
) -> tuple[bool, str]:
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

    detail = _format_pip_failure(result.stderr, result.stdout)
    return False, detail

def _sync_desktop_shortcut(
    config: dict[str, Any] | None = None,
    *,
    create_if_missing: bool,
) -> tuple[str, str]:
    if os.name != "nt":
        return "skipped", "Desktop shortcut creation is only supported on Windows."

    desktop_dir = _resolve_windows_desktop_directory()
    if desktop_dir is None:
        detail = "Unable to locate the current user's Desktop folder."
        _runtime_log_event(
            "installer.desktop_path_unavailable",
            severity="error",
            summary="Desktop shortcut sync failed because the Desktop folder could not be located.",
            context={"app_title": APP_TITLE},
        )
        return "failed", detail

    shortcut_path = desktop_dir / DESKTOP_SHORTCUT_FILENAME
    already_exists = shortcut_path.exists()
    if not create_if_missing and not already_exists:
        return "missing", ""

    from flowgrid_app.icon_io import _resolve_active_app_icon_path, _write_managed_shortcut_icon

    managed_icon_path = _managed_shortcut_icon_path()
    try:
        icon_source = _resolve_active_app_icon_path(config)
        if icon_source is None:
            detail = "Unable to find the default wrench icon or the configured custom icon."
            _runtime_log_event(
                "installer.icon_source_unavailable",
                severity="error",
                summary="Desktop shortcut sync failed because no usable icon source was available.",
                context={"shortcut_path": str(shortcut_path)},
            )
            return "failed", detail
        _write_managed_shortcut_icon(icon_source, managed_icon_path)
    except Exception as exc:
        detail = f"Failed preparing shortcut icon: {type(exc).__name__}: {exc}"
        _runtime_log_event(
            "installer.shortcut_icon_write_failed",
            severity="error",
            summary="Desktop shortcut sync failed while preparing the managed shortcut icon file.",
            exc=exc,
            context={
                "icon_source": str(icon_source),
                "managed_icon_path": str(managed_icon_path),
                "shortcut_path": str(shortcut_path),
            },
        )
        return "failed", detail

    launcher_path = _preferred_gui_python_executable()
    script_path = _flowgrid_script_path()
    if not launcher_path.exists() or not launcher_path.is_file():
        detail = f"Python launcher not found: {launcher_path}"
        _runtime_log_event(
            "installer.launcher_not_found",
            severity="error",
            summary="Desktop shortcut sync failed because the Python launcher executable could not be found.",
            context={"launcher_path": str(launcher_path), "shortcut_path": str(shortcut_path)},
        )
        return "failed", detail
    if not script_path.exists() or not script_path.is_file():
        detail = f"Flowgrid script not found: {script_path}"
        _runtime_log_event(
            "installer.script_not_found",
            severity="error",
            summary="Desktop shortcut sync failed because the Flowgrid script file could not be found.",
            context={"script_path": str(script_path), "shortcut_path": str(shortcut_path)},
        )
        return "failed", detail

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
        _runtime_log_event(
            "installer.shortcut_save_failed",
            severity="error",
            summary="Desktop shortcut save failed.",
            context={
                "shortcut_path": str(shortcut_path),
                "launcher_path": str(launcher_path),
                "managed_icon_path": str(managed_icon_path),
                "detail": detail,
            },
        )
        return "failed", f"Failed saving desktop shortcut: {detail}"

    status = "updated" if already_exists else "created"
    return status, f"Desktop shortcut {status} at {shortcut_path}"

def _launch_flowgrid_detached() -> tuple[bool, str]:
    launcher_path = _preferred_gui_python_executable()
    script_path = _flowgrid_script_path()
    if not launcher_path.exists() or not launcher_path.is_file():
        return False, f"Python launcher not found: {launcher_path}"
    if not script_path.exists() or not script_path.is_file():
        return False, f"Flowgrid script not found: {script_path}"

    try:
        subprocess.Popen([str(launcher_path), str(script_path)], cwd=str(script_path.parent))
        return True, ""
    except Exception as exc:
        _runtime_log_event(
            "installer.launch_subprocess_failed",
            severity="error",
            summary="Installer failed to launch Flowgrid after creating the desktop shortcut.",
            exc=exc,
            context={"launcher_path": str(launcher_path), "script_path": str(script_path)},
        )
        return False, f"{type(exc).__name__}: {exc}"

def _run_installer_mode(*, launch_after_install: bool) -> int:
    _safe_print(f"{APP_TITLE} shortcut setup")
    _safe_print(f"Interpreter: {sys.executable}")
    _safe_print("Legacy CLI alias: synchronizing local desktop shortcut.")

    status, detail = _sync_desktop_shortcut(create_if_missing=True)
    if status == "failed":
        _notify_launch_error("TH-1301", "Flowgrid installer could not create the desktop shortcut.", detail)
        return 1

    if detail:
        _safe_print(detail)

    if not launch_after_install:
        return 0

    launched, launch_detail = _launch_flowgrid_detached()
    if not launched:
        _notify_launch_error("TH-1302", "Flowgrid installer could not launch the app.", launch_detail)
        return 1

    _safe_print("Flowgrid launched.")
    return 0

__all__ = [
    "APP_TITLE",
    "DESKTOP_SHORTCUT_FILENAME",
    "MANAGED_SHORTCUT_ICON_FILENAME",
    "WINDOWS_SHORTCUT_DESCRIPTION",
    "_create_or_update_windows_shortcut",
    "_flowgrid_script_path",
    "_inspect_windows_shortcut",
    "_launch_flowgrid_detached",
    "_preferred_gui_python_executable",
    "_powershell_single_quote",
    "_resolve_windows_desktop_directory",
    "_run_installer_mode",
    "_shortcut_contract",
    "_sync_desktop_shortcut",
]
