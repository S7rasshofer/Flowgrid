from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from flowgrid_app import PermissionService, RuntimeOptions, UserRepository
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.installer import _inspect_windows_shortcut, _shortcut_contract
from flowgrid_app.paths import (
    APP_TITLE,
    DEPOT_DB_FILENAME,
    FLOWGRID_PROJECT_ROOT,
    _find_local_paths_config,
    _get_shared_root_from_config,
    _local_data_root,
    _paths_equal,
    _reset_path_runtime_cache,
)
from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.workflow_core import DepotDB, DepotTracker


@dataclass(slots=True)
class DiagnosticEntry:
    label: str
    status: str
    detail: str = ""
    path: str = ""


def _safe_print(message: str = "", end: str = "\n") -> None:
    try:
        print(message, end=end)
    except Exception:
        pass


def _record(results: list[DiagnosticEntry], label: str, status: str, detail: str = "", path: Path | str | None = None) -> None:
    results.append(
        DiagnosticEntry(
            label=str(label or "").strip(),
            status=str(status or "").strip().lower(),
            detail=str(detail or "").strip(),
            path=str(path or "").strip(),
        )
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


def _format_report(title: str, results: list[DiagnosticEntry]) -> str:
    lines = [str(title or "").strip(), ""]
    for item in results:
        line = f"{_step_marker(item.status)} {item.label}"
        if item.detail:
            line += f" - {item.detail}"
        lines.append(line)
        if item.path:
            lines.append(f"      {item.path}")
    failures = [item for item in results if item.status == "failed"]
    warnings = [item for item in results if item.status == "warning"]
    lines.extend(
        [
            "",
            f"Summary: {len(failures)} failed, {len(warnings)} warning, {len(results) - len(failures) - len(warnings)} ok.",
        ]
    )
    return "\n".join(lines)


def _exit_code_for_results(results: list[DiagnosticEntry]) -> int:
    return 1 if any(item.status == "failed" for item in results) else 0


def _sqlite_uri_for_read_only(path: Path) -> str:
    raw_path = str(path)
    if raw_path.startswith("\\\\"):
        return f"file:{raw_path}?mode=ro"
    try:
        return f"{path.resolve().as_uri()}?mode=ro"
    except Exception:
        return f"file:{path.as_posix()}?mode=ro"


@contextmanager
def _read_only_sqlite_connection(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(_sqlite_uri_for_read_only(db_path), uri=True, timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    try:
        yield conn
    finally:
        conn.close()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"] or "").strip() for row in rows}


def _normalize_icon_location(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(",0"):
        return text[:-2]
    return text


def _collect_permission_candidates(conn: sqlite3.Connection) -> set[str]:
    candidates = {"KIDDS"}
    queries = (
        "SELECT DISTINCT COALESCE(user_id, '') AS user_id FROM admin_users",
        "SELECT DISTINCT COALESCE(user_id, '') AS user_id FROM agents",
        "SELECT DISTINCT COALESCE(user_id, '') AS user_id FROM submissions",
    )
    for query in queries:
        try:
            for row in conn.execute(query).fetchall():
                normalized = DepotRules.normalize_user_id(str(row["user_id"] or ""))
                if normalized:
                    candidates.add(normalized)
        except Exception:
            continue
    return candidates


def collect_install_preflight_results() -> list[DiagnosticEntry]:
    results: list[DiagnosticEntry] = []
    runtime_dir = _local_data_root()
    config_path = _find_local_paths_config()
    if config_path is None:
        _record(
            results,
            "Local Flowgrid_paths.json manifest",
            "failed",
            "Flowgrid_paths.json is missing from the installed runtime folder.",
            runtime_dir / "Flowgrid_paths.json",
        )
        return results

    _record(results, "Local Flowgrid_paths.json manifest", "ok", "Installed runtime manifest located.", config_path)

    try:
        shared_root = _get_shared_root_from_config()
    except Exception as exc:
        _record(
            results,
            "Shared root resolution",
            "failed",
            f"Failed resolving shared_drive_root from Flowgrid_paths.json: {type(exc).__name__}: {exc}",
            config_path,
        )
        return results

    _record(results, "Shared root resolution", "ok", "Shared data root is reachable.", shared_root)

    if _paths_equal(config_path.parent, shared_root):
        _record(
            results,
            "Local/shared storage contract",
            "failed",
            "Flowgrid_paths.json is located in the shared root. The install must keep the manifest local.",
            config_path,
        )
    elif _paths_equal(runtime_dir, shared_root):
        _record(
            results,
            "Local/shared storage contract",
            "warning",
            "Flowgrid appears to be running directly from the shared root instead of a local installed runtime.",
            runtime_dir,
        )
    else:
        _record(
            results,
            "Local/shared storage contract",
            "ok",
            "Local runtime and shared data root remain separated.",
            runtime_dir,
        )

    required_runtime_paths = (
        (runtime_dir / "Flowgrid.pyw", "Flowgrid.pyw"),
        (runtime_dir / "flowgrid_app" / "__init__.py", "flowgrid_app package"),
        (runtime_dir / "Assets", "Assets folder"),
    )
    missing_runtime = [label for path, label in required_runtime_paths if not path.exists()]
    if missing_runtime:
        _record(
            results,
            "Installed runtime files",
            "failed",
            f"Missing runtime artifacts: {', '.join(missing_runtime)}.",
            runtime_dir,
        )
    else:
        _record(results, "Installed runtime files", "ok", "Local runtime entrypoint, package, and Assets folder are present.", runtime_dir)

    shared_db = shared_root / DEPOT_DB_FILENAME
    if not shared_db.exists() or not shared_db.is_file():
        _record(results, "Shared workflow DB", "failed", "Shared Flowgrid_depot.db is missing.", shared_db)
        return results

    try:
        with _read_only_sqlite_connection(shared_db) as conn:
            conn.execute("SELECT 1").fetchone()
            _record(results, "Shared workflow DB", "ok", "Shared Flowgrid_depot.db opened read-only.", shared_db)

            schema_contract = {
                "submissions": {"id", "entry_date", "user_id", "work_order", "touch", "created_at", "updated_at"},
                "parts": {"id", "work_order", "source_submission_id", "assigned_user_id", "qa_comment"},
                "agents": {"user_id", "tier", "icon_path"},
                "admin_users": {"user_id", "position", "access_level", "icon_path"},
            }
            schema_issues: list[str] = []
            for table_name, expected_columns in schema_contract.items():
                actual_columns = _table_columns(conn, table_name)
                if not actual_columns:
                    schema_issues.append(f"{table_name}: missing table")
                    continue
                missing_columns = sorted(column for column in expected_columns if column not in actual_columns)
                if missing_columns:
                    schema_issues.append(f"{table_name}: missing columns {', '.join(missing_columns)}")
            if schema_issues:
                _record(
                    results,
                    "Shared DB schema contract",
                    "failed",
                    "; ".join(schema_issues),
                    shared_db,
                )
            else:
                _record(
                    results,
                    "Shared DB schema contract",
                    "ok",
                    "Required submissions, parts, agents, and admin_users columns are present.",
                    shared_db,
                )

            duplicate_summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS duplicate_group_count,
                    COALESCE(SUM(duplicate_count - 1), 0) AS duplicate_row_count
                FROM (
                    SELECT COUNT(*) AS duplicate_count
                    FROM submissions
                    WHERE TRIM(COALESCE(entry_date, '')) <> ''
                    GROUP BY entry_date, user_id, work_order
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()
            duplicate_group_count = int(duplicate_summary["duplicate_group_count"] if duplicate_summary is not None else 0)
            duplicate_row_count = int(duplicate_summary["duplicate_row_count"] if duplicate_summary is not None else 0)
            duplicate_status = "warning" if duplicate_group_count > 0 or duplicate_row_count > 0 else "ok"
            duplicate_detail = (
                f"Duplicate groups={duplicate_group_count}, duplicate rows pending collapse={duplicate_row_count}."
                if duplicate_group_count > 0 or duplicate_row_count > 0
                else "No duplicate same-day submission groups detected."
            )
            _record(results, "Duplicate submission backfill check", duplicate_status, duplicate_detail, shared_db)

            unresolved_source_row = conn.execute(
                "SELECT COUNT(*) AS c FROM parts WHERE COALESCE(source_submission_id, 0)=0"
            ).fetchone()
            unresolved_source_count = int(unresolved_source_row["c"] if unresolved_source_row is not None else 0)
            orphaned_source_row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM parts
                WHERE COALESCE(source_submission_id, 0)<>0
                  AND NOT EXISTS (
                      SELECT 1
                      FROM submissions s
                      WHERE s.id=parts.source_submission_id
                  )
                """
            ).fetchone()
            orphaned_source_count = int(orphaned_source_row["c"] if orphaned_source_row is not None else 0)
            backfill_status = "warning" if unresolved_source_count > 0 or orphaned_source_count > 0 else "ok"
            backfill_detail = (
                "Pending source_submission_id backfill counts: "
                f"unresolved={unresolved_source_count}, orphaned={orphaned_source_count}."
                if unresolved_source_count > 0 or orphaned_source_count > 0
                else "No unresolved or orphaned source_submission_id rows detected."
            )
            _record(results, "Parts source_submission_id backfill check", backfill_status, backfill_detail, shared_db)

            try:
                diag_db: DepotDB | None = None
                diag_db = DepotDB(shared_db, read_only=True, ensure_schema=False)
                diag_tracker = DepotTracker(
                    diag_db,
                    startup_repairs_enabled=False,
                    allow_metadata_repairs=False,
                )
                user_repository = UserRepository(diag_tracker, DepotRules)
                permission_service = PermissionService(user_repository)
                candidate_users = sorted(_collect_permission_candidates(conn))
                qa_candidates = [
                    user_id
                    for user_id in candidate_users
                    if user_id != "KIDDS" and permission_service.can_access_qa(user_id)
                ]
                dashboard_candidates = [
                    user_id
                    for user_id in candidate_users
                    if user_id != "KIDDS" and permission_service.can_access_dashboard(user_id)
                ]
                permission_warnings: list[str] = []
                if not qa_candidates:
                    permission_warnings.append("No non-admin QA-access user found in admin_users/agents/submissions.")
                if not dashboard_candidates:
                    permission_warnings.append("No non-admin dashboard/reporting user found in admin_users/agents/submissions.")
                if permission_warnings:
                    _record(results, "Permission scenario coverage", "warning", " ".join(permission_warnings), shared_db)
                else:
                    _record(
                        results,
                        "Permission scenario coverage",
                        "ok",
                        f"Representative QA users: {', '.join(qa_candidates[:3])}. Dashboard users: {', '.join(dashboard_candidates[:3])}.",
                        shared_db,
                    )
            except Exception as exc:
                _record(
                    results,
                    "Permission scenario coverage",
                    "warning",
                    f"Permission coverage scan failed: {type(exc).__name__}: {exc}",
                    shared_db,
                )
            finally:
                try:
                    if diag_db is not None:
                        diag_db.conn.close()
                except Exception:
                    pass
    except Exception as exc:
        _record(
            results,
            "Shared workflow DB",
            "failed",
            f"Read-only open failed: {type(exc).__name__}: {exc}",
            shared_db,
        )
        return results

    shortcut_contract = _shortcut_contract()
    shortcut_path = Path(shortcut_contract["shortcut_path"])
    if os.name != "nt":
        _record(results, "Desktop shortcut contract", "warning", "Desktop shortcut validation is only supported on Windows.", shortcut_path)
        return results

    actual_shortcut = _inspect_windows_shortcut(shortcut_path)
    if actual_shortcut is None:
        _record(
            results,
            "Desktop shortcut contract",
            "warning",
            "Desktop shortcut is missing or could not be inspected.",
            shortcut_path,
        )
        return results

    expected_target = str(Path(shortcut_contract["launcher_path"]))
    expected_arguments = str(shortcut_contract["arguments"])
    expected_working_directory = str(Path(shortcut_contract["working_directory"]))
    expected_icon_path = str(Path(shortcut_contract["managed_icon_path"]))
    expected_description = str(shortcut_contract["shortcut_description"])

    actual_target = str(actual_shortcut.get("TargetPath", "")).strip()
    actual_arguments = str(actual_shortcut.get("Arguments", "")).strip()
    actual_working_directory = str(actual_shortcut.get("WorkingDirectory", "")).strip()
    actual_icon_path = _normalize_icon_location(actual_shortcut.get("IconLocation", ""))
    actual_description = str(actual_shortcut.get("Description", "")).strip()

    mismatches: list[str] = []
    if actual_target != expected_target:
        mismatches.append(f"target={actual_target!r} expected {expected_target!r}")
    if actual_arguments != expected_arguments:
        mismatches.append(f"arguments={actual_arguments!r} expected {expected_arguments!r}")
    if actual_working_directory != expected_working_directory:
        mismatches.append(f"working_directory={actual_working_directory!r} expected {expected_working_directory!r}")
    if actual_icon_path != expected_icon_path:
        mismatches.append(f"icon={actual_icon_path!r} expected {expected_icon_path!r}")
    if actual_description != expected_description:
        mismatches.append(f"description={actual_description!r} expected {expected_description!r}")

    if mismatches:
        _record(
            results,
            "Desktop shortcut contract",
            "warning",
            "; ".join(mismatches),
            shortcut_path,
        )
    else:
        _record(
            results,
            "Desktop shortcut contract",
            "ok",
            "Desktop shortcut target, arguments, working directory, icon path, and description match the expected .lnk contract.",
            shortcut_path,
        )

    return results


@contextmanager
def _temporary_smoke_manifest(shared_root: Path) -> Iterator[dict[str, Path]]:
    with tempfile.TemporaryDirectory(prefix="flowgrid_smoke_") as temp_dir_raw:
        temp_root = Path(temp_dir_raw)
        runtime_root = temp_root / "Runtime"
        config_folder = temp_root / "Config"
        data_folder = temp_root / "Data"
        queue_folder = temp_root / "Queue"
        assets_folder = temp_root / "Assets"
        manifest_path = temp_root / "Flowgrid_paths.json"
        config_path = config_folder / "Flowgrid_config.json"

        runtime_root.mkdir(parents=True, exist_ok=True)
        config_folder.mkdir(parents=True, exist_ok=True)
        data_folder.mkdir(parents=True, exist_ok=True)
        queue_folder.mkdir(parents=True, exist_ok=True)
        assets_folder.mkdir(parents=True, exist_ok=True)

        manifest_payload = {
            "shared_drive_root": str(shared_root),
            "local_paths": {
                "app_folder": str(runtime_root),
                "config_folder": str(config_folder),
                "database_folder": str(data_folder),
                "queue_folder": str(queue_folder),
                "assets_folder": str(assets_folder),
            },
        }
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, ensure_ascii=False), encoding="utf-8")

        previous_manifest = os.environ.get("FLOWGRID_PATHS_CONFIG")
        os.environ["FLOWGRID_PATHS_CONFIG"] = str(manifest_path)
        _reset_path_runtime_cache()
        try:
            yield {
                "temp_root": temp_root,
                "runtime_root": runtime_root,
                "config_folder": config_folder,
                "config_path": config_path,
                "manifest_path": manifest_path,
            }
        finally:
            if previous_manifest is None:
                os.environ.pop("FLOWGRID_PATHS_CONFIG", None)
            else:
                os.environ["FLOWGRID_PATHS_CONFIG"] = previous_manifest
            _reset_path_runtime_cache()


def _process_qt_events(app: Any, cycles: int = 3) -> None:
    for _ in range(max(1, int(cycles))):
        app.processEvents()


def _quick_tab_names(window: Any) -> list[str]:
    return [
        str(tab.get("name", "") or "").strip()
        for tab in window.config.get("quick_tabs", [])
        if isinstance(tab, dict)
    ]


def _run_ui_smoke_under_manifest(temp_paths: dict[str, Path]) -> list[DiagnosticEntry]:
    results: list[DiagnosticEntry] = []
    captured_messages: list[dict[str, Any]] = []
    window: Any | None = None

    def message_sink(payload: dict[str, Any]) -> None:
        captured_messages.append(dict(payload))

    runtime_options = RuntimeOptions(
        read_only_db=True,
        skip_shortcut_sync=True,
        skip_startup_repairs=True,
        skip_shared_icon_reconcile=True,
        message_sink=message_sink,
    )

    from PySide6.QtWidgets import QApplication

    created_app = False
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
        created_app = True
    app.setQuitOnLastWindowClosed(False)

    from flowgrid_app.window.shell import QuickInputsWindow

    try:
        window = QuickInputsWindow(runtime_options=runtime_options)
        window.show()
        _process_qt_events(app, cycles=6)

        expected_config_path = temp_paths["config_path"]
        expected_manifest_path = temp_paths["manifest_path"]
        resolved_manifest_path = _find_local_paths_config()
        manifest_matches = resolved_manifest_path is not None and _paths_equal(resolved_manifest_path, expected_manifest_path)
        config_matches = _paths_equal(window.config_path, expected_config_path)
        if manifest_matches and config_matches:
            _record(results, "Smoke manifest override", "ok", "Shell used the temporary Flowgrid_paths.json and temp local config path.", expected_manifest_path)
        else:
            _record(
                results,
                "Smoke manifest override",
                "failed",
                "Shell did not resolve the temporary Flowgrid_paths.json or temp config path.",
                expected_manifest_path,
            )

        if getattr(window.depot_db, "read_only", False):
            _record(results, "Read-only DB smoke mode", "ok", "Shell opened the shared DB in read-only mode.", window.depot_db.db_path)
        else:
            _record(results, "Read-only DB smoke mode", "failed", "Shell did not open the shared DB in read-only mode.", window.depot_db.db_path)

        if hasattr(window, "depot_import_button"):
            _record(results, "Workbook import removal", "failed", "Tracker Hub still exposes the workbook import button.", temp_paths["runtime_root"])
        else:
            _record(results, "Workbook import removal", "ok", "Tracker Hub no longer exposes workbook import UI.", temp_paths["runtime_root"])

        added_index = window._add_quick_task_tab_named("DiagTemp")
        window.rename_quick_task_tab_named("DiagSmoke", added_index)
        window.flush_pending_config_save()
        saved_payload = json.loads(expected_config_path.read_text(encoding="utf-8"))
        if "DiagSmoke" in [str(tab.get("name", "") or "").strip() for tab in saved_payload.get("quick_tabs", []) if isinstance(tab, dict)]:
            _record(results, "Quick grid save", "ok", "Quick tab add/rename persisted to the temp config file.", expected_config_path)
        else:
            _record(results, "Quick grid save", "failed", "Quick tab add/rename did not persist to the temp config file.", expected_config_path)

        window.config["theme"] = {
            "primary": "#305C4A",
            "accent": "#F0A13A",
            "surface": "#11181B",
        }
        window._theme_updated()
        window.flush_pending_config_save()
        saved_payload = json.loads(expected_config_path.read_text(encoding="utf-8"))
        saved_theme = saved_payload.get("theme", {})
        expected_theme = {"primary": "#305C4A", "accent": "#F0A13A", "surface": "#11181B"}
        if all(str(saved_theme.get(key, "")).strip().upper() == value.upper() for key, value in expected_theme.items()):
            _record(results, "Theme save", "ok", "Theme changes persisted to the temp config file.", expected_config_path)
        else:
            _record(results, "Theme save", "failed", "Theme changes did not persist to the temp config file.", expected_config_path)

        window.window_manager.close_all()
        window.close()
        window.deleteLater()
        _process_qt_events(app, cycles=6)

        window = QuickInputsWindow(runtime_options=runtime_options)
        window.show()
        _process_qt_events(app, cycles=6)

        reloaded_theme = window.config.get("theme", {})
        if "DiagSmoke" in _quick_tab_names(window):
            _record(results, "Quick grid reload", "ok", "Renamed quick tab reloaded from the temp config file.", expected_config_path)
        else:
            _record(results, "Quick grid reload", "failed", "Renamed quick tab did not reload from the temp config file.", expected_config_path)

        if all(str(reloaded_theme.get(key, "")).strip().upper() == value.upper() for key, value in expected_theme.items()):
            _record(results, "Theme reload", "ok", "Theme values reloaded from the temp config file.", expected_config_path)
        else:
            _record(results, "Theme reload", "failed", "Theme values did not reload from the temp config file.", expected_config_path)

        window.current_user = "KIDDS"
        window.config["current_user"] = "KIDDS"
        window._apply_depot_access_controls()
        _process_qt_events(app, cycles=3)

        allowed_windows = {
            "Agent": window._open_depot_agent(),
            "QA/WCS": window._open_depot_qa(),
            "User Setup": window._open_depot_admin(),
            "Data Dashboard": window._open_depot_dashboard(),
        }
        failed_allowed = [name for name, popup in allowed_windows.items() if popup is None]
        if failed_allowed:
            _record(
                results,
                "Allowed Tracker Hub launches",
                "failed",
                f"Expected admin smoke user KIDDS to open: {', '.join(failed_allowed)}.",
                window.depot_db.db_path,
            )
        else:
            _record(
                results,
                "Allowed Tracker Hub launches",
                "ok",
                "Agent, QA/WCS, User Setup, and Data Dashboard opened for admin smoke user KIDDS.",
                window.depot_db.db_path,
            )

        window.window_manager.close_all()
        _process_qt_events(app, cycles=4)

        window.current_user = "FLOWGRID_DIAG_DENIED"
        window.config["current_user"] = "FLOWGRID_DIAG_DENIED"
        window._apply_depot_access_controls()
        _process_qt_events(app, cycles=3)

        tooltip_expectations = {
            "Agent": (window.depot_agent_button, PermissionService.AGENT_ACCESS_DENIED_MESSAGE),
            "QA/WCS": (window.depot_qa_button, PermissionService.QA_ACCESS_DENIED_MESSAGE),
            "User Setup": (window.depot_admin_button, PermissionService.ADMIN_ACCESS_DENIED_MESSAGE),
            "Data Dashboard": (window.depot_dashboard_button, PermissionService.DASHBOARD_ACCESS_DENIED_MESSAGE),
        }
        tooltip_mismatches = [
            f"{label} enabled={button.isEnabled()} tooltip={button.toolTip()!r}"
            for label, (button, expected) in tooltip_expectations.items()
            if button.isEnabled() or str(button.toolTip() or "").strip() != expected
        ]
        if tooltip_mismatches:
            _record(
                results,
                "Denied Tracker Hub tooltips",
                "failed",
                "; ".join(tooltip_mismatches),
                window.depot_db.db_path,
            )
        else:
            _record(
                results,
                "Denied Tracker Hub tooltips",
                "ok",
                "Tracker Hub buttons were disabled and showed the expected permission-denial tooltips.",
                window.depot_db.db_path,
            )

        captured_messages.clear()
        denied_results = {
            "Agent": window._open_depot_agent(),
            "QA/WCS": window._open_depot_qa(),
            "User Setup": window._open_depot_admin(),
            "Data Dashboard": window._open_depot_dashboard(),
        }
        _process_qt_events(app, cycles=4)
        unexpected_open = [name for name, popup in denied_results.items() if popup is not None]
        captured_texts = [str(item.get("text", "")).strip() for item in captured_messages]
        expected_denials = [
            PermissionService.AGENT_ACCESS_DENIED_MESSAGE,
            PermissionService.QA_ACCESS_DENIED_MESSAGE,
            PermissionService.ADMIN_ACCESS_DENIED_MESSAGE,
            PermissionService.DASHBOARD_ACCESS_DENIED_MESSAGE,
        ]
        missing_denials = [message for message in expected_denials if message not in captured_texts]
        if unexpected_open or missing_denials:
            detail_parts: list[str] = []
            if unexpected_open:
                detail_parts.append(f"Unexpected windows opened: {', '.join(unexpected_open)}.")
            if missing_denials:
                detail_parts.append(f"Missing denial messages: {', '.join(missing_denials)}.")
            _record(results, "Denied Tracker Hub launch messaging", "failed", " ".join(detail_parts), window.depot_db.db_path)
        else:
            _record(
                results,
                "Denied Tracker Hub launch messaging",
                "ok",
                "Denied launch attempts returned no windows and routed the expected permission-denial messages through the diagnostic sink.",
                window.depot_db.db_path,
            )

        diag_index = next((index for index, name in enumerate(_quick_tab_names(window)) if name == "DiagSmoke"), None)
        if diag_index is None:
            _record(results, "Quick grid cleanup", "warning", "Diagnostic quick tab was already absent before cleanup.", expected_config_path)
        else:
            window.remove_quick_task_tab_at(diag_index)
            window.flush_pending_config_save()
            window.window_manager.close_all()
            window.close()
            window.deleteLater()
            _process_qt_events(app, cycles=6)

            window = QuickInputsWindow(runtime_options=runtime_options)
            window.show()
            _process_qt_events(app, cycles=6)
            if "DiagSmoke" in _quick_tab_names(window):
                _record(results, "Quick grid cleanup", "failed", "Diagnostic quick tab still exists after removal and reload.", expected_config_path)
            else:
                _record(results, "Quick grid cleanup", "ok", "Diagnostic quick tab was removed and stayed removed after reload.", expected_config_path)
    finally:
        try:
            if window is not None:
                window.window_manager.close_all()
        except Exception:
            pass
        try:
            if window is not None:
                window.close()
                window.deleteLater()
        except Exception:
            pass
        _process_qt_events(app, cycles=6)
        if created_app:
            try:
                app.quit()
            except Exception:
                pass

    return results


def collect_ui_smoke_results() -> list[DiagnosticEntry]:
    results: list[DiagnosticEntry] = []
    try:
        shared_root = _get_shared_root_from_config()
    except Exception as exc:
        _record(
            results,
            "Smoke setup",
            "failed",
            f"Failed resolving shared root before smoke run: {type(exc).__name__}: {exc}",
            FLOWGRID_PROJECT_ROOT,
        )
        return results

    previous_qt_platform = os.environ.get("QT_QPA_PLATFORM")
    if not previous_qt_platform:
        os.environ["QT_QPA_PLATFORM"] = "offscreen"

    try:
        with _temporary_smoke_manifest(shared_root) as temp_paths:
            results.extend(_run_ui_smoke_under_manifest(temp_paths))
    except Exception as exc:
        _runtime_log_event(
            "diagnostics.ui_smoke_failed",
            severity="error",
            summary="Flowgrid UI smoke diagnostics failed unexpectedly.",
            exc=exc,
            context={"runtime_root": str(FLOWGRID_PROJECT_ROOT)},
        )
        _record(
            results,
            "Offscreen UI smoke run",
            "failed",
            f"Unhandled smoke failure: {type(exc).__name__}: {exc}",
            FLOWGRID_PROJECT_ROOT,
        )
    finally:
        if previous_qt_platform is None:
            os.environ.pop("QT_QPA_PLATFORM", None)
        else:
            os.environ["QT_QPA_PLATFORM"] = previous_qt_platform

    return results


def run_install_diagnostics() -> int:
    results = collect_install_preflight_results()
    _safe_print(_format_report(f"{APP_TITLE} install diagnostics", results))
    return _exit_code_for_results(results)


def run_ui_smoke_diagnostics() -> int:
    results = collect_ui_smoke_results()
    _safe_print(_format_report(f"{APP_TITLE} UI smoke diagnostics", results))
    return _exit_code_for_results(results)


__all__ = [
    "DiagnosticEntry",
    "collect_install_preflight_results",
    "collect_ui_smoke_results",
    "run_install_diagnostics",
    "run_ui_smoke_diagnostics",
]
