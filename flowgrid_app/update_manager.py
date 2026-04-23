from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flowgrid_app.paths import (
    ASSETS_DIR_NAME,
    DEFAULT_CHANNEL_ID,
    DEFAULT_CHANNEL_LABEL,
    _get_install_state_path,
    _get_local_installer_path,
    _get_shared_root_from_config,
    _current_channel_settings,
    _local_data_root,
)
from flowgrid_app.runtime_logging import _runtime_log_event

DEFAULT_REPO_URL = "https://github.com/S7rasshofer/Flowgrid.git"
DEFAULT_REPO_BRANCH = "main"
GITHUB_API_ACCEPT = "application/vnd.github+json"
GITHUB_ARCHIVE_ACCEPT = "application/zip"
GITHUB_USER_AGENT = "Flowgrid-Updater/1.0"
UPDATE_TIMEOUT_SECONDS = 20.0

REPO_MANAGED_ROOT_FILES = ("Flowgrid.pyw", "Flowgrid_installer.pyw")
REPO_MANAGED_DIRS = ("flowgrid_app", "Assets")
REPO_MANAGED_HASHES_KEY = "repo_managed_files"
SHARED_ASSET_HASHES_KEY = "shared_asset_files"
LAST_SNAPSHOT_SYNC_AT_KEY = "last_snapshot_sync_at_utc"
LAST_SNAPSHOT_SYNC_STATUS_KEY = "last_snapshot_sync_status"
LAST_SNAPSHOT_SYNC_SUMMARY_KEY = "last_snapshot_sync_summary"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _short_sha(value: str, length: int = 12) -> str:
    text = str(value or "").strip()
    return text[:length] if text else ""


def _normalize_repo_url(value: str) -> str:
    text = str(value or "").strip()
    return text or DEFAULT_REPO_URL


def _normalize_branch(value: str) -> str:
    text = str(value or "").strip()
    return text or DEFAULT_REPO_BRANCH


def _channel_state_defaults() -> dict[str, Any]:
    settings = _current_channel_settings()
    read_only_db = bool(settings.get("read_only_db", False))
    defaults = {
        "channel_id": str(settings.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(settings.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(settings.get("channel_display_name") or "Flowgrid"),
        "read_only_db": read_only_db,
        "repo_url": _normalize_repo_url(str(settings.get("repo_url") or DEFAULT_REPO_URL)),
        "branch": _normalize_branch(str(settings.get("branch") or DEFAULT_REPO_BRANCH)),
        "snapshot_source_root": str(settings.get("snapshot_source_root") or "").strip(),
    }
    if not read_only_db:
        defaults[LAST_SNAPSHOT_SYNC_AT_KEY] = ""
        defaults[LAST_SNAPSHOT_SYNC_STATUS_KEY] = "not_applicable"
        defaults[LAST_SNAPSHOT_SYNC_SUMMARY_KEY] = "Main channel uses the production shared root directly."
    else:
        defaults[LAST_SNAPSHOT_SYNC_AT_KEY] = ""
        defaults[LAST_SNAPSHOT_SYNC_STATUS_KEY] = ""
        defaults[LAST_SNAPSHOT_SYNC_SUMMARY_KEY] = ""
    return defaults


def _update_source_label(state: dict[str, Any]) -> str:
    branch = _normalize_branch(str(state.get("branch") or DEFAULT_REPO_BRANCH))
    return f"GitHub {branch}"


def _default_install_state() -> dict[str, Any]:
    channel_defaults = _channel_state_defaults()
    return {
        "repo_url": str(channel_defaults.get("repo_url") or DEFAULT_REPO_URL),
        "branch": str(channel_defaults.get("branch") or DEFAULT_REPO_BRANCH),
        "channel_id": str(channel_defaults.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(channel_defaults.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(channel_defaults.get("channel_display_name") or "Flowgrid"),
        "read_only_db": bool(channel_defaults.get("read_only_db", False)),
        "snapshot_source_root": str(channel_defaults.get("snapshot_source_root") or ""),
        "installed_commit_sha": "",
        "installed_at_utc": "",
        "last_check_at_utc": "",
        "last_check_status": "",
        "last_check_summary": "",
        "last_remote_commit_sha": "",
        "last_shared_asset_sync_at_utc": "",
        "last_shared_asset_sync_status": "",
        "last_shared_asset_sync_summary": "",
        LAST_SNAPSHOT_SYNC_AT_KEY: str(channel_defaults.get(LAST_SNAPSHOT_SYNC_AT_KEY) or ""),
        LAST_SNAPSHOT_SYNC_STATUS_KEY: str(channel_defaults.get(LAST_SNAPSHOT_SYNC_STATUS_KEY) or ""),
        LAST_SNAPSHOT_SYNC_SUMMARY_KEY: str(channel_defaults.get(LAST_SNAPSHOT_SYNC_SUMMARY_KEY) or ""),
        REPO_MANAGED_HASHES_KEY: {},
        SHARED_ASSET_HASHES_KEY: {},
    }


def _normalize_hash_mapping(raw_value: Any) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    normalized: dict[str, str] = {}
    for raw_key, raw_hash in raw_value.items():
        key = str(raw_key or "").replace("\\", "/").strip("/")
        value = str(raw_hash or "").strip().lower()
        if key and value:
            normalized[key] = value
    return normalized


def load_install_state() -> dict[str, Any]:
    target = _get_install_state_path()
    state = _default_install_state()
    if not target.exists():
        return state

    try:
        loaded = json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:
        _runtime_log_event(
            "update.install_state_parse_failed",
            severity="warning",
            summary="Failed parsing Flowgrid install state; defaults will be used for this session.",
            exc=exc,
            context={"install_state_path": str(target)},
        )
        return state

    if not isinstance(loaded, dict):
        _runtime_log_event(
            "update.install_state_invalid",
            severity="warning",
            summary="Flowgrid install state was not a JSON object; defaults will be used for this session.",
            context={"install_state_path": str(target), "value_type": type(loaded).__name__},
        )
        return state

    merged = dict(state)
    merged.update(loaded)
    merged["channel_id"] = str(merged.get("channel_id") or state.get("channel_id") or DEFAULT_CHANNEL_ID).strip().lower() or DEFAULT_CHANNEL_ID
    merged["channel_label"] = str(merged.get("channel_label") or state.get("channel_label") or DEFAULT_CHANNEL_LABEL).strip() or DEFAULT_CHANNEL_LABEL
    merged["channel_display_name"] = str(merged.get("channel_display_name") or state.get("channel_display_name") or "Flowgrid").strip() or "Flowgrid"
    merged["read_only_db"] = bool(merged.get("read_only_db", state.get("read_only_db", False)))
    merged["snapshot_source_root"] = str(merged.get("snapshot_source_root") or state.get("snapshot_source_root") or "").strip()
    merged["repo_url"] = _normalize_repo_url(merged.get("repo_url", DEFAULT_REPO_URL))
    merged["branch"] = _normalize_branch(merged.get("branch", DEFAULT_REPO_BRANCH))
    merged[REPO_MANAGED_HASHES_KEY] = _normalize_hash_mapping(merged.get(REPO_MANAGED_HASHES_KEY))
    merged[SHARED_ASSET_HASHES_KEY] = _normalize_hash_mapping(merged.get(SHARED_ASSET_HASHES_KEY))
    return merged


def save_install_state(state: dict[str, Any]) -> Path:
    target = _get_install_state_path()
    temp_path = target.with_name(f"{target.name}.tmp")
    payload = dict(_default_install_state())
    payload.update(state if isinstance(state, dict) else {})
    payload["channel_id"] = str(payload.get("channel_id") or DEFAULT_CHANNEL_ID).strip().lower() or DEFAULT_CHANNEL_ID
    payload["channel_label"] = str(payload.get("channel_label") or DEFAULT_CHANNEL_LABEL).strip() or DEFAULT_CHANNEL_LABEL
    payload["channel_display_name"] = str(payload.get("channel_display_name") or "Flowgrid").strip() or "Flowgrid"
    payload["read_only_db"] = bool(payload.get("read_only_db", False))
    payload["snapshot_source_root"] = str(payload.get("snapshot_source_root") or "").strip()
    payload["repo_url"] = _normalize_repo_url(payload.get("repo_url", DEFAULT_REPO_URL))
    payload["branch"] = _normalize_branch(payload.get("branch", DEFAULT_REPO_BRANCH))
    payload[REPO_MANAGED_HASHES_KEY] = _normalize_hash_mapping(payload.get(REPO_MANAGED_HASHES_KEY))
    payload[SHARED_ASSET_HASHES_KEY] = _normalize_hash_mapping(payload.get(SHARED_ASSET_HASHES_KEY))

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(temp_path, target)
        return target
    except Exception as exc:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass
        _runtime_log_event(
            "update.install_state_save_failed",
            severity="error",
            summary="Failed saving Flowgrid install state.",
            exc=exc,
            context={"install_state_path": str(target)},
        )
        raise


def _split_github_repo_parts(repo_url: str) -> tuple[str, str]:
    parsed = urlparse(_normalize_repo_url(repo_url))
    if "github.com" not in parsed.netloc.lower():
        raise RuntimeError(f"Unsupported repository host for update checks: {repo_url}")

    path = str(parsed.path or "").strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    parts = [piece for piece in path.split("/") if piece]
    if len(parts) < 2:
        raise RuntimeError(f"Unable to derive GitHub owner/repo from: {repo_url}")
    return parts[0], parts[1]


def _json_request(url: str, *, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> Any:
    request = Request(
        url,
        headers={
            "User-Agent": GITHUB_USER_AGENT,
            "Accept": GITHUB_API_ACCEPT,
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _download_file(url: str, target_path: Path, *, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> Path:
    request = Request(
        url,
        headers={
            "User-Agent": GITHUB_USER_AGENT,
            "Accept": GITHUB_ARCHIVE_ACCEPT,
        },
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    with urlopen(request, timeout=timeout_seconds) as response, temp_path.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    os.replace(temp_path, target_path)
    return target_path


def fetch_remote_commit_info(
    *,
    repo_url: str | None = None,
    branch: str | None = None,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> dict[str, str]:
    resolved_repo_url = _normalize_repo_url(repo_url or DEFAULT_REPO_URL)
    resolved_branch = _normalize_branch(branch or DEFAULT_REPO_BRANCH)
    owner, repo_name = _split_github_repo_parts(resolved_repo_url)
    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/commits/{resolved_branch}"

    try:
        payload = _json_request(api_url, timeout_seconds=timeout_seconds)
    except HTTPError as exc:
        _runtime_log_event(
            "update.remote_commit_http_failed",
            severity="warning",
            summary="GitHub update check returned an HTTP failure.",
            exc=exc,
            context={"api_url": api_url, "repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"GitHub update check failed: HTTP {exc.code}") from exc
    except URLError as exc:
        _runtime_log_event(
            "update.remote_commit_network_failed",
            severity="warning",
            summary="GitHub update check failed due to a network error.",
            exc=exc,
            context={"api_url": api_url, "repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"GitHub update check failed: {exc.reason}") from exc
    except Exception as exc:
        _runtime_log_event(
            "update.remote_commit_unknown_failed",
            severity="warning",
            summary="GitHub update check failed unexpectedly.",
            exc=exc,
            context={"api_url": api_url, "repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"GitHub update check failed: {type(exc).__name__}: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("GitHub update check returned an unexpected payload.")

    sha = str(payload.get("sha") or "").strip()
    if not sha:
        raise RuntimeError("GitHub update check returned no commit SHA.")

    zipball_url = str(payload.get("zipball_url") or "").strip()
    if not zipball_url:
        zipball_url = f"https://api.github.com/repos/{owner}/{repo_name}/zipball/{sha}"

    return {
        "repo_url": resolved_repo_url,
        "branch": resolved_branch,
        "owner": owner,
        "repo_name": repo_name,
        "sha": sha,
        "short_sha": _short_sha(sha),
        "zipball_url": zipball_url,
    }


def check_for_updates(*, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> dict[str, Any]:
    state = load_install_state()
    checked_at = _utc_now_iso()
    source_label = _update_source_label(state)
    channel_display_name = str(state.get("channel_display_name") or "Flowgrid").strip() or "Flowgrid"

    try:
        remote = fetch_remote_commit_info(
            repo_url=str(state.get("repo_url", DEFAULT_REPO_URL)),
            branch=str(state.get("branch", DEFAULT_REPO_BRANCH)),
            timeout_seconds=timeout_seconds,
        )
        installed_sha = str(state.get("installed_commit_sha") or "").strip()
        if installed_sha and installed_sha == remote["sha"]:
            status = "up_to_date"
            summary = f"{channel_display_name} is up to date on {source_label} at {remote['short_sha']}."
            can_install = False
        elif installed_sha:
            status = "update_available"
            summary = (
                f"Update available on {source_label}: installed {_short_sha(installed_sha) or 'unknown'}, "
                f"remote {remote['short_sha']}."
            )
            can_install = True
        else:
            status = "update_available"
            summary = f"Remote {source_label} commit {remote['short_sha']} is available; local install version is unknown."
            can_install = True
        state.update(
            {
                "repo_url": remote["repo_url"],
                "branch": remote["branch"],
                "last_check_at_utc": checked_at,
                "last_check_status": status,
                "last_check_summary": summary,
                "last_remote_commit_sha": remote["sha"],
            }
        )
        save_install_state(state)
        return {
            "status": status,
            "summary": summary,
            "checked_at_utc": checked_at,
            "channel_id": str(state.get("channel_id") or DEFAULT_CHANNEL_ID),
            "channel_label": str(state.get("channel_label") or DEFAULT_CHANNEL_LABEL),
            "channel_display_name": channel_display_name,
            "read_only_db": bool(state.get("read_only_db", False)),
            "installed_commit_sha": installed_sha,
            "remote_commit_sha": remote["sha"],
            "remote_short_sha": remote["short_sha"],
            "repo_url": remote["repo_url"],
            "branch": remote["branch"],
            "update_source_label": source_label,
            "can_install": can_install,
        }
    except Exception as exc:
        summary = str(exc) or "GitHub update check failed."
        state.update(
            {
                "last_check_at_utc": checked_at,
                "last_check_status": "warning",
                "last_check_summary": summary,
            }
        )
        try:
            save_install_state(state)
        except Exception:
            pass
        return {
            "status": "warning",
            "summary": summary,
            "checked_at_utc": checked_at,
            "channel_id": str(state.get("channel_id") or DEFAULT_CHANNEL_ID),
            "channel_label": str(state.get("channel_label") or DEFAULT_CHANNEL_LABEL),
            "channel_display_name": channel_display_name,
            "read_only_db": bool(state.get("read_only_db", False)),
            "installed_commit_sha": str(state.get("installed_commit_sha") or "").strip(),
            "remote_commit_sha": str(state.get("last_remote_commit_sha") or "").strip(),
            "remote_short_sha": _short_sha(state.get("last_remote_commit_sha", "")),
            "repo_url": str(state.get("repo_url", DEFAULT_REPO_URL)),
            "branch": str(state.get("branch", DEFAULT_REPO_BRANCH)),
            "update_source_label": source_label,
            "can_install": False,
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


def _iter_managed_source_files(snapshot_root: Path) -> Iterator[tuple[str, Path]]:
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
            relative = source_path.relative_to(snapshot_root).as_posix()
            yield relative, source_path


def build_repo_file_manifest(snapshot_root: Path) -> dict[str, str]:
    manifest: dict[str, str] = {}
    for relative_path, source_path in _iter_managed_source_files(snapshot_root):
        manifest[relative_path] = _file_sha256(source_path)
    return manifest


def locate_snapshot_project_root(extracted_root: Path) -> Path:
    candidates: list[Path] = []
    for child in extracted_root.iterdir():
        if child.is_dir():
            candidates.append(child)
    if not candidates:
        raise RuntimeError(f"No extracted GitHub snapshot directory found in {extracted_root}")

    for candidate in candidates:
        if (candidate / "Flowgrid.pyw").exists() and (candidate / "flowgrid_app").is_dir():
            return candidate

    raise RuntimeError("GitHub snapshot did not contain Flowgrid.pyw and flowgrid_app at the expected root.")


@contextmanager
def stage_github_snapshot(
    remote_info: dict[str, Any],
    *,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="flowgrid_snapshot_") as temp_dir:
        temp_root = Path(temp_dir)
        archive_path = temp_root / "flowgrid_snapshot.zip"
        extract_root = temp_root / "extracted"
        _download_file(str(remote_info.get("zipball_url") or "").strip(), archive_path, timeout_seconds=timeout_seconds)
        extract_root.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_root)
        project_root = locate_snapshot_project_root(extract_root)
        yield {
            "temp_root": temp_root,
            "archive_path": archive_path,
            "extract_root": extract_root,
            "project_root": project_root,
            "remote_info": dict(remote_info),
        }


def sync_shared_assets(
    *,
    shared_root: Path | None = None,
    local_assets_root: Path | None = None,
) -> dict[str, Any]:
    state = load_install_state()
    synced_at = _utc_now_iso()
    resolved_local_assets = local_assets_root or (_local_data_root() / ASSETS_DIR_NAME)

    try:
        resolved_shared_root = shared_root or _get_shared_root_from_config()
    except Exception as exc:
        summary = f"Unable to resolve the shared data root for asset sync: {type(exc).__name__}: {exc}"
        _runtime_log_event(
            "update.shared_assets_shared_root_failed",
            severity="warning",
            summary="Shared asset sync skipped because the shared root could not be resolved.",
            exc=exc,
        )
        state.update(
            {
                "last_shared_asset_sync_at_utc": synced_at,
                "last_shared_asset_sync_status": "warning",
                "last_shared_asset_sync_summary": summary,
            }
        )
        try:
            save_install_state(state)
        except Exception:
            pass
        return {
            "status": "warning",
            "summary": summary,
            "synced_at_utc": synced_at,
            "added": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        }

    shared_assets_root = resolved_shared_root / ASSETS_DIR_NAME
    if not shared_assets_root.exists() or not shared_assets_root.is_dir():
        summary = f"Shared Assets folder is unavailable at {shared_assets_root}."
        _runtime_log_event(
            "update.shared_assets_missing",
            severity="warning",
            summary="Shared asset sync skipped because the shared Assets folder is unavailable.",
            context={"shared_assets_root": str(shared_assets_root)},
        )
        state.update(
            {
                "last_shared_asset_sync_at_utc": synced_at,
                "last_shared_asset_sync_status": "warning",
                "last_shared_asset_sync_summary": summary,
            }
        )
        try:
            save_install_state(state)
        except Exception:
            pass
        return {
            "status": "warning",
            "summary": summary,
            "synced_at_utc": synced_at,
            "added": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
        }

    resolved_local_assets.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, str] = {}
    added = 0
    updated = 0
    unchanged = 0
    errors = 0

    for source_path in sorted(shared_assets_root.rglob("*")):
        if not source_path.is_file():
            continue
        relative = source_path.relative_to(shared_assets_root).as_posix()
        try:
            shared_hash = _file_sha256(source_path)
            manifest[relative] = shared_hash
            target_path = resolved_local_assets / relative
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
            _runtime_log_event(
                "update.shared_assets_copy_failed",
                severity="warning",
                summary="A shared asset file could not be copied into the local runtime.",
                exc=exc,
                context={
                    "source_path": str(source_path),
                    "target_path": str((resolved_local_assets / relative) if relative else resolved_local_assets),
                },
            )

    status = "ok" if errors == 0 else "warning"
    summary = (
        f"Shared assets synced: added {added}, updated {updated}, unchanged {unchanged}."
        if errors == 0
        else f"Shared assets synced with warnings: added {added}, updated {updated}, unchanged {unchanged}, errors {errors}."
    )
    state.update(
        {
            SHARED_ASSET_HASHES_KEY: manifest,
            "last_shared_asset_sync_at_utc": synced_at,
            "last_shared_asset_sync_status": status,
            "last_shared_asset_sync_summary": summary,
        }
    )
    try:
        save_install_state(state)
    except Exception:
        pass

    _runtime_log_event(
        "update.shared_assets_sync_complete",
        severity="info" if errors == 0 else "warning",
        summary=summary,
        context={
            "shared_assets_root": str(shared_assets_root),
            "local_assets_root": str(resolved_local_assets),
            "added": added,
            "updated": updated,
            "unchanged": unchanged,
            "errors": errors,
        },
    )
    return {
        "status": status,
        "summary": summary,
        "synced_at_utc": synced_at,
        "shared_assets_root": str(shared_assets_root),
        "local_assets_root": str(resolved_local_assets),
        "added": added,
        "updated": updated,
        "unchanged": unchanged,
        "errors": errors,
    }


def current_install_status() -> dict[str, Any]:
    state = load_install_state()
    return {
        "channel_id": str(state.get("channel_id") or DEFAULT_CHANNEL_ID),
        "channel_label": str(state.get("channel_label") or DEFAULT_CHANNEL_LABEL),
        "channel_display_name": str(state.get("channel_display_name") or "Flowgrid").strip() or "Flowgrid",
        "read_only_db": bool(state.get("read_only_db", False)),
        "snapshot_source_root": str(state.get("snapshot_source_root") or "").strip(),
        "update_source_label": _update_source_label(state),
        "repo_url": str(state.get("repo_url", DEFAULT_REPO_URL)),
        "branch": str(state.get("branch", DEFAULT_REPO_BRANCH)),
        "installed_commit_sha": str(state.get("installed_commit_sha") or "").strip(),
        "installed_short_sha": _short_sha(state.get("installed_commit_sha", "")),
        "last_check_at_utc": str(state.get("last_check_at_utc") or "").strip(),
        "last_check_status": str(state.get("last_check_status") or "").strip(),
        "last_check_summary": str(state.get("last_check_summary") or "").strip(),
        "last_remote_commit_sha": str(state.get("last_remote_commit_sha") or "").strip(),
        "last_remote_short_sha": _short_sha(state.get("last_remote_commit_sha", "")),
        "last_shared_asset_sync_at_utc": str(state.get("last_shared_asset_sync_at_utc") or "").strip(),
        "last_shared_asset_sync_status": str(state.get("last_shared_asset_sync_status") or "").strip(),
        "last_shared_asset_sync_summary": str(state.get("last_shared_asset_sync_summary") or "").strip(),
        LAST_SNAPSHOT_SYNC_AT_KEY: str(state.get(LAST_SNAPSHOT_SYNC_AT_KEY) or "").strip(),
        LAST_SNAPSHOT_SYNC_STATUS_KEY: str(state.get(LAST_SNAPSHOT_SYNC_STATUS_KEY) or "").strip(),
        LAST_SNAPSHOT_SYNC_SUMMARY_KEY: str(state.get(LAST_SNAPSHOT_SYNC_SUMMARY_KEY) or "").strip(),
        "install_state_path": str(_get_install_state_path()),
        "local_installer_path": str(_get_local_installer_path()),
        "local_runtime_root": str(_local_data_root()),
    }


__all__ = [
    "DEFAULT_REPO_BRANCH",
    "DEFAULT_REPO_URL",
    "REPO_MANAGED_HASHES_KEY",
    "SHARED_ASSET_HASHES_KEY",
    "build_repo_file_manifest",
    "check_for_updates",
    "current_install_status",
    "fetch_remote_commit_info",
    "load_install_state",
    "locate_snapshot_project_root",
    "save_install_state",
    "stage_github_snapshot",
    "sync_shared_assets",
]
