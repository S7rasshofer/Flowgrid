from __future__ import annotations

import ctypes
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from flowgrid_app.paths import (
    ASSETS_DIR_NAME,
    DEFAULT_CHANNEL_ID,
    DEFAULT_CHANNEL_LABEL,
    _get_install_state_path,
    _get_local_updater_path,
    _get_shared_root_from_config,
    _current_channel_settings,
    _local_data_root,
)
from flowgrid_app.runtime_logging import _runtime_log_event

DEFAULT_REPO_URL = "https://github.com/S7rasshofer/Flowgrid.git"
DEFAULT_REPO_BRANCH = "main"
GITHUB_API_ACCEPT = "application/vnd.github+json"
GITHUB_USER_AGENT = "Flowgrid-Updater/1.0"
UPDATE_TIMEOUT_SECONDS = 20.0
GITHUB_RETRY_ATTEMPTS = 3

REPO_MANAGED_ROOT_FILES = ("Flowgrid.pyw", "Flowgrid_updater.pyw")
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


def _safe_request(
    url: str,
    *,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
    accept: str = GITHUB_API_ACCEPT,
) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": GITHUB_USER_AGENT,
            "Accept": str(accept or "*/*"),
        },
    )
    last_error: Exception | None = None
    for attempt in range(1, GITHUB_RETRY_ATTEMPTS + 1):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return response.read()
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


def _json_request(url: str, *, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> Any:
    payload = _safe_request(url, timeout_seconds=timeout_seconds, accept=GITHUB_API_ACCEPT)
    return json.loads(payload.decode("utf-8"))


def _download_file(url: str, target_path: Path, *, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> Path:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    payload = _safe_request(url, timeout_seconds=timeout_seconds, accept="application/octet-stream")
    with temp_path.open("wb") as handle:
        handle.write(payload)
    os.replace(temp_path, target_path)
    return target_path


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
    encoded_branch = quote(_normalize_branch(branch), safe="")
    if normalized_path:
        encoded_path = quote(normalized_path, safe="/")
        return f"https://api.github.com/repos/{owner}/{repo_name}/contents/{encoded_path}?ref={encoded_branch}"
    return f"https://api.github.com/repos/{owner}/{repo_name}/contents?ref={encoded_branch}"


def _fetch_repo_tree(
    *,
    repo_url: str,
    branch: str,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    owner, repo_name = _split_github_repo_parts(repo_url)
    pending_dirs: list[str] = [""]
    visited_dirs: set[str] = set()
    files: list[dict[str, Any]] = []

    while pending_dirs:
        current_dir = pending_dirs.pop()
        api_url = _build_github_contents_url(owner, repo_name, current_dir, branch)
        payload = _json_request(api_url, timeout_seconds=timeout_seconds)
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
                _runtime_log_event(
                    "update.repo_entry_skipped",
                    severity="info",
                    summary="Skipped a non-file repository entry during update discovery.",
                    context={"entry_type": entry_type or "unknown", "path": relative_path or "/"},
                )
                continue
            download_url = str(entry.get("download_url") or "").strip()
            if not relative_path or not download_url:
                _runtime_log_event(
                    "update.repo_entry_invalid",
                    severity="warning",
                    summary="Repository listing returned an invalid file entry.",
                    context={"path": relative_path or "/", "download_url": download_url},
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


def _calculate_repo_revision(files: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for entry in sorted(files, key=lambda item: str(item.get("path") or "")):
        digest.update(str(entry.get("path") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(entry.get("sha") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(entry.get("size") or 0).encode("ascii", "ignore"))
        digest.update(b"\n")
    return digest.hexdigest()


def _fetch_remote_revision_info(
    *,
    repo_url: str | None = None,
    branch: str | None = None,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    resolved_repo_url = _normalize_repo_url(repo_url or DEFAULT_REPO_URL)
    resolved_branch = _normalize_branch(branch or DEFAULT_REPO_BRANCH)

    try:
        repo_files = _fetch_repo_tree(
            repo_url=resolved_repo_url,
            branch=resolved_branch,
            timeout_seconds=timeout_seconds,
        )
    except HTTPError as exc:
        _runtime_log_event(
            "update.remote_revision_http_failed",
            severity="warning",
            summary="Repository revision check returned an HTTP failure.",
            exc=exc,
            context={"repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"Repository revision check failed: HTTP {exc.code}") from exc
    except URLError as exc:
        _runtime_log_event(
            "update.remote_revision_network_failed",
            severity="warning",
            summary="Repository revision check failed due to a network error.",
            exc=exc,
            context={"repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"Repository revision check failed: {exc.reason}") from exc
    except Exception as exc:
        _runtime_log_event(
            "update.remote_revision_failed",
            severity="warning",
            summary="Repository revision check failed unexpectedly.",
            exc=exc,
            context={"repo_url": resolved_repo_url, "branch": resolved_branch},
        )
        raise RuntimeError(f"Repository revision check failed: {type(exc).__name__}: {exc}") from exc

    managed_files = [entry for entry in repo_files if _is_repo_managed_path(str(entry.get("path") or ""))]
    if not managed_files:
        raise RuntimeError("Repository listing returned no managed runtime files.")
    sha = _calculate_repo_revision(managed_files)
    return {
        "repo_url": resolved_repo_url,
        "branch": resolved_branch,
        "sha": sha,
        "short_sha": _short_sha(sha),
        "file_count": len(managed_files),
    }


def fetch_remote_commit_info(
    *,
    repo_url: str | None = None,
    branch: str | None = None,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    resolved_repo_url = _normalize_repo_url(repo_url or DEFAULT_REPO_URL)
    resolved_branch = _normalize_branch(branch or DEFAULT_REPO_BRANCH)
    repo_files = _fetch_repo_tree(
        repo_url=resolved_repo_url,
        branch=resolved_branch,
        timeout_seconds=timeout_seconds,
    )
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


def check_for_updates(*, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> dict[str, Any]:
    state = load_install_state()
    checked_at = _utc_now_iso()
    source_label = _update_source_label(state)
    channel_display_name = str(state.get("channel_display_name") or "Flowgrid").strip() or "Flowgrid"

    try:
        remote = _fetch_remote_revision_info(
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
            summary = f"Remote {source_label} revision {remote['short_sha']} is available; local install version is unknown."
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


def _prune_empty_parents(target_path: Path, stop_root: Path) -> None:
    parent = target_path.parent
    while parent != stop_root and parent.exists():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _seed_missing_shared_assets_from_local_runtime(
    *,
    shared_root: Path,
    local_root: Path,
) -> dict[str, Any]:
    source_assets_root = local_root / ASSETS_DIR_NAME
    target_assets_root = shared_root / ASSETS_DIR_NAME
    if not source_assets_root.exists() or not source_assets_root.is_dir():
        summary = f"Local packaged Assets folder is unavailable at {source_assets_root}; shared asset seeding was skipped."
        _runtime_log_event(
            "update.shared_assets_seed_source_missing",
            severity="warning",
            summary="Shared asset baseline seeding was skipped because the local packaged Assets folder is unavailable.",
            context={"source_assets_root": str(source_assets_root), "target_assets_root": str(target_assets_root)},
        )
        return {
            "status": "warning",
            "summary": summary,
            "added": 0,
            "skipped": 0,
            "errors": 0,
        }

    added = 0
    skipped = 0
    errors = 0
    try:
        target_assets_root.mkdir(parents=True, exist_ok=True)
        for source_path in sorted(source_assets_root.rglob("*")):
            relative = source_path.relative_to(source_assets_root)
            target_path = target_assets_root / relative
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
                _runtime_log_event(
                    "update.shared_assets_seed_copy_failed",
                    severity="warning",
                    summary="A packaged asset file could not be copied into the shared Assets tree.",
                    exc=exc,
                    context={"source_path": str(source_path), "target_path": str(target_path)},
                )
    except Exception as exc:
        summary = f"Shared asset seed failed: {type(exc).__name__}: {exc}"
        _runtime_log_event(
            "update.shared_assets_seed_failed",
            severity="warning",
            summary="Shared asset baseline seeding failed.",
            exc=exc,
            context={"source_assets_root": str(source_assets_root), "target_assets_root": str(target_assets_root)},
        )
        return {
            "status": "warning",
            "summary": summary,
            "added": added,
            "skipped": skipped,
            "errors": errors + 1,
        }

    status = "ok" if errors == 0 else "warning"
    summary = (
        f"Shared asset baseline ensured: added {added}, existing {skipped}."
        if errors == 0
        else f"Shared asset baseline ensured with warnings: added {added}, existing {skipped}, errors {errors}."
    )
    _runtime_log_event(
        "update.shared_assets_seed_complete",
        severity="info" if errors == 0 else "warning",
        summary=summary,
        context={"source_assets_root": str(source_assets_root), "target_assets_root": str(target_assets_root)},
    )
    return {
        "status": status,
        "summary": summary,
        "added": added,
        "skipped": skipped,
        "errors": errors,
    }


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


@contextmanager
def stage_github_snapshot(
    remote_info: dict[str, Any],
    *,
    timeout_seconds: float = UPDATE_TIMEOUT_SECONDS,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="flowgrid_snapshot_") as temp_dir:
        temp_root = Path(temp_dir)
        snapshot_root = temp_root / "snapshot"
        snapshot_root.mkdir(parents=True, exist_ok=True)
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
            target_path = snapshot_root / Path(relative_path.replace("/", os.sep))
            _download_file(download_url, target_path, timeout_seconds=timeout_seconds)
        yield {
            "temp_root": temp_root,
            "snapshot_root": snapshot_root,
            "project_root": snapshot_root,
            "remote_info": dict(remote_info),
        }


def _apply_repo_manifest(
    snapshot_root: Path,
    previous_state: dict[str, Any],
    remote_info: dict[str, Any],
) -> dict[str, Any]:
    local_root = _local_data_root()
    source_map: dict[str, Path] = {}
    new_manifest: dict[str, str] = {}

    for relative_path, source_path in _iter_managed_source_files(snapshot_root):
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
    allowed_root_files = set(REPO_MANAGED_ROOT_FILES) | {"Flowgrid_installer.pyw"}
    for relative_path in sorted(set(previous_manifest) - set(new_manifest)):
        relative_obj = Path(relative_path)
        if relative_obj.is_absolute() or ".." in relative_obj.parts:
            continue
        if relative_path not in allowed_root_files and not any(
            relative_path.startswith(f"{dirname}/") for dirname in REPO_MANAGED_DIRS
        ):
            continue
        target_path = local_root / Path(relative_path.replace("/", os.sep))
        if not target_path.exists() or not target_path.is_file():
            continue
        try:
            target_path.unlink()
            removed += 1
            _prune_empty_parents(target_path, local_root)
        except Exception as exc:
            _runtime_log_event(
                "update.stale_managed_file_remove_failed",
                severity="warning",
                summary="Failed removing a stale repo-managed file during update.",
                exc=exc,
                context={"relative_path": relative_path, "target_path": str(target_path)},
            )

    if not previous_manifest:
        expected_package_files = {path for path in new_manifest if path.startswith("flowgrid_app/")}
        local_package = local_root / "flowgrid_app"
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
                    _prune_empty_parents(existing_file, local_root)
                except Exception as exc:
                    _runtime_log_event(
                        "update.legacy_package_prune_failed",
                        severity="warning",
                        summary="Failed pruning a stale file from the local flowgrid_app package.",
                        exc=exc,
                        context={"path": str(existing_file)},
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
                f"{str(previous_state.get('channel_display_name') or 'Flowgrid')} installed from "
                f"{_update_source_label(previous_state)} at {remote_info.get('short_sha', '')}."
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


def apply_repo_update(*, timeout_seconds: float = UPDATE_TIMEOUT_SECONDS) -> dict[str, Any]:
    state = load_install_state()
    remote = fetch_remote_commit_info(
        repo_url=str(state.get("repo_url", DEFAULT_REPO_URL)),
        branch=str(state.get("branch", DEFAULT_REPO_BRANCH)),
        timeout_seconds=timeout_seconds,
    )

    local_root = _local_data_root()
    runtime_bootstrap_needed = (
        not (local_root / "Flowgrid.pyw").exists()
        or not _get_local_updater_path().exists()
        or not (local_root / "flowgrid_app").exists()
        or not _get_install_state_path().exists()
        or not _normalize_hash_mapping(state.get(REPO_MANAGED_HASHES_KEY))
    )
    installed_sha = str(state.get("installed_commit_sha") or "").strip()

    if not runtime_bootstrap_needed and installed_sha == str(remote.get("sha") or "").strip():
        bootstrap_ok, bootstrap_summary = _bootstrap_shared_database_with_local_runtime(state=state, local_root=local_root)
        if not bootstrap_ok:
            raise RuntimeError(bootstrap_summary)
        try:
            shared_seed_result = _seed_missing_shared_assets_from_local_runtime(
                shared_root=_get_shared_root_from_config(),
                local_root=local_root,
            )
        except Exception as exc:
            shared_seed_result = {
                "status": "warning",
                "summary": f"Shared asset baseline seed skipped: {type(exc).__name__}: {exc}",
            }
        pre_sync_summary = (
            f"{str(state.get('channel_display_name') or 'Flowgrid')} already matched "
            f"{_update_source_label(state)} at {remote.get('short_sha', '')}. {bootstrap_summary}"
        )
        state.update(
            {
                "last_check_at_utc": _utc_now_iso(),
                "last_check_status": "up_to_date",
                "last_check_summary": pre_sync_summary,
                "last_remote_commit_sha": str(remote.get("sha") or "").strip(),
            }
        )
        save_install_state(state)
        asset_result = sync_shared_assets()
        final_state = load_install_state()
        final_summary = (
            f"{pre_sync_summary} {str(shared_seed_result.get('summary') or '').strip()} "
            f"{str(asset_result.get('summary') or '').strip()}"
        ).strip()
        final_state.update(
            {
                "last_check_at_utc": str(state.get("last_check_at_utc") or "").strip(),
                "last_check_status": "up_to_date",
                "last_check_summary": final_summary,
                "last_remote_commit_sha": str(remote.get("sha") or "").strip(),
            }
        )
        save_install_state(final_state)
        return {
            "status": "up_to_date",
            "summary": final_summary,
            "copied": 0,
            "unchanged": len(_normalize_hash_mapping(final_state.get(REPO_MANAGED_HASHES_KEY))),
            "removed": 0,
            "state": final_state,
            "remote": remote,
        }

    try:
        with stage_github_snapshot(remote, timeout_seconds=timeout_seconds) as snapshot_info:
            apply_result = _apply_repo_manifest(snapshot_info["project_root"], state, remote)
    except Exception as exc:
        _runtime_log_event(
            "update.apply_failed",
            severity="error",
            summary="Failed applying the repository update into the local runtime.",
            exc=exc,
            context={"repo_url": str(remote.get("repo_url") or ""), "branch": str(remote.get("branch") or "")},
        )
        raise

    updated_state = dict(apply_result["state"])
    bootstrap_ok, bootstrap_summary = _bootstrap_shared_database_with_local_runtime(state=updated_state, local_root=local_root)
    if not bootstrap_ok:
        raise RuntimeError(bootstrap_summary)
    save_install_state(updated_state)
    try:
        shared_seed_result = _seed_missing_shared_assets_from_local_runtime(
            shared_root=_get_shared_root_from_config(),
            local_root=local_root,
        )
    except Exception as exc:
        shared_seed_result = {
            "status": "warning",
            "summary": f"Shared asset baseline seed skipped: {type(exc).__name__}: {exc}",
        }
    asset_result = sync_shared_assets()
    final_state = load_install_state()
    summary = (
        f"Runtime updated from {_update_source_label(updated_state)}: copied {apply_result['copied']}, "
        f"unchanged {apply_result['unchanged']}, removed {apply_result['removed']}. "
        f"{bootstrap_summary} {str(shared_seed_result.get('summary') or '').strip()} "
        f"{str(asset_result.get('summary') or '').strip()}"
    )
    final_state.update({"last_check_summary": summary, "last_check_status": "up_to_date"})
    save_install_state(final_state)
    _runtime_log_event(
        "update.apply_complete",
        severity="info",
        summary=summary,
        context={
            "local_runtime_root": str(local_root),
            "repo_url": str(remote.get("repo_url") or ""),
            "branch": str(remote.get("branch") or ""),
            "remote_sha": str(remote.get("sha") or ""),
            "copied": apply_result["copied"],
            "unchanged": apply_result["unchanged"],
            "removed": apply_result["removed"],
        },
    )
    return {
        "status": "updated",
        "summary": summary,
        "copied": apply_result["copied"],
        "unchanged": apply_result["unchanged"],
        "removed": apply_result["removed"],
        "state": final_state,
        "remote": remote,
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
        "local_updater_path": str(_get_local_updater_path()),
        "local_runtime_root": str(_local_data_root()),
    }


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
            _runtime_log_event(
                "update.parent_wait_timeout",
                severity="warning",
                summary="Timed out waiting for the parent Flowgrid process to exit before update.",
                context={"parent_pid": int(parent_pid)},
            )
    except Exception as exc:
        _runtime_log_event(
            "update.parent_wait_failed",
            severity="warning",
            summary="Failed waiting for the parent Flowgrid process before update.",
            exc=exc,
            context={"parent_pid": int(parent_pid)},
        )
    finally:
        if handle:
            try:
                ctypes.windll.kernel32.CloseHandle(handle)
            except Exception:
                pass


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


def _preferred_cli_python_executable() -> Path:
    candidates: list[Path] = []
    for raw in (getattr(sys, "_base_executable", ""), sys.executable):
        text = str(raw or "").strip()
        if not text:
            continue
        path = Path(text)
        candidates.append(path)
        if path.name.lower() == "pythonw.exe":
            candidates.append(path.with_name("python.exe"))
    unique: list[Path] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    for candidate in unique:
        if candidate.name.lower() == "python.exe" and candidate.exists() and candidate.is_file():
            return candidate
    for candidate in unique:
        if candidate.exists() and candidate.is_file():
            return candidate
    return Path(getattr(sys, "_base_executable", "") or sys.executable)


def _bootstrap_shared_database_with_local_runtime(
    *,
    state: dict[str, Any],
    local_root: Path,
) -> tuple[bool, str]:
    if bool(state.get("read_only_db", False)):
        return True, "Read-only channel skipped shared DB bootstrap."

    try:
        shared_root = _get_shared_root_from_config()
        shared_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        detail = f"Unable to resolve or prepare the shared root: {type(exc).__name__}: {exc}"
        _runtime_log_event(
            "update.shared_db_shared_root_failed",
            severity="error",
            summary="Updater could not prepare the shared root before DB bootstrap.",
            exc=exc,
        )
        return False, detail

    shared_db = shared_root / "Flowgrid_depot.db"
    shared_db_existed = shared_db.exists() and shared_db.is_file()
    launcher_path = _preferred_cli_python_executable()
    if not launcher_path.exists() or not launcher_path.is_file():
        detail = f"Python launcher not found: {launcher_path}"
        _runtime_log_event(
            "update.shared_db_launcher_missing",
            severity="error",
            summary="Updater could not locate a Python launcher for shared DB bootstrap.",
            context={"launcher_path": str(launcher_path)},
        )
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
            "    db.close('updater.shared_db_bootstrap')",
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
        _runtime_log_event(
            "update.shared_db_bootstrap_run_failed",
            severity="error",
            summary="Updater failed running shared DB bootstrap from the local runtime.",
            exc=exc,
            context={"local_root": str(local_root), "shared_db": str(shared_db)},
        )
        return False, detail

    if result.returncode != 0:
        detail = (str(result.stderr or "").strip() or str(result.stdout or "").strip() or "Unknown shared DB bootstrap failure.")[-2000:]
        _runtime_log_event(
            "update.shared_db_bootstrap_failed",
            severity="error",
            summary="Updater failed creating or migrating the shared DB from the local runtime.",
            context={"local_root": str(local_root), "shared_db": str(shared_db), "detail": detail},
        )
        return False, detail

    if shared_db_existed:
        return True, f"Shared workflow DB already existed and was verified at {shared_db}."
    return True, f"Shared workflow DB created at {shared_db}."


def _launch_flowgrid_detached(*, skip_startup_update: bool = False) -> tuple[bool, str]:
    launcher_path = _preferred_gui_python_executable()
    script_path = _local_data_root() / "Flowgrid.pyw"
    if not launcher_path.exists() or not launcher_path.is_file():
        return False, f"Python launcher not found: {launcher_path}"
    if not script_path.exists() or not script_path.is_file():
        return False, f"Flowgrid script not found: {script_path}"
    try:
        command = [str(launcher_path), str(script_path)]
        if skip_startup_update:
            command.append("--skip-startup-update")
        subprocess.Popen(command, cwd=str(script_path.parent))
        return True, ""
    except Exception as exc:
        _runtime_log_event(
            "update.launch_subprocess_failed",
            severity="error",
            summary="Updater failed to relaunch Flowgrid after applying an update.",
            exc=exc,
            context={"launcher_path": str(launcher_path), "script_path": str(script_path)},
        )
        return False, f"{type(exc).__name__}: {exc}"


def _show_updater_message(title: str, message: str, *, is_error: bool = False) -> None:
    if os.name != "nt":
        return
    try:
        flags = 0x00000010 if is_error else 0x00000040
        ctypes.windll.user32.MessageBoxW(None, str(message), str(title), flags)
    except Exception:
        pass


def _parse_updater_cli_options(argv: list[str] | None = None) -> dict[str, Any]:
    options: dict[str, Any] = {
        "launch_after_update": True,
        "relaunch_after_update": False,
        "launch_on_failure": False,
        "parent_pid": 0,
    }
    args = list(argv if argv is not None else sys.argv[1:])
    idx = 0
    while idx < len(args):
        raw = str(args[idx] or "").strip()
        lowered = raw.lower()
        if lowered == "--no-launch":
            options["launch_after_update"] = False
        elif lowered == "--relaunch":
            options["relaunch_after_update"] = True
        elif lowered == "--launch-on-failure":
            options["launch_on_failure"] = True
        elif lowered == "--parent-pid" and idx + 1 < len(args):
            idx += 1
            try:
                options["parent_pid"] = int(str(args[idx] or "").strip())
            except Exception:
                options["parent_pid"] = 0
        elif lowered.startswith("--parent-pid="):
            try:
                options["parent_pid"] = int(lowered.split("=", 1)[1].strip())
            except Exception:
                options["parent_pid"] = 0
        idx += 1
    return options


def run_updater_mode(
    *,
    launch_after_update: bool = True,
    relaunch_after_update: bool = False,
    launch_on_failure: bool = False,
    parent_pid: int = 0,
) -> int:
    _wait_for_parent_exit(int(parent_pid))
    should_launch = bool(relaunch_after_update or launch_after_update)
    try:
        result = apply_repo_update()
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        _runtime_log_event(
            "update.updater_run_failed",
            severity="error",
            summary="The standalone updater failed.",
            exc=exc,
            context={
                "parent_pid": int(parent_pid),
                "launch_after_update": bool(launch_after_update),
                "relaunch_after_update": bool(relaunch_after_update),
                "launch_on_failure": bool(launch_on_failure),
            },
        )
        if should_launch and launch_on_failure:
            launched, launch_detail = _launch_flowgrid_detached(skip_startup_update=True)
            if launched:
                return 0
            _show_updater_message(
                "Flowgrid Update Failed",
                f"{detail}\n\nThe app also failed to relaunch:\n{launch_detail}",
                is_error=True,
            )
            return 1
        _show_updater_message("Flowgrid Update Failed", detail, is_error=True)
        return 1

    if should_launch:
        launched, detail = _launch_flowgrid_detached(skip_startup_update=True)
        if not launched:
            _show_updater_message("Flowgrid Update", detail, is_error=True)
            return 1
        return 0

    if str(result.get("status") or "").strip().lower() == "updated":
        _show_updater_message("Flowgrid Updated", str(result.get("summary") or "").strip() or "Update completed.")
    elif str(result.get("status") or "").strip().lower() == "up_to_date":
        _show_updater_message("Flowgrid Update", str(result.get("summary") or "").strip() or "Flowgrid is already up to date.")
    return 0


__all__ = [
    "apply_repo_update",
    "DEFAULT_REPO_BRANCH",
    "DEFAULT_REPO_URL",
    "REPO_MANAGED_HASHES_KEY",
    "SHARED_ASSET_HASHES_KEY",
    "build_repo_file_manifest",
    "check_for_updates",
    "current_install_status",
    "fetch_remote_commit_info",
    "load_install_state",
    "run_updater_mode",
    "save_install_state",
    "stage_github_snapshot",
    "sync_shared_assets",
]
