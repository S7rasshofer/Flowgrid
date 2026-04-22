from __future__ import annotations

from datetime import datetime, timedelta
import json

from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.ui_utils import clamp


def _submission_latest_ts_sql(alias: str = "") -> str:
    prefix = f"{str(alias).strip()}." if str(alias).strip() else ""
    return f"COALESCE(NULLIF(TRIM({prefix}updated_at), ''), {prefix}created_at)"


def _submission_entry_date_sql(alias: str = "") -> str:
    prefix = f"{str(alias).strip()}." if str(alias).strip() else ""
    return f"COALESCE(NULLIF(TRIM({prefix}entry_date), ''), SUBSTR({_submission_latest_ts_sql(alias)}, 1, 10))"


def _split_piped_values(text: str) -> list[str]:
    raw = str(text or "")
    if raw == "":
        return []
    return [str(piece or "").strip() for piece in raw.split(" | ")]


def _merged_part_detail_rows(
    lpn_text: str,
    part_number_text: str,
    part_description_text: str,
    shipping_text: str,
) -> list[tuple[str, str, str, str]]:
    lpn_values = _split_piped_values(lpn_text)
    part_values = _split_piped_values(part_number_text)
    desc_values = _split_piped_values(part_description_text)
    ship_values = _split_piped_values(shipping_text)
    row_count = max(len(lpn_values), len(part_values), len(desc_values), len(ship_values), 0)

    def value_for(values: list[str], idx: int) -> str:
        if idx < len(values):
            return str(values[idx] or "").strip()
        if len(values) == 1 and row_count > 1:
            return str(values[0] or "").strip()
        return ""

    rows: list[tuple[str, str, str, str]] = []
    for idx in range(row_count):
        rows.append(
            (
                value_for(lpn_values, idx),
                value_for(part_values, idx),
                value_for(desc_values, idx),
                value_for(ship_values, idx),
            )
        )
    return rows


def _serialize_part_detail_rows(rows: list[tuple[str, str, str, str]]) -> tuple[str, str, str, str]:
    cleaned = [
        (
            str(row[0] or "").strip(),
            str(row[1] or "").strip(),
            str(row[2] or "").strip(),
            str(row[3] or "").strip(),
        )
        for row in rows
        if any(str(piece or "").strip() for piece in row)
    ]
    return (
        " | ".join(row[0] for row in cleaned),
        " | ".join(row[1] for row in cleaned),
        " | ".join(row[2] for row in cleaned),
        " | ".join(row[3] for row in cleaned),
    )


def _installed_key_set_from_text(installed_keys_raw: str) -> set[str]:
    key_set: set[str] = set()
    serialized = str(installed_keys_raw or "").strip()
    if not serialized:
        return key_set
    try:
        parsed = json.loads(serialized)
        if isinstance(parsed, list):
            for value in parsed:
                value_text = str(value or "").strip()
                if value_text:
                    key_set.add(value_text)
            return key_set
    except Exception:
        pass
    for value in serialized.split(" | "):
        value_text = str(value or "").strip()
        if value_text:
            key_set.add(value_text)
    return key_set


def _part_detail_row_key(lpn: str, part_number: str, part_description: str, shipping_info: str) -> str:
    normalized_lpn = DepotRules.normalize_work_order(lpn)
    part_no_text = str(part_number or "").strip()
    part_desc_text = str(part_description or "").strip()
    shipping_text = str(shipping_info or "").strip()
    if normalized_lpn:
        payload = ["lpn", normalized_lpn.casefold()]
    else:
        payload = [
            "line",
            part_no_text.casefold(),
            part_desc_text.casefold(),
            shipping_text.casefold(),
        ]
    if payload == ["line", "", "", ""]:
        return ""
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _dedupe_part_detail_rows(rows: list[tuple[str, str, str, str]]) -> list[tuple[str, str, str, str]]:
    ordered_keys: list[str] = []
    by_key: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        normalized_row = tuple(str(piece or "").strip() for piece in row)
        row_key = _part_detail_row_key(*normalized_row)
        if not row_key:
            continue
        if row_key not in by_key:
            ordered_keys.append(row_key)
        by_key[row_key] = normalized_row
    return [by_key[key] for key in ordered_keys]


ALERT_QUIET_DEFAULT_HOUR = 9


def _next_alert_quiet_until(hour: int = ALERT_QUIET_DEFAULT_HOUR, now: datetime | None = None) -> str:
    reference = now if isinstance(now, datetime) else datetime.now()
    quiet_day = reference.date() + timedelta(days=1)
    quiet_dt = datetime.combine(quiet_day, datetime.min.time()).replace(hour=int(clamp(int(hour), 0, 23)))
    return quiet_dt.isoformat(timespec="seconds")


def _parse_iso_datetime_local(raw_value: str) -> datetime | None:
    stamp = str(raw_value or "").strip()
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        pass
    candidate = stamp.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(candidate[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    return None


def _alert_quiet_active(raw_value: str, now: datetime | None = None) -> bool:
    quiet_until = _parse_iso_datetime_local(raw_value)
    if quiet_until is None:
        return False
    reference = now if isinstance(now, datetime) else datetime.now()
    return bool(reference < quiet_until)


def _serialized_installed_keys(keys: list[str] | set[str] | tuple[str, ...]) -> str:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in keys:
        key_text = str(value or "").strip()
        if not key_text or key_text in seen:
            continue
        seen.add(key_text)
        ordered.append(key_text)
    return json.dumps(ordered, ensure_ascii=True, separators=(",", ":")) if ordered else ""


__all__ = [
    "ALERT_QUIET_DEFAULT_HOUR",
    "_alert_quiet_active",
    "_dedupe_part_detail_rows",
    "_installed_key_set_from_text",
    "_merged_part_detail_rows",
    "_next_alert_quiet_until",
    "_parse_iso_datetime_local",
    "_part_detail_row_key",
    "_serialize_part_detail_rows",
    "_serialized_installed_keys",
    "_submission_entry_date_sql",
    "_submission_latest_ts_sql",
]
