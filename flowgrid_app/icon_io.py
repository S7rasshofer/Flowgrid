from __future__ import annotations

import json
import os
import shutil
import struct
from pathlib import Path
from typing import Any

from PySide6.QtCore import (
    QByteArray,
    QBuffer,
    QDate,
    QEvent,
    QEasingCurve,
    QIODevice,
    QPoint,
    QPointF,
    QRect,
    QRectF,
    QSize,
    Qt,
    QProcess,
    QTimer,
    QUrl,
    Signal,
    QVariantAnimation,
)

from PySide6.QtGui import (
    QColor,
    QCursor,
    QDesktopServices,
    QFont,
    QGuiApplication,
    QIcon,
    QImage,
    QImageReader,
    QMouseEvent,
    QPainter,
    QPainterPath,
    QPen,
    QPolygon,
    QPixmap,
    QPalette,
    QRegion,
    QTextCursor,
)

from flowgrid_app.paths import (
    ASSETS_DIR_NAME,
    CONFIG_FILENAME,
    FLOWGRID_ICON_PACK_DIR_NAME,
    _data_file_path,
    _local_data_root,
    _paths_equal,
    _resolve_data_root,
)
from flowgrid_app.runtime_logging import _runtime_log_event

DEFAULT_WINDOW_ICON_FILENAME = "wrench.png"

MANAGED_SHORTCUT_ICON_FILENAME = "Flowgrid_shortcut.ico"

def _load_installer_config_snapshot() -> dict[str, Any]:
    config_path = _data_file_path(CONFIG_FILENAME)
    if not config_path.exists():
        return {}
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _runtime_log_event(
            "installer.config_snapshot_parse_failed",
            severity="warning",
            summary="Failed parsing config while preparing installer icon state; default icon will be used.",
            exc=exc,
            context={"config_path": str(config_path)},
        )
        return {}
    if isinstance(data, dict):
        return data
    _runtime_log_event(
        "installer.config_snapshot_invalid",
        severity="warning",
        summary="Config snapshot was not a JSON object; default icon will be used for shortcut sync.",
        context={"config_path": str(config_path), "value_type": type(data).__name__},
    )
    return {}

def _resolve_existing_file_path(raw_path: str) -> Path | None:
    expanded = os.path.expandvars(os.path.expanduser(str(raw_path or "").strip()))
    if not expanded:
        return None

    base_candidate = Path(expanded)
    candidates: list[Path] = [base_candidate]
    if not base_candidate.is_absolute():
        candidates.extend((_resolve_data_root() / base_candidate, _local_data_root() / base_candidate))

    unique: list[Path] = []
    for candidate in candidates:
        if any(_paths_equal(candidate, existing) for existing in unique):
            continue
        unique.append(candidate)

    for candidate in unique:
        try:
            if candidate.exists() and candidate.is_file():
                return candidate
        except Exception as exc:
            _runtime_log_event(
                "installer.icon_candidate_stat_failed",
                severity="warning",
                summary="Failed checking an icon path candidate while resolving installer icon state.",
                exc=exc,
                context={"candidate": str(candidate)},
            )
    return None

def _flowgrid_icon_pack_dir() -> Path:
    return _resolve_data_root() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME

def _ensure_flowgrid_icon_pack_dir() -> Path:
    icon_dir = _flowgrid_icon_pack_dir()
    try:
        icon_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        _runtime_log_event(
            "installer.icon_pack_dir_create_failed",
            severity="warning",
            summary="Failed creating Flowgrid icon pack directory.",
            exc=exc,
            context={"icon_dir": str(icon_dir)},
        )
    return icon_dir

def _default_wrench_icon_source_path() -> Path | None:
    target = _ensure_flowgrid_icon_pack_dir() / DEFAULT_WINDOW_ICON_FILENAME
    if target.exists() and target.is_file():
        return target

    candidate_dirs = [
        _local_data_root() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME,
        _resolve_data_root() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME,
        _local_data_root() / "ui_icons",
        _resolve_data_root() / "ui_icons",
    ]

    seen: list[Path] = []
    for directory in candidate_dirs:
        if any(_paths_equal(directory, existing) for existing in seen):
            continue
        seen.append(directory)
        candidate = directory / DEFAULT_WINDOW_ICON_FILENAME
        if not candidate.exists() or not candidate.is_file():
            continue
        if _paths_equal(candidate, target):
            return candidate
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(candidate, target)
            return target
        except Exception as exc:
            _runtime_log_event(
                "installer.default_icon_copy_failed",
                severity="warning",
                summary="Failed copying the default wrench icon into the managed icon pack directory.",
                exc=exc,
                context={"source_path": str(candidate), "target_path": str(target)},
            )
            return candidate
    return None

def _resolve_active_app_icon_path(config: dict[str, Any] | None = None) -> Path | None:
    config_data = config if isinstance(config, dict) else _load_installer_config_snapshot()
    stored = str(config_data.get("app_icon_path", "") or "").strip() if isinstance(config_data, dict) else ""
    custom_icon = _resolve_existing_file_path(stored)
    if custom_icon is not None:
        return custom_icon
    return _default_wrench_icon_source_path()

def _load_icon_image_file(icon_path: str | Path) -> QImage:
    resolved = _resolve_existing_file_path(str(icon_path))
    if resolved is None:
        return QImage()

    reader = QImageReader(str(resolved))
    reader.setAutoTransform(True)
    image = reader.read()
    if image.isNull():
        return QImage()

    max_dim = max(image.width(), image.height())
    if max_dim > 512:
        image = image.scaled(
            512,
            512,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
    return image.convertToFormat(QImage.Format.Format_ARGB32)

def _is_image_mostly_opaque(image: QImage) -> bool:
    width = image.width()
    height = image.height()
    if width <= 0 or height <= 0:
        return False

    step = max(1, min(width, height) // 64)
    total = 0
    opaque = 0
    for y in range(0, height, step):
        for x in range(0, width, step):
            total += 1
            if image.pixelColor(x, y).alpha() >= 250:
                opaque += 1

    if total == 0:
        return False
    return (opaque / total) >= 0.96

def _estimate_icon_corner_matte(image: QImage) -> QColor:
    width = image.width()
    height = image.height()
    points = [
        (0, 0),
        (min(width - 1, 1), 0),
        (0, min(height - 1, 1)),
        (width - 1, 0),
        (width - 1, min(height - 1, 1)),
        (max(0, width - 2), 0),
        (0, height - 1),
        (min(width - 1, 1), height - 1),
        (0, max(0, height - 2)),
        (width - 1, height - 1),
        (max(0, width - 2), height - 1),
        (width - 1, max(0, height - 2)),
    ]
    rs = 0
    gs = 0
    bs = 0
    count = 0
    for x, y in points:
        color = image.pixelColor(x, y)
        rs += color.red()
        gs += color.green()
        bs += color.blue()
        count += 1
    if count == 0:
        return QColor(0, 0, 0)
    return QColor(rs // count, gs // count, bs // count)

def _cleanup_icon_transparency_image(image: QImage) -> QImage:
    if image.isNull() or not _is_image_mostly_opaque(image):
        return image

    matte = _estimate_icon_corner_matte(image)
    hard = 24
    soft = 72
    cleaned = QImage(image)

    for y in range(cleaned.height()):
        for x in range(cleaned.width()):
            color = cleaned.pixelColor(x, y)
            dist = (
                abs(color.red() - matte.red())
                + abs(color.green() - matte.green())
                + abs(color.blue() - matte.blue())
            )
            alpha = color.alpha()
            if dist <= hard:
                cleaned.setPixelColor(x, y, QColor(color.red(), color.green(), color.blue(), 0))
            elif dist < soft:
                ratio = (dist - hard) / float(soft - hard)
                cleaned.setPixelColor(
                    x,
                    y,
                    QColor(color.red(), color.green(), color.blue(), int(alpha * ratio)),
                )
    return cleaned

def _build_smoothed_qicon(icon_path: str | Path) -> QIcon:
    image = _load_icon_image_file(icon_path)
    if image.isNull():
        return QIcon()

    cleaned = _cleanup_icon_transparency_image(image)
    icon = QIcon()
    for size in (16, 20, 24, 32, 40, 48, 64, 96, 128, 256):
        scaled = cleaned.scaled(
            size,
            size,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        canvas = QPixmap(size, size)
        canvas.fill(Qt.GlobalColor.transparent)
        painter = QPainter(canvas)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        x = (size - scaled.width()) // 2
        y = (size - scaled.height()) // 2
        painter.drawImage(x, y, scaled)
        painter.end()
        icon.addPixmap(canvas)
    return icon

def _normalized_icon_export_image(image: QImage, size: int = 256) -> QImage:
    canvas = QImage(size, size, QImage.Format.Format_ARGB32)
    canvas.fill(Qt.GlobalColor.transparent)
    if image.isNull():
        return canvas
    scaled = image.scaled(
        size,
        size,
        Qt.AspectRatioMode.KeepAspectRatio,
        Qt.TransformationMode.SmoothTransformation,
    )
    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
    x = (size - scaled.width()) // 2
    y = (size - scaled.height()) // 2
    painter.drawImage(x, y, scaled)
    painter.end()
    return canvas

def _qimage_to_png_bytes(image: QImage) -> bytes:
    buffer_bytes = QByteArray()
    buffer = QBuffer(buffer_bytes)
    if not buffer.open(QIODevice.OpenModeFlag.WriteOnly):
        return b""
    try:
        if not image.save(buffer, "PNG"):
            return b""
    finally:
        buffer.close()
    return bytes(buffer_bytes)

def _png_dimensions(png_bytes: bytes) -> tuple[int, int]:
    if len(png_bytes) < 24 or png_bytes[:8] != b"\x89PNG\r\n\x1a\n" or png_bytes[12:16] != b"IHDR":
        raise ValueError("PNG byte stream missing a valid IHDR header.")
    width, height = struct.unpack(">II", png_bytes[16:24])
    return int(width), int(height)

def _write_png_bytes_as_ico(png_bytes: bytes, target_path: Path) -> None:
    width, height = _png_dimensions(png_bytes)
    directory_entry = struct.pack(
        "<BBBBHHII",
        0 if width >= 256 else width,
        0 if height >= 256 else height,
        0,
        0,
        1,
        32,
        len(png_bytes),
        6 + 16,
    )
    payload = struct.pack("<HHH", 0, 1, 1) + directory_entry + png_bytes

    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    temp_path.write_bytes(payload)
    os.replace(temp_path, target_path)

def _write_managed_shortcut_icon(source_path: str | Path, target_path: Path) -> Path:
    image = _load_icon_image_file(source_path)
    if image.isNull():
        raise ValueError(f"Unable to decode icon source: {source_path}")

    cleaned = _cleanup_icon_transparency_image(image)
    export_image = _normalized_icon_export_image(cleaned, 256)
    png_bytes = _qimage_to_png_bytes(export_image)
    if not png_bytes:
        raise ValueError(f"Unable to encode icon source as PNG: {source_path}")

    _write_png_bytes_as_ico(png_bytes, target_path)
    return target_path

__all__ = [
    "DEFAULT_WINDOW_ICON_FILENAME",
    "MANAGED_SHORTCUT_ICON_FILENAME",
    "_build_smoothed_qicon",
    "_cleanup_icon_transparency_image",
    "_default_wrench_icon_source_path",
    "_ensure_flowgrid_icon_pack_dir",
    "_estimate_icon_corner_matte",
    "_flowgrid_icon_pack_dir",
    "_is_image_mostly_opaque",
    "_load_icon_image_file",
    "_normalized_icon_export_image",
    "_png_dimensions",
    "_qimage_to_png_bytes",
    "_resolve_active_app_icon_path",
    "_resolve_existing_file_path",
    "_write_managed_shortcut_icon",
    "_write_png_bytes_as_ico",
]
