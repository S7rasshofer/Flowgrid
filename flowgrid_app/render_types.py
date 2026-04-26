from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from PySide6.QtCore import QRectF
from PySide6.QtGui import QPixmap


@dataclass
class LayerRenderInfo:
    layer: dict[str, Any]
    rect: QRectF
    pixmap: QPixmap


__all__ = ["LayerRenderInfo"]
