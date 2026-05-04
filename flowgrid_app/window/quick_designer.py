from __future__ import annotations

import json
import math
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

from PySide6.QtWidgets import (
    QAbstractButton,
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QColorDialog,
    QComboBox,
    QDateEdit,
    QDialog,
    QFileDialog,
    QFontComboBox,
    QFormLayout,
    QFrame,
    QGraphicsDropShadowEffect,
    QGraphicsOpacityEffect,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QScrollBar,
    QSizePolicy,
    QSlider,
    QSpinBox,
    QStackedWidget,
    QStyle,
    QStyleOptionTab,
    QStyleOptionViewItem,
    QStylePainter,
    QStyledItemDelegate,
    QTabBar,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from flowgrid_app.paths import APP_TITLE

from .popup_support import show_flowgrid_themed_open_file_names

LAUNCH_WIDTH = 430

LAUNCH_HEIGHT = 485

TITLEBAR_HEIGHT = 34

SIDEBAR_WIDTH = 52

SHIFT_CONTEXT_SCRIPT_LAUNCHERS: dict[str, str] = {
}

PREVIOUS_THEME_PRESETS: dict[str, dict[str, str]] = {
    "Default": {"primary": "#C35A00", "accent": "#FF9A1F", "surface": "#090A0F"},
    "Classic": {"primary": "#0A246A", "accent": "#C0C0C0", "surface": "#D4D0C8"},
    "Slate": {"primary": "#3A4A6A", "accent": "#D97706", "surface": "#E8ECF3"},
    "Forest": {"primary": "#205E55", "accent": "#D66A1A", "surface": "#E9F1ED"},
    "Ocean": {"primary": "#15D3E3", "accent": "#D1A91F", "surface": "#70D7E9"},
    "Midnight": {"primary": "#1E2B3A", "accent": "#4DA3FF", "surface": "#0F141C"},
    "Desert": {"primary": "#7A4A2A", "accent": "#D9A25E", "surface": "#F1E5D6"},
    "Sage": {"primary": "#2F5D50", "accent": "#9DC66B", "surface": "#E8F0E6"},
    "Crimson": {"primary": "#6A1E1E", "accent": "#D85A5A", "surface": "#F1E6E6"},
    "Steel": {"primary": "#3C4B5C", "accent": "#8FA7BF", "surface": "#E3EAF2"},
    "Amber": {"primary": "#70420C", "accent": "#F3B33E", "surface": "#F6EDD9"},
}

DEFAULT_THEME_PRESETS: dict[str, dict[str, str]] = {
    "Default": {"primary": "#C35A00", "accent": "#FF9A1F", "surface": "#090A0F"},
    "Classic": {"primary": "#0A246A", "accent": "#9EA6B2", "surface": "#D8D6D0"},
    "Slate": {"primary": "#344763", "accent": "#C66C10", "surface": "#E6EBF2"},
    "Forest": {"primary": "#205E55", "accent": "#C95F18", "surface": "#E7F0EC"},
    "Ocean": {"primary": "#0E93A5", "accent": "#CDA320", "surface": "#D9F5FA"},
    "Midnight": {"primary": "#1E2B3A", "accent": "#5AA9FF", "surface": "#0F141C"},
    "Desert": {"primary": "#75472A", "accent": "#C98C45", "surface": "#EFE1CF"},
    "Sage": {"primary": "#2F5D50", "accent": "#83AD55", "surface": "#E5EEE4"},
    "Crimson": {"primary": "#6A1E1E", "accent": "#C84B4B", "surface": "#F0E5E6"},
    "Steel": {"primary": "#3C4B5C", "accent": "#7995AF", "surface": "#E2EAF1"},
    "Amber": {"primary": "#70420C", "accent": "#D99526", "surface": "#F4E8D2"},
}

DEFAULT_THEME_PRIMARY = "#C35A00"

DEFAULT_THEME_ACCENT = "#FF9A1F"

DEFAULT_THEME_SURFACE = "#090A0F"

LEGACY_DEFAULT_THEME_PRIMARY = "#2F6FED"

LEGACY_DEFAULT_THEME_ACCENT = "#16A085"

LEGACY_DEFAULT_THEME_SURFACE = "#E9EEF5"

DEFAULT_CONFIG: dict[str, Any] = {
    "grid_columns": 3,
    "always_on_top": False,
    "agent_window_always_on_top": True,
    "qa_window_always_on_top": True,
    "admin_window_always_on_top": False,
    "dashboard_window_always_on_top": False,
    "agent_window_compact_anchor": "TopRight",
    "sidebar_on_right": False,
    "auto_minimize_after_insert": False,
    "compact_mode": True,
    "background_tint_enabled": True,
    "window_opacity": 1.0,
    "hover_reveal_delay_s": 5,
    "hover_fade_in_s": 5,
    "hover_fade_out_s": 5,
    "popup_control_style": "Fade Left to Right",
    "popup_control_opacity": 82,
    "popup_control_tail_opacity": 0,
    "popup_control_fade_enabled": True,
    "popup_control_fade_strength": 65,
    "popup_header_color": "",
    "popup_row_hover_color": "",
    "popup_row_selected_color": "",
    "popup_auto_reinherit_enabled": True,
    "quick_button_opacity": 1.0,
    "window_position": None,
    "popup_positions": {"image_layers": None, "quick_layout": None, "depot_dashboard": None, "agent": None},
    "theme": {"primary": DEFAULT_THEME_PRIMARY, "accent": DEFAULT_THEME_ACCENT, "surface": DEFAULT_THEME_SURFACE},
    "theme_presets": DEFAULT_THEME_PRESETS,
    "selected_theme_preset": "Default",
    "theme_image_layers": [],
    "quick_button_width": 140,
    "quick_button_height": 40,
    "quick_button_font_size": 11,
    "quick_button_font_family": "Segoe UI",
    "quick_button_shape": "Soft",
    "active_quick_tab": 0,
    "current_user": "",
    "agent_theme": {
        "background": "#FFFFFF",
        "text": "#000000",
        "field_bg": "#FFFFFF",
        "transparent": False,
        "inherit_main_theme": True,
        "image_layers": [],
        "control_style": "Fade Left to Right",
        "control_opacity": 82,
        "control_tail_opacity": 0,
        "control_fade_strength": 65,
        "header_color": "",
        "row_hover_color": "",
        "row_selected_color": "",
    },
    "qa_theme": {
        "background": "#FFFFFF",
        "text": "#000000",
        "field_bg": "#FFFFFF",
        "transparent": False,
        "inherit_main_theme": True,
        "image_layers": [],
        "control_style": "Fade Left to Right",
        "control_opacity": 82,
        "control_tail_opacity": 0,
        "control_fade_strength": 65,
        "header_color": "",
        "row_hover_color": "",
        "row_selected_color": "",
    },
    "admin_theme": {
        "background": "#FFFFFF",
        "text": "#000000",
        "field_bg": "#FFFFFF",
        "transparent": False,
        "inherit_main_theme": True,
        "image_layers": [],
        "control_style": "Fade Left to Right",
        "control_opacity": 82,
        "control_tail_opacity": 0,
        "control_fade_strength": 65,
        "header_color": "",
        "row_hover_color": "",
        "row_selected_color": "",
    },
    "dashboard_theme": {
        "background": "#FFFFFF",
        "text": "#000000",
        "field_bg": "#FFFFFF",
        "transparent": False,
        "inherit_main_theme": True,
        "image_layers": [],
        "control_style": "Fade Left to Right",
        "control_opacity": 82,
        "control_tail_opacity": 0,
        "control_fade_strength": 65,
        "header_color": "",
        "row_hover_color": "",
        "row_selected_color": "",
    },
    "app_icon_path": "",
    "quick_texts": [
        {
            "title": "Greeting",
            "tooltip": "Quick opening line",
            "text": "Hi there,",
            "action": "input_sequence",
            "open_target": "",
            "app_targets": "",
            "urls": "",
            "browser_path": "",
        },
        {
            "title": "Follow-up",
            "tooltip": "Ask for updates",
            "text": "Checking in on this when you have a moment.",
            "action": "input_sequence",
            "open_target": "",
            "app_targets": "",
            "urls": "",
            "browser_path": "",
        },
    ],
}

def deep_clone(value: Any) -> Any:
    return json.loads(json.dumps(value))

def deep_merge(defaults: Any, incoming: Any) -> Any:
    if isinstance(defaults, dict):
        out: dict[str, Any] = {}
        incoming_dict = incoming if isinstance(incoming, dict) else {}
        for key, default_value in defaults.items():
            out[key] = deep_merge(default_value, incoming_dict.get(key))
        for key, value in incoming_dict.items():
            if key not in out:
                out[key] = value
        return out
    if isinstance(defaults, list):
        if isinstance(incoming, list):
            return incoming
        return deep_clone(defaults)
    return defaults if incoming is None else incoming

def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))

def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(fallback)

def normalize_hex(color: str, fallback: str = "#FFFFFF") -> str:
    if not isinstance(color, str):
        return fallback
    value = color.strip().upper()
    if len(value) == 7 and value.startswith("#"):
        try:
            int(value[1:], 16)
            return value
        except ValueError:
            return fallback
    return fallback

def hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = normalize_hex(color)
    return int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)

def rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{int(clamp(r, 0, 255)):02X}{int(clamp(g, 0, 255)):02X}{int(clamp(b, 0, 255)):02X}"

def blend(color_a: str, color_b: str, ratio: float) -> str:
    ratio = clamp(ratio, 0.0, 1.0)
    ar, ag, ab = hex_to_rgb(color_a)
    br, bg, bb = hex_to_rgb(color_b)
    return rgb_to_hex(ar + (br - ar) * ratio, ag + (bg - ag) * ratio, ab + (bb - ab) * ratio)

def luminance(color: str) -> float:
    r, g, b = hex_to_rgb(color)

    def channel(v: int) -> float:
        x = v / 255.0
        return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4

    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)

def contrast_ratio(color_a: str, color_b: str) -> float:
    l1, l2 = luminance(color_a), luminance(color_b)
    hi, lo = (l1, l2) if l1 > l2 else (l2, l1)
    return (hi + 0.05) / (lo + 0.05)

def readable_text(background: str) -> str:
    white_ratio = contrast_ratio("#FFFFFF", background)
    black_ratio = contrast_ratio("#101418", background)
    return "#FFFFFF" if white_ratio >= black_ratio else "#101418"

def shift(color: str, amount: float) -> str:
    target = "#FFFFFF" if amount >= 0 else "#000000"
    return blend(color, target, abs(amount))

def rgba_css(color: str, alpha: float) -> str:
    r, g, b = hex_to_rgb(color)
    a = int(clamp(alpha, 0.0, 1.0) * 255)
    return f"rgba({r}, {g}, {b}, {a})"

def compute_palette(theme: dict[str, str]) -> dict[str, str]:
    primary = normalize_hex(theme.get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY)
    accent = normalize_hex(theme.get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT)
    surface = normalize_hex(theme.get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE)

    shell_overlay = shift(primary, -0.60)
    sidebar_overlay = shift(primary, -0.70)
    control_bg = blend(surface, "#1E2A34", 0.22)
    input_bg = blend(surface, "#FFFFFF", 0.08)
    nav_active = blend(accent, primary, 0.35)
    text_color = readable_text(control_bg)
    button_bg = blend(primary, accent, 0.30)

    return {
        "primary": primary,
        "accent": accent,
        "surface": surface,
        "shell_overlay": shell_overlay,
        "sidebar_overlay": sidebar_overlay,
        "label_text": text_color,
        "muted_text": blend(text_color, "#AAB7C2", 0.35),
        "control_bg": control_bg,
        "input_bg": input_bg,
        "button_bg": button_bg,
        "button_text": readable_text(button_bg),
        "nav_active": nav_active,
    }

def safe_layer_defaults(layer: dict[str, Any]) -> dict[str, Any]:
    visible_raw = layer.get("visible", True)
    if isinstance(visible_raw, str):
        visible_text = visible_raw.strip().lower()
        if visible_text in {"0", "false", "no", "off"}:
            visible_value = False
        elif visible_text in {"1", "true", "yes", "on"}:
            visible_value = True
        else:
            visible_value = True
    else:
        visible_value = bool(visible_raw)
    return {
        "image_path": layer.get("image_path", ""),
        "image_x": int(layer.get("image_x", 0)),
        "image_y": int(layer.get("image_y", 0)),
        "image_scale_mode": layer.get("image_scale_mode", "Fit"),
        "image_anchor": layer.get("image_anchor", "Center"),
        "image_scale_percent": int(layer.get("image_scale_percent", 100)),
        "image_opacity": float(clamp(float(layer.get("image_opacity", 1.0)), 0.0, 1.0)),
        "visible": visible_value,
        "name": layer.get("name") or Path(layer.get("image_path", "")).name or "Layer",
    }

def build_quick_shape_polygon(shape: str, w: int, h: int) -> QPolygon | None:
    if w <= 8 or h <= 8:
        return None

    if shape == "Diamond":
        return QPolygon(
            [
                QPoint(w // 2, 0),
                QPoint(w - 1, h // 2),
                QPoint(w // 2, h - 1),
                QPoint(0, h // 2),
            ]
        )

    if shape == "Hex":
        dx = max(8, int(w * 0.14))
        return QPolygon(
            [
                QPoint(dx, 0),
                QPoint(w - dx - 1, 0),
                QPoint(w - 1, h // 2),
                QPoint(w - dx - 1, h - 1),
                QPoint(dx, h - 1),
                QPoint(0, h // 2),
            ]
        )

    if shape == "Slant":
        dx = max(8, int(w * 0.12))
        return QPolygon(
            [
                QPoint(dx, 0),
                QPoint(w - 1, 0),
                QPoint(w - dx - 1, h - 1),
                QPoint(0, h - 1),
            ]
        )

    if shape == "CutCorner":
        cut = max(6, int(min(w, h) * 0.22))
        return QPolygon(
            [
                QPoint(cut, 0),
                QPoint(w - cut - 1, 0),
                QPoint(w - 1, cut),
                QPoint(w - 1, h - cut - 1),
                QPoint(w - cut - 1, h - 1),
                QPoint(cut, h - 1),
                QPoint(0, h - cut - 1),
                QPoint(0, cut),
            ]
        )

    if shape == "Trapezoid":
        top_inset = max(8, int(w * 0.18))
        return QPolygon(
            [
                QPoint(top_inset, 0),
                QPoint(w - top_inset - 1, 0),
                QPoint(w - 1, h - 1),
                QPoint(0, h - 1),
            ]
        )

    return None

class BackgroundCanvas(QWidget):
    def __init__(self, app_window: "QuickInputsWindow") -> None:
        super().__init__()
        self.app_window = app_window
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        self.app_window.paint_background(painter, self.rect())
        super().paintEvent(event)

class TitleBar(QWidget):
    def __init__(self, app_window: "QuickInputsWindow") -> None:
        super().__init__(app_window)
        self.app_window = app_window
        self._drag_offset: QPoint | None = None
        self.setFixedHeight(TITLEBAR_HEIGHT)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 4, 4, 4)
        layout.setSpacing(6)

        self.icon_label = QLabel()
        self.icon_label.setFixedSize(16, 16)

        self.title_label = QLabel(APP_TITLE)
        self.title_label.setObjectName("TitleText")

        self.min_button = QToolButton()
        self.min_button.setObjectName("TitleMinButton")
        self.min_button.setText("-")
        self.min_button.setFixedSize(26, 22)
        self.min_button.clicked.connect(self.app_window.showMinimized)

        self.close_button = QToolButton()
        self.close_button.setObjectName("TitleCloseButton")
        self.close_button.setText("x")
        self.close_button.setFixedSize(26, 22)
        self.close_button.clicked.connect(self.app_window.close)

        layout.addWidget(self.icon_label)
        layout.addWidget(self.title_label)
        layout.addStretch(1)
        layout.addWidget(self.min_button)
        layout.addWidget(self.close_button)

    def update_icon(self, icon: QIcon | None) -> None:
        if icon and not icon.isNull():
            source = icon.pixmap(64, 64)
            if source.isNull():
                source = icon.pixmap(16, 16)
        else:
            source = QIcon.fromTheme("applications-utilities").pixmap(64, 64)

        if source.isNull():
            self.icon_label.clear()
            return

        scaled = source.scaled(16, 16, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        canvas = QPixmap(16, 16)
        canvas.fill(Qt.GlobalColor.transparent)
        painter = QPainter(canvas)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        x = (16 - scaled.width()) // 2
        y = (16 - scaled.height()) // 2
        painter.drawPixmap(x, y, scaled)
        painter.end()
        self.icon_label.setPixmap(canvas)

    def mousePressEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_offset = event.globalPosition().toPoint() - self.app_window.frameGeometry().topLeft()
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if self._drag_offset and event.buttons() & Qt.MouseButton.LeftButton:
            self.app_window.move(event.globalPosition().toPoint() - self._drag_offset)
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        self._drag_offset = None
        super().mouseReleaseEvent(event)

class QuickButtonCard(QWidget):
    edit_requested = Signal(int)
    insert_requested = Signal(int)
    move_requested = Signal(int, int, int, bool)
    _icon_cache: dict[tuple[str, int], QIcon] = {}

    def __init__(self, index: int, title: str, tooltip: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.index = index
        self._shape_for_mask = "Soft"
        self._layout_mode = False
        self._drag_anchor: QPoint | None = None
        self._drag_origin = QPoint(0, 0)
        self._drag_active = False
        self._suppress_click_once = False
        self.main_button = QPushButton(title)
        self.main_button.setToolTip(tooltip or "")
        self.main_button.clicked.connect(self._on_main_button_clicked)
        self.main_button.installEventFilter(self)
        self.main_button.setCursor(Qt.CursorShape.PointingHandCursor)

        self.edit_button = QToolButton(self.main_button)
        self.edit_button.setText("")
        self.edit_button.setToolTip("Edit")
        self.edit_button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.edit_button.clicked.connect(lambda: self.edit_requested.emit(self.index))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        layout.addWidget(self.main_button)

    def _on_main_button_clicked(self) -> None:
        if self._layout_mode:
            self._suppress_click_once = False
            return
        if self._suppress_click_once:
            self._suppress_click_once = False
            return
        self.insert_requested.emit(self.index)

    def set_layout_mode(self, enabled: bool) -> None:
        self._layout_mode = bool(enabled)
        if self._layout_mode:
            self.main_button.setCursor(Qt.CursorShape.OpenHandCursor)
        else:
            self.main_button.setCursor(Qt.CursorShape.PointingHandCursor)
            self._drag_anchor = None
            self._drag_active = False
            self._suppress_click_once = False

    def eventFilter(self, watched, event) -> bool:  # noqa: N802
        if watched is self.main_button and self._layout_mode:
            et = event.type()
            if et == QEvent.Type.MouseButtonPress and event.button() == Qt.MouseButton.LeftButton:
                self._drag_anchor = event.globalPosition().toPoint()
                self._drag_origin = self.pos()
                self._drag_active = False
                self._suppress_click_once = False
                self.main_button.setCursor(Qt.CursorShape.ClosedHandCursor)
                return True

            if et == QEvent.Type.MouseMove and event.buttons() & Qt.MouseButton.LeftButton and self._drag_anchor is not None:
                delta = event.globalPosition().toPoint() - self._drag_anchor
                if not self._drag_active and delta.manhattanLength() < 3:
                    return True
                self._drag_active = True
                self._suppress_click_once = True
                new_pos = self._drag_origin + delta
                self.move_requested.emit(self.index, int(new_pos.x()), int(new_pos.y()), False)
                return True

            if et == QEvent.Type.MouseButtonRelease and event.button() == Qt.MouseButton.LeftButton and self._drag_anchor is not None:
                if self._drag_active:
                    delta = event.globalPosition().toPoint() - self._drag_anchor
                    new_pos = self._drag_origin + delta
                    self.move_requested.emit(self.index, int(new_pos.x()), int(new_pos.y()), True)
                self._drag_anchor = None
                self._drag_active = False
                self.main_button.setCursor(Qt.CursorShape.OpenHandCursor)
                return True

        return super().eventFilter(watched, event)

    def apply_visual_style(
        self,
        width: int,
        height: int,
        font_size: int,
        font_family: str,
        shape: str,
        button_opacity: float,
        palette: dict[str, str],
        action_type: str = "input_sequence",
    ) -> None:
        width = int(clamp(width, 90, 220))
        height = int(clamp(height, 35, 100))
        font_size = int(clamp(font_size, 8, 20))
        button_opacity = float(clamp(button_opacity, 0.15, 1.0))

        # Adjust base color based on action type for subtle distinction
        base_bg = palette['button_bg']
        if action_type == "open_url":
            # Shift toward accent color for web links
            base_bg = blend(palette['button_bg'], palette['accent'], 0.25)
        elif action_type == "open_app":
            # Shift toward primary color for apps
            base_bg = blend(palette['button_bg'], palette['primary'], 0.25)
        elif action_type in {"input_sequence", "macro_sequence", "paste_text"}:
            # Shift toward surface color for input sequences.
            base_bg = blend(palette['button_bg'], palette['surface'], 0.35)

        self.main_button.setMinimumSize(width, height)
        self.main_button.setMaximumSize(width, height)
        self.main_button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.setFixedSize(width, height)
        self._shape_for_mask = shape

        temp_window = self.window()
        style_provider = None
        if temp_window is not None and hasattr(temp_window, "_quick_button_stylesheet"):
            style_provider = temp_window
        elif temp_window is not None and hasattr(temp_window, "app_window") and hasattr(temp_window.app_window, "_quick_button_stylesheet"):
            style_provider = temp_window.app_window
        style_css = None
        if style_provider is not None:
            style_css = style_provider._quick_button_stylesheet(
                font_size,
                button_opacity,
                shape,
                font_family=font_family,
                padding="2px 20px 2px 8px",
                action_type=action_type,
            )
        if style_css is None:
            safe_family = str(font_family or "Segoe UI").replace("'", "\\'")
            style_css = (
                "QPushButton {"
                f"background-color: {rgba_css(base_bg, button_opacity)};"
                f"color: {palette['button_text']};"
                f"border: 1px solid {shift(base_bg, -0.35)};"
                "border-radius: 11px;"
                f"font-size: {font_size}px;"
                f"font-family: '{safe_family}';"
                "font-weight: 700;"
                "padding: 2px 20px 2px 8px;"
                "text-align: center;"
                "}"
            )
        self.main_button.setStyleSheet(style_css)
        # User-created quick buttons stay text-only; no action icons on button faces.
        self.main_button.setIcon(QIcon())

        icon_color = shift(palette["button_text"], 0.03)
        self.edit_button.setIcon(self._build_pencil_icon(icon_color, 14))
        self.edit_button.setIconSize(QSize(12, 12))
        self.edit_button.setStyleSheet(
            "QToolButton {"
            "background: rgba(0, 0, 0, 28);"
            "border: none;"
            "border-radius: 8px;"
            "padding: 0px;"
            "}"
            "QToolButton:hover { background: rgba(255, 255, 255, 52); }"
        )
        self.edit_button.setFixedSize(16, 16)
        self._apply_shape_edge_effect(shape, palette, width, height)
        self._position_edit_button()
        self._apply_shape_mask()

    def _apply_shape_edge_effect(self, shape: str, palette: dict[str, str], width: int, height: int) -> None:
        if self._build_shape_polygon(shape, width, height) is None:
            self.main_button.setGraphicsEffect(None)
            return
        effect = self.main_button.graphicsEffect()
        if not isinstance(effect, QGraphicsDropShadowEffect):
            effect = QGraphicsDropShadowEffect(self.main_button)
            self.main_button.setGraphicsEffect(effect)
        edge = QColor(shift(palette["button_bg"], 0.28))
        edge.setAlpha(110)
        effect.setColor(edge)
        effect.setOffset(0, 0)
        effect.setBlurRadius(5.0)

    @classmethod
    def _build_pencil_icon(cls, color_hex: str, size: int) -> QIcon:
        key = (color_hex, size)
        cached = cls._icon_cache.get(key)
        if cached is not None:
            return cached

        canvas_size = 64
        pixmap = QPixmap(canvas_size, canvas_size)
        pixmap.fill(Qt.GlobalColor.transparent)

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

        body_color = QColor(color_hex)
        body_pen = QPen(body_color)
        body_pen.setWidth(8)
        body_pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        body_pen.setJoinStyle(Qt.PenJoinStyle.RoundJoin)
        painter.setPen(body_pen)
        painter.drawLine(16, 48, 44, 20)

        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(body_color)
        painter.drawPolygon(
            QPolygon(
                [
                    QPoint(44, 20),
                    QPoint(54, 10),
                    QPoint(52, 24),
                ]
            )
        )

        eraser_color = QColor("#F2F5F8")
        eraser_color.setAlpha(230)
        painter.setBrush(eraser_color)
        painter.drawPolygon(
            QPolygon(
                [
                    QPoint(12, 52),
                    QPoint(18, 58),
                    QPoint(24, 52),
                    QPoint(18, 46),
                ]
            )
        )
        painter.end()

        icon = QIcon(pixmap)
        cls._icon_cache[key] = icon
        return icon

    def _position_edit_button(self) -> None:
        pad = 4
        x = max(0, self.main_button.width() - self.edit_button.width() - pad)
        self.edit_button.move(x, pad)
        self.edit_button.raise_()

    def _build_shape_polygon(self, shape: str, w: int, h: int) -> QPolygon | None:
        return build_quick_shape_polygon(shape, w, h)

    def _apply_shape_mask(self) -> None:
        w = self.main_button.width()
        h = self.main_button.height()
        polygon = self._build_shape_polygon(self._shape_for_mask, w, h)
        if polygon is None:
            self.main_button.clearMask()
            return
        self.main_button.setMask(QRegion(polygon))

    def resizeEvent(self, event) -> None:  # noqa: N802
        self._position_edit_button()
        self._apply_shape_mask()
        super().resizeEvent(event)

class QuickButtonCanvas(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._snap_x = 1
        self._snap_y = 1
        self._snap_enabled = False
        self._show_grid = False
        self._viewport_width = 300
        self._guide_v_lines: list[int] = []
        self._guide_h_lines: list[int] = []
        self._background_drawer: Callable[[QPainter, QRect], None] | None = None
        self._cards: dict[int, QuickButtonCard] = {}
        self._placeholder = QLabel("", self)
        self._placeholder.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self._placeholder.hide()
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet("background: transparent;")

    def set_background_drawer(self, drawer: Callable[[QPainter, QRect], None] | None) -> None:
        self._background_drawer = drawer
        self.update()

    def configure_grid(self, snap_x: int = 1, snap_y: int = 1, show_grid: bool = False, snap_enabled: bool = False) -> None:
        self._snap_x = max(1, int(snap_x))
        self._snap_y = max(1, int(snap_y))
        self._show_grid = bool(show_grid)
        self._snap_enabled = bool(snap_enabled)
        self.update()

    def set_alignment_guides(self, vertical_lines: list[int], horizontal_lines: list[int]) -> None:
        self._guide_v_lines = [int(v) for v in vertical_lines]
        self._guide_h_lines = [int(h) for h in horizontal_lines]
        self.update()

    def clear_alignment_guides(self) -> None:
        self._guide_v_lines = []
        self._guide_h_lines = []
        self.update()

    def set_viewport_width(self, width: int) -> None:
        self._viewport_width = max(120, int(width))
        self._refresh_canvas_size()

    def clear_cards(self) -> None:
        for card in list(self._cards.values()):
            card.setParent(None)
            card.deleteLater()
        self._cards.clear()
        self._placeholder.hide()
        self._refresh_canvas_size()

    def set_placeholder(self, text: str, color: str) -> None:
        self._placeholder.setText(text)
        self._placeholder.setStyleSheet(
            "QLabel {"
            f"color: {color};"
            "background: transparent;"
            "font-weight: 700;"
            "}"
        )
        self._placeholder.move(8, 8)
        self._placeholder.adjustSize()
        self._placeholder.show()
        self._refresh_canvas_size()

    def place_card(self, card: QuickButtonCard, x: int, y: int, snap: bool = False) -> tuple[int, int]:
        card.setParent(self)
        card.show()
        self._cards[card.index] = card
        snapped_x, snapped_y = self.snap_position(x, y, card.width(), card.height(), snap=snap)
        card.move(snapped_x, snapped_y)
        self._refresh_canvas_size()
        return snapped_x, snapped_y

    def snap_position(self, x: int, y: int, width: int, height: int, snap: bool = False) -> tuple[int, int]:
        snapped_x = int(x)
        snapped_y = int(y)
        do_snap = bool(snap or self._snap_enabled)
        if do_snap:
            sx = max(1, self._snap_x)
            sy = max(1, self._snap_y)
            snapped_x = int(round(float(snapped_x) / float(sx)) * sx)
            snapped_y = int(round(float(snapped_y) / float(sy)) * sy)
        max_x = max(0, self._viewport_width - max(1, int(width)))
        snapped_x = int(clamp(snapped_x, 0, max_x))
        snapped_y = max(0, snapped_y)
        return snapped_x, snapped_y

    def move_card(self, index: int, x: int, y: int, snap: bool = False) -> tuple[int, int]:
        card = self._cards.get(index)
        if card is None:
            return 0, 0
        snapped_x, snapped_y = self.snap_position(x, y, card.width(), card.height(), snap=snap)
        card.move(snapped_x, snapped_y)
        self._refresh_canvas_size()
        return snapped_x, snapped_y

    def card_geometry(self, index: int) -> QRect | None:
        card = self._cards.get(index)
        if card is None:
            return None
        return QRect(card.x(), card.y(), card.width(), card.height())

    def iter_card_geometries(self, exclude_index: int | None = None) -> list[QRect]:
        rects: list[QRect] = []
        for idx, card in self._cards.items():
            if exclude_index is not None and idx == exclude_index:
                continue
            rects.append(QRect(card.x(), card.y(), card.width(), card.height()))
        return rects

    def _refresh_canvas_size(self) -> None:
        width = max(120, self._viewport_width)
        max_bottom = 0
        for card in self._cards.values():
            max_bottom = max(max_bottom, card.y() + card.height())
        if self._placeholder.isVisible():
            max_bottom = max(max_bottom, self._placeholder.y() + self._placeholder.height())
        height = max(220, max_bottom + 12)
        self.resize(width, height)
        self.setMinimumSize(width, height)
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        if self._background_drawer is not None:
            self._background_drawer(painter, self.rect())
        if self._show_grid:
            grid_pen = QPen(QColor(255, 255, 255, 28))
            grid_pen.setWidth(1)
            painter.setPen(grid_pen)
            for x in range(0, self.width(), max(1, self._snap_x)):
                painter.drawLine(x, 0, x, self.height())
            for y in range(0, self.height(), max(1, self._snap_y)):
                painter.drawLine(0, y, self.width(), y)
        if self._guide_v_lines or self._guide_h_lines:
            guide_pen = QPen(QColor(255, 216, 76, 220))
            guide_pen.setWidth(1)
            painter.setPen(guide_pen)
            for x in self._guide_v_lines:
                painter.drawLine(int(x), 0, int(x), self.height())
            for y in self._guide_h_lines:
                painter.drawLine(0, int(y), self.width(), int(y))

class QuickRadialMenu(QDialog):
    action_requested = Signal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowFlags(Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint | Qt.WindowType.NoDropShadowWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setModal(False)
        self.setFixedSize(290, 290)

        self._center = QPoint(self.width() // 2, self.height() // 2)
        self._radius = 90
        self._button_size = QSize(92, 36)
        self._buttons: dict[str, QPushButton] = {}
        self._action_meta: list[tuple[str, str, str, int]] = [
            ("add", "Add", "add", 142),
            ("layout", "Layout", "pick", 94),
            ("new_tab", "New Tab", "new", 46),
            ("rename", "Rename", "pick", -2),
            ("remove", "Remove", "reset", -50),
        ]

        self.center_chip = QLabel("+", self)
        self.center_chip.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.center_chip.setGeometry(self._center.x() - 22, self._center.y() - 22, 44, 44)

        for action_key, label, action_role, _angle in self._action_meta:
            button = QPushButton(label, self)
            button.setProperty("actionRole", action_role)
            button.setProperty("radialAction", action_key)
            button.setCursor(Qt.CursorShape.PointingHandCursor)
            button.setFixedSize(self._button_size)
            button.clicked.connect(lambda _checked=False, key=action_key: self._emit_action(key))
            self._buttons[action_key] = button

        self._layout_buttons()
        self.apply_theme_styles(compute_palette(DEFAULT_CONFIG.get("theme", {})))

    def _layout_buttons(self) -> None:
        for action_key, _label, _role, angle_deg in self._action_meta:
            button = self._buttons[action_key]
            radians = math.radians(float(angle_deg))
            cx = self._center.x() + int(math.cos(radians) * self._radius)
            cy = self._center.y() - int(math.sin(radians) * self._radius)
            button.move(cx - (button.width() // 2), cy - (button.height() // 2))

    def _emit_action(self, action_key: str) -> None:
        self.hide()
        self.action_requested.emit(action_key)

    def set_action_enabled(self, action_key: str, enabled: bool) -> None:
        button = self._buttons.get(action_key)
        if button is not None:
            button.setEnabled(bool(enabled))

    def open_anchored_to(self, anchor_widget: QWidget) -> None:
        anchor_center = anchor_widget.mapToGlobal(anchor_widget.rect().center())
        top_left = QPoint(anchor_center.x() - (self.width() // 2), anchor_center.y() - (self.height() // 2))
        screen = QGuiApplication.screenAt(anchor_center)
        if screen is not None:
            avail = screen.availableGeometry()
            top_left.setX(int(clamp(top_left.x(), avail.left(), avail.right() - self.width())))
            top_left.setY(int(clamp(top_left.y(), avail.top(), avail.bottom() - self.height())))
        self.move(top_left)
        self.show()
        self.raise_()
        self.activateWindow()

    def apply_theme_styles(self, palette_data: dict[str, str]) -> None:
        accent = palette_data["accent"]
        control_bg = palette_data["control_bg"]
        label_text = palette_data["label_text"]
        base_btn = palette_data["button_bg"]
        reset_base = shift(palette_data["surface"], -0.45)
        self.setStyleSheet(
            "QPushButton {"
            f"background-color: {rgba_css(base_btn, 0.92)};"
            f"color: {readable_text(base_btn)};"
            f"border: 2px solid {shift(base_btn, -0.62)};"
            "border-radius: 18px;"
            "font-size: 11px;"
            "font-weight: 800;"
            "padding: 2px 8px;"
            "}"
            "QPushButton:hover {"
            f"background-color: {rgba_css(shift(base_btn, 0.08), 0.96)};"
            f"border: 2px solid {shift(base_btn, -0.72)};"
            "}"
            "QPushButton:pressed {"
            f"background-color: {rgba_css(shift(base_btn, -0.06), 0.98)};"
            "}"
            "QPushButton[actionRole='add'] {"
            f"background-color: {rgba_css(palette_data['primary'], 0.94)};"
            f"color: {readable_text(palette_data['primary'])};"
            f"border: 2px solid {shift(palette_data['primary'], -0.56)};"
            "}"
            "QPushButton[actionRole='pick'] {"
            f"background-color: {rgba_css(blend(palette_data['primary'], palette_data['surface'], 0.45), 0.94)};"
            f"color: {readable_text(blend(palette_data['primary'], palette_data['surface'], 0.45))};"
            f"border: 2px solid {shift(blend(palette_data['primary'], palette_data['surface'], 0.45), -0.56)};"
            "}"
            "QPushButton[actionRole='new'] {"
            f"background-color: {rgba_css(blend(palette_data['surface'], palette_data['primary'], 0.35), 0.94)};"
            f"color: {readable_text(blend(palette_data['surface'], palette_data['primary'], 0.35))};"
            f"border: 2px solid {shift(blend(palette_data['surface'], palette_data['primary'], 0.35), -0.56)};"
            "}"
            "QPushButton[actionRole='reset'] {"
            f"background-color: {rgba_css(reset_base, 0.94)};"
            f"color: {readable_text(reset_base)};"
            f"border: 2px solid {shift(reset_base, -0.56)};"
            "}"
            "QPushButton:disabled {"
            "background-color: rgba(80,80,80,160);"
            "color: rgba(230,230,230,140);"
            "border: 2px solid rgba(60,60,60,180);"
            "}"
            "QLabel {"
            f"background: {rgba_css(control_bg, 0.92)};"
            f"color: {readable_text(control_bg)};"
            f"border: 2px solid {shift(control_bg, -0.58)};"
            "border-radius: 22px;"
            "font-size: 20px;"
            "font-weight: 900;"
            "}"
        )
        self.center_chip.setText("+")
        self.center_chip.setToolTip("Quick actions")
        self.center_chip.setStyleSheet(
            "QLabel {"
            f"background: {rgba_css(accent, 0.95)};"
            f"color: {readable_text(accent)};"
            f"border: 2px solid {shift(accent, -0.52)};"
            "border-radius: 22px;"
            "font-size: 20px;"
            "font-weight: 900;"
            "}"
        )
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(0, 0, 0, 95))
        painter.drawEllipse(self._center, 106, 106)

        guide_pen = QPen(QColor(255, 255, 255, 55))
        guide_pen.setWidth(2)
        painter.setPen(guide_pen)
        for action_key, _label, _role, _angle in self._action_meta:
            button = self._buttons[action_key]
            button_center = button.geometry().center()
            painter.drawLine(self._center, button_center)
        super().paintEvent(event)

class QuickLayoutDialog(QDialog):
    def __init__(self, app_window: "QuickInputsWindow") -> None:
        super().__init__(app_window)
        self.app_window = app_window
        self.setWindowTitle("Arrange Quick Buttons")
        self.setMinimumSize(LAUNCH_WIDTH, LAUNCH_HEIGHT)
        self.setMaximumWidth(LAUNCH_WIDTH)
        self.resize(LAUNCH_WIDTH, LAUNCH_HEIGHT)
        self.setModal(False)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self.hint_label = QLabel("Drag freely. Hold Shift while dragging to snap to center/alignment guides.")
        self.hint_label.setWordWrap(True)
        layout.addWidget(self.hint_label)

        style_form = QFormLayout()
        style_form.setContentsMargins(0, 0, 0, 0)
        style_form.setSpacing(6)

        self.style_width_slider = QSlider(Qt.Orientation.Horizontal)
        self.style_width_slider.setRange(90, 220)
        self.style_width_value = QLabel("140")
        width_row = QHBoxLayout()
        width_row.addWidget(self.style_width_slider, 1)
        width_row.addWidget(self.style_width_value)
        width_wrap = QWidget()
        width_wrap.setLayout(width_row)

        self.style_height_slider = QSlider(Qt.Orientation.Horizontal)
        self.style_height_slider.setRange(35, 100)
        self.style_height_value = QLabel("40")
        height_row = QHBoxLayout()
        height_row.addWidget(self.style_height_slider, 1)
        height_row.addWidget(self.style_height_value)
        height_wrap = QWidget()
        height_wrap.setLayout(height_row)

        self.style_font_slider = QSlider(Qt.Orientation.Horizontal)
        self.style_font_slider.setRange(8, 20)
        self.style_font_value = QLabel("11")
        font_row = QHBoxLayout()
        font_row.addWidget(self.style_font_slider, 1)
        font_row.addWidget(self.style_font_value)
        font_wrap = QWidget()
        font_wrap.setLayout(font_row)

        self.style_font_family_combo = QFontComboBox()
        self.style_font_family_combo.setEditable(True)
        self.style_font_family_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.style_font_family_combo.setMaxVisibleItems(16)
        if self.style_font_family_combo.lineEdit() is not None:
            self.style_font_family_combo.lineEdit().setPlaceholderText("Search fonts...")

        self.style_shape_combo = QComboBox()
        self.style_shape_combo.addItems(
            [
                "Soft",
                "Bordered",
                "Block",
                "Pill",
                "Outline",
                "Glass",
                "Diamond",
                "Hex",
                "Slant",
                "Raised3D",
                "Bevel3D",
                "Ridge3D",
                "Neumorph",
                "Retro3D",
                "Neon3D",
            ]
        )

        self.style_opacity_slider = QSlider(Qt.Orientation.Horizontal)
        self.style_opacity_slider.setRange(20, 100)
        self.style_opacity_value = QLabel("1.00")
        opacity_row = QHBoxLayout()
        opacity_row.addWidget(self.style_opacity_slider, 1)
        opacity_row.addWidget(self.style_opacity_value)
        opacity_wrap = QWidget()
        opacity_wrap.setLayout(opacity_row)

        style_form.addRow("Width", width_wrap)
        style_form.addRow("Height", height_wrap)
        style_form.addRow("Font", font_wrap)
        style_form.addRow("Font Family", self.style_font_family_combo)
        style_form.addRow("Shape", self.style_shape_combo)
        style_form.addRow("Opacity", opacity_wrap)
        layout.addLayout(style_form)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(False)
        self.scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
        self.canvas = QuickButtonCanvas()
        self.canvas.setObjectName("QuickLayoutCanvas")
        self.canvas.configure_grid(show_grid=False, snap_enabled=False)
        self.canvas.set_background_drawer(self._paint_canvas_background)
        self.scroll.setWidget(self.canvas)
        layout.addWidget(self.scroll, 1)

        actions = QHBoxLayout()
        self.reset_button = QPushButton("Auto Layout")
        self.done_button = QPushButton("Done")
        self.reset_button.setProperty("actionRole", "pick")
        self.done_button.setProperty("actionRole", "save")
        actions.addWidget(self.reset_button)
        actions.addStretch(1)
        actions.addWidget(self.done_button)
        layout.addLayout(actions)

        self.scroll.viewport().installEventFilter(self)
        self.style_width_slider.valueChanged.connect(self._on_style_controls_changed)
        self.style_height_slider.valueChanged.connect(self._on_style_controls_changed)
        self.style_font_slider.valueChanged.connect(self._on_style_controls_changed)
        self.style_font_family_combo.currentFontChanged.connect(lambda _font: self._on_style_controls_changed())
        self.style_shape_combo.currentTextChanged.connect(self._on_style_controls_changed)
        self.style_opacity_slider.valueChanged.connect(self._on_style_controls_changed)
        self.reset_button.clicked.connect(self.reset_auto_layout)
        self.done_button.clicked.connect(self.close)
        self._sync_style_controls_from_config()
        self.apply_theme_styles()
        self.refresh_cards()

    def closeEvent(self, event) -> None:  # noqa: N802
        self.app_window.config.setdefault("popup_positions", {})["quick_layout"] = {
            "x": self.x(),
            "y": self.y(),
        }
        self.app_window.queue_save_config()
        self.app_window.refresh_quick_grid()
        super().closeEvent(event)

    def eventFilter(self, watched, event) -> bool:  # noqa: N802
        if watched is self.scroll.viewport() and event.type() == QEvent.Type.Resize:
            self._sync_canvas_viewport_width()
            return False
        return super().eventFilter(watched, event)

    def _reference_main_canvas_width(self) -> int:
        try:
            return max(120, int(self.app_window.quick_scroll.viewport().width()))
        except Exception:
            return max(120, int(self.scroll.viewport().width()))

    def _sync_canvas_viewport_width(self) -> None:
        popup_viewport_w = max(120, int(self.scroll.viewport().width()))
        main_w = self._reference_main_canvas_width()
        target_w = min(popup_viewport_w, main_w)
        self.canvas.set_viewport_width(target_w)

    def apply_theme_styles(self) -> None:
        p = self.app_window.palette_data
        bg = rgba_css(p["shell_overlay"], 0.92)
        border = shift(p["control_bg"], -0.30)
        button_disabled_bg = rgba_css(blend(p["control_bg"], p["button_bg"], 0.18), 0.90)
        button_disabled_text = rgba_css(p["label_text"], 0.48)
        button_disabled_border = rgba_css(shift(p["control_bg"], -0.18), 0.95)
        self.setStyleSheet(
            "QDialog {"
            f"background: {bg};"
            f"color: {p['label_text']};"
            "font-family: 'Segoe UI';"
            "font-size: 13px;"
            "}"
            "QLabel { background: transparent; color: inherit; font-weight: 700; }"
            "QScrollArea { background: transparent; border: none; }"
            "QComboBox, QLineEdit, QSpinBox {"
            f"background: {rgba_css(p['input_bg'], 0.86)};"
            f"border: 1px solid {shift(p['input_bg'], -0.38)};"
            "border-radius: 3px;"
            "padding: 2px 6px;"
            "}"
            "QSlider::groove:horizontal {"
            "background: rgba(0,0,0,90);"
            "height: 6px;"
            "border-radius: 3px;"
            "}"
            "QSlider::handle:horizontal {"
            f"background: {p['accent']};"
            "width: 12px;"
            "margin: -4px 0px;"
            "border-radius: 3px;"
            "}"
            "QPushButton {"
            f"background-color: {rgba_css(p['button_bg'], 1.0)};"
            f"color: {p['button_text']};"
            f"border: 1px solid {shift(p['button_bg'], -0.40)};"
            "border-radius: 4px;"
            "padding: 4px 10px;"
            "min-height: 30px;"
            "font-size: 11px;"
            "font-weight: 700;"
            "}"
            "QPushButton[actionRole='save'] {"
            f"background-color: {rgba_css(p['accent'], 1.0)};"
            f"color: {readable_text(p['accent'])};"
            f"border: 1px solid {shift(p['accent'], -0.42)};"
            "}"
            "QPushButton:disabled {"
            f"background-color: {button_disabled_bg};"
            f"color: {button_disabled_text};"
            f"border: 1px solid {button_disabled_border};"
            "}"
        )
        self.canvas.setStyleSheet(
            "QWidget#QuickLayoutCanvas {"
            "background: transparent;"
            f"border: 1px solid {border};"
            "border-radius: 4px;"
            "}"
        )
        self.canvas.update()

    def _sync_style_controls_from_config(self) -> None:
        self.style_width_slider.blockSignals(True)
        self.style_height_slider.blockSignals(True)
        self.style_font_slider.blockSignals(True)
        self.style_font_family_combo.blockSignals(True)
        self.style_shape_combo.blockSignals(True)
        self.style_opacity_slider.blockSignals(True)

        width = int(clamp(int(self.app_window.config.get("quick_button_width", 140)), 90, 220))
        height = int(clamp(int(self.app_window.config.get("quick_button_height", 40)), 35, 100))
        font_size = int(clamp(int(self.app_window.config.get("quick_button_font_size", 11)), 8, 20))
        font_family = str(self.app_window.config.get("quick_button_font_family", "Segoe UI"))
        shape = self.app_window.config.get("quick_button_shape", "Soft")
        if self.style_shape_combo.findText(shape) == -1:
            shape = "Soft"
        opacity = float(clamp(float(self.app_window.config.get("quick_button_opacity", 1.0)), 0.2, 1.0))

        self.style_width_slider.setValue(width)
        self.style_height_slider.setValue(height)
        self.style_font_slider.setValue(font_size)
        self.style_font_family_combo.setCurrentFont(QFont(font_family))
        self.style_shape_combo.setCurrentText(shape)
        self.style_opacity_slider.setValue(int(opacity * 100))

        self.style_width_slider.blockSignals(False)
        self.style_height_slider.blockSignals(False)
        self.style_font_slider.blockSignals(False)
        self.style_font_family_combo.blockSignals(False)
        self.style_shape_combo.blockSignals(False)
        self.style_opacity_slider.blockSignals(False)

        self.style_width_value.setText(str(width))
        self.style_height_value.setText(str(height))
        self.style_font_value.setText(str(font_size))
        self.style_opacity_value.setText(f"{opacity:.2f}")

    def _on_style_controls_changed(self) -> None:
        width = int(self.style_width_slider.value())
        height = int(self.style_height_slider.value())
        font_size = int(self.style_font_slider.value())
        font_family = str(self.style_font_family_combo.currentFont().family() or "Segoe UI")
        shape = self.style_shape_combo.currentText()
        opacity = float(clamp(self.style_opacity_slider.value() / 100.0, 0.2, 1.0))

        self.app_window.config["quick_button_width"] = width
        self.app_window.config["quick_button_height"] = height
        self.app_window.config["quick_button_font_size"] = font_size
        self.app_window.config["quick_button_font_family"] = font_family
        self.app_window.config["quick_button_shape"] = shape
        self.app_window.config["quick_button_opacity"] = opacity

        self.style_width_value.setText(str(width))
        self.style_height_value.setText(str(height))
        self.style_font_value.setText(str(font_size))
        self.style_opacity_value.setText(f"{opacity:.2f}")

        self.app_window._refresh_theme_preview_buttons()
        self.app_window.refresh_quick_grid()
        self.refresh_cards()
        self.app_window.queue_save_config()

    def _default_position(self, index: int, width: int, height: int) -> tuple[int, int]:
        return self.app_window._default_quick_position(index, width, height)

    def _paint_canvas_background(self, painter: QPainter, rect: QRect) -> None:
        base = QColor(self.app_window.palette_data["surface"])
        base.setAlpha(110)
        painter.fillRect(rect, base)

        surface_size = self.app_window.surface.size()
        if surface_size.width() <= 0 or surface_size.height() <= 0:
            return
        bg = self.app_window.render_background_pixmap(surface_size)
        if bg.isNull():
            return

        source_offset = self.app_window.quick_scroll.viewport().mapTo(self.app_window.surface, QPoint(0, 0))
        painter.save()
        painter.setClipRect(rect)
        painter.drawPixmap(-source_offset.x(), -source_offset.y(), bg)
        overlay = QColor(self.app_window.palette_data["shell_overlay"])
        overlay.setAlpha(24)
        painter.fillRect(rect, overlay)
        painter.restore()

    def refresh_cards(self) -> None:
        self._sync_style_controls_from_config()
        self.canvas.clear_cards()
        self.canvas.clear_alignment_guides()
        self.canvas.configure_grid(show_grid=False, snap_enabled=False)
        self._sync_canvas_viewport_width()
        can_persist_positions = bool(
            self.isVisible() and self.scroll.viewport().width() > 120 and self._reference_main_canvas_width() > 120
        )

        quick_texts = self.app_window._active_quick_texts()
        if not quick_texts:
            self.canvas.set_placeholder("No quick input buttons yet.", self.app_window.palette_data["muted_text"])
            return

        width = int(self.app_window.config.get("quick_button_width", 140))
        height = int(self.app_window.config.get("quick_button_height", 40))
        font_size = int(self.app_window.config.get("quick_button_font_size", 11))
        font_family = str(self.app_window.config.get("quick_button_font_family", "Segoe UI"))
        shape = self.app_window.config.get("quick_button_shape", "Soft")
        button_opacity = float(clamp(float(self.app_window.config.get("quick_button_opacity", 1.0)), 0.15, 1.0))
        updated_positions = False

        for idx, item in enumerate(quick_texts):
            card = QuickButtonCard(
                idx,
                str(item.get("title", "Untitled"))[:28],
                str(item.get("tooltip", "")),
                self.canvas,
            )
            action_type = self.app_window._quick_action_kind(item)
            card.apply_visual_style(width, height, font_size, font_family, shape, button_opacity, self.app_window.palette_data, action_type)
            card.set_layout_mode(True)
            card.edit_button.hide()
            card.move_requested.connect(self.on_card_move)
            raw_x = item.get("x")
            raw_y = item.get("y")
            if isinstance(raw_x, (int, float)) and isinstance(raw_y, (int, float)):
                pos_x, pos_y = int(raw_x), int(raw_y)
            else:
                pos_x, pos_y = self._default_position(idx, width, height)
                if can_persist_positions:
                    item["x"] = int(pos_x)
                    item["y"] = int(pos_y)
                    updated_positions = True

            px, py = self.canvas.place_card(card, pos_x, pos_y, snap=False)
            if (
                can_persist_positions
                and (
                    safe_int(item.get("x", -99999), -99999) != px
                    or safe_int(item.get("y", -99999), -99999) != py
                )
            ):
                item["x"] = int(px)
                item["y"] = int(py)
                updated_positions = True

        if updated_positions:
            self.app_window.queue_save_config()

    def _alignment_guides_for(self, moving_index: int, x: int, y: int, w: int, h: int) -> tuple[list[int], list[int]]:
        tolerance = 8
        moving_left = x
        moving_right = x + w - 1
        moving_cx = x + (w // 2)
        moving_top = y
        moving_bottom = y + h - 1
        moving_cy = y + (h // 2)

        v_lines: set[int] = set()
        h_lines: set[int] = set()

        center_x = self.canvas.width() // 2
        center_y = self.canvas.height() // 2
        if abs(moving_cx - center_x) <= tolerance:
            v_lines.add(center_x)
        if abs(moving_cy - center_y) <= tolerance:
            h_lines.add(center_y)

        for rect in self.canvas.iter_card_geometries(exclude_index=moving_index):
            other_left = rect.left()
            other_right = rect.right()
            other_cx = rect.left() + (rect.width() // 2)
            other_top = rect.top()
            other_bottom = rect.bottom()
            other_cy = rect.top() + (rect.height() // 2)

            for moving_anchor in (moving_left, moving_cx, moving_right):
                for other_anchor in (other_left, other_cx, other_right):
                    if abs(moving_anchor - other_anchor) <= tolerance:
                        v_lines.add(int(other_anchor))
            for moving_anchor in (moving_top, moving_cy, moving_bottom):
                for other_anchor in (other_top, other_cy, other_bottom):
                    if abs(moving_anchor - other_anchor) <= tolerance:
                        h_lines.add(int(other_anchor))

        return sorted(v_lines), sorted(h_lines)

    @staticmethod
    def _snap_axis_to_lines(start: int, size: int, guide_lines: list[int], tolerance: int = 12) -> int:
        if not guide_lines:
            return start
        anchors = [start, start + (size // 2), start + size - 1]
        best_delta: int | None = None
        for anchor in anchors:
            for line in guide_lines:
                delta = int(line - anchor)
                if abs(delta) > tolerance:
                    continue
                if best_delta is None or abs(delta) < abs(best_delta):
                    best_delta = delta
        if best_delta is None:
            return start
        return start + best_delta

    def on_card_move(self, index: int, x: int, y: int, finished: bool) -> None:
        gx, gy = self.canvas.move_card(index, x, y, snap=False)
        card_rect = self.canvas.card_geometry(index)
        if card_rect is not None:
            v_lines, h_lines = self._alignment_guides_for(index, gx, gy, card_rect.width(), card_rect.height())
            if bool(QApplication.keyboardModifiers() & Qt.KeyboardModifier.ShiftModifier):
                snapped_x = self._snap_axis_to_lines(gx, card_rect.width(), v_lines)
                snapped_y = self._snap_axis_to_lines(gy, card_rect.height(), h_lines)
                if snapped_x != gx or snapped_y != gy:
                    gx, gy = self.canvas.move_card(index, snapped_x, snapped_y, snap=False)
                    card_rect = self.canvas.card_geometry(index)
                    if card_rect is not None:
                        v_lines, h_lines = self._alignment_guides_for(index, gx, gy, card_rect.width(), card_rect.height())
            self.canvas.set_alignment_guides(v_lines, h_lines)
        if finished:
            self.canvas.clear_alignment_guides()

        quick_texts = self.app_window._active_quick_texts()
        if 0 <= index < len(quick_texts):
            entry = quick_texts[index]
            entry["x"] = int(gx)
            entry["y"] = int(gy)
            if finished:
                self.app_window.queue_save_config()
                self.app_window.refresh_quick_grid()

    def reset_auto_layout(self) -> None:
        quick_texts = self.app_window._active_quick_texts()
        width = int(self.app_window.config.get("quick_button_width", 140))
        height = int(self.app_window.config.get("quick_button_height", 40))
        for idx, entry in enumerate(quick_texts):
            x, y = self._default_position(idx, width, height)
            entry["x"] = int(x)
            entry["y"] = int(y)
        self.app_window.queue_save_config()
        self.refresh_cards()
        self.app_window.refresh_quick_grid()

class ImageLayerPreview(QWidget):
    layer_changed = Signal(dict)

    def __init__(self, app_window: "QuickInputsWindow", get_layer: Callable[[], dict[str, Any] | None], kind: str = "main") -> None:
        super().__init__()
        self.app_window = app_window
        self.get_layer = get_layer
        self.kind = kind
        self._dragging = False
        self._drag_start = QPointF(0.0, 0.0)
        self._layer_start = QPointF(0.0, 0.0)
        self._drag_scale = 1.0
        self.setMinimumSize(260, 190)

    def _virtual_size(self) -> QSize:
        if self.kind == "main":
            size = self.app_window.surface.size()
            if size.width() <= 0 or size.height() <= 0:
                return QSize(LAUNCH_WIDTH, LAUNCH_HEIGHT)
            return size

        if self.kind == "agent":
            active_agent = getattr(self.app_window, "active_agent_window", None)
            if active_agent is not None and active_agent.isVisible():
                size = active_agent.size()
                if size.width() > 0 and size.height() > 0:
                    return size
            return QSize(460, 380)

        if self.kind == "qa":
            active_qa = getattr(self.app_window, "active_qa_window", None)
            if active_qa is not None and active_qa.isVisible():
                size = active_qa.size()
                if size.width() > 0 and size.height() > 0:
                    return size
            return QSize(500, 420)
        
        if self.kind == "dashboard":
            active_dashboard = getattr(self.app_window, "depot_dashboard_dialog", None)
            if active_dashboard is not None and active_dashboard.isVisible():
                size = active_dashboard.size()
                if size.width() > 0 and size.height() > 0:
                    return size
            return QSize(780, 420)

        if self.kind == "admin":
            active_admin = getattr(self.app_window, "admin_dialog", None)
            if active_admin is not None and active_admin.isVisible():
                size = active_admin.size()
                if size.width() > 0 and size.height() > 0:
                    return size
            return QSize(620, 500)

        return QSize(LAUNCH_WIDTH, LAUNCH_HEIGHT)

    def _mapping(self) -> tuple[QSize, QRectF, float]:
        virtual_size = self._virtual_size()
        outer = QRectF(self.rect())
        if virtual_size.width() <= 0 or virtual_size.height() <= 0 or outer.width() <= 0 or outer.height() <= 0:
            return virtual_size, outer, 1.0

        scale = min(outer.width() / virtual_size.width(), outer.height() / virtual_size.height())
        draw_w = virtual_size.width() * scale
        draw_h = virtual_size.height() * scale
        draw_rect = QRectF(
            outer.left() + (outer.width() - draw_w) / 2.0,
            outer.top() + (outer.height() - draw_h) / 2.0,
            draw_w,
            draw_h,
        )
        return virtual_size, draw_rect, scale

    @staticmethod
    def _virtual_rect_to_preview(rect: QRectF, preview_rect: QRectF, scale: float) -> QRectF:
        if scale <= 0:
            return QRectF()
        return QRectF(
            preview_rect.left() + rect.left() * scale,
            preview_rect.top() + rect.top() * scale,
            rect.width() * scale,
            rect.height() * scale,
        )

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

        virtual_size, draw_rect, scale = self._mapping()
        frame_tint = QColor(self.app_window.palette_data["shell_overlay"])
        frame_tint.setAlpha(72)
        painter.fillRect(self.rect(), frame_tint)
        bg = self.app_window.render_background_pixmap(virtual_size, kind=self.kind)
        if not bg.isNull():
            painter.drawPixmap(draw_rect.toRect(), bg)

        layer = self.get_layer()
        if not layer:
            return

        render_info = self.app_window.compute_layer_render(layer, virtual_size)
        if not render_info:
            return
        layer_rect = self._virtual_rect_to_preview(render_info.rect, draw_rect, scale)

        pen = QPen(QColor("#FFFFFF"))
        pen.setWidth(2)
        painter.setPen(pen)
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawRect(layer_rect)

    def mousePressEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        layer = self.get_layer()
        if not layer:
            return

        virtual_size, draw_rect, scale = self._mapping()
        info = self.app_window.compute_layer_render(layer, virtual_size)
        if not info:
            return
        layer_rect = self._virtual_rect_to_preview(info.rect, draw_rect, scale)

        if event.button() == Qt.MouseButton.LeftButton and layer_rect.contains(event.position()):
            self._dragging = True
            self._drag_start = event.position()
            self._layer_start = QPointF(float(layer.get("image_x", 0)), float(layer.get("image_y", 0)))
            self._drag_scale = scale if scale > 0 else 1.0
            self.setCursor(Qt.CursorShape.ClosedHandCursor)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if not self._dragging:
            return

        layer = self.get_layer()
        if not layer:
            return

        delta = event.position() - self._drag_start
        dx = delta.x() / self._drag_scale
        dy = delta.y() / self._drag_scale
        layer["image_x"] = int(round(self._layer_start.x() + dx))
        layer["image_y"] = int(round(self._layer_start.y() + dy))
        self.layer_changed.emit(layer)
        self.update()

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if event.button() == Qt.MouseButton.LeftButton:
            self._dragging = False
            self.setCursor(Qt.CursorShape.ArrowCursor)

class ImageLayersDialog(QDialog):
    def __init__(self, app_window: "QuickInputsWindow", kind: str = "main") -> None:
        super().__init__(app_window)
        self.app_window = app_window
        self.kind = kind
        title = "Image Layers - Flowgrid" if kind == "main" else f"Image Layers - {kind.title()}"
        self.setWindowTitle(title)
        self.setMinimumSize(500, 380)
        self.setModal(False)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        left = QVBoxLayout()
        self.layer_list = QListWidget()
        left.addWidget(self.layer_list, 1)

        row_buttons = QHBoxLayout()
        self.add_button = QPushButton("Add")
        self.remove_button = QPushButton("Remove")
        self.up_button = QPushButton("Up")
        self.down_button = QPushButton("Down")
        self.add_button.setProperty("actionRole", "add")
        self.remove_button.setProperty("actionRole", "reset")
        self.up_button.setProperty("actionRole", "pick")
        self.down_button.setProperty("actionRole", "pick")
        row_buttons.addWidget(self.add_button)
        row_buttons.addWidget(self.remove_button)
        row_buttons.addWidget(self.up_button)
        row_buttons.addWidget(self.down_button)
        left.addLayout(row_buttons)

        self.visible_check = QCheckBox("Visible")
        left.addWidget(self.visible_check)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(6)

        self.x_spin = QSpinBox()
        self.x_spin.setRange(-3000, 3000)
        self.y_spin = QSpinBox()
        self.y_spin.setRange(-3000, 3000)

        self.scale_mode = QComboBox()
        self.scale_mode.addItems(["Fill", "Fit", "Stretch", "Place"])

        self.anchor_combo = QComboBox()
        self.anchor_combo.addItems(["TopLeft", "Top", "TopRight", "Left", "Center", "Right", "BottomLeft", "Bottom", "BottomRight"])

        self.scale_spin = QSpinBox()
        self.scale_spin.setRange(10, 400)

        self.opacity_slider = QSlider(Qt.Orientation.Horizontal)
        self.opacity_slider.setRange(0, 100)

        form.addRow("X", self.x_spin)
        form.addRow("Y", self.y_spin)
        form.addRow("Scale", self.scale_mode)
        form.addRow("Anchor", self.anchor_combo)
        form.addRow("Scale %", self.scale_spin)
        form.addRow("Opacity", self.opacity_slider)

        left.addLayout(form)
        layout.addLayout(left, 0)

        right = QVBoxLayout()
        self.preview = ImageLayerPreview(app_window, self.current_layer, kind=self.kind)
        right.addWidget(self.preview, 1)

        hint = QLabel("Tip: drag the highlighted image in preview to reposition it.")
        hint.setWordWrap(True)
        right.addWidget(hint)

        layout.addLayout(right, 1)

        self.layer_list.currentRowChanged.connect(self._on_layer_selected)
        self.add_button.clicked.connect(self._add_image)
        self.remove_button.clicked.connect(self._remove_selected)
        self.up_button.clicked.connect(lambda: self._move_selected(-1))
        self.down_button.clicked.connect(lambda: self._move_selected(1))

        self.visible_check.toggled.connect(lambda value: self._update_layer_field("visible", value))
        self.x_spin.valueChanged.connect(lambda value: self._update_layer_field("image_x", value))
        self.y_spin.valueChanged.connect(lambda value: self._update_layer_field("image_y", value))
        self.scale_mode.currentTextChanged.connect(lambda value: self._update_layer_field("image_scale_mode", value))
        self.anchor_combo.currentTextChanged.connect(lambda value: self._update_layer_field("image_anchor", value))
        self.scale_spin.valueChanged.connect(lambda value: self._update_layer_field("image_scale_percent", value))
        self.opacity_slider.valueChanged.connect(lambda value: self._update_layer_field("image_opacity", value / 100.0))

        self.preview.layer_changed.connect(self._on_layer_dragged)
        self.apply_theme_styles()
        self.refresh_list()

    def apply_theme_styles(self) -> None:
        p = self.app_window.palette_data
        dialog_bg = rgba_css(p["shell_overlay"], 0.92)
        list_bg = rgba_css(p["input_bg"], 0.42)
        input_bg = rgba_css(p["input_bg"], 0.86)
        border = shift(p["control_bg"], -0.30)
        field_border = shift(p["input_bg"], -0.38)
        button_bg = rgba_css(p["button_bg"], 1.0)
        button_hover = rgba_css(shift(p["button_bg"], 0.08), 1.0)
        button_disabled_bg = rgba_css(blend(p["control_bg"], p["button_bg"], 0.18), 0.90)
        button_disabled_text = rgba_css(p["label_text"], 0.48)
        button_disabled_border = rgba_css(shift(p["control_bg"], -0.18), 0.95)
        self.setStyleSheet(
            "QDialog {"
            f"background: {dialog_bg};"
            f"color: {p['label_text']};"
            "font-family: 'Segoe UI';"
            "font-size: 13px;"
            "}"
            "QLabel { background: transparent; color: inherit; font-weight: 700; }"
            "QListWidget {"
            f"background: {list_bg};"
            f"border: 1px solid {field_border};"
            "border-radius: 4px;"
            "padding: 4px;"
            "}"
            "QLineEdit, QTextEdit, QSpinBox, QComboBox {"
            f"background: {input_bg};"
            f"border: 1px solid {field_border};"
            "border-radius: 3px;"
            "padding: 2px 6px;"
            "}"
            "QCheckBox { background: transparent; spacing: 8px; font-weight: 700; }"
            "QPushButton {"
            f"background: {button_bg};"
            f"color: {p['button_text']};"
            f"border: 1px solid {shift(p['button_bg'], -0.40)};"
            "border-radius: 4px;"
            "padding: 4px 10px;"
            "min-height: 28px;"
            "font-size: 11px;"
            "font-weight: 700;"
            "}"
            f"QPushButton:hover {{ background: {button_hover}; }}"
            "QPushButton[actionRole='add'] {"
            f"background-color: {rgba_css(p['primary'], 1.0)};"
            f"color: {readable_text(p['primary'])};"
            f"border: 1px solid {shift(p['primary'], -0.42)};"
            "}"
            "QPushButton[actionRole='reset'] {"
            f"background-color: {rgba_css(p['accent'], 1.0)};"
            f"color: {readable_text(p['accent'])};"
            f"border: 1px solid {shift(p['accent'], -0.42)};"
            "}"
            "QPushButton:disabled {"
            f"background-color: {button_disabled_bg};"
            f"color: {button_disabled_text};"
            f"border: 1px solid {button_disabled_border};"
            "}"
        )
        self.preview.setStyleSheet(
            "QWidget {"
            "background: transparent;"
            f"border: 1px solid {border};"
            "border-radius: 4px;"
            "}"
        )
        self.preview.update()

    def _layers_key(self) -> str:
        if self.kind == "main":
            return "theme_image_layers"
        else:
            return f"{self.kind}_theme"

    def _popup_uses_inherited_layers(self, theme: dict[str, Any]) -> bool:
        if self.kind == "main":
            return False
        if not isinstance(theme, dict):
            return True
        if bool(theme.get("inherit_main_theme", False)):
            return True
        if self.app_window._looks_like_unconfigured_popup_theme(theme):
            return True
        return False

    def _materialize_popup_layers_for_edit(self) -> None:
        if self.kind == "main":
            return
        key = self._layers_key()
        theme = self.app_window.config.setdefault(key, {})
        if not isinstance(theme, dict):
            theme = {}
            self.app_window.config[key] = theme
        if not self._popup_uses_inherited_layers(theme):
            return
        effective_layers = self._get_layers()
        theme["image_layers"] = [
            safe_layer_defaults(layer) for layer in effective_layers if isinstance(layer, dict)
        ]
        theme["inherit_main_theme"] = False

    def _get_layers(self) -> list[dict[str, Any]]:
        key = self._layers_key()
        if self.kind == "main":
            return self.app_window.config.get(key, [])
        else:
            theme = self.app_window.config.get(key, {})
            if self._popup_uses_inherited_layers(theme if isinstance(theme, dict) else {}):
                resolved = self.app_window._resolved_popup_theme(self.kind)
                inherited_layers = resolved.get("image_layers", [])
                if isinstance(inherited_layers, list):
                    return [
                        safe_layer_defaults(layer)
                        for layer in inherited_layers
                        if isinstance(layer, dict)
                    ]
                return []
            if isinstance(theme, dict):
                raw_layers = theme.get("image_layers", [])
                if isinstance(raw_layers, list):
                    return raw_layers
            return []

    def _set_layers(self, layers: list[dict[str, Any]]) -> None:
        key = self._layers_key()
        if self.kind == "main":
            self.app_window.config[key] = layers
        else:
            theme = self.app_window.config.setdefault(key, {})
            if not isinstance(theme, dict):
                theme = {}
                self.app_window.config[key] = theme
            theme["image_layers"] = layers
            theme["inherit_main_theme"] = False

    def closeEvent(self, event) -> None:  # noqa: N802
        popup_positions = self.app_window.config.setdefault("popup_positions", {})
        if self.kind == "main":
            popup_positions["image_layers"] = {"x": self.x(), "y": self.y()}
        else:
            popup_positions[f"image_layers_{self.kind}"] = {"x": self.x(), "y": self.y()}
        self.app_window.queue_save_config()
        super().closeEvent(event)

    def refresh_list(self) -> None:
        current = self.layer_list.currentRow()
        self.layer_list.blockSignals(True)
        self.layer_list.clear()
        for layer in self._get_layers():
            item = QListWidgetItem(layer.get("name") or "Layer")
            self.layer_list.addItem(item)
        self.layer_list.blockSignals(False)

        if self.layer_list.count() == 0:
            self._load_layer_to_controls(None)
            return

        if current < 0:
            current = 0
        current = int(clamp(current, 0, self.layer_list.count() - 1))
        self.layer_list.setCurrentRow(current)

    def current_layer(self) -> dict[str, Any] | None:
        row = self.layer_list.currentRow()
        layers = self._get_layers()
        if row < 0 or row >= len(layers):
            return None
        return layers[row]

    def _on_layer_selected(self, _row: int) -> None:
        self._load_layer_to_controls(self.current_layer())

    def _load_layer_to_controls(self, layer: dict[str, Any] | None) -> None:
        controls: list[QWidget] = [
            self.visible_check,
            self.x_spin,
            self.y_spin,
            self.scale_mode,
            self.anchor_combo,
            self.scale_spin,
            self.opacity_slider,
            self.remove_button,
            self.up_button,
            self.down_button,
        ]
        enabled = layer is not None
        for control in controls:
            control.setEnabled(enabled)

        if not layer:
            self.preview.update()
            return

        self.visible_check.blockSignals(True)
        self.x_spin.blockSignals(True)
        self.y_spin.blockSignals(True)
        self.scale_mode.blockSignals(True)
        self.anchor_combo.blockSignals(True)
        self.scale_spin.blockSignals(True)
        self.opacity_slider.blockSignals(True)

        self.visible_check.setChecked(bool(layer.get("visible", True)))
        self.x_spin.setValue(int(layer.get("image_x", 0)))
        self.y_spin.setValue(int(layer.get("image_y", 0)))
        self.scale_mode.setCurrentText(layer.get("image_scale_mode", "Fit"))
        self.anchor_combo.setCurrentText(layer.get("image_anchor", "Center"))
        self.scale_spin.setValue(int(layer.get("image_scale_percent", 100)))
        self.opacity_slider.setValue(int(float(layer.get("image_opacity", 1.0)) * 100))

        self.visible_check.blockSignals(False)
        self.x_spin.blockSignals(False)
        self.y_spin.blockSignals(False)
        self.scale_mode.blockSignals(False)
        self.anchor_combo.blockSignals(False)
        self.scale_spin.blockSignals(False)
        self.opacity_slider.blockSignals(False)

        self.preview.update()

    def _add_image(self) -> None:
        files, _ = show_flowgrid_themed_open_file_names(
            self,
            self.app_window,
            self.kind,
            "Add Image Layers",
            str(Path.home()),
            "Images (*.png *.jpg *.jpeg *.bmp *.gif *.webp);;All Files (*.*)",
        )
        if not files:
            return

        for file_path in files:
            layer = safe_layer_defaults(
                {
                    "image_path": file_path,
                    "name": Path(file_path).name,
                }
            )
            layers = self._get_layers()
            layers.append(layer)
            self._set_layers(layers)

        self.app_window.mark_background_dirty()
        self.app_window.queue_save_config()
        self.refresh_list()
        self.layer_list.setCurrentRow(self.layer_list.count() - 1)
        self.app_window.refresh_all_views()

    def _remove_selected(self) -> None:
        row = self.layer_list.currentRow()
        if row < 0:
            return

        layers = self._get_layers()
        if 0 <= row < len(layers):
            layers.pop(row)
            self._set_layers(layers)
            self.app_window.mark_background_dirty()
            self.app_window.queue_save_config()
            self.refresh_list()
            self.app_window.refresh_all_views()

    def _move_selected(self, direction: int) -> None:
        row = self.layer_list.currentRow()
        layers = self._get_layers()
        new_index = row + direction
        if row < 0 or new_index < 0 or new_index >= len(layers):
            return

        layers[row], layers[new_index] = layers[new_index], layers[row]
        self._set_layers(layers)
        self.refresh_list()
        self.layer_list.setCurrentRow(new_index)
        self.app_window.mark_background_dirty()
        self.app_window.queue_save_config()
        self.app_window.refresh_all_views()

    def _update_layer_field(self, field: str, value: Any) -> None:
        self._materialize_popup_layers_for_edit()
        layer = self.current_layer()
        if not layer:
            return

        layer[field] = value
        self.app_window.mark_background_dirty()
        self.app_window.queue_save_config()
        self.preview.update()
        self.app_window.refresh_all_views()

    def _on_layer_dragged(self, layer: dict[str, Any]) -> None:
        self._materialize_popup_layers_for_edit()
        editable_layer = self.current_layer()
        if editable_layer is not None:
            editable_layer["image_x"] = int(layer.get("image_x", 0))
            editable_layer["image_y"] = int(layer.get("image_y", 0))
            layer = editable_layer
        self.x_spin.blockSignals(True)
        self.y_spin.blockSignals(True)
        self.x_spin.setValue(int(layer.get("image_x", 0)))
        self.y_spin.setValue(int(layer.get("image_y", 0)))
        self.x_spin.blockSignals(False)
        self.y_spin.blockSignals(False)

        self.app_window.mark_background_dirty()
        self.app_window.queue_save_config()
        self.app_window.refresh_all_views()

__all__ = [
    "BackgroundCanvas",
    "DEFAULT_CONFIG",
    "DEFAULT_THEME_ACCENT",
    "DEFAULT_THEME_PRESETS",
    "DEFAULT_THEME_PRIMARY",
    "DEFAULT_THEME_SURFACE",
    "ImageLayerPreview",
    "ImageLayersDialog",
    "LAUNCH_HEIGHT",
    "LAUNCH_WIDTH",
    "LEGACY_DEFAULT_THEME_ACCENT",
    "LEGACY_DEFAULT_THEME_PRIMARY",
    "LEGACY_DEFAULT_THEME_SURFACE",
    "PREVIOUS_THEME_PRESETS",
    "QuickButtonCanvas",
    "QuickButtonCard",
    "QuickLayoutDialog",
    "QuickRadialMenu",
    "SHIFT_CONTEXT_SCRIPT_LAUNCHERS",
    "SIDEBAR_WIDTH",
    "TITLEBAR_HEIGHT",
    "TitleBar",
    "build_quick_shape_polygon",
    "compute_palette",
    "deep_clone",
    "deep_merge",
    "safe_layer_defaults",
]
