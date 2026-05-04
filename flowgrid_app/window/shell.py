from __future__ import annotations

import ctypes
import json
import math
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from ctypes import wintypes
from pathlib import Path
from typing import Any

from PySide6.QtCore import (
    QByteArray,
    QBuffer,
    QDate,
    QEvent,
    QEasingCurve,
    QIODevice,
    QMimeData,
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

from flowgrid_app import AppContext, PermissionService, RuntimeOptions, UserRepository, WindowManager
from flowgrid_app.depot_async import DepotAsyncLoadCoordinator
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.icon_io import (
    DEFAULT_WINDOW_ICON_FILENAME,
    _build_smoothed_qicon,
    _resolve_active_app_icon_path,
)
from flowgrid_app.installer import _preferred_gui_python_executable, _sync_desktop_shortcut
from flowgrid_app.paths import (
    APP_TITLE,
    ASSETS_DIR_NAME,
    ASSET_UI_ICON_COMPAT_DIR_NAME,
    CONFIG_FILENAME,
    FLOWGRID_ICON_PACK_DIR_NAME,
    _data_file_path,
    _get_local_updater_path,
    _local_data_root,
    _migrate_legacy_agent_icons,
    _resolve_data_root,
    _shared_workflow_db_path,
)
from flowgrid_app.runtime_logging import _runtime_log_event, detect_current_user_id
from flowgrid_app.render_types import LayerRenderInfo
from flowgrid_app.update_manager import check_for_updates, current_install_status, sync_shared_assets
from flowgrid_app.workflow_core import DepotDB, DepotRefreshCoordinator, DepotTracker

from .agent import DepotAgentWindow
from .operations import DepotAdminDialog, DepotDashboardDialog
from .popup_support import (
    FlowgridThemedDialog,
    _drag_target_widget,
    _ensure_shell_window_available,
    _is_drag_blocked_widget,
    _visible_flowgrid_shell_window,
    configure_flowgrid_shell_factory,
    mark_flowgrid_shell_window,
    restore_flowgrid_popup_position,
    show_flowgrid_themed_color,
    show_flowgrid_themed_existing_directory,
    show_flowgrid_themed_input_int,
    show_flowgrid_themed_input_item,
    show_flowgrid_themed_input_text,
    show_flowgrid_themed_message,
    show_flowgrid_themed_open_file_name,
    show_flowgrid_themed_open_file_names,
)
from .qa_qcs import DepotQAWindow
from .quick_designer import (
    BackgroundCanvas,
    DEFAULT_CONFIG,
    DEFAULT_THEME_ACCENT,
    DEFAULT_THEME_PRESETS,
    DEFAULT_THEME_PRIMARY,
    DEFAULT_THEME_SURFACE,
    ImageLayersDialog,
    LAUNCH_HEIGHT,
    LAUNCH_WIDTH,
    LEGACY_DEFAULT_THEME_ACCENT,
    LEGACY_DEFAULT_THEME_PRIMARY,
    LEGACY_DEFAULT_THEME_SURFACE,
    PREVIOUS_THEME_PRESETS,
    QuickButtonCard,
    QuickButtonCanvas,
    QuickLayoutDialog,
    QuickRadialMenu,
    SHIFT_CONTEXT_SCRIPT_LAUNCHERS,
    SIDEBAR_WIDTH,
    TitleBar,
    blend,
    build_quick_shape_polygon,
    clamp,
    compute_palette,
    contrast_ratio,
    deep_clone,
    deep_merge,
    normalize_hex,
    readable_text,
    rgba_css,
    safe_int,
    safe_layer_defaults,
    shift,
)
from .constants import DEPOT_RECENT_VIEW_TTL_MS, DEPOT_SEARCH_REFRESH_DEBOUNCE_MS, DEPOT_VIEW_TTL_MS

_RUNTIME_ESCALATED_EVENTS: set[str] = set()
VK_CONTROL = 0x11
VK_V = 0x56
KEYEVENTF_KEYUP = 0x0002
SW_RESTORE = 9
VK_TAB = 0x09
VK_ENTER = 0x0D
VK_ESCAPE = 0x1B
VK_SPACE = 0x20
VK_SHIFT = 0x10
VK_ALT = 0x12
VK_BACKSPACE = 0x08
VK_DELETE = 0x2E
VK_HOME = 0x24
VK_END = 0x23
VK_PAGE_UP = 0x21
VK_PAGE_DOWN = 0x22
VK_LEFT = 0x25
VK_RIGHT = 0x27
VK_UP = 0x26
VK_DOWN = 0x28
VK_RETURN = 0x0D
KEY_CODES: dict[str, int] = {
    "tab": VK_TAB,
    "enter": VK_RETURN,
    "return": VK_RETURN,
    "escape": VK_ESCAPE,
    "esc": VK_ESCAPE,
    "space": VK_SPACE,
    "shift": VK_SHIFT,
    "alt": VK_ALT,
    "backspace": VK_BACKSPACE,
    "delete": VK_DELETE,
    "del": VK_DELETE,
    "home": VK_HOME,
    "end": VK_END,
    "pageup": VK_PAGE_UP,
    "pagedown": VK_PAGE_DOWN,
    "left": VK_LEFT,
    "right": VK_RIGHT,
    "up": VK_UP,
    "down": VK_DOWN,
}
QUICK_ACTION_INPUT_SEQUENCE = "input_sequence"
QUICK_ACTION_OPEN_URL = "open_url"
QUICK_ACTION_OPEN_APP = "open_app"
QUICK_ACTIONS = {QUICK_ACTION_INPUT_SEQUENCE, QUICK_ACTION_OPEN_URL, QUICK_ACTION_OPEN_APP}
LEGACY_QUICK_INPUT_ACTIONS = {"paste_text", "macro_sequence"}
ULONG_PTR = getattr(wintypes, "ULONG_PTR", wintypes.WPARAM)


def _configure_user32_api(user32_api: Any) -> None:
    try:
        user32_api.GetForegroundWindow.argtypes = []
        user32_api.GetForegroundWindow.restype = wintypes.HWND
        user32_api.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
        user32_api.GetWindowThreadProcessId.restype = wintypes.DWORD
        user32_api.IsWindow.argtypes = [wintypes.HWND]
        user32_api.IsWindow.restype = wintypes.BOOL
        user32_api.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user32_api.ShowWindow.restype = wintypes.BOOL
        user32_api.SetForegroundWindow.argtypes = [wintypes.HWND]
        user32_api.SetForegroundWindow.restype = wintypes.BOOL
        user32_api.keybd_event.argtypes = [
            wintypes.BYTE,
            wintypes.BYTE,
            wintypes.DWORD,
            ULONG_PTR,
        ]
        user32_api.keybd_event.restype = None
    except Exception as exc:
        _runtime_log_event(
            "runtime.user32_api_config_failed",
            severity="warning",
            summary="Failed configuring Windows keyboard/focus API bindings; quick input automation may be unreliable.",
            exc=exc,
        )


if os.name == "nt":
    try:
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        _configure_user32_api(user32)
    except Exception as exc:
        _runtime_log_event(
            "runtime.user32_load_failed",
            severity="warning",
            summary="Failed loading Windows keyboard/focus API; quick input automation will be disabled.",
            exc=exc,
        )
        user32 = None
else:
    user32 = None


def _escalate_runtime_issue_once(
    event_key: str,
    summary: str,
    *,
    details: str = "",
    context: dict[str, Any] | None = None,
) -> None:
    if event_key in _RUNTIME_ESCALATED_EVENTS:
        return
    _RUNTIME_ESCALATED_EVENTS.add(event_key)
    _runtime_log_event(
        event_key,
        severity="error",
        summary=str(summary or ""),
        context=context or ({"details": str(details or "")} if details else None),
    )


def _ensure_depot_window_classes_loaded() -> None:
    global DepotAgentWindow, DepotQAWindow
    current_agent = globals().get("DepotAgentWindow")
    current_qa = globals().get("DepotQAWindow")
    if isinstance(current_agent, type) and isinstance(current_qa, type):
        return
    from flowgrid_app.window.agent import DepotAgentWindow as _DepotAgentWindow
    from flowgrid_app.window.qa_qcs import DepotQAWindow as _DepotQAWindow

    DepotAgentWindow = _DepotAgentWindow
    DepotQAWindow = _DepotQAWindow

class QuickInputsWindow(QMainWindow):

    def __init__(self, runtime_options: RuntimeOptions | None = None) -> None:
        super().__init__()
        self.runtime_options = runtime_options if isinstance(runtime_options, RuntimeOptions) else RuntimeOptions()
        self.channel_display_name = str(self.runtime_options.channel_display_name or APP_TITLE).strip() or APP_TITLE
        mark_flowgrid_shell_window(self)
        configure_flowgrid_shell_factory(lambda: QuickInputsWindow(runtime_options=self.runtime_options))
        self.config_path = _data_file_path(CONFIG_FILENAME)
        self.config: dict[str, Any] = self.load_config()
        self.current_user = DepotRules.normalize_user_id(self.config.get("current_user", detect_current_user_id()))
        self.config["current_user"] = self.current_user
        self._ensure_ui_icon_assets()

        self.palette_data = compute_palette(self.config.get("theme", {}))
        self._pixmap_cache: dict[str, QPixmap] = {}
        self._background_dirty = True
        self._background_cache: dict[tuple[int, int], QPixmap] = {}
        self.image_dialog: ImageLayersDialog | None = None
        self.quick_layout_dialog: QuickLayoutDialog | None = None
        self.quick_radial_menu: QuickRadialMenu | None = None
        self.quick_tabs_widget: QTabWidget | None = None
        self.quick_tab_scrolls: list[QScrollArea] = []
        self.quick_tab_canvases: list[QuickButtonCanvas] = []
        self.active_agent_window: DepotAgentWindow | None = None
        self.active_qa_window: DepotQAWindow | None = None
        self.admin_dialog: DepotAdminDialog | None = None
        self.depot_dashboard_dialog: DepotDashboardDialog | None = None
        self.last_external_hwnd: int | None = None
        self._saving_timer = QTimer(self)
        self._saving_timer.setInterval(220)
        self._saving_timer.setSingleShot(True)
        self._saving_timer.timeout.connect(self.save_config)
        self._hover_inside = False
        self._hover_revealed = False
        self._hover_delay_timer = QTimer(self)
        self._hover_delay_timer.setSingleShot(True)
        self._hover_delay_timer.timeout.connect(self._on_hover_delay_elapsed)
        self._popup_leave_timer = QTimer(self)
        self._popup_leave_timer.setSingleShot(True)
        self._popup_leave_timer.timeout.connect(self._on_popup_leave_check)
        self._ui_opacity_current = 1.0
        self._ui_opacity_anim = QVariantAnimation(self)
        self._ui_opacity_anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
        self._ui_opacity_anim.valueChanged.connect(lambda value: self._set_ui_opacity(float(value)))
        self._ui_opacity_effects: list[tuple[QGraphicsOpacityEffect, float, float]] = []
        self._corner_radius = 14
        self._drag_offset: QPoint | None = None
        self._background_task_results: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self._background_task_timer = QTimer(self)
        self._background_task_timer.setInterval(180)
        self._background_task_timer.timeout.connect(self._drain_background_task_results)
        self._background_task_timer.start()
        self._update_check_in_progress = False
        self._shared_asset_pull_in_progress = False
        self._startup_update_check_started = False
        self._pending_update_info: dict[str, Any] = {}
        self._install_status_cache: dict[str, Any] = current_install_status()
        self._local_commit_comments_cache_loaded = False
        self._local_commit_comments_cache = ""
        self._local_commit_comments_warning_logged = False
        self._shutdown_in_progress = False
        self._shutdown_completed = False
        self._shutdown_had_issues = False

        self.setWindowTitle(self._shell_window_title())
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setWindowFlag(Qt.WindowType.FramelessWindowHint, True)
        self.setWindowFlag(Qt.WindowType.Window, True)
        self.setFixedSize(LAUNCH_WIDTH, LAUNCH_HEIGHT)
        self._apply_window_mask()

        self.surface = BackgroundCanvas(self)
        self.setCentralWidget(self.surface)

        self.main_layout = QVBoxLayout(self.surface)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        self.titlebar = TitleBar(self)
        self.titlebar.title_label.setText(self._shell_window_title())
        self.shell_mode_chip = QLabel(self._shell_mode_chip_text())
        self.shell_mode_chip.setObjectName("FlowgridShellModeChip")
        self.shell_mode_chip.setStyleSheet(
            "QLabel#FlowgridShellModeChip {"
            "background-color: rgba(166, 75, 42, 0.92);"
            "color: #FFF8ED;"
            "border: 1px solid rgba(255, 225, 192, 0.85);"
            "border-radius: 9px;"
            "padding: 2px 8px;"
            "font-size: 10px;"
            "font-weight: 900;"
            "letter-spacing: 0.4px;"
            "}"
        )
        self.shell_mode_chip.setVisible(bool(self.shell_mode_chip.text()))
        titlebar_layout = self.titlebar.layout()
        if titlebar_layout is not None:
            titlebar_layout.insertWidget(2, self.shell_mode_chip, 0, Qt.AlignmentFlag.AlignVCenter)
        self.main_layout.addWidget(self.titlebar)

        self.body = QWidget()
        self.body_layout = QHBoxLayout(self.body)
        self.body_layout.setContentsMargins(6, 6, 6, 6)
        self.body_layout.setSpacing(6)
        self.main_layout.addWidget(self.body, 1)

        self.sidebar = QWidget()
        self.sidebar.setFixedWidth(SIDEBAR_WIDTH)
        self.sidebar_layout = QVBoxLayout(self.sidebar)
        self.sidebar_layout.setContentsMargins(4, 4, 4, 4)
        self.sidebar_layout.setSpacing(4)

        self.nav_buttons: dict[str, QToolButton] = {}
        self.nav_buttons["quick"] = self._make_nav_button(
            standard_icon_name="SP_MediaPlay",
            icon_filename="grid.png",
            icon_px=30,
        )
        self.nav_buttons["depot"] = self._make_nav_button(standard_icon_name="SP_DirHomeIcon", icon_px=30)

        self.nav_buttons["quick"].clicked.connect(lambda: self.switch_page("quick"))
        self.nav_buttons["depot"].clicked.connect(lambda: self.switch_page("depot"))
        self.nav_buttons["quick"].setToolTip("Input Grid")
        self.nav_buttons["depot"].setToolTip("Tracker Hub")

        self.sidebar_layout.addWidget(self.nav_buttons["quick"])
        self.sidebar_layout.addWidget(self.nav_buttons["depot"])
        self.sidebar_layout.addStretch(1)

        self.settings_button = self._make_nav_button(
            standard_icon_name="SP_FileDialogDetailedView",
            icon_filename="settings.webp",
            icon_px=31,
        )
        self.settings_button.clicked.connect(lambda: self.switch_page("settings"))
        self.settings_button.setToolTip("Settings")
        self.sidebar_layout.addWidget(self.settings_button)

        self.pages = QStackedWidget()

        try:
            self.depot_db = DepotDB(
                _shared_workflow_db_path(),
                read_only=self.runtime_options.read_only_db,
                ensure_schema=False,
            )
        except Exception as exc:
            db_path = _shared_workflow_db_path()
            _runtime_log_event(
                "depot.db.open_failed",
                severity="error",
                summary="Failed opening authoritative shared workflow database.",
                exc=exc,
                context={"db_path": str(db_path)},
            )
            raise RuntimeError(f"Unable to open shared workflow database: {db_path}") from exc
        if not self.runtime_options.skip_startup_repairs:
            _migrate_legacy_agent_icons(self.depot_db.db_path)
        self.depot_tracker = DepotTracker(
            self.depot_db,
            startup_repairs_enabled=not self.runtime_options.skip_startup_repairs,
            allow_metadata_repairs=not self.runtime_options.skip_startup_repairs,
        )
        self.depot_refresh_coordinator = DepotRefreshCoordinator()
        self.user_repository = UserRepository(self.depot_tracker, DepotRules)
        self.permission_service = PermissionService(self.user_repository)
        self.depot_tracker.user_repository = self.user_repository
        self.depot_tracker.permission_service = self.permission_service
        self.depot_async_loader = DepotAsyncLoadCoordinator(self.depot_db.db_path, parent=self)
        self.app_context = AppContext(
            current_user=self.current_user,
            config=self.config,
            db=self.depot_db,
            tracker=self.depot_tracker,
            user_repository=self.user_repository,
            permission_service=self.permission_service,
            shell=self,
            runtime_options=self.runtime_options,
        )
        self.window_manager = WindowManager(self)
        _ensure_depot_window_classes_loaded()
        self._shared_editable_icon_snapshot: tuple[tuple[str, str, int, int], ...] = ()
        if not self.runtime_options.skip_shared_icon_reconcile:
            self._refresh_shared_editable_icons(force=True)

        self.quick_page = self._build_quick_page()
        self.depot_page = self._build_depot_page()
        self.settings_page = self._build_settings_page()

        self.pages.addWidget(self.quick_page)
        self.pages.addWidget(self.depot_page)
        self.pages.addWidget(self.settings_page)

        self.page_index = {"quick": 0, "depot": 1, "settings": 2}
        self._apply_sidebar_position()
        self.switch_page("quick")

        self._foreground_timer = QTimer(self)
        self._foreground_timer.setInterval(160)
        self._foreground_timer.timeout.connect(self._capture_external_target)
        self._foreground_timer.start()

        self._restore_window_position()
        self._apply_window_flags()
        self._init_ui_opacity_effects()
        self.apply_theme_styles()
        self.refresh_quick_grid()
        self.refresh_theme_controls()
        self.refresh_settings_controls()
        self.apply_window_icon()
        if not self.runtime_options.skip_shortcut_sync:
            _sync_desktop_shortcut(self.config, create_if_missing=False)
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
        self._start_startup_maintenance()

    # ---------------------------- Config ---------------------------- #
    def load_config(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        if self.config_path.exists():
            try:
                data = json.loads(self.config_path.read_text(encoding="utf-8"))
            except Exception as exc:
                context = {"config_path": str(self.config_path)}
                _runtime_log_event(
                    "runtime.config_load_parse_failed",
                    severity="critical",
                    summary="Config parse failed; loading defaults and continuing.",
                    exc=exc,
                    context=context,
                )
                _escalate_runtime_issue_once(
                    "runtime.config_load_parse_failed",
                    "Flowgrid could not parse its config file and loaded defaults for this session.",
                    details=f"{type(exc).__name__}: {exc}",
                    context=context,
                )
                data = {}

        merged = deep_merge(DEFAULT_CONFIG, data)

        if not merged.get("theme_image_layers"):
            old_path = data.get("theme_image_path")
            if old_path:
                merged["theme_image_layers"] = [
                    safe_layer_defaults(
                        {
                            "image_path": old_path,
                            "image_x": data.get("theme_image_x", 0),
                            "image_y": data.get("theme_image_y", 0),
                            "image_scale_mode": data.get("theme_image_scale_mode", "Fit"),
                            "image_anchor": data.get("theme_image_anchor", "Center"),
                            "image_scale_percent": data.get("theme_image_scale_percent", 100),
                        }
                    )
                ]

        cleaned_layers = []
        for layer in merged.get("theme_image_layers", []):
            cleaned_layers.append(safe_layer_defaults(layer if isinstance(layer, dict) else {}))
        merged["theme_image_layers"] = cleaned_layers

        merged["theme"] = {
            "primary": normalize_hex(merged["theme"].get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY),
            "accent": normalize_hex(merged["theme"].get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT),
            "surface": normalize_hex(merged["theme"].get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE),
        }

        merged["theme_presets"] = merged.get("theme_presets") or deep_clone(DEFAULT_THEME_PRESETS)
        for preset_name, preset in list(merged["theme_presets"].items()):
            if str(preset_name or "").strip() == "Legacy Blue":
                merged["theme_presets"].pop(preset_name, None)
                continue
            if not isinstance(preset, dict):
                merged["theme_presets"].pop(preset_name, None)
                continue
            merged["theme_presets"][preset_name] = {
                "primary": normalize_hex(preset.get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY),
                "accent": normalize_hex(preset.get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT),
                "surface": normalize_hex(preset.get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE),
            }

        if not merged["theme_presets"]:
            merged["theme_presets"] = deep_clone(DEFAULT_THEME_PRESETS)

        def _normalized_theme_triplet(raw_theme: dict[str, Any]) -> dict[str, str]:
            return {
                "primary": normalize_hex(raw_theme.get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY),
                "accent": normalize_hex(raw_theme.get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT),
                "surface": normalize_hex(raw_theme.get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE),
            }

        for preset_name, default_preset in DEFAULT_THEME_PRESETS.items():
            existing_preset = merged["theme_presets"].get(preset_name)
            previous_preset = PREVIOUS_THEME_PRESETS.get(preset_name)
            if not isinstance(existing_preset, dict):
                merged["theme_presets"][preset_name] = deep_clone(default_preset)
                continue
            if isinstance(previous_preset, dict) and _normalized_theme_triplet(existing_preset) == _normalized_theme_triplet(previous_preset):
                merged["theme_presets"][preset_name] = deep_clone(default_preset)

        legacy_default_theme = {
            "primary": LEGACY_DEFAULT_THEME_PRIMARY,
            "accent": LEGACY_DEFAULT_THEME_ACCENT,
            "surface": LEGACY_DEFAULT_THEME_SURFACE,
        }
        configured_default = merged["theme_presets"].get("Default")
        if isinstance(configured_default, dict):
            normalized_default = {
                "primary": normalize_hex(configured_default.get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY),
                "accent": normalize_hex(configured_default.get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT),
                "surface": normalize_hex(configured_default.get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE),
            }
            if normalized_default == legacy_default_theme:
                merged["theme_presets"]["Default"] = deep_clone(DEFAULT_THEME_PRESETS["Default"])

        selected_preset = str(merged.get("selected_theme_preset", "") or "").strip()
        if selected_preset == "Legacy Blue":
            merged["selected_theme_preset"] = "Default"
            selected_preset = "Default"
        if selected_preset == "Default" and merged.get("theme", {}) == legacy_default_theme:
            merged["theme"] = deep_clone(DEFAULT_THEME_PRESETS["Default"])
        previous_selected_preset = PREVIOUS_THEME_PRESETS.get(selected_preset)
        if (
            isinstance(previous_selected_preset, dict)
            and selected_preset in DEFAULT_THEME_PRESETS
            and _normalized_theme_triplet(merged.get("theme", {})) == _normalized_theme_triplet(previous_selected_preset)
        ):
            merged["theme"] = deep_clone(DEFAULT_THEME_PRESETS[selected_preset])

        if merged.get("selected_theme_preset") not in merged["theme_presets"]:
            merged["selected_theme_preset"] = next(iter(merged["theme_presets"].keys()))

        for kind in ("agent", "qa", "admin", "dashboard"):
            preset_key = f"{kind}_selected_theme_preset"
            if str(merged.get(preset_key, "") or "").strip() == "Legacy Blue":
                merged[preset_key] = str(merged.get("selected_theme_preset", "Default") or "Default")

        legacy_fade_enabled = bool(merged.get("popup_control_fade_enabled", True))
        merged["popup_control_fade_strength"] = int(clamp(int(merged.get("popup_control_fade_strength", 65)), 0, 100))
        merged["popup_control_opacity"] = int(clamp(int(merged.get("popup_control_opacity", 82)), 0, 100))
        merged["popup_control_tail_opacity"] = int(clamp(int(merged.get("popup_control_tail_opacity", 0)), 0, 100))

        style = str(merged.get("popup_control_style", "") or "").strip()
        valid_styles = {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}
        if style not in valid_styles:
            style = "Fade Left to Right" if legacy_fade_enabled else "Solid"
        merged["popup_control_style"] = style
        merged["popup_control_fade_enabled"] = style != "Solid"
        merged["popup_auto_reinherit_enabled"] = bool(merged.get("popup_auto_reinherit_enabled", True))
        auto_reinherit_enabled = bool(merged.get("popup_auto_reinherit_enabled", True))

        popup_valid_styles = {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}
        for popup_key in ("agent_theme", "qa_theme", "admin_theme", "dashboard_theme"):
            popup_theme = merged.get(popup_key, {})
            if not isinstance(popup_theme, dict):
                popup_theme = {}

            popup_theme["background"] = normalize_hex(popup_theme.get("background", "#FFFFFF"), "#FFFFFF")
            popup_theme["text"] = normalize_hex(popup_theme.get("text", "#000000"), "#000000")
            popup_theme["field_bg"] = normalize_hex(popup_theme.get("field_bg", "#FFFFFF"), "#FFFFFF")
            popup_theme["transparent"] = bool(popup_theme.get("transparent", False))
            style_value = str(popup_theme.get("control_style", "Fade Left to Right") or "").strip()
            popup_theme["control_style"] = (
                style_value if style_value in popup_valid_styles else "Fade Left to Right"
            )
            popup_theme["control_opacity"] = int(clamp(safe_int(popup_theme.get("control_opacity", 82), 82), 0, 100))
            popup_theme["control_tail_opacity"] = int(
                clamp(safe_int(popup_theme.get("control_tail_opacity", 0), 0), 0, 100)
            )
            popup_theme["control_fade_strength"] = int(
                clamp(safe_int(popup_theme.get("control_fade_strength", 65), 65), 0, 100)
            )
            popup_theme["header_color"] = normalize_hex(popup_theme.get("header_color", ""), "")
            popup_theme["row_hover_color"] = normalize_hex(popup_theme.get("row_hover_color", ""), "")
            popup_theme["row_selected_color"] = normalize_hex(popup_theme.get("row_selected_color", ""), "")

            cleaned_popup_layers: list[dict[str, Any]] = []
            raw_layers = popup_theme.get("image_layers", [])
            if isinstance(raw_layers, list):
                for layer in raw_layers:
                    cleaned_popup_layers.append(safe_layer_defaults(layer if isinstance(layer, dict) else {}))
            popup_theme["image_layers"] = cleaned_popup_layers
            inherit_value = popup_theme.get("inherit_main_theme")
            if isinstance(inherit_value, bool):
                popup_theme["inherit_main_theme"] = inherit_value
            else:
                # Legacy configs that never customized popup colors/images should inherit Flowgrid theme.
                popup_theme["inherit_main_theme"] = bool(
                    popup_theme["background"] == "#FFFFFF"
                    and popup_theme["text"] == "#000000"
                    and popup_theme["field_bg"] == "#FFFFFF"
                    and not popup_theme["transparent"]
                    and not cleaned_popup_layers
                )
            has_assigned_popup_theme = bool(
                popup_theme["background"] != "#FFFFFF"
                or popup_theme["text"] != "#000000"
                or popup_theme["field_bg"] != "#FFFFFF"
                or popup_theme["transparent"]
                or cleaned_popup_layers
                or popup_theme["control_style"] != "Fade Left to Right"
                or int(popup_theme["control_opacity"]) != 82
                or int(popup_theme["control_tail_opacity"]) != 0
                or int(popup_theme["control_fade_strength"]) != 65
                or bool(popup_theme["header_color"])
                or bool(popup_theme["row_hover_color"])
                or bool(popup_theme["row_selected_color"])
            )
            # Assigned popup theme data always wins over inherited main theme.
            if has_assigned_popup_theme:
                popup_theme["inherit_main_theme"] = False
            elif auto_reinherit_enabled and self._popup_theme_needs_auto_reinherit(popup_theme):
                # Recovery path for legacy/broken configs that were forced out of inherit mode
                # while still holding untouched default values (white/empty fields).
                popup_theme["inherit_main_theme"] = True
            merged[popup_key] = popup_theme

        def normalize_quick_items(raw_items: Any) -> list[dict[str, Any]]:
            cleaned_items: list[dict[str, Any]] = []
            if not isinstance(raw_items, list):
                return cleaned_items
            for idx, item in enumerate(raw_items):
                if not isinstance(item, dict):
                    continue
                action = str(item.get("action", QUICK_ACTION_INPUT_SEQUENCE)).strip().lower()
                open_target = str(item.get("open_target", "")).strip()
                app_targets = str(item.get("app_targets", "")).strip()
                urls_text = str(item.get("urls", "")).strip()
                browser_path = str(item.get("browser_path", "")).strip()
                text_payload = str(item.get("text", ""))

                # Migrate older macro mode into supported action types.
                if action == "macro":
                    if not app_targets and open_target:
                        app_targets = open_target
                    if app_targets:
                        action = QUICK_ACTION_OPEN_APP
                    elif urls_text or text_payload.strip():
                        action = QUICK_ACTION_OPEN_URL
                    else:
                        action = QUICK_ACTION_INPUT_SEQUENCE
                elif action in LEGACY_QUICK_INPUT_ACTIONS:
                    action = QUICK_ACTION_INPUT_SEQUENCE
                elif action not in QUICK_ACTIONS:
                    _runtime_log_event(
                        "runtime.quick_action_unknown_migrated",
                        severity="warning",
                        summary="Unknown quick input action was migrated to Input Sequence.",
                        context={"index": int(idx), "action": action},
                    )
                    action = QUICK_ACTION_INPUT_SEQUENCE

                # Backward compatibility for entries saved before URL list/browser fields existed.
                if action == QUICK_ACTION_OPEN_URL and not urls_text:
                    fallback = open_target or text_payload.strip()
                    urls_text = fallback
                if action == QUICK_ACTION_OPEN_APP and not app_targets and open_target:
                    app_targets = open_target
                normalized_item: dict[str, Any] = {
                    "title": str(item.get("title", f"Item {idx + 1}"))[:64],
                    "tooltip": str(item.get("tooltip", "")),
                    "text": text_payload,
                    "action": action,
                    "open_target": open_target,
                    "app_targets": app_targets,
                    "urls": urls_text,
                    "browser_path": browser_path,
                }
                if isinstance(item.get("x"), (int, float)) and isinstance(item.get("y"), (int, float)):
                    normalized_item["x"] = int(item.get("x", 0))
                    normalized_item["y"] = int(item.get("y", 0))
                cleaned_items.append(normalized_item)
            return cleaned_items

        cleaned_quick_texts = normalize_quick_items(merged.get("quick_texts", []))
        merged["quick_texts"] = cleaned_quick_texts

        cleaned_quick_tabs: list[dict[str, Any]] = []
        raw_quick_tabs = merged.get("quick_tabs", [])
        if isinstance(raw_quick_tabs, list):
            for tab_idx, tab in enumerate(raw_quick_tabs):
                if not isinstance(tab, dict):
                    continue
                tab_name = str(tab.get("name", "")).strip()[:32]
                if not tab_name:
                    tab_name = "Main" if tab_idx == 0 else f"Task {tab_idx + 1}"
                tab_quick_texts = normalize_quick_items(tab.get("quick_texts", []))
                cleaned_quick_tabs.append({"name": tab_name, "quick_texts": tab_quick_texts})

        if not cleaned_quick_tabs:
            cleaned_quick_tabs = [{"name": "Main", "quick_texts": [dict(item) for item in cleaned_quick_texts]}]
        merged["quick_tabs"] = cleaned_quick_tabs

        active_quick_tab = safe_int(merged.get("active_quick_tab", 0), 0)
        if active_quick_tab < 0 or active_quick_tab >= len(cleaned_quick_tabs):
            active_quick_tab = 0
        merged["active_quick_tab"] = active_quick_tab
        merged["quick_texts"] = [
            dict(item) for item in cleaned_quick_tabs[active_quick_tab].get("quick_texts", []) if isinstance(item, dict)
        ]

        family = str(merged.get("quick_button_font_family", "Segoe UI")).strip()
        merged["quick_button_font_family"] = family or "Segoe UI"
        loaded_opacity = float(clamp(float(merged.get("window_opacity", 1.0)), 0.0, 1.0))
        # Prevent a saved 0.00 opacity from making the app appear like it failed to launch.
        if loaded_opacity <= 0.01:
            loaded_opacity = 0.20
        merged["window_opacity"] = loaded_opacity
        current_user = str(merged.get("current_user", "")).strip()
        merged["current_user"] = DepotRules.normalize_user_id(current_user or detect_current_user_id())
        legacy_theme_transparent = data.get("theme_page_transparent_primary_bg")
        if "background_tint_enabled" in data:
            merged["background_tint_enabled"] = bool(merged.get("background_tint_enabled", True))
        elif isinstance(legacy_theme_transparent, bool):
            merged["background_tint_enabled"] = not legacy_theme_transparent
        else:
            merged["background_tint_enabled"] = bool(merged.get("background_tint_enabled", True))

        return merged

    def save_config(self) -> None:
        payload = json.dumps(self.config, indent=2, ensure_ascii=False)
        target = self.config_path
        temp_path = target.with_name(f"{target.name}.tmp")
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(payload, encoding="utf-8")
            os.replace(temp_path, target)
        except Exception as exc:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception as cleanup_exc:
                _runtime_log_event(
                    "runtime.config_temp_cleanup_failed",
                    severity="warning",
                    summary="Config save failed and temporary config cleanup also failed.",
                    exc=cleanup_exc,
                    context={"config_path": str(target), "temp_path": str(temp_path)},
                )
            context = {"config_path": str(target)}
            _runtime_log_event(
                "runtime.config_save_failed",
                severity="critical",
                summary="Config save failed; settings may not persist.",
                exc=exc,
                context=context,
            )
            _escalate_runtime_issue_once(
                "runtime.config_save_failed",
                "Flowgrid could not save its config file. Recent settings may not persist.",
                details=f"{type(exc).__name__}: {exc}",
                context=context,
            )

    def queue_save_config(self) -> None:
        self._saving_timer.start()

    # ------------------------- Background --------------------------- #
    def mark_background_dirty(self) -> None:
        self._background_dirty = True
        self._background_cache.clear()
        self.surface.update()

    def load_layer_pixmap(self, path: str) -> QPixmap:
        if path in self._pixmap_cache:
            return self._pixmap_cache[path]

        pixmap = QPixmap()
        if path and Path(path).exists():
            reader = QImageReader(path)
            reader.setAutoTransform(True)
            image = reader.read()
            if not image.isNull():
                pixmap = QPixmap.fromImage(image)

        self._pixmap_cache[path] = pixmap
        return pixmap

    def compute_layer_render(self, layer: dict[str, Any], size: QSize) -> LayerRenderInfo | None:
        if not layer.get("visible", True):
            return None

        path = layer.get("image_path", "")
        pixmap = self.load_layer_pixmap(path)
        if pixmap.isNull() or size.width() <= 0 or size.height() <= 0:
            return None

        target_rect = QRectF(0, 0, float(size.width()), float(size.height()))
        mode = str(layer.get("image_scale_mode", "Fit"))
        anchor = str(layer.get("image_anchor", "Center"))
        scale_percent = int(clamp(int(layer.get("image_scale_percent", 100)), 10, 400)) / 100.0
        img_w = float(pixmap.width())
        img_h = float(pixmap.height())

        if mode == "Stretch":
            draw_w = target_rect.width() * scale_percent
            draw_h = target_rect.height() * scale_percent
        elif mode == "Fit":
            ratio = min(target_rect.width() / img_w, target_rect.height() / img_h)
            ratio *= scale_percent
            draw_w = img_w * ratio
            draw_h = img_h * ratio
        elif mode == "Place":
            draw_w = img_w * scale_percent
            draw_h = img_h * scale_percent
        else:
            ratio = max(target_rect.width() / img_w, target_rect.height() / img_h)
            ratio *= scale_percent
            draw_w = img_w * ratio
            draw_h = img_h * ratio

        x = target_rect.left()
        y = target_rect.top()

        if "Right" in anchor:
            x = target_rect.right() - draw_w
        elif anchor in {"Top", "Center", "Bottom"}:
            x = target_rect.left() + (target_rect.width() - draw_w) / 2

        if "Bottom" in anchor:
            y = target_rect.bottom() - draw_h
        elif anchor in {"Left", "Center", "Right"}:
            y = target_rect.top() + (target_rect.height() - draw_h) / 2

        x += int(layer.get("image_x", 0))
        y += int(layer.get("image_y", 0))

        rect = QRectF(x, y, draw_w, draw_h)
        return LayerRenderInfo(layer=layer, rect=rect, pixmap=pixmap)

    @staticmethod
    def _popup_theme_needs_auto_reinherit(theme: dict[str, Any]) -> bool:
        """True when a popup theme is custom-mode but still carries untouched default values."""
        if not isinstance(theme, dict):
            return False
        if bool(theme.get("inherit_main_theme", False)):
            return False

        background = normalize_hex(theme.get("background", "#FFFFFF"), "#FFFFFF")
        text = normalize_hex(theme.get("text", "#000000"), "#000000")
        field_bg = normalize_hex(theme.get("field_bg", "#FFFFFF"), "#FFFFFF")
        transparent = bool(theme.get("transparent", False))
        control_style = str(theme.get("control_style", "Fade Left to Right") or "").strip()
        control_opacity = int(clamp(safe_int(theme.get("control_opacity", 82), 82), 0, 100))
        control_tail_opacity = int(clamp(safe_int(theme.get("control_tail_opacity", 0), 0), 0, 100))
        control_fade_strength = int(clamp(safe_int(theme.get("control_fade_strength", 65), 65), 0, 100))
        header_color = normalize_hex(theme.get("header_color", ""), "")
        row_hover_color = normalize_hex(theme.get("row_hover_color", ""), "")
        row_selected_color = normalize_hex(theme.get("row_selected_color", ""), "")
        raw_layers = theme.get("image_layers", [])
        has_layers = isinstance(raw_layers, list) and any(isinstance(layer, dict) for layer in raw_layers)

        has_custom_data = bool(
            background != "#FFFFFF"
            or text != "#000000"
            or field_bg != "#FFFFFF"
            or transparent
            or has_layers
            or control_style != "Fade Left to Right"
            or control_opacity != 82
            or control_tail_opacity != 0
            or control_fade_strength != 65
            or bool(header_color)
            or bool(row_hover_color)
            or bool(row_selected_color)
        )
        return not has_custom_data

    @staticmethod
    def _looks_like_unconfigured_popup_theme(theme: dict[str, Any]) -> bool:
        background = normalize_hex(theme.get("background", "#FFFFFF"), "#FFFFFF")
        text = normalize_hex(theme.get("text", "#000000"), "#000000")
        field_bg = normalize_hex(theme.get("field_bg", "#FFFFFF"), "#FFFFFF")
        transparent = bool(theme.get("transparent", False))
        control_style = str(theme.get("control_style", "Fade Left to Right") or "").strip()
        control_opacity = int(clamp(safe_int(theme.get("control_opacity", 82), 82), 0, 100))
        control_tail_opacity = int(clamp(safe_int(theme.get("control_tail_opacity", 0), 0), 0, 100))
        control_fade_strength = int(clamp(safe_int(theme.get("control_fade_strength", 65), 65), 0, 100))
        header_color = normalize_hex(theme.get("header_color", ""), "")
        row_hover_color = normalize_hex(theme.get("row_hover_color", ""), "")
        row_selected_color = normalize_hex(theme.get("row_selected_color", ""), "")
        raw_layers = theme.get("image_layers", [])
        has_layers = isinstance(raw_layers, list) and any(isinstance(layer, dict) for layer in raw_layers)
        has_assigned_data = bool(
            background != "#FFFFFF"
            or text != "#000000"
            or field_bg != "#FFFFFF"
            or transparent
            or has_layers
            or control_style != "Fade Left to Right"
            or control_opacity != 82
            or control_tail_opacity != 0
            or control_fade_strength != 65
            or bool(header_color)
            or bool(row_hover_color)
            or bool(row_selected_color)
        )
        if has_assigned_data:
            return False
        if "inherit_main_theme" in theme:
            return bool(theme.get("inherit_main_theme", False))
        return True

    def _auto_reinherit_popup_defaults(self) -> bool:
        """Recover popup themes that are custom-mode but still effectively default/unconfigured."""
        if not bool(self.config.get("popup_auto_reinherit_enabled", True)):
            return False
        changed = False
        for kind in ("agent", "qa", "admin", "dashboard"):
            key = f"{kind}_theme"
            theme = self.config.get(key, {})
            if not isinstance(theme, dict):
                theme = {}
                self.config[key] = theme
            if self._popup_theme_needs_auto_reinherit(theme):
                theme["inherit_main_theme"] = True
                changed = True
        return changed

    def _default_popup_theme_from_main(self) -> dict[str, Any]:
        layers: list[dict[str, Any]] = []
        for layer in self.config.get("theme_image_layers", []):
            if isinstance(layer, dict):
                layers.append(safe_layer_defaults(layer))
        return {
            "background": normalize_hex(
                self.palette_data.get("control_bg", self.palette_data.get("surface", DEFAULT_THEME_SURFACE)),
                DEFAULT_THEME_SURFACE,
            ),
            "text": normalize_hex(self.palette_data.get("label_text", "#000000"), "#000000"),
            "field_bg": normalize_hex(self.palette_data.get("input_bg", "#FFFFFF"), "#FFFFFF"),
            "transparent": False,
            "inherit_main_theme": True,
            "image_layers": layers,
            "control_style": str(self.config.get("popup_control_style", "Fade Left to Right") or "Fade Left to Right"),
            "control_opacity": int(clamp(safe_int(self.config.get("popup_control_opacity", 82), 82), 0, 100)),
            "control_tail_opacity": int(
                clamp(safe_int(self.config.get("popup_control_tail_opacity", 0), 0), 0, 100)
            ),
            "control_fade_strength": int(
                clamp(safe_int(self.config.get("popup_control_fade_strength", 65), 65), 0, 100)
            ),
            "header_color": normalize_hex(self.config.get("popup_header_color", ""), ""),
            "row_hover_color": normalize_hex(self.config.get("popup_row_hover_color", ""), ""),
            "row_selected_color": normalize_hex(self.config.get("popup_row_selected_color", ""), ""),
        }

    def _resolved_popup_theme(self, kind: str) -> dict[str, Any]:
        base = self._default_popup_theme_from_main()
        raw_theme = self.config.get(f"{kind}_theme", {})
        if not isinstance(raw_theme, dict) or self._looks_like_unconfigured_popup_theme(raw_theme):
            return base

        resolved: dict[str, Any] = {
            "background": normalize_hex(raw_theme.get("background", base["background"]), base["background"]),
            "text": normalize_hex(raw_theme.get("text", base["text"]), base["text"]),
            "field_bg": normalize_hex(raw_theme.get("field_bg", base["field_bg"]), base["field_bg"]),
            "transparent": bool(raw_theme.get("transparent", False)),
            "inherit_main_theme": False,
            "image_layers": [],
            "control_style": str(raw_theme.get("control_style", base["control_style"]) or base["control_style"]),
            "control_opacity": int(
                clamp(safe_int(raw_theme.get("control_opacity", base["control_opacity"]), base["control_opacity"]), 0, 100)
            ),
            "control_tail_opacity": int(
                clamp(
                    safe_int(raw_theme.get("control_tail_opacity", base["control_tail_opacity"]), base["control_tail_opacity"]),
                    0,
                    100,
                )
            ),
            "control_fade_strength": int(
                clamp(
                    safe_int(raw_theme.get("control_fade_strength", base["control_fade_strength"]), base["control_fade_strength"]),
                    0,
                    100,
                )
            ),
            "header_color": normalize_hex(raw_theme.get("header_color", base["header_color"]), base["header_color"]),
            "row_hover_color": normalize_hex(
                raw_theme.get("row_hover_color", base["row_hover_color"]), base["row_hover_color"]
            ),
            "row_selected_color": normalize_hex(
                raw_theme.get("row_selected_color", base["row_selected_color"]), base["row_selected_color"]
            ),
        }
        valid_styles = {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}
        if resolved["control_style"] not in valid_styles:
            resolved["control_style"] = base["control_style"] if base["control_style"] in valid_styles else "Fade Left to Right"
        raw_layers = raw_theme.get("image_layers", [])
        if isinstance(raw_layers, list):
            cleaned: list[dict[str, Any]] = []
            for layer in raw_layers:
                if isinstance(layer, dict):
                    cleaned.append(safe_layer_defaults(layer))
            resolved["image_layers"] = cleaned
        return resolved

    def render_background_pixmap(self, size: QSize, kind: str = "main") -> QPixmap:
        key = (kind, size.width(), size.height())
        cached = self._background_cache.get(key)
        if cached is not None and not self._background_dirty:
            return cached

        pixmap = QPixmap(size)
        pixmap.fill(Qt.GlobalColor.transparent)

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)

        if kind == "main":
            layers = self.config.get("theme_image_layers", [])
        else:
            layers = self._resolved_popup_theme(kind).get("image_layers", [])

        for layer in layers:
            info = self.compute_layer_render(layer, size)
            if not info:
                continue
            opacity = float(clamp(float(layer.get("image_opacity", 1.0)), 0.0, 1.0))
            painter.setOpacity(opacity)
            painter.drawPixmap(info.rect.toRect(), info.pixmap)
            painter.setOpacity(1.0)
        painter.end()

        self._background_cache[key] = pixmap
        self._background_dirty = False
        return pixmap

    def _background_tint_enabled(self) -> bool:
        return bool(self.config.get("background_tint_enabled", True))

    def _has_visible_background_layers(self) -> bool:
        for layer in self.config.get("theme_image_layers", []):
            if not isinstance(layer, dict):
                continue
            if not bool(layer.get("visible", True)):
                continue
            path = str(layer.get("image_path", "")).strip()
            if not path or not Path(path).exists():
                continue
            if float(clamp(float(layer.get("image_opacity", 1.0)), 0.0, 1.0)) <= 0.01:
                continue
            if path:
                return True
        return False

    def _window_has_background_layers(self, kind: str) -> bool:
        normalized_kind = str(kind or "main").strip().lower() or "main"
        if normalized_kind == "main":
            return self._has_visible_background_layers()
        resolved = self._resolved_popup_theme(normalized_kind)
        for layer in resolved.get("image_layers", []):
            if not isinstance(layer, dict):
                continue
            if not bool(layer.get("visible", True)):
                continue
            path = str(layer.get("image_path", "")).strip()
            if not path or not Path(path).exists():
                continue
            if float(clamp(float(layer.get("image_opacity", 1.0)), 0.0, 1.0)) <= 0.01:
                continue
            return True
        return False

    def _effective_popup_transparency(self, kind: str) -> bool:
        normalized_kind = str(kind or "main").strip().lower() or "main"
        if normalized_kind == "main":
            return bool(not self._background_tint_enabled() and self._window_has_background_layers("main"))
        resolved = self._resolved_popup_theme(normalized_kind)
        return bool(resolved.get("transparent", False) and self._window_has_background_layers(normalized_kind))

    def _effective_shell_idle_opacity(self) -> float:
        requested = self._base_opacity()
        if self._window_has_background_layers("main"):
            return float(clamp(requested, 0.0, 1.0))
        return float(clamp(requested, 0.05, 1.0))

    def paint_background(self, painter: QPainter, rect: QRect) -> None:
        shell_opacity = float(clamp(getattr(self, "_ui_opacity_current", 1.0), 0.0, 1.0))
        tint_enabled = self._background_tint_enabled()
        if tint_enabled or not self._has_visible_background_layers():
            surface_color = QColor(self.palette_data["surface"])
            surface_color.setAlpha(int(255 * shell_opacity))
            painter.fillRect(rect, surface_color)

        bg = self.render_background_pixmap(rect.size())
        painter.drawPixmap(rect, bg)

        if tint_enabled:
            overlay_color = QColor(self.palette_data["shell_overlay"])
            overlay_color.setAlpha(int(50 * shell_opacity))
            painter.fillRect(rect, overlay_color)

    # -------------------------- UI Build ---------------------------- #
    def _resolve_standard_icon(self, icon_name: str, fallback_name: str = "SP_FileIcon") -> QIcon:
        style = self.style() if self.style() is not None else QApplication.style()
        if style is None:
            return QIcon()
        fallback_enum = getattr(QStyle.StandardPixmap, fallback_name, QStyle.StandardPixmap.SP_FileIcon)
        icon_enum = getattr(QStyle.StandardPixmap, str(icon_name or "").strip(), fallback_enum)
        return style.standardIcon(icon_enum)

    def _ui_icon_dir(self) -> Path:
        icon_dir = _resolve_data_root() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME
        if not self.runtime_options.skip_startup_repairs:
            try:
                icon_dir.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                _runtime_log_event(
                    "ui.main_icon_dir_create_failed",
                    severity="warning",
                    summary="Failed creating local UI icon directory.",
                    exc=exc,
                    context={"icon_dir": str(icon_dir)},
                )
        return icon_dir

    def _ensure_ui_icon_assets(self) -> None:
        if self.runtime_options.skip_startup_repairs:
            return
        # Keep icon handling file-based: migrate legacy icon files into Assets if present.
        icon_dir = self._ui_icon_dir()
        legacy_candidates = [
            _resolve_data_root() / "ui_icons",
            _resolve_data_root() / ASSETS_DIR_NAME / ASSET_UI_ICON_COMPAT_DIR_NAME,
        ]
        for source_dir in legacy_candidates:
            if not source_dir.exists() or not source_dir.is_dir():
                continue
            try:
                for source_file in source_dir.rglob("*"):
                    if not source_file.is_file():
                        continue
                    target_path = icon_dir / source_file.name
                    if target_path.exists():
                        continue
                    shutil.copy2(source_file, target_path)
            except Exception as exc:
                _runtime_log_event(
                    "ui.main_icon_asset_migrate_failed",
                    severity="warning",
                    summary="Failed migrating legacy UI icon assets into Assets.",
                    exc=exc,
                    context={"source_dir": str(source_dir), "target_dir": str(icon_dir)},
                )

    def _load_ui_icon(self, filename: str, fallback_standard: str = "SP_FileIcon") -> QIcon:
        clean = str(filename or "").strip()
        if clean:
            search_paths = [
                self._ui_icon_dir() / clean,
                _resolve_data_root() / ASSETS_DIR_NAME / ASSET_UI_ICON_COMPAT_DIR_NAME / clean,
                _resolve_data_root() / "ui_icons" / clean,
                _local_data_root() / ASSETS_DIR_NAME / FLOWGRID_ICON_PACK_DIR_NAME / clean,
                _local_data_root() / ASSETS_DIR_NAME / ASSET_UI_ICON_COMPAT_DIR_NAME / clean,
                _local_data_root() / "ui_icons" / clean,
            ]
            for path in search_paths:
                if not path.exists() or not path.is_file():
                    continue
                icon = QIcon(str(path))
                if not icon.isNull():
                    return icon
        return self._resolve_standard_icon(fallback_standard, "SP_FileIcon")

    def _make_nav_button(
        self,
        icon_text: str = "",
        *,
        standard_icon_name: str = "",
        icon_filename: str = "",
        icon_px: int = 30,
    ) -> QToolButton:
        btn = QToolButton()
        btn.setText(str(icon_text or ""))
        btn.setFixedSize(42, 42)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        font = QFont("Segoe UI Symbol", 15, QFont.Weight.Bold)
        btn.setFont(font)
        icon = QIcon()
        if icon_filename:
            fallback = standard_icon_name if standard_icon_name else "SP_FileIcon"
            icon = self._load_ui_icon(icon_filename, fallback)
        elif standard_icon_name:
            icon = self._resolve_standard_icon(standard_icon_name, "SP_FileIcon")
        if not icon.isNull():
            px = int(clamp(safe_int(icon_px, 30), 16, 40))
            btn.setIcon(icon)
            btn.setIconSize(QSize(px, px))
        return btn

    def _build_quick_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(6)

        head_row = QHBoxLayout()
        self.quick_actions_button = QPushButton("")
        self.quick_actions_button.setObjectName("QuickActionsTrigger")
        self.quick_actions_button.setProperty("actionRole", "add")
        self.quick_actions_button.setToolTip("Quick actions")
        self.quick_actions_button.setFixedSize(36, 36)
        self.quick_actions_button.setIcon(self._resolve_standard_icon("SP_DialogOpenButton", "SP_ArrowRight"))
        self.quick_actions_button.setIconSize(QSize(20, 20))
        self.quick_actions_button.clicked.connect(self.toggle_quick_radial_menu)
        head_row.addWidget(self.quick_actions_button, 0)
        head_row.addStretch(1)
        layout.addLayout(head_row)

        self._build_quick_radial_menu()

        self.quick_tabs_widget = QTabWidget()
        self.quick_tabs_widget.setMovable(False)
        self.quick_tabs_widget.setTabPosition(QTabWidget.TabPosition.North)
        self.quick_tabs_widget.currentChanged.connect(self._on_quick_tab_changed)
        layout.addWidget(self.quick_tabs_widget, 1)
        self._rebuild_quick_tab_widgets()

        self._build_quick_editor_dialog()
        return page

    def _build_depot_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        title = QLabel("Tracker Hub")
        title.setProperty("section", True)
        subtitle = QLabel("Launch windows and maintenance actions.")
        subtitle.setProperty("muted", True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        self.depot_agent_button = QPushButton("Open Agent")
        self.depot_agent_button.setProperty("actionRole", "launch")
        self.depot_agent_button.setIcon(self._load_ui_icon("wrench.png", "SP_ComputerIcon"))
        self.depot_agent_button.clicked.connect(self._open_depot_agent)
        self.depot_agent_button.setToolTip("Open the Agent popup window.")

        self.depot_qa_button = QPushButton("Open QA/WCS")
        self.depot_qa_button.setProperty("actionRole", "launch")
        self.depot_qa_button.setIcon(self._load_ui_icon("qa.png", "SP_DialogApplyButton"))
        self.depot_qa_button.clicked.connect(self._open_depot_qa)
        self.depot_qa_button.setToolTip("Open the QA/WCS popup window.")

        self.depot_admin_button = QPushButton("User Setup")
        self.depot_admin_button.setProperty("actionRole", "pick")
        self.depot_admin_button.setIcon(self._load_ui_icon("user-admin.svg", "SP_FileDialogDetailedView"))
        self.depot_admin_button.clicked.connect(self._open_depot_admin)
        self.depot_admin_button.setToolTip("Open the User Setup window.")
        self.depot_dashboard_button = QPushButton("Open Data Dashboard")
        self.depot_dashboard_button.setProperty("actionRole", "pick")
        self.depot_dashboard_button.setIcon(self._load_ui_icon("dash.webp", "SP_FileDialogContentsView"))
        self.depot_dashboard_button.clicked.connect(self._open_depot_dashboard)
        self.depot_dashboard_button.setToolTip("Open data dashboard in a separate window.")

        for btn in (
            self.depot_agent_button,
            self.depot_qa_button,
            self.depot_admin_button,
            self.depot_dashboard_button,
        ):
            btn.setMinimumHeight(44)
            btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            btn.setIconSize(QSize(24, 24))

        actions_grid = QGridLayout()
        actions_grid.setHorizontalSpacing(8)
        actions_grid.setVerticalSpacing(8)
        actions_grid.setColumnStretch(0, 1)
        actions_grid.setColumnStretch(1, 1)
        actions_grid.addWidget(self.depot_agent_button, 0, 0)
        actions_grid.addWidget(self.depot_qa_button, 0, 1)
        actions_grid.addWidget(self.depot_admin_button, 1, 0)
        actions_grid.addWidget(self.depot_dashboard_button, 1, 1)
        layout.addLayout(actions_grid)
        layout.addStretch(1)
        self._apply_depot_access_controls()

        return page

    def _show_shell_message(
        self,
        icon: QMessageBox.Icon,
        title: str,
        text: str,
        *,
        theme_kind: str = "main",
    ) -> None:
        """Non-native QMessageBox from the main window, styled like configured popup themes."""
        show_flowgrid_themed_message(self, self, theme_kind, icon, title, text)

    def _show_access_denied_message(self, title: str, text: str, *, theme_kind: str) -> None:
        self._show_shell_message(QMessageBox.Icon.Warning, title, text, theme_kind=theme_kind)

    def _apply_depot_access_controls(self) -> None:
        permission_service = getattr(self, "permission_service", None)
        if permission_service is None:
            return
        agent_allowed = permission_service.can_open_agent_window(self.current_user)
        qa_allowed = permission_service.can_access_qa(self.current_user)
        admin_allowed = permission_service.can_access_admin(self.current_user)
        dashboard_allowed = permission_service.can_access_dashboard(self.current_user)

        if hasattr(self, "depot_agent_button") and self.depot_agent_button is not None:
            self.depot_agent_button.setEnabled(agent_allowed)
            self.depot_agent_button.setToolTip(
                "Open the Agent popup window." if agent_allowed else PermissionService.AGENT_ACCESS_DENIED_MESSAGE
            )
        if hasattr(self, "depot_qa_button") and self.depot_qa_button is not None:
            self.depot_qa_button.setEnabled(qa_allowed)
            self.depot_qa_button.setToolTip(
                "Open the QA/WCS popup window." if qa_allowed else PermissionService.QA_ACCESS_DENIED_MESSAGE
            )
        if hasattr(self, "depot_admin_button") and self.depot_admin_button is not None:
            self.depot_admin_button.setEnabled(admin_allowed)
            self.depot_admin_button.setToolTip(
                "Open the User Setup window." if admin_allowed else PermissionService.ADMIN_ACCESS_DENIED_MESSAGE
            )
        if hasattr(self, "depot_dashboard_button") and self.depot_dashboard_button is not None:
            self.depot_dashboard_button.setEnabled(dashboard_allowed)
            self.depot_dashboard_button.setToolTip(
                "Open data dashboard in a separate window."
                if dashboard_allowed
                else PermissionService.DASHBOARD_ACCESS_DENIED_MESSAGE
            )

    def _refresh_depot_dashboard_combo_popup_width(self) -> None:
        dashboard_dialog = self.window_manager.get_window("dashboard")
        if dashboard_dialog is not None:
            dashboard_dialog.refresh_combo_popup_width()

    def _refresh_depot_dashboard(self) -> None:
        dashboard_dialog = self.window_manager.get_window("dashboard")
        if dashboard_dialog is not None and dashboard_dialog.isVisible():
            dashboard_dialog.refresh_dashboard()

    @staticmethod
    def _all_depot_refresh_sections() -> tuple[str, ...]:
        return (
            "admin_admins",
            "admin_agents",
            "admin_roles",
            "admin_qa_flags",
            "agent_category",
            "agent_client_followup",
            "agent_missing_po",
            "agent_parts",
            "agent_recent",
            "agent_rtv",
            "agent_team_client_followup",
            "agent_work_chart",
            "dashboard_completed",
            "dashboard_metrics",
            "dashboard_notes",
            "qa_assigned",
            "qa_category",
            "qa_client_followup",
            "qa_client_jo",
            "qa_delivered",
            "qa_flags",
            "qa_missing_po",
            "qa_owner",
            "qa_recent",
            "qa_rtv",
        )

    def _invalidate_depot_views(self, *sections: str, reason: str = "") -> None:
        coordinator = getattr(self, "depot_refresh_coordinator", None)
        if coordinator is None:
            return
        coordinator.invalidate_views(*sections, reason=reason)
        async_loader = getattr(self, "depot_async_loader", None)
        if async_loader is not None:
            for section in sections:
                normalized_section = str(section or "").strip()
                if normalized_section in {"dashboard_metrics", "dashboard_completed", "dashboard_notes"}:
                    async_loader.cancel_view(normalized_section, reason=f"invalidate:{reason}")

    def start_depot_read(
        self,
        view_key: str,
        state_key: Any,
        *,
        reason: str = "",
        force: bool = False,
        loader,
        on_success,
        on_error,
    ):
        coordinator = getattr(self, "depot_async_loader", None)
        if coordinator is None:
            _runtime_log_event(
                "depot.async_read_start_failed",
                severity="warning",
                summary="Could not start a shared workflow background read because the coordinator is unavailable.",
                context={"view": str(view_key or ""), "reason": str(reason or "")},
            )
            return None
        return coordinator.start_read(
            view_key,
            state_key,
            reason=reason,
            force=force,
            loader=loader,
            on_success=on_success,
            on_error=on_error,
        )

    def cancel_depot_reads(self, reason: str = "", *view_keys: str) -> None:
        coordinator = getattr(self, "depot_async_loader", None)
        if coordinator is None:
            return
        if view_keys:
            for view_key in view_keys:
                coordinator.cancel_view(str(view_key or ""), reason=reason)
            return
        coordinator.cancel_all(reason=reason)

    def _refresh_visible_depot_views(
        self,
        *sections: str,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
        window_scope: str | None = None,
    ) -> None:
        requested = {str(section or "").strip() for section in sections if str(section or "").strip()}
        if not requested:
            return
        scope = str(window_scope or "").strip().lower()
        if scope and scope not in {"agent", "qa", "dashboard", "admin"}:
            _runtime_log_event(
                "sync.depot_view_refresh_scope_invalid",
                severity="warning",
                summary="A shared workflow view refresh used an unknown window scope.",
                context={"window_scope": scope, "reason": str(reason or ""), "sections": sorted(requested)},
            )
            scope = ""

        agent_window = getattr(self, "active_agent_window", None)
        if scope in {"", "agent"} and agent_window is not None and agent_window.isVisible():
            current_agent_index = int(agent_window.agent_tabs.currentIndex()) if hasattr(agent_window, "agent_tabs") else -1
            current_agent_key = agent_window._tab_key_for_index(current_agent_index) if current_agent_index >= 0 else ""
            on_agent_work_tab = current_agent_index == int(agent_window.agent_tabs.indexOf(agent_window.work_tab)) if hasattr(agent_window, "agent_tabs") else False
            if "agent_recent" in requested and on_agent_work_tab:
                agent_window._refresh_recent_submissions_label(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_work_chart" in requested and on_agent_work_tab:
                agent_window._refresh_work_touch_chart(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_client_followup" in requested and current_agent_key == "client":
                agent_window._refresh_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_rtv" in requested and getattr(agent_window, "rtv_tab", None) is not None and current_agent_key == "rtv":
                agent_window._refresh_rtv_rows(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_team_client_followup" in requested and getattr(agent_window, "team_client_tab", None) is not None and current_agent_key == "team_client":
                agent_window._refresh_team_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_parts" in requested and current_agent_key == "parts":
                agent_window._refresh_agent_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_category" in requested and current_agent_key == "cat_parts":
                agent_window._refresh_category_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            if "agent_missing_po" in requested and current_agent_key == "missing_po":
                agent_window._refresh_missing_po_followups(force=force, reason=reason, ttl_ms=ttl_ms)

        qa_window = getattr(self, "active_qa_window", None)
        if scope in {"", "qa"} and qa_window is not None and qa_window.isVisible():
            current_qa_index = int(qa_window.qa_tabs.currentIndex()) if hasattr(qa_window, "qa_tabs") else -1
            current_qa_widget = qa_window.qa_tabs.widget(current_qa_index) if current_qa_index >= 0 and hasattr(qa_window, "qa_tabs") else None
            current_qa_key = qa_window._qa_tab_key_for_index(current_qa_index) if current_qa_index >= 0 else ""
            if "qa_owner" in requested and current_qa_widget is qa_window.submit_tab:
                qa_window._refresh_repair_owner_preview(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_flags" in requested:
                qa_window._populate_flags()
            if "qa_recent" in requested:
                qa_window._refresh_recent_submissions_label(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_assigned" in requested and current_qa_widget is qa_window.assigned_tab:
                qa_window._refresh_assigned_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_category" in requested and current_qa_key == "cat_parts":
                qa_window._refresh_qa_category_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_client_followup" in requested and current_qa_key == "client":
                qa_window._refresh_qa_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_rtv" in requested and current_qa_key == "rtv":
                qa_window._refresh_qa_rtv_rows(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_delivered" in requested and current_qa_widget is qa_window.delivered_tab:
                qa_window._refresh_delivered_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_client_jo" in requested and current_qa_key == "client_jo":
                qa_window._refresh_qa_client_jo_rows(force=force, reason=reason, ttl_ms=ttl_ms)
            if "qa_missing_po" in requested and current_qa_key == "missing_po":
                qa_window._refresh_missing_po_followups(force=force, reason=reason, ttl_ms=ttl_ms)

        dashboard_dialog = getattr(self, "depot_dashboard_dialog", None)
        if scope in {"", "dashboard"} and dashboard_dialog is not None and dashboard_dialog.isVisible():
            if "dashboard_metrics" in requested:
                dashboard_dialog.refresh_dashboard(force=force, reason=reason)
            if "dashboard_completed" in requested:
                dashboard_dialog.refresh_completed_parts(force=force, reason=reason)
            if "dashboard_notes" in requested:
                dashboard_dialog.refresh_notes_rows(force=force, reason=reason)

        admin_dialog = getattr(self, "admin_dialog", None)
        if scope in {"", "admin"} and admin_dialog is not None and admin_dialog.isVisible():
            if "admin_agents" in requested or "admin_admins" in requested:
                admin_dialog.refresh_users()
            if "admin_roles" in requested:
                admin_dialog.refresh_roles()
            if "admin_qa_flags" in requested:
                admin_dialog.refresh_qa_flags()

    def _refresh_shared_editable_icon_views(self) -> None:
        sections = (
            "admin_admins",
            "admin_agents",
            "admin_roles",
            "admin_qa_flags",
            "agent_missing_po",
            "dashboard_completed",
            "qa_assigned",
            "qa_delivered",
            "qa_flags",
            "qa_missing_po",
            "qa_owner",
        )
        self._invalidate_depot_views(*sections, reason="shared_editable_icons")
        self._refresh_visible_depot_views(
            *sections,
            force=False,
            reason="shared_editable_icons",
        )

    def _refresh_shared_editable_icons(self, force: bool = False) -> None:
        if self.runtime_options.skip_shared_icon_reconcile:
            return
        try:
            self.depot_tracker.reconcile_shared_editable_icons()
            latest_snapshot = self.depot_tracker.shared_editable_icon_snapshot()
        except Exception as exc:
            _runtime_log_event(
                "sync.shared_editable_icons_refresh_failed",
                severity="warning",
                summary="Shared editable icon refresh failed.",
                exc=exc,
            )
            return

        if not force and latest_snapshot == self._shared_editable_icon_snapshot:
            return

        self._shared_editable_icon_snapshot = latest_snapshot
        self._refresh_shared_editable_icon_views()

    def _depot_window_kind_for_widget(self, widget: QWidget | None) -> str:
        current = widget
        while current is not None:
            if current is getattr(self, "active_agent_window", None):
                return "agent"
            if current is getattr(self, "active_qa_window", None):
                return "qa"
            if current is getattr(self, "admin_dialog", None):
                return "admin"
            if current is getattr(self, "depot_dashboard_dialog", None):
                return "dashboard"
            try:
                current = current.parentWidget()
            except Exception:
                current = None
        return ""

    def _resolve_depot_refresh_source_kind(self, source_window: QWidget | None = None) -> str:
        for candidate in (source_window, QApplication.focusWidget(), QApplication.activeWindow()):
            kind = self._depot_window_kind_for_widget(candidate)
            if kind:
                return kind
        return ""

    def _refresh_shared_linked_views(
        self,
        *sections: str,
        force: bool = False,
        reason: str = "linked_refresh",
        refresh_scope: str = "active",
        source_window: QWidget | None = None,
    ) -> None:
        """Invalidate targeted shared workflow views and refresh only the requested scope."""
        requested_sections = tuple(str(section or "").strip() for section in sections if str(section or "").strip())
        if not requested_sections:
            requested_sections = self._all_depot_refresh_sections()
        self._invalidate_depot_views(*requested_sections, reason=reason)
        normalized_scope = str(refresh_scope or "active").strip().lower()
        if normalized_scope == "none":
            return
        if normalized_scope == "all":
            try:
                self._refresh_visible_depot_views(
                    *requested_sections,
                    force=force,
                    reason=reason,
                    ttl_ms=DEPOT_VIEW_TTL_MS,
                )
            except Exception as exc:
                _runtime_log_event(
                    "sync.depot_linked_refresh_failed",
                    severity="warning",
                    summary="A linked shared workflow refresh failed after invalidation.",
                    exc=exc,
                    context={"views": list(requested_sections), "reason": str(reason or ""), "refresh_scope": "all"},
                )
            return
        if normalized_scope != "active":
            _runtime_log_event(
                "sync.depot_linked_refresh_scope_invalid",
                severity="warning",
                summary="A linked shared workflow refresh used an unknown scope; defaulting to the active window.",
                context={"refresh_scope": normalized_scope, "reason": str(reason or "")},
            )
        source_kind = self._resolve_depot_refresh_source_kind(source_window)
        if not source_kind:
            _runtime_log_event(
                "sync.depot_linked_refresh_source_unresolved",
                severity="info",
                summary="Shared workflow views were invalidated without an immediate source-window refresh.",
                context={"views": list(requested_sections), "reason": str(reason or "")},
            )
            return
        try:
            self._refresh_visible_depot_views(
                *requested_sections,
                force=force,
                reason=reason,
                ttl_ms=DEPOT_VIEW_TTL_MS,
                window_scope=source_kind,
            )
        except Exception as exc:
            _runtime_log_event(
                "sync.depot_linked_refresh_failed",
                severity="warning",
                summary="A linked shared workflow refresh failed after invalidation.",
                exc=exc,
                context={"views": list(requested_sections), "reason": str(reason or ""), "refresh_scope": source_kind},
            )

    def _export_depot_dashboard(self) -> None:
        dashboard_dialog = self.window_manager.get_window("dashboard")
        if dashboard_dialog is None:
            dashboard_dialog = self._open_depot_dashboard()
        if dashboard_dialog is not None:
            dashboard_dialog.export_csv()

    def _open_depot_dashboard(self) -> DepotDashboardDialog | None:
        self._reveal_immediately()
        return self.window_manager.show_controlled_window(
            "dashboard",
            lambda: DepotDashboardDialog(self),
            can_open=lambda: self.permission_service.can_access_dashboard(self.current_user),
            on_denied=lambda: self._show_access_denied_message(
                "Access Denied",
                PermissionService.DASHBOARD_ACCESS_DENIED_MESSAGE,
                theme_kind="dashboard",
            ),
            prepare=self._prepare_depot_dashboard_window,
        )

    def _prepare_depot_dashboard_window(self, dialog: DepotDashboardDialog) -> None:
        if not dialog.isVisible():
            restore_flowgrid_popup_position(dialog, self.config, "depot_dashboard", queue_save=self.queue_save_config)
        dialog.apply_theme_styles()
        dialog.refresh_combo_popup_width()

    def _prepare_depot_agent_window(self, dialog: DepotAgentWindow) -> None:
        if not dialog.isVisible():
            restore_flowgrid_popup_position(dialog, self.config, "agent", queue_save=self.queue_save_config)
        dialog.apply_theme_styles()

    def _open_depot_agent(self) -> DepotAgentWindow | None:
        _ensure_depot_window_classes_loaded()
        self._reveal_immediately()
        return self.window_manager.show_controlled_window(
            "agent",
            lambda: DepotAgentWindow(self.depot_tracker, self.current_user, app_window=self),
            can_open=lambda: self.permission_service.can_open_agent_window(self.current_user),
            on_denied=lambda: self._show_access_denied_message(
                "Access Denied",
                PermissionService.AGENT_ACCESS_DENIED_MESSAGE,
                theme_kind="agent",
            ),
            prepare=self._prepare_depot_agent_window,
        )

    def _open_depot_qa(self) -> DepotQAWindow | None:
        _ensure_depot_window_classes_loaded()
        self._reveal_immediately()
        return self.window_manager.show_controlled_window(
            "qa",
            lambda: DepotQAWindow(self.depot_tracker, self.current_user, app_window=self),
            can_open=lambda: self.permission_service.can_access_qa(self.current_user),
            on_denied=lambda: self._show_access_denied_message(
                "Access Denied",
                PermissionService.QA_ACCESS_DENIED_MESSAGE,
                theme_kind="qa",
            ),
        )

    def _open_depot_admin(self) -> DepotAdminDialog | None:
        self._reveal_immediately()
        return self.window_manager.show_controlled_window(
            "admin",
            lambda: DepotAdminDialog(self.depot_tracker, self.current_user, app_window=self),
            can_open=lambda: self.permission_service.can_access_admin(self.current_user),
            on_denied=lambda: self._show_access_denied_message(
                "Access Denied",
                PermissionService.ADMIN_ACCESS_DENIED_MESSAGE,
                theme_kind="admin",
            ),
            prepare=self._prepare_depot_admin_window,
        )

    def _prepare_depot_admin_window(self, dialog: DepotAdminDialog) -> None:
        dialog.apply_theme_styles()

    def _build_quick_editor_dialog(self) -> None:
        self.quick_editor_dialog = FlowgridThemedDialog(self, self, "main")
        self.quick_editor_dialog.setObjectName("QuickEditorDialog")
        self.quick_editor_dialog.setWindowTitle("Edit Quick Button")
        self.quick_editor_dialog.setModal(False)
        self.quick_editor_dialog.setMinimumSize(390, 320)
        self.quick_editor_dialog.resize(430, 390)
        self.quick_editor_dialog.apply_theme_styles(force_opaque_root=True)

        editor_layout = QVBoxLayout(self.quick_editor_dialog)
        editor_layout.setContentsMargins(10, 10, 10, 10)
        editor_layout.setSpacing(6)

        self.editor_title = QLineEdit()
        self.editor_tooltip = QLineEdit()
        self.editor_action_combo = QComboBox()
        self.editor_action_combo.addItem("Input Sequence", QUICK_ACTION_INPUT_SEQUENCE)
        self.editor_action_combo.addItem("Open URL(s)", QUICK_ACTION_OPEN_URL)
        self.editor_action_combo.addItem("Open App/File", QUICK_ACTION_OPEN_APP)

        self.editor_text = QTextEdit(self.quick_editor_dialog)
        self.editor_text.setFixedHeight(82)
        self.editor_text.hide()
        
        self.editor_macro = QTextEdit()
        self.editor_macro.setFixedHeight(82)
        self.editor_macro.setPlaceholderText("Format: Bin location [enter]")
        
        # Macro helper buttons
        macro_buttons_layout = QHBoxLayout()
        macro_buttons_layout.setContentsMargins(0, 0, 0, 0)
        macro_buttons_layout.setSpacing(4)
        
        macro_btn_tab = QPushButton("[Tab]")
        macro_btn_tab.setFixedHeight(28)
        macro_btn_tab.setMaximumWidth(70)
        macro_btn_tab.clicked.connect(lambda: self._insert_macro_simple("[tab]"))
        
        macro_btn_enter = QPushButton("[Enter]")
        macro_btn_enter.setFixedHeight(28)
        macro_btn_enter.setMaximumWidth(70)
        macro_btn_enter.clicked.connect(lambda: self._insert_macro_simple("[enter]"))
        
        macro_btn_delay = QPushButton("Add Delay")
        macro_btn_delay.setFixedHeight(28)
        macro_btn_delay.clicked.connect(self._insert_macro_delay)

        macro_buttons_layout.addWidget(macro_btn_tab, 0)
        macro_buttons_layout.addWidget(macro_btn_enter, 0)
        macro_buttons_layout.addWidget(macro_btn_delay, 1)
        
        self.editor_macro_wrap = QWidget()
        macro_wrap_layout = QVBoxLayout(self.editor_macro_wrap)
        macro_wrap_layout.setContentsMargins(0, 0, 0, 0)
        macro_wrap_layout.setSpacing(4)
        macro_wrap_layout.addWidget(self.editor_macro)
        macro_wrap_layout.addLayout(macro_buttons_layout)
        
        self.editor_apps = QTextEdit()
        self.editor_apps.setFixedHeight(82)
        self.editor_apps_browse = QPushButton("Browse Files/Apps...")
        self.editor_apps_browse.setToolTip("Open file explorer to select one or more app/file targets.")
        self.editor_apps_browse.setProperty("actionRole", "pick")
        self.editor_apps_browse.setFixedHeight(30)
        self.editor_apps_browse_folder = QPushButton("Browse Folder...")
        self.editor_apps_browse_folder.setToolTip("Open folder picker and add a folder target.")
        self.editor_apps_browse_folder.setProperty("actionRole", "pick")
        self.editor_apps_browse_folder.setFixedHeight(30)
        apps_action_row = QHBoxLayout()
        apps_action_row.setContentsMargins(0, 0, 0, 0)
        apps_action_row.addWidget(self.editor_apps_browse, 0)
        apps_action_row.addWidget(self.editor_apps_browse_folder, 0)
        apps_action_row.addStretch(1)
        self.editor_apps_wrap = QWidget()
        apps_wrap_layout = QVBoxLayout(self.editor_apps_wrap)
        apps_wrap_layout.setContentsMargins(0, 0, 0, 0)
        apps_wrap_layout.setSpacing(4)
        apps_wrap_layout.addWidget(self.editor_apps)
        apps_wrap_layout.addLayout(apps_action_row)

        self.editor_urls = QTextEdit()
        self.editor_urls.setFixedHeight(82)

        self.editor_browser_combo = QComboBox()
        self.editor_refresh_browsers_button = QPushButton("Detect")
        self.editor_refresh_browsers_button.setProperty("actionRole", "pick")
        self.editor_refresh_browsers_button.setFixedHeight(30)
        browser_row = QHBoxLayout()
        browser_row.setContentsMargins(0, 0, 0, 0)
        browser_row.setSpacing(4)
        browser_row.addWidget(self.editor_browser_combo, 1)
        browser_row.addWidget(self.editor_refresh_browsers_button, 0)
        self.editor_browser_wrap = QWidget()
        self.editor_browser_wrap.setLayout(browser_row)

        form = QFormLayout()
        form.setSpacing(4)
        form.setContentsMargins(0, 0, 0, 0)
        form.addRow("Title", self.editor_title)
        form.addRow("Context", self.editor_tooltip)
        form.addRow("Action", self.editor_action_combo)
        form.addRow("Input Sequence", self.editor_macro_wrap)
        form.addRow("Apps/Files", self.editor_apps_wrap)
        form.addRow("URLs", self.editor_urls)
        form.addRow("Browser", self.editor_browser_wrap)
        self.editor_form = form
        self.editor_text_label = None
        self.editor_macro_label = form.labelForField(self.editor_macro_wrap)
        self.editor_apps_label = form.labelForField(self.editor_apps_wrap)
        self.editor_urls_label = form.labelForField(self.editor_urls)
        self.editor_browser_label = form.labelForField(self.editor_browser_wrap)
        editor_layout.addLayout(form)

        action_row = QHBoxLayout()
        self.editor_save_btn = QPushButton("Save")
        self.editor_save_btn.setProperty("actionRole", "save")
        self.editor_delete_btn = QPushButton("Delete")
        self.editor_delete_btn.setProperty("actionRole", "reset")
        self.editor_cancel_btn = QPushButton("Cancel")
        self.editor_cancel_btn.setProperty("actionRole", "pick")
        action_row.addWidget(self.editor_save_btn)
        action_row.addWidget(self.editor_delete_btn)
        action_row.addWidget(self.editor_cancel_btn)
        editor_layout.addLayout(action_row)

        self.editor_save_btn.clicked.connect(self.save_quick_editor)
        self.editor_delete_btn.clicked.connect(self.delete_quick_editor)
        self.editor_cancel_btn.clicked.connect(self.close_quick_editor)
        self.editor_action_combo.currentIndexChanged.connect(self._update_quick_editor_action_ui)
        self.editor_apps_browse.clicked.connect(self.browse_quick_apps)
        self.editor_apps_browse_folder.clicked.connect(self.browse_quick_app_folder)
        self.editor_refresh_browsers_button.clicked.connect(self._refresh_available_browsers)

        self._editing_index: int | None = None
        self._refresh_available_browsers()
        self._update_quick_editor_action_ui()

    def _build_theme_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.theme_tabs = QTabWidget()
        self.theme_tabs.setUsesScrollButtons(True)
        self.theme_tabs.tabBar().setElideMode(Qt.TextElideMode.ElideNone)
        
        # Main (Flowgrid) theme tab
        main_theme_tab = self._build_main_theme_tab()
        
        # Agent theme tab
        agent_theme_tab = self._build_agent_theme_tab()
        
        # QA theme tab
        qa_theme_tab = self._build_qa_theme_tab()
        
        # Admin theme tab
        admin_theme_tab = self._build_admin_theme_tab()
        
        # Dashboard theme tab
        dashboard_theme_tab = self._build_dashboard_theme_tab()
        
        self.theme_tabs.addTab(self._wrap_scrollable_page(main_theme_tab), "Flowgrid")
        self.theme_tabs.addTab(self._wrap_scrollable_page(agent_theme_tab), "Agent")
        self.theme_tabs.addTab(self._wrap_scrollable_page(qa_theme_tab), "QA")
        self.theme_tabs.addTab(self._wrap_scrollable_page(admin_theme_tab), "Admin")
        self.theme_tabs.addTab(self._wrap_scrollable_page(dashboard_theme_tab), "Dashboard")
        self.theme_tabs.setTabToolTip(0, "Base Flowgrid colors and background layers used as popup defaults.")
        self.theme_tabs.setTabToolTip(1, "Agent popup theme overrides.")
        self.theme_tabs.setTabToolTip(2, "QA/WCS popup theme overrides.")
        self.theme_tabs.setTabToolTip(3, "Admin popup theme overrides.")
        self.theme_tabs.setTabToolTip(4, "Dashboard popup theme overrides.")
        
        layout.addWidget(self.theme_tabs, 1)
        return page

    @staticmethod
    def _wrap_scrollable_page(content: QWidget) -> QScrollArea:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setWidget(content)
        return scroll

    def _build_main_theme_tab(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        colors_title = QLabel("Colors")
        colors_title.setProperty("section", True)
        layout.addWidget(colors_title)

        preset_row = QHBoxLayout()
        preset_row.setSpacing(4)
        self.theme_preset_combo = QComboBox()
        self.theme_preset_new = QPushButton("New")
        self.theme_preset_save = QPushButton("Save")
        self.theme_preset_combo.setToolTip("Select a saved Flowgrid color preset.")
        self.theme_preset_new.setToolTip("Create a new preset from the current Flowgrid colors.")
        self.theme_preset_save.setToolTip("Save current Flowgrid colors into the selected preset.")
        self.theme_preset_new.setProperty("actionRole", "new")
        self.theme_preset_save.setProperty("actionRole", "save")
        self.theme_preset_combo.setFixedHeight(28)
        self.theme_preset_new.setFixedSize(54, 28)
        self.theme_preset_save.setFixedSize(54, 28)
        preset_row.addWidget(QLabel("Preset"))
        preset_row.addWidget(self.theme_preset_combo, 1)
        preset_row.addWidget(self.theme_preset_new)
        preset_row.addWidget(self.theme_preset_save)
        layout.addLayout(preset_row)

        self.color_swatches: dict[str, QPushButton] = {}
        colors_grid = QGridLayout()
        colors_grid.setHorizontalSpacing(8)
        colors_grid.setVerticalSpacing(8)
        colors_grid.setColumnStretch(1, 1)

        for row_index, (key, label) in enumerate((("primary", "Primary"), ("accent", "Accent"), ("surface", "Surface"))):
            text = QLabel(label)
            text.setFixedWidth(52)
            swatch = QPushButton()
            swatch.setProperty("actionRole", "pick")
            swatch.setFixedSize(180, 32)
            swatch.setToolTip(f"Pick the {label.lower()} color for the Flowgrid theme.")

            colors_grid.addWidget(text, row_index, 0)
            colors_grid.addWidget(swatch, row_index, 1)

            self.color_swatches[key] = swatch
            swatch.clicked.connect(lambda _=False, c=key: self.pick_theme_color(c))

        layout.addLayout(colors_grid)

        color_actions = QHBoxLayout()
        color_actions.setSpacing(6)
        self.reset_theme_btn = QPushButton("Reset")
        self.image_layers_btn = QPushButton("Background Images")
        self.reset_theme_btn.setToolTip("Reset Flowgrid colors to the selected preset values.")
        self.image_layers_btn.setToolTip("Edit Flowgrid background image layers (position, scale, blend, visibility).")
        self.reset_theme_btn.setProperty("actionRole", "reset")
        self.image_layers_btn.setProperty("actionRole", "pick")
        for btn in (self.reset_theme_btn, self.image_layers_btn):
            btn.setFixedHeight(32)
            color_actions.addWidget(btn, 1)
        layout.addLayout(color_actions)

        self.theme_transparent_bg_check = QCheckBox("Transparent Background")
        self.theme_transparent_bg_check.setToolTip(
            "Disable the Flowgrid tint layer so background images render directly behind controls."
        )
        layout.addWidget(self.theme_transparent_bg_check)
        self.popup_auto_reinherit_check = QCheckBox("Auto-Reinherit Popup Defaults")
        self.popup_auto_reinherit_check.setToolTip(
            "When enabled, popups stuck in an unconfigured custom state are automatically repaired to inherit Flowgrid defaults."
        )
        layout.addWidget(self.popup_auto_reinherit_check)

        defaults_title = QLabel("Popup Control Defaults")
        defaults_title.setProperty("section", True)
        layout.addWidget(defaults_title)

        defaults_form = QFormLayout()
        defaults_form.setContentsMargins(0, 0, 0, 0)
        defaults_form.setSpacing(6)

        self.main_control_style_combo = QComboBox()
        self.main_control_style_combo.addItems(["Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"])
        self.main_control_style_combo.setToolTip("Default fill style used by popup controls that inherit Flowgrid theme settings.")
        defaults_form.addRow("Fill Style", self.main_control_style_combo)

        self.main_control_fade_slider = QSlider(Qt.Orientation.Horizontal)
        self.main_control_fade_slider.setRange(0, 100)
        self.main_control_fade_slider.setToolTip("Default strength for inherited popup control gradients.")
        self.main_control_fade_value = QLabel("65%")
        self.main_control_fade_value.setToolTip(self.main_control_fade_slider.toolTip())
        fade_row = QHBoxLayout()
        fade_row.setContentsMargins(0, 0, 0, 0)
        fade_row.setSpacing(4)
        fade_row.addWidget(self.main_control_fade_slider, 1)
        fade_row.addWidget(self.main_control_fade_value, 0)
        fade_wrap = QWidget()
        fade_wrap.setLayout(fade_row)
        defaults_form.addRow("Fade", fade_wrap)

        self.main_control_opacity_slider = QSlider(Qt.Orientation.Horizontal)
        self.main_control_opacity_slider.setRange(0, 100)
        self.main_control_opacity_slider.setToolTip("Default opacity for controls in popups that inherit Flowgrid theme settings.")
        self.main_control_opacity_value = QLabel("82%")
        self.main_control_opacity_value.setToolTip(self.main_control_opacity_slider.toolTip())
        opacity_row = QHBoxLayout()
        opacity_row.setContentsMargins(0, 0, 0, 0)
        opacity_row.setSpacing(4)
        opacity_row.addWidget(self.main_control_opacity_slider, 1)
        opacity_row.addWidget(self.main_control_opacity_value, 0)
        opacity_wrap = QWidget()
        opacity_wrap.setLayout(opacity_row)
        defaults_form.addRow("Opacity", opacity_wrap)

        self.main_control_tail_opacity_slider = QSlider(Qt.Orientation.Horizontal)
        self.main_control_tail_opacity_slider.setRange(0, 100)
        self.main_control_tail_opacity_slider.setToolTip("Default opacity at the faded end of inherited popup control gradients.")
        self.main_control_tail_opacity_value = QLabel("0%")
        self.main_control_tail_opacity_value.setToolTip(self.main_control_tail_opacity_slider.toolTip())
        tail_row = QHBoxLayout()
        tail_row.setContentsMargins(0, 0, 0, 0)
        tail_row.setSpacing(4)
        tail_row.addWidget(self.main_control_tail_opacity_slider, 1)
        tail_row.addWidget(self.main_control_tail_opacity_value, 0)
        tail_wrap = QWidget()
        tail_wrap.setLayout(tail_row)
        defaults_form.addRow("End Opacity", tail_wrap)

        layout.addLayout(defaults_form)
        layout.addStretch(1)

        self.theme_preset_combo.currentTextChanged.connect(self.on_theme_preset_selected)
        self.theme_preset_new.clicked.connect(self.create_theme_preset)
        self.theme_preset_save.clicked.connect(self.save_theme_preset)
        self.reset_theme_btn.clicked.connect(self.reset_theme)
        self.image_layers_btn.clicked.connect(lambda _checked=False: self.open_image_layers_dialog("main"))
        self.theme_transparent_bg_check.toggled.connect(self.on_theme_page_background_option_changed)
        self.popup_auto_reinherit_check.toggled.connect(self.on_popup_auto_reinherit_changed)
        self.main_control_style_combo.currentTextChanged.connect(lambda _text: self.on_main_popup_control_changed())
        self.main_control_fade_slider.valueChanged.connect(lambda _value: self.on_main_popup_control_changed())
        self.main_control_opacity_slider.valueChanged.connect(lambda _value: self.on_main_popup_control_changed())
        self.main_control_tail_opacity_slider.valueChanged.connect(lambda _value: self.on_main_popup_control_changed())
        return page

    def _build_agent_theme_tab(self) -> QWidget:
        return self._build_popup_theme_tab("agent", "Agent Window")

    def _build_qa_theme_tab(self) -> QWidget:
        return self._build_popup_theme_tab("qa", "QA/WCS Window")

    def _build_admin_theme_tab(self) -> QWidget:
        return self._build_popup_theme_tab("admin", "Admin Window")

    def _build_dashboard_theme_tab(self) -> QWidget:
        return self._build_popup_theme_tab("dashboard", "Dashboard Window")

    @staticmethod
    def _popup_window_on_top_config_key(kind: str) -> str:
        return {
            "agent": "agent_window_always_on_top",
            "qa": "qa_window_always_on_top",
            "admin": "admin_window_always_on_top",
            "dashboard": "dashboard_window_always_on_top",
        }.get(str(kind or "").strip().lower(), "")

    def _popup_window_for_kind(self, kind: str) -> DepotFramelessToolWindow | None:
        normalized = str(kind or "").strip().lower()
        manager = getattr(self, "window_manager", None)
        if manager is not None:
            window = manager.get_window(normalized)
            if window is not None:
                return window
        if normalized == "agent":
            return self.active_agent_window
        if normalized == "qa":
            return self.active_qa_window
        if normalized == "admin":
            return self.admin_dialog
        if normalized == "dashboard":
            return self.depot_dashboard_dialog
        return None

    def _apply_popup_window_on_top_preference(self, kind: str, enabled: bool) -> bool:
        keep_on_top = bool(enabled)
        config_key = self._popup_window_on_top_config_key(kind)
        if not config_key:
            return keep_on_top
        self.config[config_key] = keep_on_top
        dialog = self._popup_window_for_kind(kind)
        if dialog is not None:
            dialog._window_always_on_top = keep_on_top
            if hasattr(dialog, "_always_on_top_config_key"):
                dialog._always_on_top_config_key = config_key
            dialog.set_window_always_on_top(keep_on_top)
        return keep_on_top

    def _build_popup_theme_tab(self, kind: str, label: str) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        title = QLabel(f"{label} Theme")
        title.setProperty("section", True)
        layout.addWidget(title)

        preset_combo_key = f"{kind}_theme_preset_combo"
        preset_row = QHBoxLayout()
        preset_row.setSpacing(4)
        preset_row.addWidget(QLabel("Preset"), 0)
        preset_combo = QComboBox()
        preset_combo.setToolTip(f"Select a preset to seed {label.lower()} colors.")
        setattr(self, preset_combo_key, preset_combo)
        preset_row.addWidget(preset_combo, 1)
        layout.addLayout(preset_row)

        colors_grid = QGridLayout()
        colors_grid.setHorizontalSpacing(8)
        colors_grid.setVerticalSpacing(8)
        colors_grid.setColumnStretch(1, 1)

        swatches_key = f"{kind}_color_swatches"
        if not hasattr(self, swatches_key):
            setattr(self, swatches_key, {})
        color_swatches = getattr(self, swatches_key)

        for row_index, (fld, fld_label) in enumerate((("background", "Background"), ("text", "Text"), ("field_bg", "Controls"))):
            text = QLabel(fld_label)
            text.setMinimumWidth(96)
            swatch = QPushButton()
            swatch.setProperty("actionRole", "pick")
            swatch.setFixedSize(180, 32)
            swatch.setToolTip(f"Pick the {fld_label.lower()} color for this popup.")

            colors_grid.addWidget(text, row_index, 0)
            colors_grid.addWidget(swatch, row_index, 1)

            color_swatches[fld] = swatch
            swatch.clicked.connect(lambda _=False, k=kind, f=fld: self._pick_popup_theme_color(k, f))

        layout.addLayout(colors_grid)

        transparent_check_key = f"{kind}_transparent_bg_check"
        transparent_check = QCheckBox("Transparent Background")
        transparent_check.setToolTip("Make container/frame surfaces transparent while keeping controls and lists readable.")
        setattr(self, transparent_check_key, transparent_check)
        layout.addWidget(transparent_check)

        transparent_check.toggled.connect(lambda checked: self.on_popup_background_option_changed(kind, checked))

        always_on_top_key = f"{kind}_window_always_on_top_check"
        always_on_top_check = QCheckBox("Keep this window always on top")
        always_on_top_check.setToolTip(f"Persist whether the {label.lower()} stays above normal windows when opened.")
        setattr(self, always_on_top_key, always_on_top_check)
        layout.addWidget(always_on_top_check)

        always_on_top_check.toggled.connect(lambda checked: self.on_popup_window_always_on_top_changed(kind, checked))

        control_form = QFormLayout()
        control_form.setContentsMargins(0, 0, 0, 0)
        control_form.setSpacing(6)

        if kind == "agent":
            compact_anchor_key = f"{kind}_compact_anchor_combo"
            compact_anchor_combo = QComboBox()
            compact_anchor_combo.addItems(
                ["TopLeft", "Top", "TopRight", "Left", "Center", "Right", "BottomLeft", "Bottom", "BottomRight"]
            )
            compact_anchor_combo.setToolTip("Anchor used when the Agent Work tab toggles compact submission mode.")
            setattr(self, compact_anchor_key, compact_anchor_combo)
            control_form.addRow("Compact Anchor", compact_anchor_combo)
            compact_anchor_combo.currentTextChanged.connect(self.on_agent_compact_anchor_changed)

        style_combo_key = f"{kind}_control_style_combo"
        style_combo = QComboBox()
        style_combo.addItems(["Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"])
        style_combo.setToolTip("Choose how input/table control backgrounds are filled.")
        setattr(self, style_combo_key, style_combo)
        control_form.addRow("Fill Style", style_combo)

        fade_slider_key = f"{kind}_control_fade_slider"
        fade_value_key = f"{kind}_control_fade_value"
        fade_slider = QSlider(Qt.Orientation.Horizontal)
        fade_slider.setRange(0, 100)
        fade_slider.setToolTip("How strongly the control fill gradient fades.")
        fade_value = QLabel("65%")
        fade_row = QHBoxLayout()
        fade_row.setContentsMargins(0, 0, 0, 0)
        fade_row.setSpacing(4)
        fade_row.addWidget(fade_slider, 1)
        fade_row.addWidget(fade_value, 0)
        fade_wrap = QWidget()
        fade_wrap.setLayout(fade_row)
        setattr(self, fade_slider_key, fade_slider)
        setattr(self, fade_value_key, fade_value)
        control_form.addRow("Fade", fade_wrap)

        opacity_slider_key = f"{kind}_control_opacity_slider"
        opacity_value_key = f"{kind}_control_opacity_value"
        opacity_slider = QSlider(Qt.Orientation.Horizontal)
        opacity_slider.setRange(0, 100)
        opacity_slider.setToolTip("Primary opacity of controls (inputs, lists, table cells).")
        opacity_value = QLabel("82%")
        opacity_row = QHBoxLayout()
        opacity_row.setContentsMargins(0, 0, 0, 0)
        opacity_row.setSpacing(4)
        opacity_row.addWidget(opacity_slider, 1)
        opacity_row.addWidget(opacity_value, 0)
        opacity_wrap = QWidget()
        opacity_wrap.setLayout(opacity_row)
        setattr(self, opacity_slider_key, opacity_slider)
        setattr(self, opacity_value_key, opacity_value)
        control_form.addRow("Opacity", opacity_wrap)

        tail_slider_key = f"{kind}_control_tail_opacity_slider"
        tail_value_key = f"{kind}_control_tail_opacity_value"
        tail_slider = QSlider(Qt.Orientation.Horizontal)
        tail_slider.setRange(0, 100)
        tail_slider.setToolTip("Opacity at the end of the gradient fill (higher means less fade-out).")
        tail_value = QLabel("0%")
        tail_row = QHBoxLayout()
        tail_row.setContentsMargins(0, 0, 0, 0)
        tail_row.setSpacing(4)
        tail_row.addWidget(tail_slider, 1)
        tail_row.addWidget(tail_value, 0)
        tail_wrap = QWidget()
        tail_wrap.setLayout(tail_row)
        setattr(self, tail_slider_key, tail_slider)
        setattr(self, tail_value_key, tail_value)
        control_form.addRow("End Opacity", tail_wrap)

        optional_swatches_key = f"{kind}_optional_color_swatches"
        setattr(self, optional_swatches_key, {})
        optional_swatches = getattr(self, optional_swatches_key)
        for field, row_label in (
            ("header_color", "List Header"),
            ("row_hover_color", "Row Hover"),
            ("row_selected_color", "Row Selected"),
        ):
            swatch = QPushButton()
            swatch.setProperty("actionRole", "pick")
            swatch.setFixedHeight(28)
            swatch.setToolTip(f"Override {row_label.lower()} color for this popup.")
            clear_btn = QPushButton("Auto")
            clear_btn.setProperty("actionRole", "reset")
            clear_btn.setFixedHeight(28)
            clear_btn.setToolTip(f"Use automatic {row_label.lower()} color based on current theme.")
            color_row = QHBoxLayout()
            color_row.setContentsMargins(0, 0, 0, 0)
            color_row.setSpacing(4)
            color_row.addWidget(swatch, 1)
            color_row.addWidget(clear_btn, 0)
            color_wrap = QWidget()
            color_wrap.setLayout(color_row)
            control_form.addRow(row_label, color_wrap)
            optional_swatches[field] = swatch
            swatch.clicked.connect(lambda _=False, k=kind, f=field: self._pick_popup_optional_color(k, f))
            clear_btn.clicked.connect(lambda _=False, k=kind, f=field: self._clear_popup_optional_color(k, f))

        layout.addLayout(control_form)

        image_row = QHBoxLayout()
        image_btn = QPushButton("Background Images")
        image_btn.setProperty("actionRole", "pick")
        image_btn.setToolTip(f"Edit background image layers for the {label.lower()}.")
        image_row.addWidget(image_btn)
        image_row.addStretch(1)
        layout.addLayout(image_row)

        image_btn.clicked.connect(lambda: self.open_image_layers_dialog(kind))
        preset_combo.currentTextChanged.connect(lambda name, k=kind: self.on_popup_theme_preset_selected(k, name))
        style_combo.currentTextChanged.connect(lambda _value, k=kind: self.on_popup_theme_control_changed(k))
        fade_slider.valueChanged.connect(lambda _value, k=kind: self.on_popup_theme_control_changed(k))
        opacity_slider.valueChanged.connect(lambda _value, k=kind: self.on_popup_theme_control_changed(k))
        tail_slider.valueChanged.connect(lambda _value, k=kind: self.on_popup_theme_control_changed(k))
        
        layout.addStretch(1)
        return page

    def _build_app_settings_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        form = QFormLayout()
        form.setContentsMargins(0, 0, 0, 0)
        form.setSpacing(8)

        self.opacity_slider = QSlider(Qt.Orientation.Horizontal)
        self.opacity_slider.setRange(0, 100)
        opacity_tip = "Base opacity for the Flowgrid shell while idle. Lower values make the shell more transparent."
        self.opacity_slider.setToolTip(opacity_tip)
        self.opacity_value = QLabel("1.00")
        self.opacity_value.setToolTip(opacity_tip)
        opacity_row = QHBoxLayout()
        opacity_row.addWidget(self.opacity_slider, 1)
        opacity_row.addWidget(self.opacity_value)
        opacity_wrap = QWidget()
        opacity_wrap.setLayout(opacity_row)

        self.hover_delay_slider = QSlider(Qt.Orientation.Horizontal)
        self.hover_delay_slider.setRange(0, 10)
        hover_delay_tip = "Seconds to wait before auto-revealing full opacity when your cursor enters the window."
        self.hover_delay_slider.setToolTip(hover_delay_tip)
        self.hover_delay_value = QLabel("5s")
        self.hover_delay_value.setToolTip(hover_delay_tip)
        hover_delay_row = QHBoxLayout()
        hover_delay_row.addWidget(self.hover_delay_slider, 1)
        hover_delay_row.addWidget(self.hover_delay_value)
        hover_delay_wrap = QWidget()
        hover_delay_wrap.setLayout(hover_delay_row)

        self.hover_fade_in_slider = QSlider(Qt.Orientation.Horizontal)
        self.hover_fade_in_slider.setRange(0, 10)
        hover_in_tip = "How quickly the shell fades up to full opacity on hover."
        self.hover_fade_in_slider.setToolTip(hover_in_tip)
        self.hover_fade_in_value = QLabel("5s")
        self.hover_fade_in_value.setToolTip(hover_in_tip)
        hover_fade_in_row = QHBoxLayout()
        hover_fade_in_row.addWidget(self.hover_fade_in_slider, 1)
        hover_fade_in_row.addWidget(self.hover_fade_in_value)
        hover_fade_in_wrap = QWidget()
        hover_fade_in_wrap.setLayout(hover_fade_in_row)

        self.hover_fade_out_slider = QSlider(Qt.Orientation.Horizontal)
        self.hover_fade_out_slider.setRange(0, 10)
        hover_out_tip = "How quickly the shell returns to idle opacity after hover ends."
        self.hover_fade_out_slider.setToolTip(hover_out_tip)
        self.hover_fade_out_value = QLabel("5s")
        self.hover_fade_out_value.setToolTip(hover_out_tip)
        hover_fade_out_row = QHBoxLayout()
        hover_fade_out_row.addWidget(self.hover_fade_out_slider, 1)
        hover_fade_out_row.addWidget(self.hover_fade_out_value)
        hover_fade_out_wrap = QWidget()
        hover_fade_out_wrap.setLayout(hover_fade_out_row)

        form.addRow("Idle Opacity", opacity_wrap)
        form.addRow("Hover Delay", hover_delay_wrap)
        form.addRow("Fade In", hover_fade_in_wrap)
        form.addRow("Fade Out", hover_fade_out_wrap)
        layout.addLayout(form)

        self.always_on_top_check = QCheckBox("Always on top")
        self.always_on_top_check.setToolTip("Keep the main Flowgrid shell above normal windows.")
        self.sidebar_right_switch = QCheckBox()
        self.sidebar_right_switch.setProperty("switch", True)
        self.sidebar_right_switch.setTristate(False)
        self.sidebar_right_switch.setToolTip("Move the sidebar to the right side of the main Flowgrid window.")
        self.sidebar_switch_status = QLabel()
        self.sidebar_switch_status.setProperty("muted", True)
        self.sidebar_switch_status.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        self.sidebar_switch_status.setMinimumWidth(170)
        layout.addWidget(self.always_on_top_check)
        sidebar_row = QHBoxLayout()
        sidebar_row.setContentsMargins(0, 0, 0, 0)
        sidebar_row.setSpacing(6)
        self.sidebar_side_caption = QLabel("Sidebar Side")
        self.sidebar_side_caption.setMinimumWidth(90)
        self.sidebar_left_label = QLabel("Left")
        self.sidebar_left_label.setProperty("muted", True)
        self.sidebar_right_label = QLabel("Right")
        self.sidebar_right_label.setProperty("muted", True)
        self.sidebar_switch_status.setToolTip("Shows the active sidebar side.")
        sidebar_row.addWidget(self.sidebar_side_caption, 0)
        sidebar_row.addWidget(self.sidebar_left_label, 0)
        sidebar_row.addWidget(self.sidebar_right_switch, 0)
        sidebar_row.addWidget(self.sidebar_right_label, 0)
        sidebar_row.addStretch(1)
        layout.addLayout(sidebar_row)
        layout.addWidget(self.sidebar_switch_status)

        icon_row = QHBoxLayout()
        self.pick_icon_button = QPushButton("Set Icon")
        self.clear_icon_button = QPushButton("Clear Icon")
        self.pick_icon_button.setToolTip("Pick a custom icon for the main Flowgrid title bar and desktop shortcut.")
        self.clear_icon_button.setToolTip("Reset the Flowgrid title bar and desktop shortcut back to the default wrench icon.")
        self.pick_icon_button.setProperty("actionRole", "pick")
        self.clear_icon_button.setProperty("actionRole", "reset")
        icon_row.addWidget(self.pick_icon_button)
        icon_row.addWidget(self.clear_icon_button)
        icon_row.addStretch(1)
        layout.addLayout(icon_row)
        layout.addStretch(1)

        self.opacity_slider.valueChanged.connect(self.on_opacity_changed)
        self.hover_delay_slider.valueChanged.connect(self.on_hover_settings_changed)
        self.hover_fade_in_slider.valueChanged.connect(self.on_hover_settings_changed)
        self.hover_fade_out_slider.valueChanged.connect(self.on_hover_settings_changed)
        self.always_on_top_check.toggled.connect(self.on_settings_changed)
        self.sidebar_right_switch.toggled.connect(self.on_sidebar_position_changed)
        self.pick_icon_button.clicked.connect(self.pick_custom_icon)
        self.clear_icon_button.clicked.connect(self.clear_custom_icon)
        return tab

    def _build_updates_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        updates_title = QLabel("Updates")
        updates_title.setProperty("section", True)
        layout.addWidget(updates_title)

        updates_form = QFormLayout()
        updates_form.setContentsMargins(0, 0, 0, 0)
        updates_form.setHorizontalSpacing(8)
        updates_form.setVerticalSpacing(7)
        updates_form.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop)

        self.channel_mode_status_label = QLabel("")
        self.channel_mode_status_label.setProperty("muted", True)
        self.channel_mode_status_label.setWordWrap(True)

        self.update_commit_status_label = QLabel("-")
        self.update_commit_status_label.setProperty("muted", True)
        self.update_check_status_label = QLabel("Not checked yet.")
        self.update_check_status_label.setProperty("muted", True)
        self.update_check_status_label.setWordWrap(True)
        self.shared_assets_status_label = QLabel("Not synced yet.")
        self.shared_assets_status_label.setProperty("muted", True)
        self.shared_assets_status_label.setWordWrap(True)
        updates_form.addRow("Channel", self.channel_mode_status_label)
        updates_form.addRow("Installed", self.update_commit_status_label)
        updates_form.addRow("Update", self.update_check_status_label)
        updates_form.addRow("Assets", self.shared_assets_status_label)
        layout.addLayout(updates_form)

        comments_title = QLabel("Last Commit Comments")
        comments_title.setProperty("section", True)
        layout.addWidget(comments_title)

        self.last_commit_comments_label = QLabel("Not available until an update check records commit metadata.")
        self.last_commit_comments_label.setProperty("muted", True)
        self.last_commit_comments_label.setWordWrap(True)
        self.last_commit_comments_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.last_commit_comments_label.setMinimumHeight(62)
        self.last_commit_comments_label.setMaximumHeight(94)
        self.last_commit_comments_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        layout.addWidget(self.last_commit_comments_label)

        update_actions_grid = QGridLayout()
        update_actions_grid.setContentsMargins(0, 0, 0, 0)
        update_actions_grid.setHorizontalSpacing(6)
        update_actions_grid.setVerticalSpacing(6)
        update_actions_grid.setColumnStretch(0, 1)
        update_actions_grid.setColumnStretch(1, 1)
        self.check_updates_button = QPushButton("Check for Updates")
        self.install_update_button = QPushButton("Install Update")
        self.pull_shared_assets_button = QPushButton("Pull Shared Assets")
        self.check_updates_button.setProperty("actionRole", "pick")
        self.install_update_button.setProperty("actionRole", "save")
        self.pull_shared_assets_button.setProperty("actionRole", "pick")
        self.install_update_button.setEnabled(False)
        update_actions_grid.addWidget(self.check_updates_button, 0, 0)
        update_actions_grid.addWidget(self.install_update_button, 0, 1)
        update_actions_grid.addWidget(self.pull_shared_assets_button, 1, 0, 1, 2)
        layout.addLayout(update_actions_grid)
        layout.addStretch(1)

        self.check_updates_button.clicked.connect(self.on_check_updates_clicked)
        self.install_update_button.clicked.connect(self.on_install_update_clicked)
        self.pull_shared_assets_button.clicked.connect(self.on_pull_shared_assets_clicked)
        return tab

    def _shell_window_title(self) -> str:
        title = str(self.channel_display_name or APP_TITLE).strip() or APP_TITLE
        if self.runtime_options.read_only_db:
            return f"{title} [READ ONLY]"
        return title

    def _shell_mode_chip_text(self) -> str:
        channel_label = str(self.runtime_options.channel_label or "").strip()
        channel_id = str(self.runtime_options.channel_id or "").strip().lower()
        if self.runtime_options.read_only_db and channel_label:
            return f"{channel_label.upper()} READ ONLY"
        if self.runtime_options.read_only_db:
            return "READ ONLY"
        if channel_id and channel_id != "main":
            return channel_label.upper() if channel_label else channel_id.upper()
        return ""

    def _default_update_source_label(self) -> str:
        branch = str(self.runtime_options.branch or "main").strip() or "main"
        return f"GitHub {branch}"

    def _load_local_git_commit_comments(self) -> str:
        if self._local_commit_comments_cache_loaded:
            return self._local_commit_comments_cache
        self._local_commit_comments_cache_loaded = True

        repo_root = _local_data_root()
        if not (repo_root / ".git").exists():
            self._local_commit_comments_cache = ""
            return ""

        run_kwargs: dict[str, Any] = {
            "cwd": str(repo_root),
            "capture_output": True,
            "text": True,
            "timeout": 2.5,
        }
        if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            run_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

        try:
            completed = subprocess.run(["git", "log", "-1", "--format=%s%n%b"], **run_kwargs)
        except Exception as exc:
            if not self._local_commit_comments_warning_logged:
                self._local_commit_comments_warning_logged = True
                _runtime_log_event(
                    "update.local_git_commit_comments_failed",
                    severity="warning",
                    summary="Failed reading local Git commit comments for the Updates tab.",
                    exc=exc,
                    context={"repo_root": str(repo_root)},
                )
            self._local_commit_comments_cache = ""
            return ""

        if int(completed.returncode) != 0:
            if not self._local_commit_comments_warning_logged:
                self._local_commit_comments_warning_logged = True
                _runtime_log_event(
                    "update.local_git_commit_comments_failed",
                    severity="warning",
                    summary="Local Git commit comments command returned a non-zero exit code.",
                    context={
                        "repo_root": str(repo_root),
                        "returncode": int(completed.returncode),
                        "stderr": str(completed.stderr or "").strip()[:800],
                    },
                )
            self._local_commit_comments_cache = ""
            return ""

        self._local_commit_comments_cache = str(completed.stdout or "").strip()
        return self._local_commit_comments_cache

    def _update_commit_comments_text(self) -> str:
        local_comments = self._load_local_git_commit_comments()
        if local_comments:
            return local_comments

        remote_comments = str(self._install_status_cache.get("last_remote_commit_message") or "").strip()
        if remote_comments:
            return remote_comments

        installed_comments = str(self._install_status_cache.get("installed_commit_message") or "").strip()
        if installed_comments:
            return installed_comments

        return "Not available until an update check records commit metadata."

    @staticmethod
    def _compact_update_comments(text: str, *, max_lines: int = 5, max_chars: int = 420) -> str:
        normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not normalized:
            return "Not available until an update check records commit metadata."

        lines = [line.rstrip() for line in normalized.splitlines()]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()

        truncated = False
        if len(lines) > max_lines:
            lines = lines[:max_lines]
            truncated = True
        compact = "\n".join(lines).strip()
        if len(compact) > max_chars:
            compact = compact[: max(0, max_chars - 3)].rstrip()
            truncated = True
        if truncated:
            compact = f"{compact}\n..."
        return compact or "Not available until an update check records commit metadata."

    def _build_settings_page(self) -> QWidget:
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(8, 6, 8, 8)
        layout.setSpacing(8)

        self.settings_tabs = QTabWidget()
        self.settings_tabs.setObjectName("SettingsTabs")
        self.settings_tabs.setUsesScrollButtons(True)
        main_tab_bar = self.settings_tabs.tabBar()
        main_tab_bar.setObjectName("SettingsMainTabBar")
        main_tab_bar.setElideMode(Qt.TextElideMode.ElideNone)
        self.settings_tabs.addTab(self._build_app_settings_tab(), "Settings")
        self.settings_tabs.addTab(self._build_updates_tab(), "Updates")
        self.settings_tabs.addTab(self._build_theme_page(), "Themes")
        self.settings_tabs.setTabIcon(0, self._resolve_standard_icon("SP_FileDialogDetailedView", "SP_FileIcon"))
        self.settings_tabs.setTabIcon(1, self._resolve_standard_icon("SP_BrowserReload", "SP_FileDialogInfoView"))
        self.settings_tabs.setTabIcon(2, self._resolve_standard_icon("SP_DirOpenIcon", "SP_FileIcon"))
        self.settings_tabs.setTabToolTip(0, "General app behavior and window controls.")
        self.settings_tabs.setTabToolTip(1, "Update status, commit notes, and shared asset sync.")
        self.settings_tabs.setTabToolTip(2, "Theme colors, backgrounds, and popup styling.")
        layout.addWidget(self.settings_tabs, 1)
        return page

    def _quick_button_style_tokens(self, font_size: int, button_opacity: float, shape: str, action_type: str = QUICK_ACTION_INPUT_SEQUENCE) -> tuple[int, int, str, str, str, str]:
        font_size = int(clamp(font_size, 8, 20))
        button_opacity = float(clamp(button_opacity, 0.15, 1.0))
        palette = self.palette_data

        # Adjust base color based on action type for subtle distinction
        base_bg = palette["button_bg"]
        if action_type == QUICK_ACTION_OPEN_URL:
            # Shift toward accent color for web links
            base_bg = blend(palette['button_bg'], palette['accent'], 0.25)
        elif action_type == QUICK_ACTION_OPEN_APP:
            # Shift toward primary color for apps
            base_bg = blend(palette['button_bg'], palette['primary'], 0.25)
        elif action_type in {QUICK_ACTION_INPUT_SEQUENCE, "macro_sequence", "paste_text"}:
            # Shift toward surface color for input sequences.
            base_bg = blend(palette['button_bg'], palette['surface'], 0.35)

        radius = 11
        border = 1
        bg_hex = base_bg
        bg_alpha = button_opacity
        border_color = shift(bg_hex, -0.35)
        text_color = readable_text(bg_hex)
        hover_alpha = min(1.0, bg_alpha + 0.08)

        if shape == "Bordered":
            radius = 4
            border = 2
            bg_hex = base_bg
            bg_alpha = min(1.0, button_opacity * 0.92)
            border_color = shift(bg_hex, -0.45)
        elif shape == "Block":
            radius = 0
            border = 1
            bg_hex = shift(base_bg, -0.08)
            bg_alpha = button_opacity
            border_color = shift(bg_hex, -0.45)
        elif shape == "Pill":
            radius = 999
            border = 1
            bg_hex = base_bg
            bg_alpha = button_opacity
            border_color = shift(bg_hex, -0.42)
        elif shape == "Ghost":
            radius = 10
            border = 2
            bg_hex = base_bg
            bg_alpha = max(0.15, button_opacity * 0.32)
            border_color = shift(bg_hex, -0.62)
            hover_alpha = max(0.45, min(1.0, bg_alpha + 0.22))
        elif shape == "Glass":
            radius = 12
            border = 1
            bg_hex = blend(palette["surface"], palette["button_bg"], 0.48)
            bg_alpha = max(0.35, button_opacity * 0.75)
            border_color = shift(bg_hex, -0.45)
        elif shape == "Outline":
            radius = 9
            border = 2
            bg_hex = palette["surface"]
            bg_alpha = max(0.10, button_opacity * 0.12)
            border_color = shift(palette["button_bg"], -0.68)
            text_color = readable_text(shift(palette["button_bg"], -0.15))
            hover_alpha = max(0.42, min(1.0, button_opacity * 0.55))
        elif shape == "Inset":
            radius = 8
            border = 2
            bg_hex = shift(palette["button_bg"], -0.18)
            bg_alpha = button_opacity
            border_color = shift(bg_hex, -0.42)
        elif shape == "Flat":
            radius = 2
            border = 0
            bg_hex = blend(palette["button_bg"], palette["surface"], 0.18)
            bg_alpha = button_opacity
            border_color = bg_hex
            text_color = readable_text(bg_hex)
        elif shape == "Raised3D":
            radius = 10
            border = 2
            top = shift(base_bg, 0.20)
            bottom = shift(base_bg, -0.20)
            bg_hex = base_bg
            shape_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(top, min(1.0, button_opacity))},"
                f" stop:1 {rgba_css(bottom, min(1.0, button_opacity))})"
            )
            border_color = shift(bottom, -0.35)
            text_color = readable_text(bottom)
            hover_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(shift(top, 0.06), min(1.0, button_opacity))},"
                f" stop:1 {rgba_css(shift(bottom, 0.06), min(1.0, button_opacity))})"
            )
            return radius, border, shape_bg, border_color, hover_bg, text_color
        elif shape == "Bevel3D":
            radius = 8
            border = 2
            top = shift(base_bg, 0.12)
            bottom = shift(base_bg, -0.30)
            bg_hex = blend(top, bottom, 0.55)
            shape_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(top, button_opacity)},"
                f" stop:0.52 {rgba_css(bg_hex, button_opacity)},"
                f" stop:1 {rgba_css(bottom, button_opacity)})"
            )
            border_color = shift(bottom, -0.40)
            text_color = readable_text(bg_hex)
            hover_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(shift(top, 0.06), button_opacity)},"
                f" stop:0.52 {rgba_css(shift(bg_hex, 0.05), button_opacity)},"
                f" stop:1 {rgba_css(shift(bottom, 0.05), button_opacity)})"
            )
            return radius, border, shape_bg, border_color, hover_bg, text_color
        elif shape == "Ridge3D":
            radius = 6
            border = 2
            c1 = shift(base_bg, 0.20)
            c2 = shift(base_bg, -0.08)
            c3 = shift(base_bg, 0.08)
            c4 = shift(base_bg, -0.28)
            bg_hex = c2
            shape_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(c1, button_opacity)},"
                f" stop:0.33 {rgba_css(c2, button_opacity)},"
                f" stop:0.66 {rgba_css(c3, button_opacity)},"
                f" stop:1 {rgba_css(c4, button_opacity)})"
            )
            border_color = shift(c4, -0.35)
            text_color = readable_text(c2)
            hover_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(shift(c1, 0.05), button_opacity)},"
                f" stop:0.33 {rgba_css(shift(c2, 0.05), button_opacity)},"
                f" stop:0.66 {rgba_css(shift(c3, 0.05), button_opacity)},"
                f" stop:1 {rgba_css(shift(c4, 0.05), button_opacity)})"
            )
            return radius, border, shape_bg, border_color, hover_bg, text_color
        elif shape == "Neumorph":
            radius = 14
            border = 1
            bg_hex = blend(palette["surface"], palette["button_bg"], 0.30)
            bg_alpha = max(0.55, button_opacity * 0.85)
            border_color = shift(bg_hex, -0.22)
            text_color = readable_text(bg_hex)
            hover_alpha = min(1.0, bg_alpha + 0.08)
        elif shape == "Retro3D":
            radius = 4
            border = 2
            top = shift(palette["button_bg"], 0.22)
            bottom = shift(palette["button_bg"], -0.30)
            bg_hex = blend(top, bottom, 0.5)
            shape_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(top, button_opacity)},"
                f" stop:1 {rgba_css(bottom, button_opacity)})"
            )
            border_color = shift(bottom, -0.38)
            text_color = readable_text(bottom)
            hover_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(shift(top, 0.06), button_opacity)},"
                f" stop:1 {rgba_css(shift(bottom, 0.06), button_opacity)})"
            )
            return radius, border, shape_bg, border_color, hover_bg, text_color
        elif shape == "Neon3D":
            radius = 10
            border = 2
            top = blend(palette["accent"], base_bg, 0.30)
            bottom = blend(palette["primary"], shift(base_bg, -0.14), 0.50)
            bg_hex = blend(top, bottom, 0.5)
            shape_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(top, min(1.0, button_opacity * 0.95))},"
                f" stop:1 {rgba_css(bottom, min(1.0, button_opacity * 0.95))})"
            )
            border_color = shift(bottom, -0.45)
            text_color = readable_text(bottom)
            hover_bg = (
                "qlineargradient(x1:0,y1:0,x2:0,y2:1,"
                f" stop:0 {rgba_css(shift(top, 0.08), min(1.0, button_opacity))},"
                f" stop:1 {rgba_css(shift(bottom, 0.08), min(1.0, button_opacity))})"
            )
            return radius, border, shape_bg, border_color, hover_bg, text_color
        # Backward compatibility with older saved style names.
        elif shape in {"Neon", "Ocean"}:
            shape = "Neon3D"
            return self._quick_button_style_tokens(font_size, button_opacity, shape)
        elif shape in {"Retro", "Ember"}:
            shape = "Retro3D"
            return self._quick_button_style_tokens(font_size, button_opacity, shape)
        elif shape in {"Royal", "Slate", "Danger", "Forest", "Candy", "Frost"}:
            shape = "Raised3D"
            return self._quick_button_style_tokens(font_size, button_opacity, shape)

        shape_bg = rgba_css(bg_hex, bg_alpha)
        hover_bg = rgba_css(shift(bg_hex, 0.08), hover_alpha)
        return radius, border, shape_bg, border_color, hover_bg, text_color

    def _quick_button_stylesheet(
        self,
        font_size: int,
        button_opacity: float,
        shape: str,
        font_family: str | None = None,
        padding: str = "2px 20px 2px 8px",
        action_type: str = QUICK_ACTION_INPUT_SEQUENCE,
    ) -> str:
        radius, border, shape_bg, border_color, hover_bg, text_color = self._quick_button_style_tokens(font_size, button_opacity, shape, action_type)
        family_value = str(font_family or self.config.get("quick_button_font_family", "Segoe UI"))
        family_value = family_value.replace("'", "\\'")
        return (
            "QPushButton {"
            f"background-color: {shape_bg};"
            f"color: {text_color};"
            f"border: {border}px solid {border_color};"
            f"border-radius: {radius}px;"
            f"font-size: {int(clamp(font_size, 8, 20))}px;"
            f"font-family: '{family_value}';"
            "font-weight: 700;"
            f"padding: {padding};"
            "text-align: center;"
            "}"
            f"QPushButton:hover {{ background-color: {hover_bg}; }}"
            f"QPushButton:pressed {{ background-color: {hover_bg}; }}"
        )

    def _refresh_theme_preview_buttons(self) -> None:
        if not hasattr(self, "theme_preset_new") or not hasattr(self, "theme_preset_save"):
            return
        font_size = int(clamp(int(self.config.get("quick_button_font_size", 11)), 8, 14))
        font_family = str(self.config.get("quick_button_font_family", "Segoe UI"))
        button_opacity = float(clamp(float(self.config.get("quick_button_opacity", 1.0)), 0.2, 1.0))
        shape = self.config.get("quick_button_shape", "Soft")
        css = self._quick_button_stylesheet(font_size, button_opacity, shape, font_family=font_family, padding="2px 8px 2px 8px")
        for preview_btn in (self.theme_preset_new, self.theme_preset_save):
            preview_btn.setStyleSheet(css)
            poly = build_quick_shape_polygon(shape, preview_btn.width(), preview_btn.height())
            if poly is None:
                preview_btn.clearMask()
            else:
                preview_btn.setMask(QRegion(poly))

    def _apply_sidebar_position(self) -> None:
        while self.body_layout.count() > 0:
            item = self.body_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.setParent(self.body)

        sidebar_on_right = bool(self.config.get("sidebar_on_right", False))
        if sidebar_on_right:
            self.body_layout.addWidget(self.pages, 1)
            self.body_layout.addWidget(self.sidebar, 0)
        else:
            self.body_layout.addWidget(self.sidebar, 0)
            self.body_layout.addWidget(self.pages, 1)

    # --------------------------- Styling ---------------------------- #
    def apply_theme_styles(self) -> None:
        p = self.palette_data
        compact_mode = bool(self.config.get("compact_mode", True))
        base_font_px = 12 if compact_mode else 13
        section_font_px = 13 if compact_mode else 14
        field_padding = "1px 5px" if compact_mode else "2px 6px"
        text_edit_padding = "3px 5px" if compact_mode else "4px 6px"
        checkbox_spacing = 6 if compact_mode else 8
        checkbox_indicator_px = 12 if compact_mode else 14
        button_padding = "2px 8px" if compact_mode else "4px 10px"
        button_min_height = 24 if compact_mode else 28
        button_font_px = 10 if compact_mode else 11
        scrollbar_width = 8 if compact_mode else 10
        scrollbar_handle_min_height = 14 if compact_mode else 18
        slider_groove_height = 5 if compact_mode else 6
        slider_handle_width = 10 if compact_mode else 12
        slider_handle_margin = "-3px 0px" if compact_mode else "-4px 0px"
        sidebar_color = QColor(p["sidebar_overlay"])
        sidebar_color.setAlpha(125)
        shared_button_bg = rgba_css(p["button_bg"], 1.0)
        shared_button_hover = rgba_css(shift(p["button_bg"], 0.08), 1.0)
        shared_button_pressed = rgba_css(shift(p["button_bg"], -0.06), 1.0)
        disabled_button_bg = rgba_css(blend(p["control_bg"], p["button_bg"], 0.18), 0.90)
        disabled_button_text = rgba_css(p["label_text"], 0.48)
        disabled_button_border = rgba_css(shift(p["control_bg"], -0.18), 0.95)
        add_base = p["primary"]
        apply_base = p["accent"]
        pick_base = blend(p["primary"], p["surface"], 0.45)
        save_base = blend(p["primary"], p["accent"], 0.22)
        new_base = blend(p["surface"], p["primary"], 0.35)
        reset_base = shift(p["surface"], -0.45)
        title_min_base = blend(p["button_bg"], p["surface"], 0.20)
        title_min_bg = rgba_css(title_min_base, 0.92)
        title_min_border = rgba_css(shift(p["accent"], -0.42), 0.95)
        title_min_hover = rgba_css(shift(title_min_base, 0.10), 1.0)
        title_min_text = readable_text(title_min_base)
        titlebar_bg = rgba_css(blend(p["shell_overlay"], p["surface"], 0.18), 0.84)
        title_badge_bg = rgba_css(blend(p["button_bg"], p["surface"], 0.15), 0.74)
        title_badge_border = rgba_css(shift(p["accent"], -0.42), 0.82)
        checkbox_fill = rgba_css(blend(p["input_bg"], p["surface"], 0.18), 0.92)
        checkbox_fill_checked = rgba_css(blend(p["accent"], p["input_bg"], 0.18), 0.96)
        checkbox_fill_disabled = rgba_css(p["input_bg"], 0.58)
        checkbox_border = shift(p["input_bg"], -0.46)

        self.surface.setStyleSheet("background: transparent;")
        self.body.setStyleSheet("background: transparent;")
        self.pages.setStyleSheet("background: transparent;")

        self.sidebar.setStyleSheet(
            "QWidget {"
            f"background: rgba({sidebar_color.red()}, {sidebar_color.green()}, {sidebar_color.blue()}, {sidebar_color.alpha()});"
            "border-radius: 0px;"
            "}"
        )

        self.titlebar.setStyleSheet(
            "QWidget {"
            f"background: {titlebar_bg};"
            f"color: {p['label_text']};"
            "}"
            "QLabel#TitleText {"
            "font-size: 13px;"
            "font-weight: 700;"
            f"background: {title_badge_bg};"
            f"border: 1px solid {title_badge_border};"
            "border-radius: 11px;"
            "padding: 2px 10px;"
            "}"
            "QToolButton {"
            "background: transparent;"
            "border: none;"
            "font-size: 13px;"
            "font-weight: 600;"
            f"color: {p['label_text']};"
            "}"
            "QToolButton#TitleMinButton {"
            f"background: {title_min_bg};"
            f"border: 1px solid {title_min_border};"
            f"color: {title_min_text};"
            "border-radius: 11px;"
            "font-size: 12px;"
            "font-weight: 800;"
            "}"
            f"QToolButton#TitleMinButton:hover {{ background: {title_min_hover}; }}"
            "QToolButton#TitleCloseButton {"
            "background: rgba(225,80,80,220);"
            "border: 1px solid rgba(255,135,135,235);"
            "color: #FFFFFF;"
            "border-radius: 11px;"
            "font-size: 12px;"
            "font-weight: 800;"
            "}"
            "QToolButton#TitleCloseButton:hover { background: rgba(235,95,95,240); }"
        )

        global_style = (
            "QWidget {"
            f"color: {p['label_text']};"
            "font-family: 'Segoe UI';"
            f"font-size: {base_font_px}px;"
            "background: transparent;"
            "}"
            "QLabel {"
            "font-weight: 700;"
            "background: transparent;"
            "}"
            "QLabel[muted='true'] {"
            "font-weight: 600;"
            f"color: {rgba_css(p['label_text'], 0.82)};"
            "}"
            "QLabel[section='true'] {"
            f"font-size: {section_font_px}px;"
            "font-weight: 700;"
            "padding-bottom: 2px;"
            "}"
            "QLineEdit, QTextEdit, QSpinBox, QComboBox {"
            f"background: rgba({QColor(p['input_bg']).red()}, {QColor(p['input_bg']).green()}, {QColor(p['input_bg']).blue()}, 228);"
            f"border: 1px solid {shift(p['input_bg'], -0.38)};"
            "border-radius: 3px;"
            f"padding: {field_padding};"
            f"selection-background-color: {p['primary']};"
            f"selection-color: {readable_text(p['primary'])};"
            "}"
            f"QTextEdit {{ padding: {text_edit_padding}; }}"
            f"QCheckBox {{ background: transparent; spacing: {checkbox_spacing}px; font-weight: 700; }}"
            f"QCheckBox::indicator {{ width: {checkbox_indicator_px}px; height: {checkbox_indicator_px}px; border-radius: 3px; background: {checkbox_fill}; border: 1px solid {checkbox_border}; }}"
            f"QCheckBox::indicator:checked {{ background: {checkbox_fill_checked}; border: 1px solid {shift(p['accent'], -0.45)}; }}"
            f"QCheckBox::indicator:disabled {{ background: {checkbox_fill_disabled}; border: 1px solid {shift(checkbox_border, 0.08)}; }}"
            "QCheckBox[switch='true']::indicator {"
            "width: 44px;"
            "height: 20px;"
            "border-radius: 10px;"
            f"background: {rgba_css(p['button_bg'], 0.55)};"
            f"border: 1px solid {shift(p['button_bg'], -0.40)};"
            "}"
            "QCheckBox[switch='true']::indicator:unchecked {"
            f"background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 {rgba_css(p['accent'], 0.62)}, stop:0.45 {rgba_css(p['button_bg'], 0.35)}, stop:1 {rgba_css(p['button_bg'], 0.25)});"
            "}"
            "QCheckBox[switch='true']::indicator:checked {"
            f"background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 {rgba_css(p['button_bg'], 0.25)}, stop:0.55 {rgba_css(p['button_bg'], 0.35)}, stop:1 {rgba_css(p['accent'], 0.70)});"
            f"border: 1px solid {shift(p['accent'], -0.45)};"
            "}"
            "QTabBar#SettingsMainTabBar::tab {"
            "font-weight: 800;"
            "}"
            "QScrollArea { background: transparent; border: none; }"
            "QScrollArea > QWidget > QWidget { background: transparent; }"
            "QScrollBar:vertical {"
            "background: rgba(0,0,0,40);"
            f"width: {scrollbar_width}px;"
            "margin: 0px;"
            "border: none;"
            "}"
            "QScrollBar::handle:vertical {"
            "background: rgba(255,255,255,95);"
            f"min-height: {scrollbar_handle_min_height}px;"
            "border-radius: 5px;"
            "}"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {"
            "height: 0px;"
            "}"
            "QSlider::groove:horizontal {"
            "background: rgba(0,0,0,90);"
            f"height: {slider_groove_height}px;"
            "border-radius: 3px;"
            "}"
            "QSlider::handle:horizontal {"
            f"background: {p['accent']};"
            f"width: {slider_handle_width}px;"
            f"margin: {slider_handle_margin};"
            "border-radius: 3px;"
            "}"
            "QPushButton {"
            f"background-color: {shared_button_bg};"
            f"color: {p['button_text']};"
            f"border: 1px solid {shift(p['button_bg'], -0.40)};"
            "border-radius: 4px;"
            f"padding: {button_padding};"
            f"min-height: {button_min_height}px;"
            f"font-size: {button_font_px}px;"
            "font-weight: 700;"
            "}"
            f"QPushButton:hover {{ background-color: {shared_button_hover}; }}"
            f"QPushButton:pressed {{ background-color: {shared_button_pressed}; }}"
            f"QPushButton:checked {{ background-color: {rgba_css(p['accent'], 1.0)}; color: {readable_text(p['accent'])}; border: 1px solid {shift(p['accent'], -0.42)}; }}"
            "QPushButton[actionRole='add'] {"
            f"background-color: {rgba_css(add_base, 1.0)};"
            f"color: {readable_text(add_base)};"
            f"border: 1px solid {shift(add_base, -0.42)};"
            "}"
            "QPushButton[actionRole='apply'] {"
            f"background-color: {rgba_css(apply_base, 1.0)};"
            f"color: {readable_text(apply_base)};"
            f"border: 1px solid {shift(apply_base, -0.42)};"
            "}"
            "QPushButton[actionRole='pick'] {"
            f"background-color: {rgba_css(pick_base, 1.0)};"
            f"color: {readable_text(pick_base)};"
            f"border: 1px solid {shift(pick_base, -0.42)};"
            "}"
            "QPushButton[actionRole='new'] {"
            f"background-color: {rgba_css(new_base, 1.0)};"
            f"color: {readable_text(new_base)};"
            f"border: 1px solid {shift(new_base, -0.42)};"
            "}"
            "QPushButton[actionRole='save'] {"
            f"background-color: {rgba_css(save_base, 1.0)};"
            f"color: {readable_text(save_base)};"
            f"border: 1px solid {shift(save_base, -0.42)};"
            "}"
            "QPushButton[actionRole='reset'] {"
            f"background-color: {rgba_css(reset_base, 1.0)};"
            f"color: {readable_text(reset_base)};"
            f"border: 1px solid {shift(reset_base, -0.42)};"
            "}"
            "QPushButton:disabled {"
            f"background-color: {disabled_button_bg};"
            f"color: {disabled_button_text};"
            f"border: 1px solid {disabled_button_border};"
            "}"
        )
        self.setStyleSheet(global_style)
        if hasattr(self, "quick_actions_button"):
            plus_bg = blend(p["accent"], p["primary"], 0.35)
            plus_hover = shift(plus_bg, 0.08)
            plus_pressed = shift(plus_bg, -0.06)
            plus_border = shift(plus_bg, -0.64)
            self.quick_actions_button.setStyleSheet(
                "QPushButton#QuickActionsTrigger {"
                f"background-color: {rgba_css(plus_bg, 0.94)};"
                f"color: {readable_text(plus_bg)};"
                f"border: 2px solid {plus_border};"
                "border-radius: 18px;"
                "font-size: 22px;"
                "font-weight: 900;"
                "padding: 0px;"
                "}"
                "QPushButton#QuickActionsTrigger:hover {"
                f"background-color: {rgba_css(plus_hover, 0.96)};"
                f"border: 2px solid {shift(plus_border, -0.08)};"
                "}"
                "QPushButton#QuickActionsTrigger:pressed {"
                f"background-color: {rgba_css(plus_pressed, 0.98)};"
                f"border: 2px solid {shift(plus_border, -0.12)};"
                "}"
            )
        if self.quick_radial_menu is not None:
            self.quick_radial_menu.apply_theme_styles(p)

        # Depot tab buttons should be fully opaque (no transparency) as requested
        if hasattr(self, "depot_page"):
            opaque_button_bg = rgba_css(p["button_bg"], 1.0)
            opaque_button_hover = rgba_css(shift(p["button_bg"], 0.08), 1.0)
            opaque_button_pressed = rgba_css(shift(p["button_bg"], -0.06), 1.0)
            opaque_border = shift(p["button_bg"], -0.62)
            depot_disabled_bg = rgba_css(blend(p["control_bg"], p["button_bg"], 0.18), 0.92)
            depot_disabled_text = rgba_css(p["label_text"], 0.48)
            depot_disabled_border = rgba_css(shift(p["control_bg"], -0.18), 0.95)
            depot_button_css = (
                "QPushButton {"
                f"background-color: {opaque_button_bg};"
                f"color: {p['button_text']};"
                f"border: 2px solid {opaque_border};"
                "border-radius: 4px;"
                f"padding: {button_padding};"
                f"min-height: {button_min_height}px;"
                f"font-size: {button_font_px}px;"
                "font-weight: 800;"
                "}"
                f"QPushButton:hover {{ background-color: {opaque_button_hover}; border: 2px solid {shift(opaque_border, -0.08)}; }}"
                f"QPushButton:pressed {{ background-color: {opaque_button_pressed}; border: 2px solid {shift(opaque_border, -0.12)}; }}"
                "QPushButton:disabled {"
                f"background-color: {depot_disabled_bg};"
                f"color: {depot_disabled_text};"
                f"border: 2px solid {depot_disabled_border};"
                "}"
            )
            self.depot_page.setStyleSheet(depot_button_css)
            self._refresh_depot_dashboard_combo_popup_width()

        editor_border = shift(p["control_bg"], -0.30)
        editor_field_border = shift(p["input_bg"], -0.38)
        editor_button_border = shift(p["button_bg"], -0.40)
        editor_button_hover = rgba_css(p["button_bg"], 0.18)
        if hasattr(self, "quick_editor_dialog"):
            base_popup_css = self._popup_theme_stylesheet("main", force_opaque_root=True)
            self.quick_editor_dialog.setStyleSheet(
                base_popup_css
                + (
                    "QDialog#QuickEditorDialog {"
                    f"background: {rgba_css(p['shell_overlay'], 0.90)};"
                    f"color: {p['label_text']};"
                    f"border: 1px solid {editor_border};"
                    "border-radius: 8px;"
                    "}"
                    "QDialog#QuickEditorDialog QLabel {"
                    f"color: {p['label_text']};"
                    "background: transparent;"
                    "font-weight: 700;"
                    "}"
                    "QDialog#QuickEditorDialog QLineEdit, QDialog#QuickEditorDialog QTextEdit, QDialog#QuickEditorDialog QComboBox {"
                    "background: transparent;"
                    f"border: 1px solid {editor_field_border};"
                    "border-radius: 3px;"
                    f"color: {p['label_text']};"
                    "padding: 2px 6px;"
                    "}"
                    "QDialog#QuickEditorDialog QLineEdit:focus, QDialog#QuickEditorDialog QTextEdit:focus, QDialog#QuickEditorDialog QComboBox:focus {"
                    f"border: 1px solid {editor_border};"
                    "background: transparent;"
                    "}"
                    "QDialog#QuickEditorDialog QPushButton {"
                    "background: transparent;"
                    f"border: 1px solid {editor_button_border};"
                    "border-radius: 6px;"
                    f"color: {p['label_text']};"
                    "font-weight: 700;"
                    "}"
                    "QDialog#QuickEditorDialog QPushButton:hover {"
                    f"background: {editor_button_hover};"
                    "}"
                )
            )
        if self.image_dialog is not None:
            self.image_dialog.apply_theme_styles()
        if self.quick_layout_dialog is not None:
            self.quick_layout_dialog.apply_theme_styles()
        if self.depot_dashboard_dialog is not None:
            self.depot_dashboard_dialog.apply_theme_styles()
            self.depot_dashboard_dialog.refresh_combo_popup_width()

        for key, button in self.nav_buttons.items():
            active = self.pages.currentIndex() == self.page_index.get(key)
            self._style_nav_button(button, active)
        self._style_nav_button(self.settings_button, self.pages.currentIndex() == self.page_index["settings"])
        self._refresh_theme_preview_buttons()
        self.refresh_quick_grid()

    def _style_nav_button(self, button: QToolButton, active: bool) -> None:
        p = self.palette_data
        if active:
            bg = rgba_css(p["nav_active"], 0.75)
            fg = readable_text(p["nav_active"])
            border = shift(p["nav_active"], -0.45)
        else:
            bg = rgba_css(p["button_bg"], 0.75)
            fg = p["label_text"]
            border = shift(p["button_bg"], -0.40)
        hover_bg = rgba_css(blend(p["accent"], p["surface"], 0.25), 0.46)

        button.setStyleSheet(
            "QToolButton {"
            f"background: {bg};"
            f"color: {fg};"
            f"border: 1px solid {border};"
            "border-radius: 0px;"
            "padding: 0px;"
            "}"
            f"QToolButton:hover {{ background: {hover_bg}; }}"
        )

    # ------------------------- Page Actions ------------------------- #
    def switch_page(self, page: str) -> None:
        if page != "quick" and self.quick_radial_menu is not None and self.quick_radial_menu.isVisible():
            self.quick_radial_menu.hide()
        index = self.page_index.get(page, 0)
        self.pages.setCurrentIndex(index)
        for key, btn in self.nav_buttons.items():
            self._style_nav_button(btn, key == page)
        self._style_nav_button(self.settings_button, page == "settings")
        self.refresh_all_views()

    def refresh_all_views(self) -> None:
        self.surface.update()
        self.quick_page.update()
        self.settings_page.update()
        
        # Update popup windows if they exist
        if hasattr(self, "active_agent_window") and self.active_agent_window is not None and self.active_agent_window.isVisible():
            self.active_agent_window.update()
        if hasattr(self, "active_qa_window") and self.active_qa_window is not None and self.active_qa_window.isVisible():
            self.active_qa_window.update()
        if self.admin_dialog is not None and self.admin_dialog.isVisible():
            self.admin_dialog.update()
        
        # Update image dialogs if they exist
        if self.image_dialog is not None and self.image_dialog.isVisible():
            self.image_dialog.update()
        if hasattr(self, "agent_image_dialog") and self.agent_image_dialog is not None and self.agent_image_dialog.isVisible():
            self.agent_image_dialog.update()
        if hasattr(self, "qa_image_dialog") and self.qa_image_dialog is not None and self.qa_image_dialog.isVisible():
            self.qa_image_dialog.update()
        if hasattr(self, "admin_image_dialog") and self.admin_image_dialog is not None and self.admin_image_dialog.isVisible():
            self.admin_image_dialog.update()
        if hasattr(self, "dashboard_image_dialog") and self.dashboard_image_dialog is not None and self.dashboard_image_dialog.isVisible():
            self.dashboard_image_dialog.update()
        if self.depot_dashboard_dialog is not None and self.depot_dashboard_dialog.isVisible():
            self.depot_dashboard_dialog.update()

    def _init_ui_opacity_effects(self) -> None:
        self._ui_opacity_effects.clear()
        targets = [
            (self.titlebar, 0.05, 0.00),
            (self.sidebar, 0.05, 0.00),
            (self.settings_page, 0.05, 0.00),
            (self.quick_actions_button, 0.05, 0.00),
        ]
        for widget, no_bg_floor, with_bg_floor in targets:
            effect = QGraphicsOpacityEffect(widget)
            widget.setGraphicsEffect(effect)
            self._ui_opacity_effects.append((effect, float(no_bg_floor), float(with_bg_floor)))
        self._set_ui_opacity(self._effective_shell_idle_opacity())

    def _set_ui_opacity(self, value: float) -> None:
        requested_opacity = float(clamp(value, 0.0, 1.0))
        has_background_layers = self._window_has_background_layers("main")
        self._ui_opacity_current = requested_opacity if has_background_layers else float(max(requested_opacity, 0.05))
        for effect, no_bg_floor, with_bg_floor in self._ui_opacity_effects:
            floor = with_bg_floor if has_background_layers else no_bg_floor
            effect.setOpacity(float(max(requested_opacity, floor)))
        self.surface.update()

    def _has_active_popup(self) -> bool:
        app = QApplication.instance()
        return bool(app and app.activePopupWidget() is not None)

    def _has_active_internal_dialog(self) -> bool:
        dialogs = (self.image_dialog, self.quick_layout_dialog, self.depot_dashboard_dialog)
        for dialog in dialogs:
            if dialog is not None and dialog.isVisible() and dialog.isActiveWindow():
                return True
        return False

    def _cursor_inside_window(self) -> bool:
        pos = self.mapFromGlobal(QCursor.pos())
        return self.rect().contains(pos)

    def _begin_fade_out(self) -> None:
        self._hover_inside = False
        self._hover_revealed = False
        self._hover_delay_timer.stop()
        self._start_opacity_animation(self._effective_shell_idle_opacity(), self._hover_fade_out_ms())

    def _on_popup_leave_check(self) -> None:
        if self._has_active_popup():
            self._popup_leave_timer.start(120)
            return
        if self._has_active_internal_dialog():
            return
        if not self._cursor_inside_window():
            self._begin_fade_out()

    def _reveal_immediately(self) -> None:
        self._hover_inside = True
        self._hover_revealed = True
        self._hover_delay_timer.stop()
        self._popup_leave_timer.stop()
        self._ui_opacity_anim.stop()
        self._set_ui_opacity(1.0)

    # ----------------------- Quick Input Screen --------------------- #
    def _build_quick_radial_menu(self) -> None:
        self.quick_radial_menu = QuickRadialMenu(self)
        self.quick_radial_menu.action_requested.connect(self._handle_quick_radial_action)
        self.quick_radial_menu.apply_theme_styles(self.palette_data)
        self._sync_quick_tab_actions()

    def toggle_quick_radial_menu(self) -> None:
        if self.quick_radial_menu is None:
            self._build_quick_radial_menu()
        if self.quick_radial_menu is None:
            return
        if self.quick_radial_menu.isVisible():
            self.quick_radial_menu.hide()
            return
        self._reveal_immediately()
        self._sync_quick_tab_actions()
        self.quick_radial_menu.open_anchored_to(self.quick_actions_button)

    def _handle_quick_radial_action(self, action_key: str) -> None:
        if action_key == "add":
            self.open_quick_editor(None)
        elif action_key == "layout":
            self.open_quick_layout_dialog()
        elif action_key == "new_tab":
            self.add_quick_task_tab()
        elif action_key == "rename":
            self.rename_quick_task_tab()
        elif action_key == "remove":
            self.remove_quick_task_tab()

    def _quick_tabs(self) -> list[dict[str, Any]]:
        raw_tabs = self.config.get("quick_tabs")
        if not isinstance(raw_tabs, list):
            raw_tabs = []
            self.config["quick_tabs"] = raw_tabs

        if not raw_tabs:
            legacy_items = self.config.get("quick_texts", [])
            if not isinstance(legacy_items, list):
                legacy_items = []
            raw_tabs.append({"name": "Main", "quick_texts": legacy_items})

        changed = False
        for idx, tab in enumerate(raw_tabs):
            if not isinstance(tab, dict):
                raw_tabs[idx] = {"name": "Main" if idx == 0 else f"Task {idx + 1}", "quick_texts": []}
                changed = True
                continue
            name = str(tab.get("name", "")).strip()[:32]
            if not name:
                name = "Main" if idx == 0 else f"Task {idx + 1}"
            if str(tab.get("name", "")) != name:
                tab["name"] = name
                changed = True
            if not isinstance(tab.get("quick_texts"), list):
                tab["quick_texts"] = []
                changed = True

        if changed:
            self.queue_save_config()
        return raw_tabs

    def _active_quick_tab_index(self) -> int:
        tabs = self._quick_tabs()
        idx = safe_int(self.config.get("active_quick_tab", 0), 0)
        if idx < 0 or idx >= len(tabs):
            idx = 0
            self.config["active_quick_tab"] = idx
        return idx

    def _sync_legacy_quick_texts(self, tab_index: int | None = None) -> None:
        tabs = self._quick_tabs()
        if not tabs:
            self.config["quick_texts"] = []
            return
        idx = self._active_quick_tab_index() if tab_index is None else safe_int(tab_index, 0)
        if idx < 0 or idx >= len(tabs):
            idx = 0
        tab_items = tabs[idx].get("quick_texts", [])
        self.config["quick_texts"] = tab_items if isinstance(tab_items, list) else []

    def _active_quick_texts(self) -> list[dict[str, Any]]:
        tabs = self._quick_tabs()
        idx = self._active_quick_tab_index()
        tab = tabs[idx]
        entries = tab.get("quick_texts", [])
        if not isinstance(entries, list):
            entries = []
            tab["quick_texts"] = entries
        self.config["active_quick_tab"] = idx
        self._sync_legacy_quick_texts(idx)
        return entries

    def _sync_quick_tab_actions(self) -> None:
        has_tabs = bool(self._quick_tabs())
        can_remove = len(self._quick_tabs()) > 1
        if self.quick_radial_menu is not None:
            self.quick_radial_menu.set_action_enabled("rename", has_tabs)
            self.quick_radial_menu.set_action_enabled("remove", can_remove)

    def _make_quick_tab_canvas_page(self) -> tuple[QWidget, QScrollArea, QuickButtonCanvas]:
        page = QWidget()
        wrapper = QVBoxLayout(page)
        wrapper.setContentsMargins(0, 0, 0, 0)
        wrapper.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(False)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.viewport().setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        canvas = QuickButtonCanvas()
        scroll.setWidget(canvas)
        wrapper.addWidget(scroll, 1)
        return page, scroll, canvas

    def _rebuild_quick_tab_widgets(self) -> None:
        if self.quick_tabs_widget is None:
            return

        active_index = self._active_quick_tab_index()
        for scroll in self.quick_tab_scrolls:
            try:
                scroll.viewport().removeEventFilter(self)
            except Exception as exc:
                _runtime_log_event(
                    "ui.quick_tab_event_filter_remove_failed",
                    severity="warning",
                    summary="Failed removing viewport event filter while rebuilding quick tab widgets.",
                    exc=exc,
                    context={"scroll_object": repr(scroll)},
                )
        self.quick_tab_scrolls.clear()
        self.quick_tab_canvases.clear()

        tabs = self._quick_tabs()
        self.quick_tabs_widget.blockSignals(True)
        self.quick_tabs_widget.clear()
        for idx, tab in enumerate(tabs):
            page, scroll, canvas = self._make_quick_tab_canvas_page()
            self.quick_tabs_widget.addTab(page, str(tab.get("name", f"Task {idx + 1}")))
            self.quick_tab_scrolls.append(scroll)
            self.quick_tab_canvases.append(canvas)
            scroll.viewport().installEventFilter(self)

        if self.quick_tab_scrolls:
            if active_index < 0 or active_index >= len(self.quick_tab_scrolls):
                active_index = 0
            self.quick_tabs_widget.setCurrentIndex(active_index)
            self.quick_scroll = self.quick_tab_scrolls[active_index]
            self.quick_canvas = self.quick_tab_canvases[active_index]
            self.config["active_quick_tab"] = active_index
            self._sync_legacy_quick_texts(active_index)
        self.quick_tabs_widget.blockSignals(False)
        self._sync_quick_tab_actions()

    def _on_quick_tab_changed(self, index: int) -> None:
        if index < 0 or index >= len(self.quick_tab_scrolls):
            return
        self.quick_scroll = self.quick_tab_scrolls[index]
        self.quick_canvas = self.quick_tab_canvases[index]
        self.config["active_quick_tab"] = int(index)
        self._sync_legacy_quick_texts(index)
        self.refresh_quick_grid()
        if self.quick_layout_dialog is not None and self.quick_layout_dialog.isVisible():
            self.quick_layout_dialog.refresh_cards()
        self.queue_save_config()

    def flush_pending_config_save(self) -> None:
        if self._saving_timer.isActive():
            self._saving_timer.stop()
        self.save_config()

    def _add_quick_task_tab_named(self, tab_name: str) -> int:
        tabs = self._quick_tabs()
        default_name = f"Task {len(tabs) + 1}"
        normalized_name = str(tab_name or "").strip()[:32] or default_name
        tabs.append({"name": normalized_name, "quick_texts": []})
        new_index = len(tabs) - 1
        self.config["active_quick_tab"] = new_index
        self._sync_legacy_quick_texts(new_index)
        self._rebuild_quick_tab_widgets()
        if self.quick_tabs_widget is not None:
            self.quick_tabs_widget.setCurrentIndex(new_index)
        self.refresh_quick_grid()
        if self.quick_layout_dialog is not None and self.quick_layout_dialog.isVisible():
            self.quick_layout_dialog.refresh_cards()
        self.queue_save_config()
        return int(new_index)

    def rename_quick_task_tab_named(self, tab_name: str, index: int | None = None) -> int:
        tabs = self._quick_tabs()
        if not tabs:
            return -1
        idx = self._active_quick_tab_index() if index is None else int(clamp(index, 0, len(tabs) - 1))
        current_name = str(tabs[idx].get("name", f"Task {idx + 1}"))
        updated_name = str(tab_name or "").strip()[:32] or current_name
        tabs[idx]["name"] = updated_name
        if self.quick_tabs_widget is not None and 0 <= idx < self.quick_tabs_widget.count():
            self.quick_tabs_widget.setTabText(idx, updated_name)
        self.queue_save_config()
        return int(idx)

    def remove_quick_task_tab_at(self, index: int | None = None) -> int:
        tabs = self._quick_tabs()
        if len(tabs) <= 1:
            raise ValueError("At least one quick-input tab is required.")
        idx = self._active_quick_tab_index() if index is None else int(clamp(index, 0, len(tabs) - 1))
        tabs.pop(idx)
        next_index = min(idx, len(tabs) - 1)
        self.config["active_quick_tab"] = max(0, next_index)
        self._sync_legacy_quick_texts(self.config["active_quick_tab"])
        self.close_quick_editor()
        self._rebuild_quick_tab_widgets()
        if self.quick_tabs_widget is not None:
            self.quick_tabs_widget.setCurrentIndex(self.config["active_quick_tab"])
        self.refresh_quick_grid()
        if self.quick_layout_dialog is not None and self.quick_layout_dialog.isVisible():
            self.quick_layout_dialog.refresh_cards()
        self.queue_save_config()
        return int(self.config["active_quick_tab"])

    def add_quick_task_tab(self) -> None:
        default_name = f"Task {len(self._quick_tabs()) + 1}"
        name, ok = show_flowgrid_themed_input_text(
            self,
            self,
            "main",
            "New Input Grid Tab",
            "Tab name:",
            default_name,
        )
        if not ok:
            return
        self._add_quick_task_tab_named(str(name))

    def rename_quick_task_tab(self) -> None:
        tabs = self._quick_tabs()
        if not tabs:
            return
        idx = self._active_quick_tab_index()
        current_name = str(tabs[idx].get("name", f"Task {idx + 1}"))
        name, ok = show_flowgrid_themed_input_text(
            self,
            self,
            "main",
            "Rename Input Grid Tab",
            "Tab name:",
            current_name,
        )
        if not ok:
            return
        self.rename_quick_task_tab_named(str(name), idx)

    def remove_quick_task_tab(self) -> None:
        if len(self._quick_tabs()) <= 1:
            self._show_shell_message(
                QMessageBox.Icon.Information,
                "Input Grid",
                "At least one quick-input tab is required.",
                theme_kind="main",
            )
            return
        self.remove_quick_task_tab_at()

    def _default_quick_position(self, index: int, width: int, height: int) -> tuple[int, int]:
        gap_x = 10
        gap_y = 10
        if hasattr(self, "quick_scroll"):
            available_width = max(120, int(self.quick_scroll.viewport().width()))
        else:
            available_width = max(120, LAUNCH_WIDTH - SIDEBAR_WIDTH - 24)
        columns = max(1, available_width // max(1, width + gap_x))
        col = index % columns
        row = index // columns
        return col * (width + gap_x), row * (height + gap_y)

    def _quick_viewport_width(self) -> int:
        try:
            return max(0, int(self.quick_scroll.viewport().width()))
        except Exception:
            return 0

    def _quick_positions_can_persist(self) -> bool:
        # Avoid persisting clamped placeholder geometry during early startup before viewport layout settles.
        return bool(self.isVisible() and self._quick_viewport_width() > 120)

    def refresh_quick_grid(self) -> None:
        self.quick_canvas.clear_cards()
        self.quick_canvas.clear_alignment_guides()

        quick_texts = self._active_quick_texts()
        self.quick_canvas.configure_grid(show_grid=False, snap_enabled=False)
        self.quick_canvas.set_viewport_width(max(120, self._quick_viewport_width()))
        can_persist_positions = self._quick_positions_can_persist()

        if not quick_texts:
            self.quick_canvas.set_placeholder("No quick input buttons yet.", self.palette_data["muted_text"])
            return

        width = int(self.config.get("quick_button_width", 140))
        height = int(self.config.get("quick_button_height", 40))
        font_size = int(self.config.get("quick_button_font_size", 11))
        font_family = str(self.config.get("quick_button_font_family", "Segoe UI"))
        shape = self.config.get("quick_button_shape", "Soft")
        button_opacity = float(clamp(float(self.config.get("quick_button_opacity", 1.0)), 0.15, 1.0))
        updated_positions = False

        for idx, item in enumerate(quick_texts):
            card = QuickButtonCard(
                idx,
                str(item.get("title", "Untitled"))[:28],
                str(item.get("tooltip", "")),
                self.quick_canvas,
            )
            action_type = self._quick_action_kind(item)
            card.apply_visual_style(width, height, font_size, font_family, shape, button_opacity, self.palette_data, action_type)
            card.set_layout_mode(False)
            card.insert_requested.connect(self.insert_quick_text)
            card.edit_requested.connect(self.open_quick_editor)

            raw_x = item.get("x")
            raw_y = item.get("y")
            if isinstance(raw_x, (int, float)) and isinstance(raw_y, (int, float)):
                pos_x, pos_y = int(raw_x), int(raw_y)
            else:
                pos_x, pos_y = self._default_quick_position(idx, width, height)
                if can_persist_positions:
                    item["x"] = int(pos_x)
                    item["y"] = int(pos_y)
                    updated_positions = True

            snapped_x, snapped_y = self.quick_canvas.place_card(card, pos_x, pos_y, snap=False)
            if (
                can_persist_positions
                and (
                    safe_int(item.get("x", -99999), -99999) != snapped_x
                    or safe_int(item.get("y", -99999), -99999) != snapped_y
                )
            ):
                item["x"] = int(snapped_x)
                item["y"] = int(snapped_y)
                updated_positions = True

        if updated_positions:
            self.queue_save_config()

    def open_quick_editor(self, index: int | None) -> None:
        quick_texts = self._active_quick_texts()
        self._editing_index = index
        self._refresh_available_browsers()

        if index is None:
            self.editor_title.setText("")
            self.editor_tooltip.setText("")
            self.editor_action_combo.setCurrentIndex(0)
            self.editor_text.setPlainText("")
            self.editor_macro.setPlainText("")
            self.editor_apps.setPlainText("")
            self.editor_urls.setPlainText("")
            self.editor_browser_combo.setCurrentIndex(0)
            self.editor_delete_btn.setEnabled(False)
        else:
            if index < 0 or index >= len(quick_texts):
                return
            entry = quick_texts[index]
            self.editor_title.setText(str(entry.get("title", "")))
            self.editor_tooltip.setText(str(entry.get("tooltip", "")))
            action = self._quick_action_kind(entry)
            action_index = self.editor_action_combo.findData(action)
            if action_index < 0:
                action_index = self.editor_action_combo.findData(QUICK_ACTION_INPUT_SEQUENCE)
            self.editor_action_combo.setCurrentIndex(max(0, action_index))

            if action == QUICK_ACTION_INPUT_SEQUENCE:
                self.editor_macro.setPlainText(str(entry.get("text", "")))
            else:
                self.editor_macro.setPlainText("")
            self.editor_text.setPlainText("")
            
            app_targets = str(entry.get("app_targets", "")).strip()
            if not app_targets:
                app_targets = str(entry.get("open_target", "")).strip()
            self.editor_apps.setPlainText(app_targets)
            self.editor_urls.setPlainText(str(entry.get("urls", "")))
            browser_path = str(entry.get("browser_path", "")).strip()
            browser_index = self.editor_browser_combo.findData(browser_path)
            if browser_index < 0:
                browser_index = 0
            self.editor_browser_combo.setCurrentIndex(browser_index)
            self.editor_delete_btn.setEnabled(True)

        # Opening the editor is an explicit interaction; reveal full shell opacity immediately.
        self._reveal_immediately()
        if not self.quick_editor_dialog.isVisible():
            px = int(self.x() + max(8, (self.width() - self.quick_editor_dialog.width()) / 2))
            py = int(self.y() + max(36, (self.height() - self.quick_editor_dialog.height()) / 2))
            self.quick_editor_dialog.move(px, py)
        self.quick_editor_dialog.show()
        self.quick_editor_dialog.raise_()
        self.quick_editor_dialog.activateWindow()

    @staticmethod
    def _input_sequence_contains_blocked_credentials(sequence_text: str) -> bool:
        text = str(sequence_text or "")
        if not text.strip():
            return False
        lowered = text.lower()
        if re.search(r"\b(user(name)?|user[_\s-]?id|login|sign[\s-]?in|pass(word)?|passwd|pwd)\b", lowered):
            return True
        if re.search(r"\b(user(name)?|user[_\s-]?id|pass(word)?|passwd|pwd)\s*[:=]", lowered):
            return True
        # Block common "email/username [tab] password" style sequences.
        if re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}.*\[(tab|enter)\]", text, re.IGNORECASE):
            return True
        return False

    def save_quick_editor(self) -> None:
        title = self.editor_title.text().strip() or "Untitled"
        tooltip = self.editor_tooltip.text().strip()
        action = str(self.editor_action_combo.currentData() or QUICK_ACTION_INPUT_SEQUENCE)
        text = self.editor_macro.toPlainText()
        app_targets = self.editor_apps.toPlainText().strip()
        urls = self.editor_urls.toPlainText().strip()
        browser_path = str(self.editor_browser_combo.currentData() or "").strip()

        if action in LEGACY_QUICK_INPUT_ACTIONS:
            action = QUICK_ACTION_INPUT_SEQUENCE
        elif action not in QUICK_ACTIONS:
            _runtime_log_event(
                "ui.quick_editor_unknown_action_defaulted",
                severity="warning",
                summary="Quick input editor encountered an unknown action and defaulted to Input Sequence.",
                context={"action": action, "title": title},
            )
            action = QUICK_ACTION_INPUT_SEQUENCE

        if action == QUICK_ACTION_INPUT_SEQUENCE:
            app_targets = ""
            urls = ""
            browser_path = ""
            if self._input_sequence_contains_blocked_credentials(text):
                _runtime_log_event(
                    "ui.quick_input_sequence_sensitive_content_blocked",
                    severity="warning",
                    summary="Blocked saving input sequence containing credential-like content.",
                    context={"title": title, "user_id": str(self.current_user)},
                )
                self._show_shell_message(
                    QMessageBox.Icon.Warning,
                    "Input Sequence Blocked",
                    "Input Sequences cannot store username/password or login credentials.\n"
                    "Please remove sensitive fields and save again.",
                )
                return
        elif action == QUICK_ACTION_OPEN_URL:
            text = ""
            app_targets = ""
        elif action == QUICK_ACTION_OPEN_APP:
            text = ""
            urls = ""
            browser_path = ""

        quick_texts = self._active_quick_texts()

        if self._editing_index is None:
            pos_x, pos_y = self._default_quick_position(
                len(quick_texts),
                int(self.config.get("quick_button_width", 140)),
                int(self.config.get("quick_button_height", 40)),
            )
            entry = {
                "title": title,
                "tooltip": tooltip,
                "text": text,
                "action": action,
                "open_target": "",
                "app_targets": app_targets,
                "urls": urls,
                "browser_path": browser_path,
                "x": int(pos_x),
                "y": int(pos_y),
            }
            quick_texts.append(entry)
        elif 0 <= self._editing_index < len(quick_texts):
            existing = quick_texts[self._editing_index]
            entry = {
                "title": title,
                "tooltip": tooltip,
                "text": text,
                "action": action,
                "open_target": "",
                "app_targets": app_targets,
                "urls": urls,
                "browser_path": browser_path,
                "x": safe_int(existing.get("x", 0), 0) if isinstance(existing, dict) else 0,
                "y": safe_int(existing.get("y", 0), 0) if isinstance(existing, dict) else 0,
            }
            quick_texts[self._editing_index] = entry

        self.queue_save_config()
        self.close_quick_editor()
        self.refresh_quick_grid()
        if self.quick_layout_dialog is not None and self.quick_layout_dialog.isVisible():
            self.quick_layout_dialog.refresh_cards()

    def delete_quick_editor(self) -> None:
        if self._editing_index is None:
            return

        quick_texts = self._active_quick_texts()
        if 0 <= self._editing_index < len(quick_texts):
            quick_texts.pop(self._editing_index)
            self.queue_save_config()
            self.close_quick_editor()
            self.refresh_quick_grid()
            if self.quick_layout_dialog is not None and self.quick_layout_dialog.isVisible():
                self.quick_layout_dialog.refresh_cards()

    def close_quick_editor(self) -> None:
        self._editing_index = None
        self.quick_editor_dialog.hide()

    def _capture_external_target(self) -> bool:
        if os.name != "nt" or user32 is None:
            return False

        try:
            hwnd = user32.GetForegroundWindow()
            if not hwnd:
                return False

            pid = wintypes.DWORD(0)
            user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
            if int(pid.value) != int(os.getpid()):
                self.last_external_hwnd = int(hwnd)
                return True
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_target_capture_failed",
                severity="warning",
                summary="Failed capturing the external input target for quick input buttons.",
                exc=exc,
            )
        return False

    def _restore_quick_input_target(self) -> bool:
        if os.name != "nt" or user32 is None:
            _runtime_log_event(
                "runtime.quick_input_platform_unsupported",
                severity="warning",
                summary="Quick input could not restore a target window because Windows keyboard automation is unavailable.",
            )
            return False

        if not self.last_external_hwnd:
            _runtime_log_event(
                "runtime.quick_input_target_missing",
                severity="warning",
                summary="Quick input was requested before Flowgrid had captured an external input target.",
            )
            return False

        hwnd = int(self.last_external_hwnd)
        try:
            if not user32.IsWindow(hwnd):
                _runtime_log_event(
                    "runtime.quick_input_target_invalid",
                    severity="warning",
                    summary="Quick input target window no longer exists.",
                    context={"hwnd": hwnd},
                )
                self.last_external_hwnd = None
                return False

            user32.ShowWindow(hwnd, SW_RESTORE)
            restored = bool(user32.SetForegroundWindow(hwnd))
            time.sleep(0.08)
            if not restored:
                _runtime_log_event(
                    "runtime.quick_input_target_restore_failed",
                    severity="warning",
                    summary="Quick input could not restore the previous target window before sending text.",
                    context={"hwnd": hwnd, "last_error": int(ctypes.get_last_error())},
                )
                return False
            return True
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_target_restore_failed",
                severity="warning",
                summary="Failed restoring the previous target window before sending quick input.",
                exc=exc,
                context={"hwnd": hwnd},
            )
            return False

    def _is_shift_pressed(self) -> bool:
        return bool(QApplication.keyboardModifiers() & Qt.KeyboardModifier.ShiftModifier)

    @staticmethod
    def _resolve_context_script_path(script_name: str) -> Path | None:
        candidates = [
            Path(__file__).with_name(script_name),
            Path.cwd() / script_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _entry_context_text(entry: dict[str, Any]) -> str:
        context = str(entry.get("context", "")).strip()
        if context:
            return context
        return str(entry.get("tooltip", "")).strip()

    def _launch_shift_context_script_for_entry(self, entry: dict[str, Any]) -> bool:
        if not self._is_shift_pressed():
            return False

        context_text = self._entry_context_text(entry).lower()
        if not context_text:
            return False

        for context_keyword, script_name in SHIFT_CONTEXT_SCRIPT_LAUNCHERS.items():
            if context_keyword.lower() not in context_text:
                continue
            script_path = self._resolve_context_script_path(script_name)
            if script_path is None:
                return False
            return self._open_app_target(str(script_path))
        return False

    def _send_ctrl_v(self) -> None:
        if os.name != "nt" or user32 is None:
            return
        user32.keybd_event(VK_CONTROL, 0, 0, 0)
        user32.keybd_event(VK_V, 0, 0, 0)
        user32.keybd_event(VK_V, 0, KEYEVENTF_KEYUP, 0)
        user32.keybd_event(VK_CONTROL, 0, KEYEVENTF_KEYUP, 0)

    def _send_key(self, key_code: int, shift_held: bool = False, alt_held: bool = False) -> None:
        """Send a single key press with optional modifiers."""
        if os.name != "nt" or user32 is None:
            return
        
        modifiers: list[int] = []
        if shift_held:
            modifiers.append(VK_SHIFT)
        if alt_held:
            modifiers.append(VK_ALT)
        
        # Press modifiers
        for mod in modifiers:
            user32.keybd_event(mod, 0, 0, 0)
        
        # If key is return, set as return for consistent behavior
        if key_code == VK_ENTER:
            key_code = VK_RETURN

        # Press and release the key
        user32.keybd_event(key_code, 0, 0, 0)
        user32.keybd_event(key_code, 0, KEYEVENTF_KEYUP, 0)
        
        # Release modifiers
        for mod in reversed(modifiers):
            user32.keybd_event(mod, 0, KEYEVENTF_KEYUP, 0)

    @staticmethod
    def _append_input_sequence_text(commands: list[dict[str, Any]], text: str) -> None:
        if not text:
            return
        if commands and commands[-1].get("action") == "type":
            commands[-1]["text"] = str(commands[-1].get("text", "")) + text
        else:
            commands.append({"action": "type", "text": text})

    def _parse_macro_sequence(self, sequence: str) -> list[dict[str, Any]]:
        """Parse an input sequence into literal text, key commands, and delay commands."""
        sequence_text = str(sequence or "")
        commands: list[dict[str, Any]] = []
        cursor = 0

        for match in re.finditer(r"\[[^\]]*\]", sequence_text):
            if match.start() > cursor:
                self._append_input_sequence_text(commands, sequence_text[cursor : match.start()])

            token = match.group(0)
            content = token[1:-1].strip()
            handled = False
            if ":" in content:
                cmd_type, cmd_value = content.split(":", 1)
                if cmd_type.strip().lower() == "delay":
                    raw_delay = cmd_value.strip()
                    try:
                        delay_ms = int(raw_delay)
                        if 0 <= delay_ms <= 60000:
                            commands.append({"action": "delay", "ms": delay_ms})
                            handled = True
                        else:
                            _runtime_log_event(
                                "runtime.input_sequence_delay_out_of_range",
                                severity="warning",
                                summary="Input Sequence delay command was outside the supported 0-60000 ms range and will be pasted literally.",
                                context={"raw_value": raw_delay},
                            )
                    except ValueError as exc:
                        _runtime_log_event(
                            "runtime.input_sequence_delay_parse_failed",
                            severity="warning",
                            summary="Invalid Input Sequence delay command value; command will be pasted literally.",
                            exc=exc,
                            context={"raw_value": raw_delay},
                        )
            else:
                cmd_name = content.lower()
                if cmd_name == "tab":
                    commands.append({"action": "key", "key": "tab"})
                    handled = True
                elif cmd_name in {"enter", "return"}:
                    commands.append({"action": "key", "key": "enter"})
                    handled = True

            if not handled:
                self._append_input_sequence_text(commands, token)
            cursor = match.end()

        if cursor < len(sequence_text):
            self._append_input_sequence_text(commands, sequence_text[cursor:])

        return commands

    def _clone_clipboard_mime_data(self) -> QMimeData | None:
        try:
            source = QGuiApplication.clipboard().mimeData()
            clone = QMimeData()
            if source is None:
                return clone
            if source.hasText():
                clone.setText(source.text())
            if source.hasHtml():
                clone.setHtml(source.html())
            if source.hasUrls():
                clone.setUrls(source.urls())
            if source.hasImage():
                clone.setImageData(source.imageData())
            if source.hasColor():
                clone.setColorData(source.colorData())
            for fmt in source.formats():
                if fmt not in clone.formats():
                    clone.setData(fmt, QByteArray(source.data(fmt)))
            return clone
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_clipboard_snapshot_failed",
                severity="critical",
                summary="Quick input could not snapshot the clipboard before temporary paste.",
                exc=exc,
            )
            _escalate_runtime_issue_once(
                "runtime.quick_input_clipboard_snapshot_failed",
                "Quick input could not safely preserve the clipboard.",
                details=f"{type(exc).__name__}: {exc}",
            )
            return None

    def _restore_clipboard_mime_data(self, snapshot: QMimeData | None) -> None:
        if snapshot is None:
            return
        try:
            QGuiApplication.clipboard().setMimeData(snapshot)
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_clipboard_restore_failed",
                severity="critical",
                summary="Quick input failed to restore the previous clipboard content.",
                exc=exc,
            )
            _escalate_runtime_issue_once(
                "runtime.quick_input_clipboard_restore_failed",
                "Quick input could not restore the previous clipboard.",
                details=f"{type(exc).__name__}: {exc}",
            )

    def _paste_input_sequence_text(self, text: str, *, send_paste_keys: bool = True) -> bool:
        if not text:
            return True
        try:
            QGuiApplication.clipboard().setText(text)
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_clipboard_set_failed",
                severity="critical",
                summary="Quick input failed to place temporary text on the clipboard.",
                exc=exc,
                context={"text_length": len(text)},
            )
            _escalate_runtime_issue_once(
                "runtime.quick_input_clipboard_set_failed",
                "Quick input could not place temporary text on the clipboard.",
                details=f"{type(exc).__name__}: {exc}",
            )
            return False

        if not send_paste_keys:
            return True

        try:
            time.sleep(0.05)
            self._send_ctrl_v()
            time.sleep(0.08)
            return True
        except Exception as exc:
            _runtime_log_event(
                "runtime.quick_input_paste_keys_failed",
                severity="critical",
                summary="Quick input failed while sending paste keystrokes.",
                exc=exc,
                context={"text_length": len(text)},
            )
            _escalate_runtime_issue_once(
                "runtime.quick_input_paste_keys_failed",
                "Quick input could not send paste keystrokes.",
                details=f"{type(exc).__name__}: {exc}",
            )
            return False

    def _execute_macro_sequence(self, sequence: str, *, send_paste_keys: bool = True) -> None:
        """Execute a parsed input sequence."""
        if send_paste_keys and not self._restore_quick_input_target():
            return

        commands = self._parse_macro_sequence(sequence)
        needs_clipboard = any(cmd.get("action") == "type" and str(cmd.get("text", "")) for cmd in commands)
        clipboard_snapshot = self._clone_clipboard_mime_data() if needs_clipboard else None
        if needs_clipboard and clipboard_snapshot is None:
            return

        try:
            for cmd in commands:
                action = cmd.get("action", "")

                if action == "type":
                    text = str(cmd.get("text", ""))
                    if text and not self._paste_input_sequence_text(text, send_paste_keys=send_paste_keys):
                        return

                elif action == "key":
                    key_name = str(cmd.get("key", ""))
                    shift_held = bool(cmd.get("shift", False))
                    alt_held = bool(cmd.get("alt", False))

                    if send_paste_keys and key_name in KEY_CODES:
                        key_code = KEY_CODES[key_name]
                        self._send_key(key_code, shift_held=shift_held, alt_held=alt_held)
                        time.sleep(0.06)

                elif action == "delay":
                    delay_ms = safe_int(cmd.get("ms", 0), 0)
                    if send_paste_keys:
                        time.sleep(delay_ms / 1000.0)
        finally:
            if needs_clipboard:
                self._restore_clipboard_mime_data(clipboard_snapshot)

    def _quick_action_kind(self, entry: dict[str, Any]) -> str:
        action = str(entry.get("action", QUICK_ACTION_INPUT_SEQUENCE)).strip().lower()
        if action in LEGACY_QUICK_INPUT_ACTIONS:
            return QUICK_ACTION_INPUT_SEQUENCE
        if action not in QUICK_ACTIONS:
            _runtime_log_event(
                "runtime.quick_action_unknown_defaulted",
                severity="warning",
                summary="Unknown quick input action was defaulted to Input Sequence at runtime.",
                context={"action": action, "title": str(entry.get("title", ""))},
            )
            return QUICK_ACTION_INPUT_SEQUENCE
        return action

    def _detect_installed_browsers(self) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        seen: set[str] = set()

        def add_browser(label: str, path: str) -> None:
            if not path:
                return
            path_obj = Path(path)
            if not path_obj.exists():
                return
            key = str(path_obj).lower()
            if key in seen:
                return
            seen.add(key)
            found.append((label, str(path_obj)))

        checks = [
            (
                "Microsoft Edge",
                ["msedge.exe"],
                [
                    ("PROGRAMFILES(X86)", r"Microsoft\Edge\Application\msedge.exe"),
                    ("PROGRAMFILES", r"Microsoft\Edge\Application\msedge.exe"),
                ],
            ),
            (
                "Google Chrome",
                ["chrome.exe"],
                [
                    ("PROGRAMFILES", r"Google\Chrome\Application\chrome.exe"),
                    ("PROGRAMFILES(X86)", r"Google\Chrome\Application\chrome.exe"),
                    ("LOCALAPPDATA", r"Google\Chrome\Application\chrome.exe"),
                ],
            ),
            (
                "Mozilla Firefox",
                ["firefox.exe"],
                [
                    ("PROGRAMFILES", r"Mozilla Firefox\firefox.exe"),
                    ("PROGRAMFILES(X86)", r"Mozilla Firefox\firefox.exe"),
                ],
            ),
            (
                "Brave",
                ["brave.exe"],
                [
                    ("PROGRAMFILES", r"BraveSoftware\Brave-Browser\Application\brave.exe"),
                    ("PROGRAMFILES(X86)", r"BraveSoftware\Brave-Browser\Application\brave.exe"),
                    ("LOCALAPPDATA", r"BraveSoftware\Brave-Browser\Application\brave.exe"),
                ],
            ),
            (
                "Opera",
                ["opera.exe", "launcher.exe"],
                [
                    ("LOCALAPPDATA", r"Programs\Opera\opera.exe"),
                    ("PROGRAMFILES", r"Opera\launcher.exe"),
                    ("PROGRAMFILES(X86)", r"Opera\launcher.exe"),
                ],
            ),
            (
                "Vivaldi",
                ["vivaldi.exe"],
                [
                    ("LOCALAPPDATA", r"Vivaldi\Application\vivaldi.exe"),
                    ("PROGRAMFILES", r"Vivaldi\Application\vivaldi.exe"),
                ],
            ),
        ]

        for label, exe_names, rel_paths in checks:
            for exe_name in exe_names:
                located = shutil.which(exe_name)
                if located:
                    add_browser(label, located)
            for env_key, rel_path in rel_paths:
                base = os.environ.get(env_key, "")
                if not base:
                    continue
                add_browser(label, str(Path(base) / rel_path))

        return found

    def _refresh_available_browsers(self, preferred_path: str | None = None) -> None:
        if not hasattr(self, "editor_browser_combo"):
            return
        current_path = preferred_path if preferred_path is not None else str(self.editor_browser_combo.currentData() or "").strip()
        self.editor_browser_combo.blockSignals(True)
        self.editor_browser_combo.clear()
        self.editor_browser_combo.addItem("Default Browser", "")
        for label, path in self._detect_installed_browsers():
            self.editor_browser_combo.addItem(label, path)
        target_index = self.editor_browser_combo.findData(current_path)
        if target_index < 0:
            target_index = 0
        self.editor_browser_combo.setCurrentIndex(target_index)
        self.editor_browser_combo.blockSignals(False)

    def _set_editor_row_visible(self, label: QWidget | None, field: QWidget, visible: bool) -> None:
        if label is not None:
            label.setVisible(visible)
        field.setVisible(visible)

    def _update_quick_editor_action_ui(self) -> None:
        action = str(self.editor_action_combo.currentData() or QUICK_ACTION_INPUT_SEQUENCE)
        if action in LEGACY_QUICK_INPUT_ACTIONS or action not in QUICK_ACTIONS:
            action = QUICK_ACTION_INPUT_SEQUENCE
        input_mode = action == QUICK_ACTION_INPUT_SEQUENCE
        url_mode = action == QUICK_ACTION_OPEN_URL
        app_mode = action == QUICK_ACTION_OPEN_APP

        self._set_editor_row_visible(self.editor_text_label, self.editor_text, False)
        self._set_editor_row_visible(self.editor_macro_label, self.editor_macro_wrap, input_mode)
        self._set_editor_row_visible(self.editor_apps_label, self.editor_apps_wrap, app_mode)
        self._set_editor_row_visible(self.editor_urls_label, self.editor_urls, url_mode)
        self._set_editor_row_visible(self.editor_browser_label, self.editor_browser_wrap, url_mode)

        if action == QUICK_ACTION_INPUT_SEQUENCE:
            self.editor_macro.setPlaceholderText("Format: Bin location [enter]  (credentials are blocked)")
            self.editor_macro.setEnabled(True)
            self.editor_text.setPlaceholderText("")
            self.editor_text.setEnabled(False)
            self.editor_apps.setEnabled(False)
            self.editor_apps_browse.setEnabled(False)
            self.editor_apps_browse_folder.setEnabled(False)
            self.editor_urls.setEnabled(False)
            self.editor_browser_combo.setEnabled(False)
            self.editor_refresh_browsers_button.setEnabled(False)
        elif action == QUICK_ACTION_OPEN_URL:
            self.editor_urls.setPlaceholderText("One URL per line.")
            self.editor_macro.setPlaceholderText("")
            self.editor_macro.setEnabled(False)
            self.editor_apps.setEnabled(False)
            self.editor_apps_browse.setEnabled(False)
            self.editor_apps_browse_folder.setEnabled(False)
            self.editor_urls.setEnabled(True)
            self.editor_browser_combo.setEnabled(True)
            self.editor_refresh_browsers_button.setEnabled(True)
        else:
            self.editor_apps.setPlaceholderText("One app/file/folder path per line. Use Browse buttons to pick targets.")
            self.editor_macro.setPlaceholderText("")
            self.editor_macro.setEnabled(False)
            self.editor_apps.setEnabled(True)
            self.editor_apps_browse.setEnabled(True)
            self.editor_apps_browse_folder.setEnabled(True)
            self.editor_urls.setEnabled(False)
            self.editor_browser_combo.setEnabled(False)
            self.editor_refresh_browsers_button.setEnabled(False)
        self._resize_quick_editor_for_action(action)

    def _resize_quick_editor_for_action(self, action: str) -> None:
        if not hasattr(self, "quick_editor_dialog"):
            return
        desired_height = {
            QUICK_ACTION_INPUT_SEQUENCE: 390,
            QUICK_ACTION_OPEN_APP: 410,
            QUICK_ACTION_OPEN_URL: 410,
        }.get(str(action or "").strip(), 390)
        self.quick_editor_dialog.resize(max(430, int(self.quick_editor_dialog.width())), desired_height)

    def browse_quick_apps(self) -> None:
        action = str(self.editor_action_combo.currentData() or QUICK_ACTION_INPUT_SEQUENCE)
        if action != QUICK_ACTION_OPEN_APP:
            return
        current_lines = [line.strip() for line in self.editor_apps.toPlainText().splitlines() if line.strip()]
        current = current_lines[-1] if current_lines else ""
        start_dir = str(Path(current).parent) if current else str(Path.home())
        if not Path(start_dir).exists():
            start_dir = str(Path.home())
        selected_paths, _ = show_flowgrid_themed_open_file_names(
            self,
            self,
            "main",
            "Select App/File Targets",
            start_dir,
            "All Files (*.*);;Programs (*.exe *.lnk *.bat *.cmd *.ps1);;Executables (*.exe *.bat *.cmd);;Shortcuts (*.lnk)",
        )
        if not selected_paths:
            return
        lines = [line.strip() for line in self.editor_apps.toPlainText().splitlines() if line.strip()]
        for selected in selected_paths:
            normalized = str(selected or "").strip()
            if normalized and normalized not in lines:
                lines.append(normalized)
        self.editor_apps.setPlainText("\n".join(lines))

    def browse_quick_app_folder(self) -> None:
        action = str(self.editor_action_combo.currentData() or QUICK_ACTION_INPUT_SEQUENCE)
        if action != QUICK_ACTION_OPEN_APP:
            return
        current_lines = [line.strip() for line in self.editor_apps.toPlainText().splitlines() if line.strip()]
        current = current_lines[-1] if current_lines else ""
        start_dir = str(Path(current).parent) if current else str(Path.home())
        if not Path(start_dir).exists():
            start_dir = str(Path.home())
        selected_dir = show_flowgrid_themed_existing_directory(self, self, "main", "Select Folder Target", start_dir)
        selected_dir = str(selected_dir or "").strip()
        if not selected_dir:
            return
        lines = [line.strip() for line in self.editor_apps.toPlainText().splitlines() if line.strip()]
        if selected_dir not in lines:
            lines.append(selected_dir)
        self.editor_apps.setPlainText("\n".join(lines))

    def _insert_macro_command(self, command: str) -> None:
        """Insert an input-sequence token into the sequence text box."""
        cursor = self.editor_macro.textCursor()
        cursor.movePosition(QTextCursor.End)
        text = self.editor_macro.toPlainText()
        if text and not text[-1].isspace():
            cursor.insertText(" ")
        cursor.insertText(command)
        self.editor_macro.setTextCursor(cursor)

    def _insert_macro_delay(self) -> None:
        """Open dialog to insert [delay: ms] command."""
        seconds, ok = show_flowgrid_themed_input_int(
            self,
            self,
            "main",
            "Insert Delay Command",
            "Enter delay in seconds (0-60):",
            1,
            0,
            60,
            1,
        )
        if ok:
            milliseconds = seconds * 1000
            self._insert_macro_command(f"[delay: {milliseconds}]")

    def _insert_macro_simple(self, command: str) -> None:
        """Insert a simple predefined command."""
        self._insert_macro_command(command)

    def _open_url_target(self, target: str, browser_path: str = "") -> bool:
        target = target.strip()
        if not target:
            return False
        url = QUrl.fromUserInput(target)
        if not url.isValid():
            return False
        browser_path = browser_path.strip()
        if browser_path and Path(browser_path).exists():
            detached = QProcess.startDetached(browser_path, [url.toString()])
            if isinstance(detached, tuple):
                return bool(detached[0])
            return bool(detached)
        return bool(QDesktopServices.openUrl(url))

    def _parse_urls(self, urls_text: str) -> list[str]:
        urls: list[str] = []
        for part in urls_text.replace(";", "\n").splitlines():
            value = part.strip()
            if value:
                urls.append(value)
        return urls

    def _parse_targets(self, targets_text: str) -> list[str]:
        targets: list[str] = []
        for part in targets_text.replace(";", "\n").splitlines():
            value = part.strip()
            if value:
                targets.append(value)
        return targets

    def _open_url_targets(self, urls_text: str, browser_path: str = "", fallback_target: str = "", fallback_text: str = "") -> bool:
        urls = self._parse_urls(urls_text)
        if not urls:
            if fallback_target.strip():
                urls = [fallback_target.strip()]
            elif fallback_text.strip():
                urls = [fallback_text.strip()]
        browser_path = browser_path.strip()
        if browser_path and Path(browser_path).exists() and len(urls) > 1:
            args: list[str] = []
            for target in urls:
                url = QUrl.fromUserInput(target)
                if url.isValid():
                    args.append(url.toString())
            if args:
                detached = QProcess.startDetached(browser_path, args)
                if isinstance(detached, tuple):
                    return bool(detached[0])
                return bool(detached)
        opened = False
        for url in urls:
            opened = self._open_url_target(url, browser_path=browser_path) or opened
        return opened

    def _open_app_target(self, target: str) -> bool:
        target = target.strip()
        if not target:
            return False
        if os.name == "nt":
            try:
                os.startfile(target)  # type: ignore[attr-defined]
                return True
            except Exception as exc:
                _runtime_log_event(
                    "runtime.quick_open_app_startfile_failed",
                    severity="warning",
                    summary="os.startfile failed for quick action target; trying alternate open methods.",
                    exc=exc,
                    context={"target": target},
                )
        if Path(target).exists():
            return bool(QDesktopServices.openUrl(QUrl.fromLocalFile(str(Path(target).resolve()))))
        return bool(QDesktopServices.openUrl(QUrl.fromUserInput(target)))

    def _open_app_targets(self, targets_text: str, fallback_target: str = "", fallback_text: str = "") -> bool:
        targets = self._parse_targets(targets_text)
        if not targets:
            if fallback_target.strip():
                targets = [fallback_target.strip()]
            elif fallback_text.strip():
                targets = [fallback_text.strip()]
        opened = False
        for target in targets:
            opened = self._open_app_target(target) or opened
        return opened

    def insert_quick_text(self, index: int) -> None:
        quick_texts = self._active_quick_texts()
        if index < 0 or index >= len(quick_texts):
            return
        entry = quick_texts[index]
        if not isinstance(entry, dict):
            return

        if self._launch_shift_context_script_for_entry(entry):
            return

        action = self._quick_action_kind(entry)
        text = str(entry.get("text", ""))
        target = str(entry.get("open_target", "")).strip()
        app_targets = str(entry.get("app_targets", "")).strip()
        urls = str(entry.get("urls", ""))
        browser_path = str(entry.get("browser_path", "")).strip()

        if action == QUICK_ACTION_OPEN_URL:
            opened = self._open_url_targets(urls, browser_path=browser_path, fallback_target=target, fallback_text=text)
            if not opened:
                context = {
                    "index": int(index),
                    "title": str(entry.get("title", "")),
                    "urls": urls,
                    "browser_path": browser_path,
                    "fallback_target": target,
                }
                _runtime_log_event(
                    "runtime.quick_action_open_url_failed",
                    severity="critical",
                    summary="Quick action failed to open any URL target.",
                    context=context,
                )
                _escalate_runtime_issue_once(
                    "runtime.quick_action_open_url_failed",
                    "Quick action could not open the configured URL target(s).",
                    details="Review the URLs and browser path configured for this quick action.",
                    context=context,
                )
        elif action == QUICK_ACTION_OPEN_APP:
            opened = self._open_app_targets(app_targets, fallback_target=target, fallback_text=text)
            if not opened:
                context = {
                    "index": int(index),
                    "title": str(entry.get("title", "")),
                    "app_targets": app_targets,
                    "fallback_target": target,
                }
                _runtime_log_event(
                    "runtime.quick_action_open_app_failed",
                    severity="critical",
                    summary="Quick action failed to open any application or file target.",
                    context=context,
                )
                _escalate_runtime_issue_once(
                    "runtime.quick_action_open_app_failed",
                    "Quick action could not open the configured app/file target(s).",
                    details="Verify the path(s) and launch permissions for this quick action.",
                    context=context,
                )
        else:
            self._execute_macro_sequence(text)

    # ------------------------- Theme Screen ------------------------- #
    def refresh_theme_controls(self) -> None:
        theme = self.config.get("theme", {})
        for key in ("primary", "accent", "surface"):
            value = normalize_hex(theme.get(key, "#FFFFFF"), "#FFFFFF")
            self.color_swatches[key].setText(value)
            self.color_swatches[key].setStyleSheet(
                "QPushButton {"
                f"background-color: {rgba_css(value, 0.75)};"
                f"color: {readable_text(value)};"
                f"border: 1px solid {shift(value, -0.45)};"
                "font-weight: 700;"
                "}"
            )

        presets = self.config.get("theme_presets", {})
        current = self.config.get("selected_theme_preset")
        self.theme_preset_combo.blockSignals(True)
        self.theme_preset_combo.clear()
        self.theme_preset_combo.addItems(list(presets.keys()))
        if current in presets:
            self.theme_preset_combo.setCurrentText(current)
        self.theme_preset_combo.blockSignals(False)
        self.theme_transparent_bg_check.blockSignals(True)
        self.theme_transparent_bg_check.setChecked(not bool(self.config.get("background_tint_enabled", True)))
        self.theme_transparent_bg_check.blockSignals(False)
        self.popup_auto_reinherit_check.blockSignals(True)
        self.popup_auto_reinherit_check.setChecked(bool(self.config.get("popup_auto_reinherit_enabled", True)))
        self.popup_auto_reinherit_check.blockSignals(False)
        self._refresh_main_popup_control_defaults()

        self._refresh_popup_theme_tab("agent")
        self._refresh_popup_theme_tab("qa")
        self._refresh_popup_theme_tab("admin")
        self._refresh_popup_theme_tab("dashboard")

        self._refresh_theme_preview_buttons()

    def pick_theme_color(self, key: str) -> None:
        current = QColor(self.config.get("theme", {}).get(key, "#FFFFFF"))
        color = show_flowgrid_themed_color(self, self, "main", f"Pick {key.title()} Color", current)
        if not color.isValid():
            return
        self.config["theme"][key] = normalize_hex(color.name().upper(), self.config["theme"].get(key, "#FFFFFF"))
        self._theme_updated()

    def _theme_updated(self) -> None:
        self.palette_data = compute_palette(self.config.get("theme", {}))
        self.mark_background_dirty()
        self.apply_theme_styles()
        self.refresh_theme_controls()
        self.refresh_all_views()
        self.queue_save_config()
        self._refresh_popup_themes()

    def _refresh_main_popup_control_defaults(self) -> None:
        style_combo = getattr(self, "main_control_style_combo", None)
        fade_slider = getattr(self, "main_control_fade_slider", None)
        opacity_slider = getattr(self, "main_control_opacity_slider", None)
        tail_slider = getattr(self, "main_control_tail_opacity_slider", None)
        if style_combo is None or fade_slider is None or opacity_slider is None or tail_slider is None:
            return

        style = str(self.config.get("popup_control_style", "Fade Left to Right") or "Fade Left to Right").strip()
        if style not in {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}:
            style = "Fade Left to Right"
        style_combo.blockSignals(True)
        style_index = style_combo.findText(style)
        style_combo.setCurrentIndex(style_index if style_index >= 0 else 1)
        style_combo.blockSignals(False)

        values = (
            (fade_slider, getattr(self, "main_control_fade_value", None), "popup_control_fade_strength", 65),
            (opacity_slider, getattr(self, "main_control_opacity_value", None), "popup_control_opacity", 82),
            (tail_slider, getattr(self, "main_control_tail_opacity_value", None), "popup_control_tail_opacity", 0),
        )
        for slider, value_label, config_key, default_value in values:
            numeric_value = int(clamp(safe_int(self.config.get(config_key, default_value), default_value), 0, 100))
            slider.blockSignals(True)
            slider.setValue(numeric_value)
            slider.blockSignals(False)
            if value_label is not None:
                value_label.setText(f"{numeric_value}%")

    def on_main_popup_control_changed(self) -> None:
        style_combo = getattr(self, "main_control_style_combo", None)
        fade_slider = getattr(self, "main_control_fade_slider", None)
        opacity_slider = getattr(self, "main_control_opacity_slider", None)
        tail_slider = getattr(self, "main_control_tail_opacity_slider", None)
        if style_combo is None or fade_slider is None or opacity_slider is None or tail_slider is None:
            return

        style = str(style_combo.currentText() or "Fade Left to Right").strip()
        if style not in {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}:
            style = "Fade Left to Right"
        fade_value = int(clamp(int(fade_slider.value()), 0, 100))
        opacity_value = int(clamp(int(opacity_slider.value()), 0, 100))
        tail_value = int(clamp(int(tail_slider.value()), 0, 100))

        self.config["popup_control_style"] = style
        self.config["popup_control_fade_enabled"] = style != "Solid"
        self.config["popup_control_fade_strength"] = fade_value
        self.config["popup_control_opacity"] = opacity_value
        self.config["popup_control_tail_opacity"] = tail_value

        if hasattr(self, "main_control_fade_value"):
            self.main_control_fade_value.setText(f"{fade_value}%")
        if hasattr(self, "main_control_opacity_value"):
            self.main_control_opacity_value.setText(f"{opacity_value}%")
        if hasattr(self, "main_control_tail_opacity_value"):
            self.main_control_tail_opacity_value.setText(f"{tail_value}%")

        self._refresh_popup_theme_tab("agent")
        self._refresh_popup_theme_tab("qa")
        self._refresh_popup_theme_tab("admin")
        self._refresh_popup_theme_tab("dashboard")
        self._refresh_popup_themes()
        self.queue_save_config()

    def _popup_control_fill_css(self, kind: str, popup_theme: dict[str, Any], field_bg: str) -> str:
        style = str(popup_theme.get("control_style", "Fade Left to Right") or "Fade Left to Right").strip()
        if style not in {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}:
            style = "Fade Left to Right"
        fade_strength = int(clamp(safe_int(popup_theme.get("control_fade_strength", 65), 65), 0, 100))
        effective_transparent = self._effective_popup_transparency(kind)
        base_alpha = float(clamp(safe_int(popup_theme.get("control_opacity", 82), 82), 0, 100)) / 100.0
        tail_alpha = float(clamp(safe_int(popup_theme.get("control_tail_opacity", 0), 0), 0, 100)) / 100.0
        base_alpha = max(base_alpha, 0.54 if effective_transparent else 0.34)
        tail_alpha = max(tail_alpha, 0.28 if effective_transparent else 0.14)
        if style == "Solid" or fade_strength <= 0:
            return rgba_css(field_bg, base_alpha)

        computed_end = base_alpha * max(0.0, 1.0 - (fade_strength / 100.0))
        end_alpha = max(tail_alpha, computed_end)
        mid_alpha = max(end_alpha, min(1.0, (base_alpha + end_alpha) / 2.0 + 0.04))
        if style == "Fade Right to Left":
            return (
                "qlineargradient(x1:1,y1:0,x2:0,y2:0,"
                f"stop:0 {rgba_css(field_bg, base_alpha)},"
                f"stop:0.58 {rgba_css(field_bg, mid_alpha)},"
                f"stop:1 {rgba_css(field_bg, end_alpha)})"
            )
        if style == "Fade Center Out":
            return (
                "qlineargradient(x1:0,y1:0,x2:1,y2:0,"
                f"stop:0 {rgba_css(field_bg, end_alpha)},"
                f"stop:0.5 {rgba_css(field_bg, base_alpha)},"
                f"stop:1 {rgba_css(field_bg, end_alpha)})"
            )
        return (
            "qlineargradient(x1:0,y1:0,x2:1,y2:0,"
            f"stop:0 {rgba_css(field_bg, base_alpha)},"
            f"stop:0.58 {rgba_css(field_bg, mid_alpha)},"
            f"stop:1 {rgba_css(field_bg, end_alpha)})"
        )

    def _materialize_popup_theme_for_edit(self, kind: str) -> dict[str, Any]:
        """Freeze inherited popup theme values into the kind-specific config before first customization."""
        key = f"{kind}_theme"
        theme = self.config.setdefault(key, {})
        if not isinstance(theme, dict):
            theme = {}
            self.config[key] = theme

        base = self._resolved_popup_theme(kind)
        inherited_mode = bool(theme.get("inherit_main_theme", False) or self._looks_like_unconfigured_popup_theme(theme))
        if inherited_mode:
            theme["background"] = normalize_hex(base.get("background", "#FFFFFF"), "#FFFFFF")
            theme["text"] = normalize_hex(base.get("text", "#000000"), "#000000")
            theme["field_bg"] = normalize_hex(base.get("field_bg", "#FFFFFF"), "#FFFFFF")
            inherited_layers = base.get("image_layers", [])
            theme["image_layers"] = [
                safe_layer_defaults(layer) for layer in inherited_layers if isinstance(layer, dict)
            ]
        else:
            theme["background"] = normalize_hex(theme.get("background", base["background"]), base["background"])
            theme["text"] = normalize_hex(theme.get("text", base["text"]), base["text"])
            theme["field_bg"] = normalize_hex(theme.get("field_bg", base["field_bg"]), base["field_bg"])

            raw_layers = theme.get("image_layers")
            if isinstance(raw_layers, list):
                cleaned_layers: list[dict[str, Any]] = []
                for layer in raw_layers:
                    if isinstance(layer, dict):
                        cleaned_layers.append(safe_layer_defaults(layer))
                theme["image_layers"] = cleaned_layers
            else:
                inherited_layers = base.get("image_layers", [])
                theme["image_layers"] = [
                    safe_layer_defaults(layer) for layer in inherited_layers if isinstance(layer, dict)
                ]
        return theme

    def _popup_theme_stylesheet(self, kind: str, force_opaque_root: bool = False) -> str:
        theme = self._resolved_popup_theme(kind)
        compact_mode = bool(self.config.get("compact_mode", True))
        tab_padding = "2px 8px" if compact_mode else "4px 10px"
        field_padding = "1px 5px" if compact_mode else "2px 6px"
        header_padding = "3px 5px" if compact_mode else "4px 6px"
        button_padding = "1px 7px" if compact_mode else "2px 8px"
        check_indicator_px = 12 if compact_mode else 14
        effective_transparent = self._effective_popup_transparency(kind)
        bg = normalize_hex(theme.get("background", self.palette_data["surface"]), self.palette_data["surface"])
        text = normalize_hex(theme.get("text", self.palette_data["label_text"]), self.palette_data["label_text"])
        field_bg = normalize_hex(theme.get("field_bg", self.palette_data["input_bg"]), self.palette_data["input_bg"])
        field_fill = self._popup_control_fill_css(kind, theme, field_bg)
        field_border = shift(field_bg, -0.38)
        selection_bg = self.palette_data["accent"]
        selection_text = readable_text(selection_bg)
        row_selected_bg = normalize_hex(theme.get("row_selected_color", selection_bg), selection_bg)
        row_selected_text = readable_text(row_selected_bg)
        hover_bg = normalize_hex(theme.get("row_hover_color", ""), "")
        if not hover_bg:
            hover_bg = blend(row_selected_bg, field_bg, 0.55)
        hover_text = readable_text(hover_bg)
        header_bg = normalize_hex(theme.get("header_color", ""), "")
        if not header_bg:
            header_bg = blend(field_bg, bg, 0.25)
        button_bg = blend(self.palette_data["button_bg"], field_bg, 0.30)
        button_hover = shift(button_bg, 0.08)
        button_pressed = shift(button_bg, -0.08)
        button_text = readable_text(button_bg)
        button_border = shift(button_bg, -0.40)
        save_bg = blend(selection_bg, button_bg, 0.50)
        save_border = shift(save_bg, -0.40)
        save_text = readable_text(save_bg)
        pick_bg = blend(button_bg, field_bg, 0.25)
        pick_border = shift(pick_bg, -0.38)
        pick_text = readable_text(pick_bg)
        reset_bg = blend("#BE4E4E", button_bg, 0.45)
        reset_border = shift(reset_bg, -0.42)
        reset_text = readable_text(reset_bg)
        new_bg = blend(self.palette_data["primary"], button_bg, 0.45)
        new_border = shift(new_bg, -0.40)
        new_text = readable_text(new_bg)
        button_bg_css = rgba_css(button_bg, 1.0)
        button_hover_css = rgba_css(button_hover, 1.0)
        button_pressed_css = rgba_css(button_pressed, 1.0)
        save_bg_css = rgba_css(save_bg, 1.0)
        new_bg_css = rgba_css(new_bg, 1.0)
        pick_bg_css = rgba_css(pick_bg, 1.0)
        reset_bg_css = rgba_css(reset_bg, 1.0)
        disabled_button_bg = rgba_css(blend(field_bg, bg, 0.56), 0.92)
        disabled_button_text = rgba_css(text, 0.46)
        disabled_button_border = rgba_css(shift(field_bg, -0.28), 0.90)
        title_badge_bg = rgba_css(blend(header_bg, bg, 0.18), 0.82 if effective_transparent else 0.62)
        title_badge_border = rgba_css(shift(header_bg, -0.38), 0.90)
        checkbox_fill = rgba_css(blend(field_bg, bg, 0.16), 0.88 if effective_transparent else 0.94)
        checkbox_fill_checked = rgba_css(blend(selection_bg, field_bg, 0.20), 0.96)
        checkbox_fill_disabled = rgba_css(field_bg, 0.58)
        checkbox_border = shift(field_bg, -0.46)
        requested_transparent = bool(theme.get("transparent", False))
        if force_opaque_root:
            root_bg = bg
        elif effective_transparent:
            root_bg = "transparent"
        elif requested_transparent:
            root_bg = rgba_css(bg, 0.05)
        else:
            root_bg = bg
        tab_bg = "transparent"
        tab_hover = rgba_css(selection_bg, 0.26)
        tab_selected_bg = rgba_css(selection_bg, 0.34)
        tab_pane_bg = "transparent"
        root_selector = "QDialog"
        container_transparent_css = (
            "QWidget, QFrame, QGroupBox, QScrollArea, QTabWidget, QStackedWidget {"
            "background-color: transparent;"
            "}"
        )
        return (
            f"{root_selector} {{"
            f"background-color: {root_bg};"
            f"color: {text};"
            "}"
            "QLabel {"
            "background-color: transparent;"
            f"color: {text};"
            "font-weight: 700;"
            "}"
            f"QLabel[section='true'] {{ background-color: {title_badge_bg}; border: 1px solid {title_badge_border}; border-radius: 7px; padding: 2px 8px; }}"
            + container_transparent_css
            + (
            "QTabWidget::pane {"
            f"background-color: {tab_pane_bg};"
            f"border: 1px solid {field_border};"
            "border-radius: 4px;"
            "}"
            "QTabBar::tab {"
            f"background-color: {tab_bg};"
            f"color: {text};"
            f"border: 1px solid {field_border};"
            f"padding: {tab_padding};"
            "border-top-left-radius: 4px;"
            "border-top-right-radius: 4px;"
            "margin-right: 2px;"
            "}"
            "QTabBar::tab:selected {"
            f"background-color: {tab_selected_bg};"
            f"color: {selection_text};"
            "}"
            "QTabBar::tab:hover {"
            f"background-color: {tab_hover};"
            f"color: {selection_text};"
            "}"
            "QLineEdit, QTextEdit, QPlainTextEdit, QSpinBox, QDateEdit, QComboBox, QListWidget, QTableWidget, QListView, QTreeView {"
            f"background: {field_fill};"
            f"color: {text};"
            f"border: 1px solid {field_border};"
            "border-radius: 4px;"
            f"padding: {field_padding};"
            f"selection-background-color: {selection_bg};"
            f"selection-color: {selection_text};"
            "}"
            f"QCheckBox::indicator {{ width: {check_indicator_px}px; height: {check_indicator_px}px; border-radius: 3px; background: {checkbox_fill}; border: 1px solid {checkbox_border}; }}"
            f"QCheckBox::indicator:checked {{ background: {checkbox_fill_checked}; border: 1px solid {shift(selection_bg, -0.42)}; }}"
            f"QCheckBox::indicator:disabled {{ background: {checkbox_fill_disabled}; border: 1px solid {shift(checkbox_border, 0.08)}; }}"
            "QTableWidget, QListView, QTreeView {"
            "gridline-color: "
            f"{shift(field_border, -0.10)};"
            "alternate-background-color: transparent;"
            "}"
            "QHeaderView::section {"
            f"background-color: {header_bg};"
            f"color: {text};"
            f"border: 1px solid {field_border};"
            f"padding: {header_padding};"
            "font-weight: 700;"
            "}"
            "QComboBox QAbstractItemView {"
            f"background: {field_fill};"
            f"color: {text};"
            f"border: 1px solid {field_border};"
            f"selection-background-color: {row_selected_bg};"
            f"selection-color: {row_selected_text};"
            "}"
            "QListWidget::item, QTableWidget::item, QListView::item, QTreeView::item, QComboBox QAbstractItemView::item {"
            "background-color: transparent;"
            "}"
            "QListWidget::item:hover, QTableWidget::item:hover, QListView::item:hover, QTreeView::item:hover, QComboBox QAbstractItemView::item:hover {"
            f"background-color: {hover_bg};"
            f"color: {hover_text};"
            "}"
            "QListWidget::item:selected, QTableWidget::item:selected, QListView::item:selected, QTreeView::item:selected, QComboBox QAbstractItemView::item:selected {"
            f"background-color: {row_selected_bg};"
            f"color: {row_selected_text};"
            "}"
            "QPushButton {"
            f"background-color: {button_bg_css};"
            f"color: {button_text};"
            f"border: 1px solid {button_border};"
            "border-radius: 4px;"
            f"padding: {button_padding};"
            "}"
            "QPushButton#DepotFramelessCloseButton {"
            "background-color: rgba(225,80,80,220);"
            "border: 1px solid rgba(255,135,135,235);"
            "border-radius: 11px;"
            "color: #FFFFFF;"
            "font-size: 12px;"
            "font-weight: 800;"
            "padding: 0px;"
            "}"
            "QPushButton#DepotFramelessCloseButton:hover {"
            "background-color: rgba(235,95,95,240);"
            "}"
            "QPushButton#DepotFramelessCloseButton:pressed {"
            "background-color: rgba(198,74,74,240);"
            "}"
            "QPushButton:hover {"
            f"background-color: {button_hover_css};"
            "}"
            "QPushButton:pressed {"
            f"background-color: {button_pressed_css};"
            "}"
            "QPushButton[actionRole='save'] {"
            f"background-color: {save_bg_css};"
            f"color: {save_text};"
            f"border: 1px solid {save_border};"
            "}"
            "QPushButton[actionRole='new'] {"
            f"background-color: {new_bg_css};"
            f"color: {new_text};"
            f"border: 1px solid {new_border};"
            "}"
            "QPushButton[actionRole='pick'] {"
            f"background-color: {pick_bg_css};"
            f"color: {pick_text};"
            f"border: 1px solid {pick_border};"
            "}"
            "QPushButton[actionRole='reset'] {"
            f"background-color: {reset_bg_css};"
            f"color: {reset_text};"
            f"border: 1px solid {reset_border};"
            "}"
            "QPushButton:disabled {"
            f"background-color: {disabled_button_bg};"
            f"color: {disabled_button_text};"
            f"border: 1px solid {disabled_button_border};"
            "}"
            )
        )

    def _select_popup_color(self, kind: str, field: str) -> None:
        current = QColor(self._resolved_popup_theme(kind).get(field, "#FFFFFF"))
        color = show_flowgrid_themed_color(self, self, kind, f"Pick {kind.title()} {field.title()} Color", current)
        if not color.isValid():
            return
        theme = self._materialize_popup_theme_for_edit(kind)
        theme[field] = normalize_hex(color.name().upper(), theme.get(field, "#FFFFFF"))
        theme["inherit_main_theme"] = False
        self.queue_save_config()
        self._refresh_popup_themes()

    def _pick_popup_theme_color(self, kind: str, field: str) -> None:
        current = QColor(self._resolved_popup_theme(kind).get(field, "#FFFFFF"))
        color = show_flowgrid_themed_color(self, self, kind, f"Pick {kind.title()} {field.title()} Color", current)
        if not color.isValid():
            return
        theme = self._materialize_popup_theme_for_edit(kind)
        theme[field] = normalize_hex(color.name().upper(), theme.get(field, "#FFFFFF"))
        theme["inherit_main_theme"] = False
        self._refresh_popup_theme_tab(kind)
        self._refresh_popup_themes()
        self.queue_save_config()

    def _popup_optional_default_color(self, kind: str, field: str) -> str:
        theme = self._resolved_popup_theme(kind)
        field_bg = normalize_hex(theme.get("field_bg", self.palette_data.get("input_bg", "#FFFFFF")), "#FFFFFF")
        selected = normalize_hex(
            theme.get("row_selected_color", self.palette_data.get("accent", DEFAULT_THEME_ACCENT)),
            DEFAULT_THEME_ACCENT,
        )
        if field == "header_color":
            return blend(
                field_bg,
                normalize_hex(theme.get("background", self.palette_data.get("surface", DEFAULT_THEME_SURFACE)), DEFAULT_THEME_SURFACE),
                0.25,
            )
        if field == "row_hover_color":
            return blend(selected, field_bg, 0.55)
        if field == "row_selected_color":
            return selected
        return field_bg

    def _pick_popup_optional_color(self, kind: str, field: str) -> None:
        theme = self._materialize_popup_theme_for_edit(kind)
        current_hex = normalize_hex(theme.get(field, ""), "")
        if not current_hex:
            current_hex = self._popup_optional_default_color(kind, field)
        color = show_flowgrid_themed_color(
            self,
            self,
            kind,
            f"Pick {kind.title()} {field.replace('_', ' ').title()} Color",
            QColor(current_hex),
        )
        if not color.isValid():
            return
        theme[field] = normalize_hex(color.name().upper(), "")
        theme["inherit_main_theme"] = False
        self._refresh_popup_theme_tab(kind)
        self._refresh_popup_themes()
        self.queue_save_config()

    def _clear_popup_optional_color(self, kind: str, field: str) -> None:
        theme = self._materialize_popup_theme_for_edit(kind)
        theme[field] = ""
        theme["inherit_main_theme"] = False
        self._refresh_popup_theme_tab(kind)
        self._refresh_popup_themes()
        self.queue_save_config()

    def on_popup_theme_control_changed(self, kind: str) -> None:
        theme = self._materialize_popup_theme_for_edit(kind)
        style_combo = getattr(self, f"{kind}_control_style_combo", None)
        fade_slider = getattr(self, f"{kind}_control_fade_slider", None)
        opacity_slider = getattr(self, f"{kind}_control_opacity_slider", None)
        tail_slider = getattr(self, f"{kind}_control_tail_opacity_slider", None)
        if style_combo is None or fade_slider is None or opacity_slider is None or tail_slider is None:
            return
        style = str(style_combo.currentText() or "Fade Left to Right").strip()
        if style not in {"Solid", "Fade Left to Right", "Fade Right to Left", "Fade Center Out"}:
            style = "Fade Left to Right"
        theme["control_style"] = style
        theme["control_fade_strength"] = int(clamp(int(fade_slider.value()), 0, 100))
        theme["control_opacity"] = int(clamp(int(opacity_slider.value()), 0, 100))
        theme["control_tail_opacity"] = int(clamp(int(tail_slider.value()), 0, 100))
        theme["inherit_main_theme"] = False

        fade_value = getattr(self, f"{kind}_control_fade_value", None)
        if fade_value is not None:
            fade_value.setText(f"{int(theme['control_fade_strength'])}%")
        opacity_value = getattr(self, f"{kind}_control_opacity_value", None)
        if opacity_value is not None:
            opacity_value.setText(f"{int(theme['control_opacity'])}%")
        tail_value = getattr(self, f"{kind}_control_tail_opacity_value", None)
        if tail_value is not None:
            tail_value.setText(f"{int(theme['control_tail_opacity'])}%")

        self._refresh_popup_themes()
        self.queue_save_config()

    def _refresh_popup_themes(self) -> None:
        if hasattr(self, "active_agent_window") and self.active_agent_window is not None:
            self.active_agent_window.apply_theme_styles()
        if hasattr(self, "active_qa_window") and self.active_qa_window is not None:
            self.active_qa_window.apply_theme_styles()
        if hasattr(self, "admin_dialog") and self.admin_dialog is not None:
            self.admin_dialog.apply_theme_styles()
        if hasattr(self, "depot_dashboard_dialog") and self.depot_dashboard_dialog is not None:
            self.depot_dashboard_dialog.apply_theme_styles()

    def on_popup_theme_preset_selected(self, kind: str, name: str) -> None:
        preset_name = str(name or "").strip()
        if not preset_name:
            return
        presets = self.config.get("theme_presets", {})
        preset = presets.get(preset_name)
        if not isinstance(preset, dict):
            return
        primary = normalize_hex(str(preset.get("primary", DEFAULT_THEME_PRIMARY)), DEFAULT_THEME_PRIMARY)
        accent = normalize_hex(str(preset.get("accent", DEFAULT_THEME_ACCENT)), DEFAULT_THEME_ACCENT)
        surface = normalize_hex(str(preset.get("surface", DEFAULT_THEME_SURFACE)), DEFAULT_THEME_SURFACE)
        popup_palette = compute_palette({"primary": primary, "accent": accent, "surface": surface})
        theme = self.config.setdefault(f"{kind}_theme", {})
        theme["background"] = normalize_hex(popup_palette.get("control_bg", surface), surface)
        theme["text"] = normalize_hex(popup_palette.get("label_text", readable_text(surface)), readable_text(surface))
        theme["field_bg"] = normalize_hex(
            popup_palette.get("input_bg", blend(surface, primary, 0.18)),
            blend(surface, primary, 0.18),
        )
        theme["inherit_main_theme"] = False
        self.config[f"{kind}_selected_theme_preset"] = preset_name
        self._refresh_popup_theme_tab(kind)
        self._refresh_popup_themes()
        self.queue_save_config()

    def _refresh_popup_theme_tab(self, kind: str) -> None:
        swatches_key = f"{kind}_color_swatches"
        if not hasattr(self, swatches_key):
            return
        color_swatches = getattr(self, swatches_key)
        theme = self._resolved_popup_theme(kind)
        config_key = self._popup_window_on_top_config_key(kind)

        preset_combo = getattr(self, f"{kind}_theme_preset_combo", None)
        if preset_combo is not None:
            presets = self.config.get("theme_presets", {})
            selected = str(self.config.get(f"{kind}_selected_theme_preset", "") or "").strip()
            if not selected:
                selected = str(self.config.get("selected_theme_preset", "") or "").strip()
            preset_combo.blockSignals(True)
            preset_combo.clear()
            preset_combo.addItems(list(presets.keys()))
            if selected in presets:
                preset_combo.setCurrentText(selected)
            elif preset_combo.count() > 0:
                preset_combo.setCurrentIndex(0)
            preset_combo.blockSignals(False)

        for fld, swatch in color_swatches.items():
            value = normalize_hex(theme.get(fld, "#FFFFFF"), "#FFFFFF")
            swatch.setText(value)
            swatch.setStyleSheet(
                "QPushButton {"
                f"background-color: {rgba_css(value, 0.75)};"
                f"color: {readable_text(value)};"
                f"border: 1px solid {shift(value, -0.45)};"
                "font-weight: 700;"
                "}"
            )

        transparent_check_key = f"{kind}_transparent_bg_check"
        if hasattr(self, transparent_check_key):
            transparent_check = getattr(self, transparent_check_key)
            transparent_check.blockSignals(True)
            transparent_check.setChecked(bool(theme.get("transparent", False)))
            transparent_check.blockSignals(False)

        always_on_top_check = getattr(self, f"{kind}_window_always_on_top_check", None)
        if always_on_top_check is not None:
            default_on_top = kind in {"agent", "qa"}
            always_on_top_check.blockSignals(True)
            always_on_top_check.setChecked(bool(self.config.get(config_key, default_on_top)))
            always_on_top_check.blockSignals(False)

        compact_anchor_combo = getattr(self, f"{kind}_compact_anchor_combo", None)
        if compact_anchor_combo is not None:
            anchor_value = str(self.config.get("agent_window_compact_anchor", "TopRight") or "TopRight").strip()
            if compact_anchor_combo.findText(anchor_value) < 0:
                anchor_value = "TopRight"
            compact_anchor_combo.blockSignals(True)
            compact_anchor_combo.setCurrentText(anchor_value)
            compact_anchor_combo.blockSignals(False)

        style_combo = getattr(self, f"{kind}_control_style_combo", None)
        if style_combo is not None:
            style_combo.blockSignals(True)
            style_value = str(theme.get("control_style", "Fade Left to Right") or "Fade Left to Right")
            style_idx = style_combo.findText(style_value)
            style_combo.setCurrentIndex(style_idx if style_idx >= 0 else 1)
            style_combo.blockSignals(False)

        fade_slider = getattr(self, f"{kind}_control_fade_slider", None)
        fade_value = getattr(self, f"{kind}_control_fade_value", None)
        if fade_slider is not None:
            fade_slider.blockSignals(True)
            fade_num = int(clamp(int(theme.get("control_fade_strength", 65)), 0, 100))
            fade_slider.setValue(fade_num)
            fade_slider.blockSignals(False)
            if fade_value is not None:
                fade_value.setText(f"{fade_num}%")

        opacity_slider = getattr(self, f"{kind}_control_opacity_slider", None)
        opacity_value = getattr(self, f"{kind}_control_opacity_value", None)
        if opacity_slider is not None:
            opacity_slider.blockSignals(True)
            opacity_num = int(clamp(int(theme.get("control_opacity", 82)), 0, 100))
            opacity_slider.setValue(opacity_num)
            opacity_slider.blockSignals(False)
            if opacity_value is not None:
                opacity_value.setText(f"{opacity_num}%")

        tail_slider = getattr(self, f"{kind}_control_tail_opacity_slider", None)
        tail_value = getattr(self, f"{kind}_control_tail_opacity_value", None)
        if tail_slider is not None:
            tail_slider.blockSignals(True)
            tail_num = int(clamp(int(theme.get("control_tail_opacity", 0)), 0, 100))
            tail_slider.setValue(tail_num)
            tail_slider.blockSignals(False)
            if tail_value is not None:
                tail_value.setText(f"{tail_num}%")

        optional_swatches_key = f"{kind}_optional_color_swatches"
        if hasattr(self, optional_swatches_key):
            optional_swatches = getattr(self, optional_swatches_key)
            for field, swatch in optional_swatches.items():
                raw_value = normalize_hex(theme.get(field, ""), "")
                color_value = raw_value or self._popup_optional_default_color(kind, field)
                label = raw_value if raw_value else f"Auto ({color_value})"
                swatch.setText(label)
                swatch.setStyleSheet(
                    "QPushButton {"
                    f"background-color: {rgba_css(color_value, 0.75)};"
                    f"color: {readable_text(color_value)};"
                    f"border: 1px solid {shift(color_value, -0.45)};"
                    "font-weight: 700;"
                    "}"
                )

    def on_theme_page_background_option_changed(self, checked: bool) -> None:
        self.config["background_tint_enabled"] = not bool(checked)
        self.refresh_settings_controls()
        self.refresh_all_views()
        self.queue_save_config()

    def on_popup_auto_reinherit_changed(self, checked: bool) -> None:
        enabled = bool(checked)
        self.config["popup_auto_reinherit_enabled"] = enabled
        if enabled:
            repaired = self._auto_reinherit_popup_defaults()
            if repaired:
                self.mark_background_dirty()
                self.refresh_theme_controls()
                self._refresh_popup_themes()
        self.queue_save_config()

    def on_popup_background_option_changed(self, kind: str, checked: bool) -> None:
        theme = self._materialize_popup_theme_for_edit(kind)
        theme["inherit_main_theme"] = False
        theme["transparent"] = bool(checked)
        self._refresh_popup_themes()
        self.queue_save_config()

    def on_popup_window_always_on_top_changed(self, kind: str, checked: bool) -> None:
        self._apply_popup_window_on_top_preference(kind, checked)
        self.queue_save_config()

    def on_agent_compact_anchor_changed(self, value: str) -> None:
        anchor_value = str(value or "").strip() or "TopRight"
        self.config["agent_window_compact_anchor"] = anchor_value
        self.queue_save_config()

    def reset_theme(self) -> None:
        selected = self.config.get("selected_theme_preset")
        presets = self.config.get("theme_presets", {})
        fallback = presets.get(selected) or next(iter(presets.values()))
        self.config["theme"] = deep_clone(fallback)
        self._theme_updated()

    def on_theme_preset_selected(self, name: str) -> None:
        if not name:
            return
        preset = self.config.get("theme_presets", {}).get(name)
        if not preset:
            return
        self.config["selected_theme_preset"] = name
        self.config["theme"] = deep_clone(preset)
        self._theme_updated()

    def create_theme_preset(self) -> None:
        name = f"Preset {len(self.config.get('theme_presets', {})) + 1}"
        base = name
        suffix = 1
        while name in self.config["theme_presets"]:
            suffix += 1
            name = f"{base} {suffix}"
        self.config["theme_presets"][name] = deep_clone(self.config["theme"])
        self.config["selected_theme_preset"] = name
        self.refresh_theme_controls()
        self.queue_save_config()

    def save_theme_preset(self) -> None:
        name = self.theme_preset_combo.currentText().strip()
        if not name:
            return
        self.config["theme_presets"][name] = deep_clone(self.config["theme"])
        self.config["selected_theme_preset"] = name
        self.refresh_theme_controls()
        self.queue_save_config()

    def open_image_layers_dialog(self, kind: str | bool = "main") -> None:
        if isinstance(kind, bool):
            kind = "main"
        if not isinstance(kind, str):
            kind = "main"
        kind = kind.strip().lower()
        if kind not in {"main", "agent", "qa", "admin", "dashboard"}:
            kind = "main"

        if kind == "main":
            if self.image_dialog is None:
                self.image_dialog = ImageLayersDialog(self, kind="main")

            popup_pos = self.config.get("popup_positions", {}).get("image_layers")
            if isinstance(popup_pos, dict) and "x" in popup_pos and "y" in popup_pos and not self.image_dialog.isVisible():
                self.image_dialog.move(int(popup_pos["x"]), int(popup_pos["y"]))

            self.image_dialog.refresh_list()
            self.image_dialog.apply_theme_styles()
            self.image_dialog.show()
            self.image_dialog.raise_()
            self.image_dialog.activateWindow()
        else:
            # Popup dialogs (agent, qa, admin, dashboard)
            dialog_key = f"{kind}_image_dialog"
            if not hasattr(self, dialog_key):
                setattr(self, dialog_key, ImageLayersDialog(self, kind=kind))
            dialog = getattr(self, dialog_key)

            popup_pos = self.config.get("popup_positions", {}).get(f"image_layers_{kind}")
            if isinstance(popup_pos, dict) and "x" in popup_pos and "y" in popup_pos and not dialog.isVisible():
                dialog.move(int(popup_pos["x"]), int(popup_pos["y"]))

            dialog.refresh_list()
            dialog.apply_theme_styles()
            dialog.show()
            dialog.raise_()
            dialog.activateWindow()

    def open_quick_layout_dialog(self) -> None:
        self._reveal_immediately()
        if self.quick_layout_dialog is None:
            self.quick_layout_dialog = QuickLayoutDialog(self)

        popup_pos = self.config.get("popup_positions", {}).get("quick_layout")
        if isinstance(popup_pos, dict) and "x" in popup_pos and "y" in popup_pos and not self.quick_layout_dialog.isVisible():
            self.quick_layout_dialog.move(int(popup_pos["x"]), int(popup_pos["y"]))

        self.quick_layout_dialog.apply_theme_styles()
        self.quick_layout_dialog.refresh_cards()
        self.quick_layout_dialog.show()
        self.quick_layout_dialog.raise_()
        self.quick_layout_dialog.activateWindow()

    # ----------------------- Update Flow --------------------------- #
    def _refresh_install_status_cache(self) -> None:
        try:
            self._install_status_cache = current_install_status()
        except Exception as exc:
            _runtime_log_event(
                "update.status_cache_refresh_failed",
                severity="warning",
                summary="Failed refreshing the local install/update status cache.",
                exc=exc,
            )

    def _refresh_update_status_labels(self) -> None:
        self._refresh_install_status_cache()
        source_label = str(self._install_status_cache.get("update_source_label") or self._default_update_source_label()).strip() or self._default_update_source_label()
        channel_display_name = str(self._install_status_cache.get("channel_display_name") or self.channel_display_name or APP_TITLE).strip() or APP_TITLE
        read_only_db = bool(self._install_status_cache.get("read_only_db", self.runtime_options.read_only_db))
        snapshot_source_root = str(
            self._install_status_cache.get("snapshot_source_root") or self.runtime_options.snapshot_source_root or ""
        ).strip()
        installed_short = str(self._install_status_cache.get("installed_short_sha") or "").strip() or "-"
        updater_path = str(self._install_status_cache.get("local_updater_path") or "").strip()
        update_summary = str(self._install_status_cache.get("last_check_summary") or "").strip() or "Not checked yet."
        asset_summary = str(self._install_status_cache.get("last_shared_asset_sync_summary") or "").strip() or "Not synced yet."
        commit_comments = self._update_commit_comments_text()

        if self._update_check_in_progress:
            update_summary = f"Checking {source_label} for updates..."
        if self._shared_asset_pull_in_progress:
            asset_summary = "Syncing shared assets..."

        if hasattr(self, "channel_mode_status_label"):
            mode_text = f"{channel_display_name} | {'Read-only snapshot' if read_only_db else 'Read/write shared root'}"
            if snapshot_source_root and read_only_db:
                mode_text += f" | Snapshot source: {snapshot_source_root}"
            self.channel_mode_status_label.setText(mode_text)
            self.channel_mode_status_label.setToolTip(updater_path or mode_text)
        if hasattr(self, "update_commit_status_label"):
            self.update_commit_status_label.setText(installed_short)
            self.update_commit_status_label.setToolTip(updater_path or "Local updater path unavailable.")
        if hasattr(self, "update_check_status_label"):
            self.update_check_status_label.setText(update_summary)
        if hasattr(self, "shared_assets_status_label"):
            self.shared_assets_status_label.setText(asset_summary)
        if hasattr(self, "last_commit_comments_label"):
            self.last_commit_comments_label.setText(self._compact_update_comments(commit_comments))
            self.last_commit_comments_label.setToolTip(commit_comments)

        pending_update = bool(self._pending_update_info.get("can_install"))
        if hasattr(self, "check_updates_button"):
            self.check_updates_button.setEnabled(not self._update_check_in_progress)
        if hasattr(self, "pull_shared_assets_button"):
            self.pull_shared_assets_button.setEnabled(not self._shared_asset_pull_in_progress)
        if hasattr(self, "install_update_button"):
            self.install_update_button.setEnabled(
                pending_update and not self._update_check_in_progress and not self._shared_asset_pull_in_progress
            )
            if pending_update:
                remote_short = str(self._pending_update_info.get("remote_short_sha") or "").strip()
                self.install_update_button.setToolTip(
                    f"Install the available {source_label} update ({remote_short or 'new commit'}) and relaunch {channel_display_name}."
                )
            else:
                self.install_update_button.setToolTip(f"No {source_label} update is currently pending.")

    def _run_background_task(self, task_name: str, worker) -> None:
        def _target() -> None:
            try:
                payload = worker()
            except Exception as exc:
                _runtime_log_event(
                    "update.background_task_failed",
                    severity="warning",
                    summary="A Flowgrid background update task failed unexpectedly.",
                    exc=exc,
                    context={"task_name": task_name},
                )
                payload = {
                    "status": "warning",
                    "summary": f"{task_name} failed: {type(exc).__name__}: {exc}",
                }
            self._background_task_results.put(
                (task_name, dict(payload) if isinstance(payload, dict) else {"status": "warning"})
            )

        thread = threading.Thread(target=_target, name=f"flowgrid-{task_name}", daemon=True)
        thread.start()

    def _drain_background_task_results(self) -> None:
        while True:
            try:
                task_name, payload = self._background_task_results.get_nowait()
            except queue.Empty:
                break
            if task_name == "startup_maintenance":
                self._update_check_in_progress = False
                self._shared_asset_pull_in_progress = False
                self._handle_update_check_result(payload.get("update", {}), show_dialog=False)
                self._handle_shared_asset_result(payload.get("assets", {}), show_dialog=False)
            elif task_name == "manual_update_check":
                self._update_check_in_progress = False
                self._handle_update_check_result(payload, show_dialog=True)
            elif task_name == "manual_asset_pull":
                self._shared_asset_pull_in_progress = False
                self._handle_shared_asset_result(payload, show_dialog=True)
            self._refresh_update_status_labels()

    def _perform_startup_maintenance(self) -> dict[str, Any]:
        return {
            "update": check_for_updates(),
            "assets": sync_shared_assets(),
        }

    def _start_startup_maintenance(self) -> None:
        if self._startup_update_check_started:
            return
        self._startup_update_check_started = True
        self._update_check_in_progress = True
        self._shared_asset_pull_in_progress = True
        self._refresh_update_status_labels()
        self._run_background_task("startup_maintenance", self._perform_startup_maintenance)

    def _handle_update_check_result(self, payload: dict[str, Any], *, show_dialog: bool) -> None:
        status = str(payload.get("status") or "").strip().lower()
        summary = str(payload.get("summary") or "").strip() or "Update check completed."
        if status == "update_available":
            self._pending_update_info = dict(payload)
        elif status == "up_to_date":
            self._pending_update_info = {}

        if show_dialog:
            if status == "warning":
                self._show_shell_message(QMessageBox.Icon.Warning, "Update Check", summary)
            elif status == "update_available":
                self._show_shell_message(QMessageBox.Icon.Information, "Update Available", summary)
            else:
                self._show_shell_message(QMessageBox.Icon.Information, "Update Check", summary)

    def _handle_shared_asset_result(self, payload: dict[str, Any], *, show_dialog: bool) -> None:
        status = str(payload.get("status") or "").strip().lower()
        summary = str(payload.get("summary") or "").strip() or "Shared asset sync completed."
        if show_dialog:
            if status == "warning":
                self._show_shell_message(QMessageBox.Icon.Warning, "Shared Assets", summary)
            else:
                self._show_shell_message(QMessageBox.Icon.Information, "Shared Assets", summary)

    def on_check_updates_clicked(self) -> None:
        if self._update_check_in_progress:
            return
        self._update_check_in_progress = True
        self._refresh_update_status_labels()
        self._run_background_task("manual_update_check", check_for_updates)

    def on_pull_shared_assets_clicked(self) -> None:
        if self._shared_asset_pull_in_progress:
            return
        self._shared_asset_pull_in_progress = True
        self._refresh_update_status_labels()
        self._run_background_task("manual_asset_pull", sync_shared_assets)

    def on_install_update_clicked(self) -> None:
        pending = dict(self._pending_update_info)
        source_label = str(pending.get("update_source_label") or self._default_update_source_label()).strip() or self._default_update_source_label()
        if not pending or not bool(pending.get("can_install")):
            self._show_shell_message(QMessageBox.Icon.Information, "Install Update", f"No {source_label} update is currently pending.")
            return

        updater_path = _get_local_updater_path()
        if not updater_path.exists() or not updater_path.is_file():
            self._show_shell_message(
                QMessageBox.Icon.Warning,
                "Install Update",
                f"Local updater copy is missing:\n{updater_path}",
            )
            return

        launcher_path = _preferred_gui_python_executable()
        if not launcher_path.exists() or not launcher_path.is_file():
            self._show_shell_message(
                QMessageBox.Icon.Warning,
                "Install Update",
                f"Python launcher not found:\n{launcher_path}",
            )
            return

        try:
            subprocess.Popen(
                [
                    str(launcher_path),
                    str(updater_path),
                    "--parent-pid",
                    str(os.getpid()),
                    "--relaunch",
                ],
                cwd=str(updater_path.parent),
            )
        except Exception as exc:
            _runtime_log_event(
                "update.apply_launch_failed",
                severity="error",
                summary="Failed launching the standalone updater for an app update.",
                exc=exc,
                context={"updater_path": str(updater_path), "launcher_path": str(launcher_path)},
            )
            self._show_shell_message(
                QMessageBox.Icon.Warning,
                "Install Update",
                f"Could not launch the updater:\n{type(exc).__name__}: {exc}",
            )
            return

        _runtime_log_event(
            "update.apply_requested",
            severity="info",
            summary="Flowgrid update install was requested from Settings.",
            context={
                "updater_path": str(updater_path),
                "remote_commit_sha": str(pending.get("remote_commit_sha") or ""),
            },
        )
        self._close_shell_internal_dialogs()
        if hasattr(self, "window_manager") and self.window_manager is not None:
            self.window_manager.close_all()
        self.save_config()
        app = QApplication.instance()
        if app is not None:
            QTimer.singleShot(150, app.quit)

    # ----------------------- Settings Screen ------------------------ #
    def refresh_settings_controls(self) -> None:
        self.opacity_slider.blockSignals(True)
        self.hover_delay_slider.blockSignals(True)
        self.hover_fade_in_slider.blockSignals(True)
        self.hover_fade_out_slider.blockSignals(True)
        self.always_on_top_check.blockSignals(True)
        self.sidebar_right_switch.blockSignals(True)

        opacity = float(clamp(float(self.config.get("window_opacity", 1.0)), 0.0, 1.0))
        self.opacity_slider.setValue(int(opacity * 100))
        self.opacity_value.setText(f"{opacity:.2f}")
        delay_s = int(clamp(int(self.config.get("hover_reveal_delay_s", 5)), 0, 10))
        fade_in_s = int(clamp(int(self.config.get("hover_fade_in_s", 5)), 0, 10))
        fade_out_s = int(clamp(int(self.config.get("hover_fade_out_s", 5)), 0, 10))
        self.hover_delay_slider.setValue(delay_s)
        self.hover_fade_in_slider.setValue(fade_in_s)
        self.hover_fade_out_slider.setValue(fade_out_s)
        self.hover_delay_value.setText(f"{delay_s}s")
        self.hover_fade_in_value.setText(f"{fade_in_s}s")
        self.hover_fade_out_value.setText(f"{fade_out_s}s")
        self.always_on_top_check.setChecked(bool(self.config.get("always_on_top", False)))
        self.sidebar_right_switch.setChecked(bool(self.config.get("sidebar_on_right", False)))

        self.opacity_slider.blockSignals(False)
        self.hover_delay_slider.blockSignals(False)
        self.hover_fade_in_slider.blockSignals(False)
        self.hover_fade_out_slider.blockSignals(False)
        self.always_on_top_check.blockSignals(False)
        self.sidebar_right_switch.blockSignals(False)
        self._refresh_sidebar_switch_caption()
        self._refresh_update_status_labels()

    def on_settings_changed(self) -> None:
        self.config["always_on_top"] = bool(self.always_on_top_check.isChecked())
        self._apply_window_flags()
        self.apply_theme_styles()
        self.refresh_all_views()
        self.queue_save_config()

    def on_sidebar_position_changed(self, checked: bool) -> None:
        self.config["sidebar_on_right"] = bool(checked)
        self._refresh_sidebar_switch_caption()
        self._apply_sidebar_position()
        self.refresh_all_views()
        self.queue_save_config()

    def _refresh_sidebar_switch_caption(self) -> None:
        is_right = bool(self.sidebar_right_switch.isChecked())
        if hasattr(self, "sidebar_switch_status"):
            self.sidebar_switch_status.setText("Sidebar position: Right" if is_right else "Sidebar position: Left")
        if hasattr(self, "sidebar_left_label"):
            self.sidebar_left_label.setStyleSheet("font-weight: 800;" if not is_right else "font-weight: 500;")
        if hasattr(self, "sidebar_right_label"):
            self.sidebar_right_label.setStyleSheet("font-weight: 800;" if is_right else "font-weight: 500;")

    def on_hover_settings_changed(self) -> None:
        self.config["hover_reveal_delay_s"] = int(self.hover_delay_slider.value())
        self.config["hover_fade_in_s"] = int(self.hover_fade_in_slider.value())
        self.config["hover_fade_out_s"] = int(self.hover_fade_out_slider.value())
        self.hover_delay_value.setText(f"{self.config['hover_reveal_delay_s']}s")
        self.hover_fade_in_value.setText(f"{self.config['hover_fade_in_s']}s")
        self.hover_fade_out_value.setText(f"{self.config['hover_fade_out_s']}s")
        if not self._hover_inside:
            self._set_ui_opacity(self._effective_shell_idle_opacity())
        self.queue_save_config()

    def _base_opacity(self) -> float:
        return float(clamp(float(self.config.get("window_opacity", 1.0)), 0.0, 1.0))

    def _hover_delay_ms(self) -> int:
        return int(clamp(int(self.config.get("hover_reveal_delay_s", 5)), 0, 10) * 1000)

    def _hover_fade_in_ms(self) -> int:
        return int(clamp(int(self.config.get("hover_fade_in_s", 5)), 0, 10) * 1000)

    def _hover_fade_out_ms(self) -> int:
        return int(clamp(int(self.config.get("hover_fade_out_s", 5)), 0, 10) * 1000)

    def _start_opacity_animation(self, target_opacity: float, duration_ms: int) -> None:
        target = float(clamp(target_opacity, 0.0, 1.0))
        self._ui_opacity_anim.stop()
        if duration_ms <= 0:
            self._set_ui_opacity(target)
            return
        self._ui_opacity_anim.setDuration(duration_ms)
        self._ui_opacity_anim.setStartValue(self._ui_opacity_current)
        self._ui_opacity_anim.setEndValue(target)
        self._ui_opacity_anim.start()

    def _on_hover_delay_elapsed(self) -> None:
        if not self._hover_inside:
            return
        if self._base_opacity() >= 0.999:
            return
        self._hover_revealed = True
        self._start_opacity_animation(1.0, self._hover_fade_in_ms())

    def on_opacity_changed(self, slider_value: int) -> None:
        opacity = clamp(slider_value / 100.0, 0.0, 1.0)
        self.config["window_opacity"] = opacity
        self.opacity_value.setText(f"{opacity:.2f}")
        if self._hover_revealed:
            self._set_ui_opacity(1.0)
        else:
            self._set_ui_opacity(self._effective_shell_idle_opacity())
        self.queue_save_config()

    def _apply_window_flags(self) -> None:
        self.setWindowOpacity(1.0)
        self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint, bool(self.config.get("always_on_top", False)))
        self.show()
        if self._hover_revealed:
            self._set_ui_opacity(1.0)
        else:
            self._set_ui_opacity(self._effective_shell_idle_opacity())

    def eventFilter(self, watched, event) -> bool:  # noqa: N802
        if event.type() == QEvent.Type.Resize:
            try:
                if watched in [scroll.viewport() for scroll in self.quick_tab_scrolls]:
                    self.refresh_quick_grid()
                    return False
            except Exception as exc:
                _runtime_log_event(
                    "ui.event_filter_resize_handler_failed",
                    severity="warning",
                    summary="Resize event filter handling failed; continuing with default event processing.",
                    exc=exc,
                    context={"watched": repr(watched)},
                )
        if isinstance(watched, QWidget) and watched is not None and (watched is self or self.isAncestorOf(watched)):
            if event.type() == QEvent.Type.MouseButtonPress and isinstance(event, QMouseEvent):
                self._reveal_immediately()
                if event.button() == Qt.MouseButton.LeftButton and not _is_drag_blocked_widget(watched):
                    self._drag_offset = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
                    event.accept()
                    return True
            elif event.type() == QEvent.Type.MouseMove and isinstance(event, QMouseEvent):
                if self._drag_offset is not None and event.buttons() & Qt.MouseButton.LeftButton:
                    self.move(event.globalPosition().toPoint() - self._drag_offset)
                    event.accept()
                    return True
            elif event.type() == QEvent.Type.MouseButtonRelease and self._drag_offset is not None:
                self._drag_offset = None
                if isinstance(event, QMouseEvent):
                    event.accept()
                    return True
        return super().eventFilter(watched, event)

    # ---------------------- Icon / Titlebar ------------------------- #
    def pick_custom_icon(self) -> None:
        icon_path, _ = show_flowgrid_themed_open_file_name(
            self,
            self,
            "main",
            "Select Window Icon",
            str(Path.home()),
            "Images (*.png *.ico *.jpg *.jpeg *.bmp *.webp);;All Files (*.*)",
        )
        if not icon_path:
            return
        self.config["app_icon_path"] = icon_path
        self.apply_window_icon()
        self._sync_desktop_shortcut_after_icon_change()
        self.queue_save_config()

    def clear_custom_icon(self) -> None:
        self.config["app_icon_path"] = ""
        self.apply_window_icon()
        self._sync_desktop_shortcut_after_icon_change()
        self.queue_save_config()

    def _build_smoothed_icon(self, icon_path: str) -> QIcon:
        return _build_smoothed_qicon(icon_path)

    def _sync_desktop_shortcut_after_icon_change(self) -> None:
        if self.runtime_options.skip_shortcut_sync:
            return
        status, detail = _sync_desktop_shortcut(self.config, create_if_missing=False)
        if status == "failed":
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Shortcut Update Failed",
                f"{detail}\n\nSee the runtime log for additional diagnostics.",
            )

    def apply_window_icon(self) -> None:
        icon_source = _resolve_active_app_icon_path(self.config)
        icon = self._build_smoothed_icon(str(icon_source)) if icon_source is not None else QIcon()
        if icon.isNull():
            icon = QApplication.style().standardIcon(QApplication.style().StandardPixmap.SP_DesktopIcon)
        self.setWindowIcon(icon)
        app = QApplication.instance()
        if app is not None:
            app.setWindowIcon(icon)
        self.titlebar.update_icon(icon)

    # ------------------------- Window events ------------------------ #
    def _apply_window_mask(self) -> None:
        if self.width() <= 0 or self.height() <= 0:
            return
        path = QPainterPath()
        path.addRoundedRect(QRectF(self.rect()), self._corner_radius, self._corner_radius)
        self.setMask(QRegion(path.toFillPolygon().toPolygon()))

    def _restore_window_position(self) -> None:
        pos = self.config.get("window_position")
        if isinstance(pos, dict) and "x" in pos and "y" in pos:
            x = safe_int(pos.get("x", 0), 0)
            y = safe_int(pos.get("y", 0), 0)
            win_w = max(120, int(self.width()))
            win_h = max(120, int(self.height()))
            target_rect = QRect(int(x), int(y), win_w, win_h)

            screens = QGuiApplication.screens()
            visible_geometry: QRect | None = None
            for screen in screens:
                try:
                    geometry = screen.availableGeometry()
                except Exception as exc:
                    _runtime_log_event(
                        "ui.restore_window_screen_geometry_failed",
                        severity="warning",
                        summary="Failed reading screen geometry while restoring window position; checking next screen.",
                        exc=exc,
                    )
                    continue
                if geometry.intersects(target_rect):
                    visible_geometry = geometry
                    break

            if visible_geometry is None:
                primary = QGuiApplication.primaryScreen()
                if primary is not None:
                    geometry = primary.availableGeometry()
                    x = int(geometry.left() + max(0, (geometry.width() - win_w) / 2))
                    y = int(geometry.top() + max(0, (geometry.height() - win_h) / 2))
                    self.config["window_position"] = {"x": x, "y": y}
                    self.queue_save_config()
                self.move(int(x), int(y))
                return

            max_x = int(visible_geometry.right() - win_w + 1)
            max_y = int(visible_geometry.bottom() - win_h + 1)
            clamped_x = int(clamp(x, visible_geometry.left(), max_x))
            clamped_y = int(clamp(y, visible_geometry.top(), max_y))
            if clamped_x != x or clamped_y != y:
                self.config["window_position"] = {"x": clamped_x, "y": clamped_y}
                self.queue_save_config()
            self.move(clamped_x, clamped_y)

    def mousePressEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        target = _drag_target_widget(self, event.position().toPoint())
        if event.button() == Qt.MouseButton.LeftButton and not _is_drag_blocked_widget(target):
            self._reveal_immediately()
            self._drag_offset = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if self._drag_offset is not None and event.buttons() & Qt.MouseButton.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_offset)
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        self._drag_offset = None
        super().mouseReleaseEvent(event)

    def resizeEvent(self, event) -> None:  # noqa: N802
        self._apply_window_mask()
        super().resizeEvent(event)

    def enterEvent(self, event) -> None:  # noqa: N802
        self._capture_external_target()
        self._hover_inside = True
        self._hover_delay_timer.stop()
        self._popup_leave_timer.stop()
        self._ui_opacity_anim.stop()

        # If we were already at full opacity, keep it stable when re-entering quickly.
        if self._ui_opacity_current >= 0.985:
            self._hover_revealed = True
            self._set_ui_opacity(1.0)
        elif self._base_opacity() < 0.999:
            self._hover_revealed = False
            self._hover_delay_timer.start(self._hover_delay_ms())
        else:
            self._hover_revealed = True
            self._set_ui_opacity(1.0)
        super().enterEvent(event)

    def leaveEvent(self, event) -> None:  # noqa: N802
        # Start fade-out check immediately when pointer exits the app.
        self._popup_leave_timer.stop()
        self._on_popup_leave_check()
        super().leaveEvent(event)

    def moveEvent(self, event) -> None:  # noqa: N802
        self.config["window_position"] = {"x": int(self.x()), "y": int(self.y())}
        self.queue_save_config()
        super().moveEvent(event)

    def changeEvent(self, event) -> None:  # noqa: N802
        if event.type() == QEvent.Type.ActivationChange and not self.isActiveWindow():
            if not self._has_active_popup() and not self._has_active_internal_dialog():
                self._popup_leave_timer.stop()
                self._begin_fade_out()
        super().changeEvent(event)

    def _has_visible_controlled_windows(self) -> bool:
        manager = getattr(self, "window_manager", None)
        if manager is None:
            return False
        for window in list(getattr(manager, "_windows", {}).values()):
            if window is None:
                continue
            try:
                if window.isVisible():
                    return True
            except Exception as exc:
                _runtime_log_event(
                    "ui.controlled_window_visibility_check_failed",
                    severity="warning",
                    summary="Failed checking whether a managed Flowgrid tool window is still visible.",
                    exc=exc,
                    context={"window_object": repr(window)},
                )
        return False

    def _persist_shell_close_state(self) -> None:
        self.config["window_position"] = {"x": int(self.x()), "y": int(self.y())}
        if self.image_dialog is not None:
            self.config.setdefault("popup_positions", {})["image_layers"] = {
                "x": int(self.image_dialog.x()),
                "y": int(self.image_dialog.y()),
            }
        if self.quick_layout_dialog is not None:
            self.config.setdefault("popup_positions", {})["quick_layout"] = {
                "x": int(self.quick_layout_dialog.x()),
                "y": int(self.quick_layout_dialog.y()),
            }

    def _close_shell_internal_dialogs(self) -> list[str]:
        failed_dialogs: list[str] = []
        dialog_targets = [
            ("image_dialog", self.image_dialog),
            ("quick_layout_dialog", self.quick_layout_dialog),
            ("quick_radial_menu", self.quick_radial_menu),
            ("quick_editor_dialog", getattr(self, "quick_editor_dialog", None)),
        ]
        for dialog_name, dialog in dialog_targets:
            if dialog is None:
                continue
            try:
                dialog.close()
            except Exception as exc:
                failed_dialogs.append(dialog_name)
                _runtime_log_event(
                    "ui.shell_internal_dialog_close_failed",
                    severity="warning",
                    summary="Failed closing a shell-owned Flowgrid dialog during shell cleanup.",
                    exc=exc,
                    context={"dialog_name": dialog_name},
                )
        return failed_dialogs

    def _shutdown_application(self, reason: str) -> bool:
        if self._shutdown_completed:
            return not self._shutdown_had_issues
        if self._shutdown_in_progress:
            return False

        shutdown_reason = str(reason or "").strip() or "shutdown"
        self._shutdown_in_progress = True
        issues: list[str] = []

        def _log_shutdown_issue(
            issue_key: str,
            event_key: str,
            summary: str,
            *,
            exc: BaseException | None = None,
            context: dict[str, Any] | None = None,
        ) -> None:
            issues.append(issue_key)
            event_context = {"reason": shutdown_reason}
            if context:
                event_context.update(context)
            _runtime_log_event(
                event_key,
                severity="warning",
                summary=summary,
                exc=exc,
                context=event_context,
            )

        try:
            self._persist_shell_close_state()

            self._drag_offset = None
            self._hover_inside = False
            self._hover_revealed = False
            self._update_check_in_progress = False
            self._shared_asset_pull_in_progress = False
            try:
                for timer_name in (
                    "_foreground_timer",
                    "_shared_sync_timer",
                    "_background_task_timer",
                    "_hover_delay_timer",
                    "_popup_leave_timer",
                ):
                    timer = getattr(self, timer_name, None)
                    if timer is not None:
                        timer.stop()
                self._ui_opacity_anim.stop()
            except Exception as exc:
                _log_shutdown_issue(
                    "timers",
                    "runtime.shutdown_stop_timers_failed",
                    "Failed stopping one or more shell timers during shutdown.",
                    exc=exc,
                )

            try:
                async_loader = getattr(self, "depot_async_loader", None)
                if async_loader is not None:
                    async_loader.cancel_all(reason=f"shell:{shutdown_reason}")
            except Exception as exc:
                _log_shutdown_issue(
                    "depot_async_cancel",
                    "runtime.shutdown_depot_async_cancel_failed",
                    "Failed cancelling shared workflow background reads during shutdown.",
                    exc=exc,
                )

            failed_internal_dialogs = self._close_shell_internal_dialogs()
            if failed_internal_dialogs:
                issues.extend([f"internal_dialog:{name}" for name in failed_internal_dialogs])

            try:
                if hasattr(self, "window_manager") and self.window_manager is not None:
                    self.window_manager.close_all()
            except Exception as exc:
                _log_shutdown_issue(
                    "controlled_windows",
                    "runtime.shutdown_controlled_windows_close_failed",
                    "Failed closing one or more managed Flowgrid tool windows during shutdown.",
                    exc=exc,
                )

            try:
                self.flush_pending_config_save()
            except Exception as exc:
                _log_shutdown_issue(
                    "config_flush",
                    "runtime.shutdown_flush_config_failed",
                    "Failed flushing the Flowgrid config during shutdown.",
                    exc=exc,
                    context={"config_path": str(self.config_path)},
                )

            app = QApplication.instance()
            if app is not None:
                try:
                    app.removeEventFilter(self)
                except Exception as exc:
                    _log_shutdown_issue(
                        "app_event_filter",
                        "runtime.shutdown_remove_app_event_filter_failed",
                        "Failed removing the application event filter during Flowgrid shutdown.",
                        exc=exc,
                    )
            for scroll in self.quick_tab_scrolls:
                try:
                    scroll.viewport().removeEventFilter(self)
                except Exception as exc:
                    _log_shutdown_issue(
                        "scroll_event_filter",
                        "ui.close_remove_event_filter_failed",
                        "Failed removing viewport event filter during app close.",
                        exc=exc,
                        context={"scroll_object": repr(scroll)},
                    )

            try:
                db_closed = self.depot_db.close(f"shell:{shutdown_reason}")
                if not db_closed:
                    issues.append("depot_db_close")
            except Exception as exc:
                _log_shutdown_issue(
                    "depot_db_close",
                    "runtime.shutdown_depot_db_close_failed",
                    "The Flowgrid shared workflow database close call raised unexpectedly during shutdown.",
                    exc=exc,
                    context={"db_path": str(getattr(self.depot_db, "db_path", ""))},
                )
        finally:
            self._shutdown_in_progress = False
            self._shutdown_completed = True
            self._shutdown_had_issues = bool(issues)
            _runtime_log_event(
                "runtime.shutdown_completed_with_issues" if issues else "runtime.shutdown_completed",
                severity="warning" if issues else "info",
                summary=(
                    "Flowgrid shutdown completed with cleanup issues."
                    if issues
                    else "Flowgrid shutdown completed and released runtime resources."
                ),
                context={
                    "reason": shutdown_reason,
                    "db_path": str(getattr(self.depot_db, "db_path", "")),
                    "issue_count": len(issues),
                    "issues": issues,
                },
            )
        return not issues

    def closeEvent(self, event) -> None:  # noqa: N802
        if self._has_visible_controlled_windows():
            self._persist_shell_close_state()
            self._close_shell_internal_dialogs()
            self.save_config()
            self._drag_offset = None
            self._hover_inside = False
            self._hover_revealed = False
            self._hover_delay_timer.stop()
            self._popup_leave_timer.stop()
            self._ui_opacity_anim.stop()
            self.hide()
            event.ignore()
            _runtime_log_event(
                "ui.shell_hidden_while_tool_windows_open",
                severity="info",
                summary="The Flowgrid shell was hidden because managed tool windows are still open.",
            )
            return

        self._shutdown_application("shell.closeEvent")
        super().closeEvent(event)

__all__ = [
    "QuickInputsWindow",
]
