from __future__ import annotations

from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any
import sqlite3
import time

from PySide6.QtCore import QPoint, QRect, QSize, Qt, QTimer
from PySide6.QtGui import QColor, QGuiApplication, QIcon, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFormLayout,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStyle,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from flowgrid_app import PermissionDeniedError, PermissionService
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import normalize_hex, safe_int

from .common import (
    AlertPulseTabBar,
    TouchDistributionBar,
    format_working_updated_stamp,
    note_preview,
    parse_iso_date,
    parse_iso_datetime,
    part_age_label,
)
from .constants import DEPOT_RECENT_VIEW_TTL_MS, DEPOT_SEARCH_REFRESH_DEBOUNCE_MS, DEPOT_VIEW_TTL_MS
from .popup_support import (
    DepotFramelessToolWindow,
    FlowgridThemedDialog,
    _ensure_shell_window_available,
    _visible_flowgrid_shell_window,
    show_flowgrid_themed_input_item,
)
from .query_support import (
    _dedupe_part_detail_rows,
    _installed_key_set_from_text,
    _merged_part_detail_rows,
    _part_detail_row_key,
    _submission_latest_ts_sql,
)
from .shared_actions import (
    _copy_work_order_with_notice,
    _edit_aux_queue_comment,
    _edit_part_notes,
    _populate_missing_po_followup_table,
    _reassign_missing_po_followup,
    _resolve_missing_po_followup,
)
from .table_support import (
    _center_table_item,
    _resolve_user_icon_from_agent_meta,
    _select_table_row_by_context_pos,
    _selected_part_id_from_table,
    configure_standard_table,
)

DepotTracker = Any

class DeliveredInstallPickerDialog(FlowgridThemedDialog):
    def __init__(
        self,
        work_order: str,
        delivered_rows: list[dict[str, str]],
        selected_keys: set[str],
        app_window: "QuickInputsWindow" | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent, app_window, "agent")
        self.setWindowTitle(f"Installed Parts - {work_order}")
        self.setModal(True)
        self.resize(560, 340)
        self._checks: list[tuple[QCheckBox, str]] = []

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        summary = QLabel(
            f"Work Order: {work_order}\nSelect the delivered part lines that were actually installed."
        )
        summary.setWordWrap(True)
        layout.addWidget(summary)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        content = QWidget(scroll)
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(6)

        for row in delivered_rows:
            row_key = str(row.get("row_key", "") or "").strip()
            label_text = (
                f"LPN: {str(row.get('lpn', '') or '').strip() or '-'}\n"
                f"Part #: {str(row.get('part_number', '') or '').strip() or '-'}\n"
                f"Description: {str(row.get('part_description', '') or '').strip() or '-'}\n"
                f"Shipping: {str(row.get('shipping_info', '') or '').strip() or '-'}"
            )
            check = QCheckBox(label_text, content)
            check.setChecked(row_key in selected_keys)
            check.setProperty("row_key", row_key)
            content_layout.addWidget(check)
            self._checks.append((check, row_key))
        content_layout.addStretch(1)
        scroll.setWidget(content)
        layout.addWidget(scroll, 1)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(8)
        buttons.addStretch(1)
        save_btn = QPushButton("Save")
        save_btn.setProperty("actionRole", "save")
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setProperty("actionRole", "reset")
        save_btn.clicked.connect(self.accept)
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(save_btn, 0)
        buttons.addWidget(cancel_btn, 0)
        layout.addLayout(buttons)

        self.apply_theme_styles(force_opaque_root=True)

    def selected_row_keys(self) -> list[str]:
        return [row_key for check, row_key in self._checks if check.isChecked() and row_key]


class DepotAgentWindow(DepotFramelessToolWindow):
    FLAG_WORK_REVERT_HOURS = 2
    _COMPACT_ANCHORS = ("TopLeft", "Top", "TopRight", "Left", "Center", "Right", "BottomLeft", "Bottom", "BottomRight")

    def __init__(self, tracker: DepotTracker, current_user: str, app_window: "QuickInputsWindow" | None = None):
        super().__init__(
            app_window,
            window_title="Agent",
            theme_kind="agent",
            size=(780, 292),
            minimum_size=(740, 278),
        )
        self.tracker = tracker
        self.current_user = DepotRules.normalize_user_id(current_user)
        permission_service = getattr(self.tracker, "permission_service", None)
        if permission_service is not None:
            permission_service.require_agent_access(self.current_user)
        elif not self.tracker.can_open_agent_window(self.current_user):
            raise PermissionDeniedError(PermissionService.AGENT_ACCESS_DENIED_MESSAGE)
        self._always_on_top_config_key = "agent_window_always_on_top"
        self._window_always_on_top = self._load_window_always_on_top_preference(self._always_on_top_config_key, default=True)
        self.set_window_always_on_top(self._window_always_on_top)
        self._is_admin_user = self.tracker.is_admin_user(self.current_user)
        self._current_agent_tier = self._resolve_current_agent_tier()
        self._is_tech3_user = int(self._current_agent_tier) == 3
        # Temporarily disable the Missing PO tab for agents.
        self._can_view_missing_po_tab = False
        self._work_compact_active = False
        self._work_compact_layout_updating = False
        self._expanded_window_size = QSize(int(self.width()), int(self.height()))
        self._expanded_minimum_size = QSize(int(self.minimumWidth()), int(self.minimumHeight()))

        self.agent_tabs = QTabWidget(self)
        self._agent_tab_bar = AlertPulseTabBar(self.agent_tabs)
        self.agent_tabs.setTabBar(self._agent_tab_bar)
        self.root_layout.addWidget(self.agent_tabs)

        self.work_tab = QWidget()
        self.parts_tab = QWidget()
        self.cat_parts_tab = QWidget()
        self.client_tab = QWidget()
        self.rtv_tab: QWidget | None = QWidget() if self._is_tech3_user else None
        self.missing_po_tab: QWidget | None = QWidget() if self._can_view_missing_po_tab else None
        self.team_client_tab: QWidget | None = None

        self.agent_tabs.addTab(self.work_tab, "Work")
        self.agent_tabs.addTab(self.parts_tab, "Parts In")
        self.agent_tabs.addTab(self.cat_parts_tab, "Cat Parts")
        self.agent_tabs.addTab(self.client_tab, "Client Est.")
        if self.rtv_tab is not None:
            self.agent_tabs.addTab(self.rtv_tab, "RTV")
        if self.missing_po_tab is not None:
            self.agent_tabs.addTab(self.missing_po_tab, "Missing PO")
        if self.team_client_tab is not None:
            self.agent_tabs.addTab(self.team_client_tab, "Team Client")

        self._tab_indices: dict[str, int] = {
            "parts": int(self.agent_tabs.indexOf(self.parts_tab)),
            "cat_parts": int(self.agent_tabs.indexOf(self.cat_parts_tab)),
            "client": int(self.agent_tabs.indexOf(self.client_tab)),
        }
        if self.rtv_tab is not None:
            self._tab_indices["rtv"] = int(self.agent_tabs.indexOf(self.rtv_tab))
        if self.missing_po_tab is not None:
            self._tab_indices["missing_po"] = int(self.agent_tabs.indexOf(self.missing_po_tab))
        if self.team_client_tab is not None:
            self._tab_indices["team_client"] = int(self.agent_tabs.indexOf(self.team_client_tab))
        self._tab_titles: dict[str, str] = {"parts": "Parts In", "cat_parts": "Cat Parts", "client": "Client Est."}
        if self.rtv_tab is not None:
            self._tab_titles["rtv"] = "RTV"
        if self.missing_po_tab is not None:
            self._tab_titles["missing_po"] = "Missing PO"
        if self.team_client_tab is not None:
            self._tab_titles["team_client"] = "Team Client"
        self._tab_alert_states: dict[str, bool] = {"parts": False, "cat_parts": False, "client": False}
        if self.rtv_tab is not None:
            self._tab_alert_states["rtv"] = False
        if self.missing_po_tab is not None:
            self._tab_alert_states["missing_po"] = False
        if self.team_client_tab is not None:
            self._tab_alert_states["team_client"] = False
        self._tab_alert_ack_states: dict[str, bool] = {"parts": False, "cat_parts": False, "client": False}
        if self.rtv_tab is not None:
            self._tab_alert_ack_states["rtv"] = False
        if self.missing_po_tab is not None:
            self._tab_alert_ack_states["missing_po"] = False
        if self.team_client_tab is not None:
            self._tab_alert_ack_states["team_client"] = False
        self._tab_flash_on = True
        self._parts_has_flagged_rows = False
        self._cat_parts_has_flagged_rows = False
        self._parts_has_urgent_flagged_rows = False
        self._parts_has_in_progress_flagged_rows = False
        self._cat_parts_has_urgent_flagged_rows = False
        self._cat_parts_has_in_progress_flagged_rows = False
        self._client_due_ack_ids: set[int] = set()
        self._client_due_active_ids: set[int] = set()
        self._missing_po_followup_ids: set[int] = set()
        self._recent_submission_rows_by_id: dict[int, dict[str, Any]] = {}
        self._team_client_due_count = 0
        self._tab_flash_timer = QTimer(self)
        self._tab_flash_timer.setInterval(700)
        self._tab_flash_timer.timeout.connect(self._on_tab_alert_flash_tick)
        self._tab_flash_timer.start()
        self._flag_watchdog_timer = QTimer(self)
        self._flag_watchdog_timer.setInterval(60000)
        self._flag_watchdog_timer.timeout.connect(self._refresh_flag_alert_watchdog)
        self._flag_watchdog_timer.start()
        self._agent_tabs_ready = False
        self.agent_tabs.currentChanged.connect(self._on_agent_tab_changed)
        self._agent_window_initialized = False
        self._parts_search_timer = QTimer(self)
        self._parts_search_timer.setSingleShot(True)
        self._parts_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._parts_search_timer.timeout.connect(lambda: self._refresh_agent_parts(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._cat_parts_search_timer = QTimer(self)
        self._cat_parts_search_timer.setSingleShot(True)
        self._cat_parts_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._cat_parts_search_timer.timeout.connect(lambda: self._refresh_category_parts(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._agent_rtv_search_timer = QTimer(self)
        self._agent_rtv_search_timer.setSingleShot(True)
        self._agent_rtv_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._agent_rtv_search_timer.timeout.connect(lambda: self._refresh_rtv_rows(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._agent_missing_po_search_timer = QTimer(self)
        self._agent_missing_po_search_timer.setSingleShot(True)
        self._agent_missing_po_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._agent_missing_po_search_timer.timeout.connect(lambda: self._refresh_missing_po_followups(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))

        self._build_work_tab()
        self._build_parts_tab()
        self._build_cat_parts_tab()
        self._build_client_tab()
        if self.rtv_tab is not None:
            self._build_rtv_tab()
        if self.missing_po_tab is not None:
            self._build_missing_po_tab()
        if self.team_client_tab is not None:
            self._build_team_client_tab()
        self._agent_tabs_ready = True

        if self.app_window is not None:
            self.apply_theme_styles()

    def _build_work_tab(self):
        root = QHBoxLayout(self.work_tab)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)
        self._work_tab_root_layout = root

        left_wrap = QWidget(self.work_tab)
        self.work_left_wrap = left_wrap
        left_col = QVBoxLayout(left_wrap)
        left_col.setContentsMargins(0, 0, 0, 0)
        left_col.setSpacing(4)

        form_wrap = QWidget(left_wrap)
        self.work_form_wrap = form_wrap
        left_layout = QFormLayout(form_wrap)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setHorizontalSpacing(6)
        left_layout.setVerticalSpacing(3)
        left_layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        self.work_user_lbl = QLabel(self.current_user)
        self.work_order_input = QLineEdit()
        self.work_status = QComboBox()
        self.work_status.addItems(["Complete", "Junk Out", "Other", "Part Order", "RTV", "Triaged"])
        self.work_category = QComboBox()
        self.work_category.addItems(list(DepotRules.CATEGORY_OPTIONS))
        self.work_client_check = QCheckBox("")
        self.work_client_hint = QLabel("checked & other will add to Client Est. tab")
        work_client_hint_font = self.work_client_hint.font()
        work_client_hint_font.setItalic(True)
        self.work_client_hint.setFont(work_client_hint_font)
        self.work_client_hint.setProperty("muted", True)
        self.work_comments = QLineEdit()
        self.work_submit_btn = QPushButton("Submit")
        self.work_submit_btn.clicked.connect(self._submit_work_entry)
        self.work_submit_btn.setFixedHeight(24)
        self.work_compact_mode_check = QCheckBox("Compact input view")
        self.work_compact_mode_check.setToolTip("Hide the full Touch Summary panel on Work and keep only today's total.")
        self.work_compact_mode_check.toggled.connect(self._on_work_compact_mode_toggled)

        user_row_wrap = QWidget(form_wrap)
        user_row = QHBoxLayout(user_row_wrap)
        user_row.setContentsMargins(0, 0, 0, 0)
        user_row.setSpacing(8)
        user_row.addWidget(self.work_user_lbl, 0)
        user_row.addWidget(self.work_compact_mode_check, 0, Qt.AlignmentFlag.AlignVCenter)
        user_row.addStretch(1)

        client_row_wrap = QWidget(form_wrap)
        client_row = QHBoxLayout(client_row_wrap)
        client_row.setContentsMargins(0, 0, 0, 0)
        client_row.setSpacing(6)
        client_row.addWidget(self.work_client_check, 0)
        client_row.addWidget(self.work_client_hint, 1)

        action_row_wrap = QWidget(form_wrap)
        action_row = QHBoxLayout(action_row_wrap)
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(6)
        action_row.addStretch(1)
        action_row.addWidget(self.work_submit_btn, 0)

        left_layout.addRow("User", user_row_wrap)
        left_layout.addRow("Work Order", self.work_order_input)
        left_layout.addRow("Status Update", self.work_status)
        left_layout.addRow("Category", self.work_category)
        left_layout.addRow("Client", client_row_wrap)
        left_layout.addRow("Comments", self.work_comments)
        left_layout.addRow("", action_row_wrap)
        left_col.addWidget(form_wrap, 0)

        self.work_today_compact_summary_label = QLabel("Today: 0")
        self.work_today_compact_summary_label.setProperty("section", True)
        self.work_today_compact_summary_label.setVisible(False)
        left_col.addWidget(self.work_today_compact_summary_label, 0)

        self.recent_submissions_label = QLabel()
        self.recent_submissions_label.setWordWrap(False)
        self.recent_submissions_label.setOpenExternalLinks(False)
        self.recent_submissions_label.setTextInteractionFlags(
            Qt.TextInteractionFlag.LinksAccessibleByMouse | Qt.TextInteractionFlag.TextSelectableByMouse
        )
        self.recent_submissions_label.linkActivated.connect(self._on_recent_submission_link_activated)
        self.recent_submissions_label.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
        line_height = max(14, int(self.recent_submissions_label.fontMetrics().lineSpacing()))
        self.recent_submissions_label.setFixedHeight((line_height * 4) + 6)
        left_col.addWidget(self.recent_submissions_label, 0)
        left_col.addStretch(1)
        root.addWidget(left_wrap, 3)

        chart_wrap = QWidget(self.work_tab)
        self.agent_touch_chart_wrap = chart_wrap
        chart_wrap.setMinimumWidth(304)
        chart_layout = QVBoxLayout(chart_wrap)
        chart_layout.setContentsMargins(0, 0, 0, 0)
        chart_layout.setSpacing(1)

        chart_title = QLabel("Touch Summary")
        chart_title.setProperty("section", True)
        chart_layout.addWidget(chart_title)

        self.agent_touch_bars: dict[str, TouchDistributionBar] = {}
        self.agent_touch_legends: dict[str, QLabel] = {}
        self.agent_touch_row_totals: dict[str, QLabel] = {}
        for key, heading in (("today", "Today"), ("last_7", "7 Day"), ("last_30", "30 Day")):
            bar = TouchDistributionBar(chart_wrap)
            bar.setFixedHeight(12)
            self.agent_touch_bars[key] = bar
            row = QHBoxLayout()
            row.setContentsMargins(0, 0, 0, 0)
            row.setSpacing(3)
            lbl = QLabel(heading)
            lbl.setProperty("section", True)
            lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            lbl.setFixedWidth(58)
            row.addWidget(lbl, 0)
            row.addWidget(bar, 1)
            total_lbl = QLabel("0")
            total_lbl.setProperty("section", True)
            total_lbl.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            total_font = total_lbl.font()
            if total_font.pointSizeF() > 0:
                total_font.setPointSizeF(max(total_font.pointSizeF(), 11.0))
            total_font.setBold(True)
            total_lbl.setFont(total_font)
            total_lbl.setFixedWidth(max(42, int(total_lbl.fontMetrics().horizontalAdvance("000") + 14)))
            self.agent_touch_row_totals[key] = total_lbl
            row.addWidget(total_lbl, 0)
            chart_layout.addLayout(row)
            legend = QLabel("No scans.")
            legend.setWordWrap(False)
            legend.setAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignVCenter)
            legend.setMargin(0)
            self.agent_touch_legends[key] = legend
            chart_layout.addWidget(legend)

        root.addWidget(chart_wrap, 2)

        self.work_order_input.returnPressed.connect(self._submit_work_entry)

    def apply_theme_styles(self) -> None:
        super().apply_theme_styles()
        self._apply_tab_alert_visuals()

    def showEvent(self, event) -> None:  # noqa: N802
        super().showEvent(event)
        force_refresh = not bool(self._agent_window_initialized)
        self._refresh_agent_visible_views(force=force_refresh, reason="window-show")
        self._agent_window_initialized = True

    def closeEvent(self, event) -> None:  # noqa: N802
        self._persist_agent_popup_position()
        super().closeEvent(event)
        if event.isAccepted() and _visible_flowgrid_shell_window() is None:
            _ensure_shell_window_available(self.app_window)

    def _resolve_current_agent_tier(self) -> int:
        return int(self.tracker.get_agent_tier(self.current_user, default=1))

    def _tab_normal_text_color(self) -> QColor:
        if self.app_window is not None:
            return QColor(normalize_hex(self.app_window.palette_data.get("label_text", "#FFFFFF"), "#FFFFFF"))
        return QColor("#FFFFFF")

    def _tab_key_for_index(self, index: int) -> str:
        for key, idx in self._tab_indices.items():
            if int(idx) == int(index):
                return key
        return ""

    def _work_tab_is_current(self) -> bool:
        return int(self.agent_tabs.currentIndex()) == int(self.agent_tabs.indexOf(self.work_tab))

    def _work_compact_requested(self) -> bool:
        check = getattr(self, "work_compact_mode_check", None)
        return bool(check is not None and check.isChecked())

    def _agent_compact_anchor(self) -> str:
        anchor = "TopRight"
        if self.app_window is not None:
            anchor = str(self.app_window.config.get("agent_window_compact_anchor", anchor) or anchor).strip() or anchor
        return anchor if anchor in self._COMPACT_ANCHORS else "TopRight"

    @classmethod
    def _anchor_factors(cls, anchor: str) -> tuple[float, float]:
        normalized = str(anchor or "").strip()
        x_factor = 0.0
        y_factor = 0.0
        if "Right" in normalized:
            x_factor = 1.0
        elif normalized in {"Top", "Center", "Bottom"}:
            x_factor = 0.5
        if "Bottom" in normalized:
            y_factor = 1.0
        elif normalized in {"Left", "Center", "Right"}:
            y_factor = 0.5
        return x_factor, y_factor

    @classmethod
    def _anchor_point_for_rect(cls, rect: QRect, anchor: str) -> QPoint:
        x_factor, y_factor = cls._anchor_factors(anchor)
        if x_factor >= 1.0:
            x = int(rect.right())
        elif x_factor <= 0.0:
            x = int(rect.left())
        else:
            x = int(round(rect.left() + (max(0, rect.width() - 1) / 2.0)))
        if y_factor >= 1.0:
            y = int(rect.bottom())
        elif y_factor <= 0.0:
            y = int(rect.top())
        else:
            y = int(round(rect.top() + (max(0, rect.height() - 1) / 2.0)))
        return QPoint(x, y)

    @classmethod
    def _rect_from_anchor_point(cls, anchor_point: QPoint, size: QSize, anchor: str) -> QRect:
        width = max(1, int(size.width()))
        height = max(1, int(size.height()))
        x_factor, y_factor = cls._anchor_factors(anchor)
        if x_factor >= 1.0:
            x = int(anchor_point.x() - width + 1)
        elif x_factor <= 0.0:
            x = int(anchor_point.x())
        else:
            x = int(round(anchor_point.x() - ((width - 1) / 2.0)))
        if y_factor >= 1.0:
            y = int(anchor_point.y() - height + 1)
        elif y_factor <= 0.0:
            y = int(anchor_point.y())
        else:
            y = int(round(anchor_point.y() - ((height - 1) / 2.0)))
        return QRect(int(x), int(y), width, height)

    def _anchored_rect_for_resize(self, reference_rect: QRect, target_size: QSize) -> QRect:
        anchor = self._agent_compact_anchor()
        anchor_point = self._anchor_point_for_rect(reference_rect, anchor)
        return self._rect_from_anchor_point(anchor_point, target_size, anchor)

    def _screen_geometry_for_rect(self, reference_rect: QRect, *, context: str) -> QRect | None:
        visited_screens: set[int] = set()
        for point in (reference_rect.center(), reference_rect.topLeft(), reference_rect.bottomRight()):
            screen = QGuiApplication.screenAt(point)
            if screen is None or id(screen) in visited_screens:
                continue
            visited_screens.add(id(screen))
            try:
                return screen.availableGeometry()
            except Exception as exc:
                _runtime_log_event(
                    "ui.agent_compact_screen_geometry_failed",
                    severity="warning",
                    summary="Failed reading screen geometry while resizing Agent compact layout; trying another screen.",
                    exc=exc,
                    context={"context": str(context), "window_title": str(self.windowTitle())},
                )
        for screen in QGuiApplication.screens():
            if id(screen) in visited_screens:
                continue
            try:
                geometry = screen.availableGeometry()
            except Exception as exc:
                _runtime_log_event(
                    "ui.agent_compact_screen_geometry_failed",
                    severity="warning",
                    summary="Failed reading screen geometry while resizing Agent compact layout; trying another screen.",
                    exc=exc,
                    context={"context": str(context), "window_title": str(self.windowTitle())},
                )
                continue
            if geometry.intersects(reference_rect):
                return geometry
        primary = QGuiApplication.primaryScreen()
        if primary is not None:
            try:
                return primary.availableGeometry()
            except Exception as exc:
                _runtime_log_event(
                    "ui.agent_compact_primary_geometry_failed",
                    severity="warning",
                    summary="Failed reading primary screen geometry while resizing Agent compact layout.",
                    exc=exc,
                    context={"context": str(context), "window_title": str(self.windowTitle())},
                )
        return None

    def _clamp_rect_to_visible_screen(self, target_rect: QRect, reference_rect: QRect, *, context: str) -> QRect:
        geometry = self._screen_geometry_for_rect(reference_rect, context=context)
        if geometry is None:
            return QRect(target_rect)
        max_x = int(geometry.right() - target_rect.width() + 1)
        max_y = int(geometry.bottom() - target_rect.height() + 1)
        if max_x < int(geometry.left()):
            max_x = int(geometry.left())
        if max_y < int(geometry.top()):
            max_y = int(geometry.top())
        clamped_rect = QRect(target_rect)
        clamped_rect.moveTo(
            int(max(int(geometry.left()), min(int(target_rect.x()), max_x))),
            int(max(int(geometry.top()), min(int(target_rect.y()), max_y))),
        )
        return clamped_rect

    def _expanded_window_target_size(self) -> QSize:
        return QSize(
            max(int(self._expanded_window_size.width()), int(self._expanded_minimum_size.width())),
            max(int(self._expanded_window_size.height()), int(self._expanded_minimum_size.height())),
        )

    def _compact_target_width(self) -> int:
        expanded_size = self._expanded_window_target_size()
        left_hint = max(
            int(self.work_form_wrap.sizeHint().width()) if hasattr(self, "work_form_wrap") else 0,
            int(self.work_left_wrap.sizeHint().width()) if hasattr(self, "work_left_wrap") else 0,
            int(self.work_today_compact_summary_label.sizeHint().width()) if hasattr(self, "work_today_compact_summary_label") else 0,
            360,
        )
        work_width = int(self.work_tab.width())
        left_width = int(self.work_left_wrap.width()) if hasattr(self, "work_left_wrap") else 0
        chart_width = 0
        if hasattr(self, "agent_touch_chart_wrap"):
            chart_width = max(int(self.agent_touch_chart_wrap.width()), int(self.agent_touch_chart_wrap.sizeHint().width()))
        if work_width > 0 and left_width > 0:
            window_extra = max(0, int(self.width()) - work_width)
            layout_extra = max(0, work_width - left_width - chart_width)
        else:
            root_margins = self.root_layout.contentsMargins()
            work_margins = self._work_tab_root_layout.contentsMargins() if hasattr(self, "_work_tab_root_layout") else self.work_tab.contentsMargins()
            window_extra = int(root_margins.left() + root_margins.right())
            layout_extra = int(work_margins.left() + work_margins.right() + self._work_tab_root_layout.spacing())
        compact_width = left_hint + window_extra + layout_extra
        return int(max(420, min(int(expanded_size.width()), compact_width)))

    def _set_work_compact_visual_state(self, compact: bool) -> None:
        if hasattr(self, "agent_touch_chart_wrap"):
            self.agent_touch_chart_wrap.setVisible(not compact)
        if hasattr(self, "work_today_compact_summary_label"):
            self.work_today_compact_summary_label.setVisible(compact)

    def _sync_work_compact_state(self, *, reason: str = "") -> None:
        if self._work_compact_layout_updating:
            return
        should_compact = bool(self._work_tab_is_current() and self._work_compact_requested())
        current_rect = QRect(self.geometry())
        try:
            self._work_compact_layout_updating = True
            self.setUpdatesEnabled(False)
            if should_compact:
                if not self._work_compact_active:
                    self._expanded_window_size = QSize(
                        max(int(self.width()), int(self._expanded_minimum_size.width())),
                        max(int(self.height()), int(self._expanded_minimum_size.height())),
                    )
                compact_size = QSize(self._compact_target_width(), int(self._expanded_window_target_size().height()))
                self.setMinimumSize(compact_size.width(), int(self._expanded_minimum_size.height()))
                self._set_work_compact_visual_state(True)
                target_rect = self._clamp_rect_to_visible_screen(
                    self._anchored_rect_for_resize(current_rect, compact_size),
                    current_rect,
                    context=f"compact-{reason or 'sync'}",
                )
                self.setGeometry(target_rect)
                self._work_compact_active = True
            else:
                expanded_size = self._expanded_window_target_size()
                self.setMinimumSize(int(self._expanded_minimum_size.width()), int(self._expanded_minimum_size.height()))
                needs_expand = bool(
                    self._work_compact_active
                    or int(self.width()) < int(expanded_size.width())
                    or int(self.height()) < int(expanded_size.height())
                )
                if needs_expand:
                    target_rect = (
                        self._anchored_rect_for_resize(current_rect, expanded_size)
                        if self._work_compact_active
                        else QRect(current_rect.topLeft(), expanded_size)
                    )
                    target_rect = self._clamp_rect_to_visible_screen(
                        target_rect,
                        current_rect,
                        context=f"expand-{reason or 'sync'}",
                    )
                    self.setGeometry(target_rect)
                self._set_work_compact_visual_state(False)
                self._work_compact_active = False
        finally:
            self.setUpdatesEnabled(True)
            self.updateGeometry()
            self.update()
            self._work_compact_layout_updating = False

    def _full_size_geometry_for_save(self) -> QRect:
        current_rect = QRect(self.geometry())
        if not self._work_compact_active:
            return current_rect
        expanded_rect = self._anchored_rect_for_resize(current_rect, self._expanded_window_target_size())
        return self._clamp_rect_to_visible_screen(expanded_rect, current_rect, context="close-save")

    def _persist_agent_popup_position(self) -> None:
        if self.app_window is None:
            return
        try:
            save_rect = self._full_size_geometry_for_save()
            self.app_window.config.setdefault("popup_positions", {})["agent"] = {
                "x": int(save_rect.x()),
                "y": int(save_rect.y()),
            }
            self.app_window.queue_save_config()
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_popup_position_persist_failed",
                severity="warning",
                summary="Failed persisting Agent popup position; the next launch may reopen at the previous location.",
                exc=exc,
                context={"window_title": str(self.windowTitle())},
            )

    def _on_work_compact_mode_toggled(self, checked: bool) -> None:
        self._sync_work_compact_state(reason="toggle-on" if checked else "toggle-off")

    def _acknowledge_tab_alert(self, key: str) -> None:
        if key not in self._tab_alert_states:
            return
        if not bool(self._tab_alert_states.get(key, False)):
            return
        if bool(self._tab_alert_ack_states.get(key, False)):
            return
        self._tab_alert_ack_states[key] = True
        self._apply_tab_alert_visuals()

    def _refresh_agent_tab_for_key(
        self,
        key: str,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if key == "parts":
            self._refresh_agent_parts(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "cat_parts":
            self._refresh_category_parts(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "client":
            self._refresh_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "rtv" and self.rtv_tab is not None:
            self._refresh_rtv_rows(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "missing_po" and self.missing_po_tab is not None:
            self._refresh_missing_po_followups(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "team_client" and self.team_client_tab is not None:
            self._refresh_team_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)

    def _refresh_agent_visible_views(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        self._refresh_recent_submissions_label(force=force, reason=reason, ttl_ms=DEPOT_RECENT_VIEW_TTL_MS)
        self._refresh_work_touch_chart(force=force, reason=reason, ttl_ms=DEPOT_RECENT_VIEW_TTL_MS)
        current_index = int(self.agent_tabs.currentIndex()) if hasattr(self, "agent_tabs") else -1
        if current_index == int(self.agent_tabs.indexOf(self.work_tab)):
            return
        self._refresh_agent_tab_for_key(
            self._tab_key_for_index(current_index),
            force=force,
            reason=reason,
            ttl_ms=ttl_ms,
        )

    def _on_agent_tab_changed(self, index: int) -> None:
        if not bool(getattr(self, "_agent_tabs_ready", False)):
            return
        self._sync_work_compact_state(reason="tab-change")
        if int(index) == int(self.agent_tabs.indexOf(self.work_tab)):
            self._refresh_recent_submissions_label(reason="tab-change", ttl_ms=DEPOT_RECENT_VIEW_TTL_MS)
            self._refresh_work_touch_chart(reason="tab-change", ttl_ms=DEPOT_RECENT_VIEW_TTL_MS)
            return
        key = self._tab_key_for_index(index)
        if key in {"client", "missing_po"}:
            self._acknowledge_tab_alert(key)
        self._refresh_agent_tab_for_key(key, reason="tab-change", ttl_ms=DEPOT_VIEW_TTL_MS)

    def _set_tab_alert(self, key: str, enabled: bool, acknowledged: bool | None = None) -> None:
        if key not in self._tab_alert_states:
            return
        was_enabled = bool(self._tab_alert_states.get(key, False))
        enabled_now = bool(enabled)
        self._tab_alert_states[key] = enabled_now
        if acknowledged is not None:
            self._tab_alert_ack_states[key] = bool(enabled_now and acknowledged)
        else:
            if enabled_now and not was_enabled:
                self._tab_alert_ack_states[key] = False
            elif not enabled_now:
                self._tab_alert_ack_states[key] = False
            if enabled_now:
                tab_idx = int(self._tab_indices.get(key, -1))
                if tab_idx >= 0 and tab_idx == int(self.agent_tabs.currentIndex()):
                    self._tab_alert_ack_states[key] = True
        self._apply_tab_alert_visuals()

    def _update_tab_alert_states(self) -> None:
        parts_alert = bool(self._parts_has_urgent_flagged_rows or self._parts_has_in_progress_flagged_rows)
        parts_ack = bool(self._parts_has_in_progress_flagged_rows and not self._parts_has_urgent_flagged_rows)
        self._set_tab_alert("parts", parts_alert, acknowledged=parts_ack)

        cat_urgent = bool(self._is_tech3_user and self._cat_parts_has_urgent_flagged_rows)
        cat_in_progress = bool(self._is_tech3_user and self._cat_parts_has_in_progress_flagged_rows)
        cat_alert = bool(cat_urgent or cat_in_progress)
        cat_ack = bool(cat_in_progress and not cat_urgent)
        self._set_tab_alert("cat_parts", cat_alert, acknowledged=cat_ack)

        client_alert = bool(
            any(
                int(part_id) not in self._client_due_ack_ids
                for part_id in getattr(self, "_client_due_active_ids", set())
            )
        )
        self._set_tab_alert("client", client_alert)
        if "rtv" in self._tab_alert_states:
            self._set_tab_alert("rtv", False, acknowledged=True)
        if "missing_po" in self._tab_alert_states:
            missing_po_alert = bool(getattr(self, "_missing_po_followup_ids", set()))
            missing_po_ack = bool(self._tab_alert_ack_states.get("missing_po", False))
            if missing_po_alert and int(self._tab_indices.get("missing_po", -1)) == int(self.agent_tabs.currentIndex()):
                missing_po_ack = True
            self._set_tab_alert("missing_po", missing_po_alert, acknowledged=missing_po_ack)
        if "team_client" in self._tab_alert_states:
            self._set_tab_alert("team_client", bool(int(self._team_client_due_count) > 0), acknowledged=False)

    def _apply_tab_alert_visuals(self) -> None:
        if not hasattr(self, "agent_tabs"):
            return
        tab_bar = self.agent_tabs.tabBar()
        normal_color = self._tab_normal_text_color()
        alert_indices: set[int] = set()
        ack_indices: set[int] = set()
        for key, idx in self._tab_indices.items():
            if idx < 0 or idx >= int(self.agent_tabs.count()):
                continue
            base_text = self._tab_titles.get(key, self.agent_tabs.tabText(idx))
            self.agent_tabs.setTabText(idx, base_text)
            should_alert = bool(self._tab_alert_states.get(key, False))
            if should_alert:
                alert_indices.add(int(idx))
                if bool(self._tab_alert_ack_states.get(key, False)):
                    ack_indices.add(int(idx))
        if isinstance(tab_bar, AlertPulseTabBar):
            tab_bar.set_alert_visual_state(alert_indices, ack_indices, bool(self._tab_flash_on), normal_color)
        else:
            flashing_color = QColor("#F4BCBC")
            acknowledged_color = QColor("#E6C177")
            for key, idx in self._tab_indices.items():
                if idx < 0 or idx >= int(self.agent_tabs.count()):
                    continue
                if idx not in alert_indices:
                    tab_bar.setTabTextColor(idx, normal_color)
                    continue
                if idx in ack_indices:
                    tab_bar.setTabTextColor(idx, acknowledged_color)
                else:
                    tab_bar.setTabTextColor(idx, flashing_color if self._tab_flash_on else normal_color)

    def _on_tab_alert_flash_tick(self) -> None:
        if not any(bool(value) for value in self._tab_alert_states.values()):
            self._tab_flash_on = True
            self._apply_tab_alert_visuals()
            return
        self._tab_flash_on = not bool(self._tab_flash_on)
        self._apply_tab_alert_visuals()

    @staticmethod
    def _agent_touch_color(touch: str) -> str:
        palette = {
            DepotRules.TOUCH_COMPLETE: "#21B46D",
            DepotRules.TOUCH_JUNK: "#D95A5A",
            DepotRules.TOUCH_PART_ORDER: "#D3A327",
            DepotRules.TOUCH_RTV: "#4F86D9",
            "Triaged": "#20AFA8",
            DepotRules.TOUCH_OTHER: "#8E97A8",
        }
        return normalize_hex(palette.get(str(touch or "").strip(), "#6F7C91"), "#6F7C91")

    def _query_agent_touch_metrics(
        self,
        start_date: str,
        end_date: str,
        *,
        include_latest_workload_mix: bool = False,
    ) -> dict[str, Any]:
        return self.tracker.get_touch_mix_metrics(
            start_date=start_date,
            end_date=end_date,
            user_id=self.current_user,
            include_latest_workload_mix=include_latest_workload_mix,
        )

    def _set_agent_touch_legend(
        self,
        key: str,
        segments: list[tuple[str, int, str]],
        *,
        latest_workload_mix: bool = False,
    ) -> None:
        if not segments:
            self.agent_touch_legends[key].setTextFormat(Qt.TextFormat.PlainText)
            self.agent_touch_legends[key].setText("No scans.")
            return
        legend_cells = [
            (
                "<td align='center' style='padding:0 4px;'>"
                f"<span style='color:{color}; font-weight:700'>{DepotRules.chart_touch_label(touch)}</span>: {count}"
                "</td>"
            )
            for touch, count, color in segments
        ]
        if latest_workload_mix:
            header_cell = (
                "<td align='center' style='padding:0 8px; color:#A8B4C1;'>"
                "Latest WO mix"
                "</td>"
            )
            legend_text = "<table width='100%' cellspacing='0' cellpadding='0'><tr>" + header_cell + "".join(legend_cells) + "</tr></table>"
        else:
            legend_text = "<table width='100%' cellspacing='0' cellpadding='0'><tr>" + "".join(legend_cells) + "</tr></table>"
        self.agent_touch_legends[key].setTextFormat(Qt.TextFormat.RichText)
        self.agent_touch_legends[key].setText(legend_text)

    def _refresh_work_touch_chart(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_RECENT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "agent_touch_bars"):
            return
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("agent_work_chart", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        today = datetime.now().date()
        ranges = {
            "today": (today, today),
            "last_7": (today - timedelta(days=6), today),
            "last_30": (today - timedelta(days=29), today),
        }
        touch_order = (
            DepotRules.TOUCH_COMPLETE,
            DepotRules.TOUCH_JUNK,
            DepotRules.TOUCH_PART_ORDER,
            DepotRules.TOUCH_RTV,
            "Triaged",
            DepotRules.TOUCH_OTHER,
        )
        started = time.monotonic()
        totals: dict[str, int] = {"today": 0, "last_7": 0, "last_30": 0}
        try:
            for key, (start_dt, end_dt) in ranges.items():
                latest_workload_mix = key == "last_30"
                touch_metrics = self._query_agent_touch_metrics(
                    start_dt.isoformat(),
                    end_dt.isoformat(),
                    include_latest_workload_mix=latest_workload_mix,
                )
                total = int(max(0, safe_int(touch_metrics.get("total_submissions", 0), 0)))
                counts_raw = touch_metrics.get("by_touch", {})
                counts_latest = touch_metrics.get("latest_by_touch", {})
                counts = counts_latest if latest_workload_mix and isinstance(counts_latest, dict) else counts_raw
                if not isinstance(counts, dict):
                    counts = {}
                totals[key] = total
                ordered: list[str] = []
                for touch in touch_order:
                    if touch in counts:
                        ordered.append(touch)
                for touch in sorted(counts.keys()):
                    if touch not in ordered:
                        ordered.append(touch)

                segments = [
                    (touch, int(max(0, safe_int(counts.get(touch, 0), 0))), self._agent_touch_color(touch))
                    for touch in ordered
                    if int(max(0, safe_int(counts.get(touch, 0), 0))) > 0
                ]
                self.agent_touch_bars[key].set_segments(segments)
                if key in self.agent_touch_row_totals:
                    self.agent_touch_row_totals[key].setText(str(int(total)))
                self._set_agent_touch_legend(key, segments, latest_workload_mix=latest_workload_mix)
            if hasattr(self, "work_today_compact_summary_label"):
                self.work_today_compact_summary_label.setText(f"Today: {int(totals.get('today', 0))}")
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_touch_chart_query_failed",
                severity="warning",
                summary="Failed refreshing agent touch summary chart.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            for key in ("today", "last_7", "last_30"):
                self.agent_touch_bars[key].set_segments([])
                self.agent_touch_legends[key].setTextFormat(Qt.TextFormat.PlainText)
                self.agent_touch_legends[key].setText("Unavailable")
                if key in self.agent_touch_row_totals:
                    self.agent_touch_row_totals[key].setText("0")
            if hasattr(self, "work_today_compact_summary_label"):
                self.work_today_compact_summary_label.setText("Today: unavailable")
            return

        self._mark_depot_view_refreshed(
            "agent_work_chart",
            state_key,
            payload=dict(totals),
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=sum(int(value) for value in totals.values()),
        )

    def _submit_work_entry(self):
        wo = self.work_order_input.text().strip()
        if not wo:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Work order is required.")
            return
        touch = self.work_status.currentText()
        category = self.work_category.currentText() if hasattr(self, "work_category") else ""
        client_unit = self.work_client_check.isChecked()
        comments = self.work_comments.text().strip()

        try:
            blocking_submission = self.tracker.get_blocking_work_submission(wo)
            if blocking_submission is not None:
                latest_touch = str(blocking_submission.get("touch", "") or "").strip() or "Unknown"
                latest_user = DepotRules.normalize_user_id(str(blocking_submission.get("user_id", "") or ""))
                latest_stamp = self._format_submission_stamp(str(blocking_submission.get("latest_stamp", "") or ""))
                latest_bits: list[str] = [latest_touch]
                if latest_user:
                    latest_bits.append(f"by {latest_user}")
                if latest_stamp:
                    latest_bits.append(f"at {latest_stamp}")
                detail_text = " ".join(latest_bits).strip()
                if latest_touch in DepotRules.CLOSING_TOUCHES:
                    guidance = (
                        "Closing submissions cannot be removed from Recent submissions because they clear queue state."
                    )
                elif latest_user and latest_user != self.current_user:
                    guidance = f"The latest submission belongs to {latest_user} and must be corrected before a new work update is added."
                else:
                    guidance = "Remove that recent submission first if this needs to be corrected."
                if detail_text:
                    guidance = f"Latest submission: {detail_text}.\n\n{guidance}"
                self._show_themed_message(
                    QMessageBox.Icon.Warning,
                    "Already Submitted",
                    f"This workorder has already been submitted as {latest_touch}.\n\n{guidance}",
                )
                return
            self.tracker.submit_work(self.current_user, wo, touch, client_unit, comments, category)
            self.work_order_input.clear()
            self.work_comments.clear()
            self.work_order_input.setFocus()
            impacted_sections = self._work_touch_impacted_sections(touch, client_unit)
            self._refresh_after_work_change(impacted_sections, reason="submit_work")
        except Exception as exc:
            self._show_themed_message(QMessageBox.Icon.Critical, "Error", f"Failed to save: {exc}")

    def _work_touch_impacted_sections(self, touch: str, client_unit: bool) -> list[str]:
        resolved_touch = str(touch or "").strip()
        impacted_sections: list[str] = []

        def add_section(section: str) -> None:
            if section and section not in impacted_sections:
                impacted_sections.append(section)

        for base_section in ("agent_recent", "agent_work_chart", "dashboard_metrics"):
            add_section(base_section)
        if resolved_touch in DepotRules.FOLLOW_UP_TOUCHES:
            add_section("agent_client_followup")
            add_section("qa_client_followup")
            if self.team_client_tab is not None:
                add_section("agent_team_client_followup")
        if resolved_touch == DepotRules.TOUCH_PART_ORDER:
            for section in (
                "agent_parts",
                "agent_category",
                "agent_missing_po",
                "qa_assigned",
                "qa_category",
                "qa_missing_po",
                "qa_owner",
            ):
                add_section(section)
        if resolved_touch == DepotRules.TOUCH_RTV:
            add_section("agent_rtv")
            add_section("qa_rtv")
            add_section("dashboard_notes")
        if client_unit and resolved_touch == DepotRules.TOUCH_JUNK:
            add_section("qa_client_jo")
            add_section("dashboard_notes")
        if resolved_touch in DepotRules.CLOSING_TOUCHES:
            for section in (
                "agent_parts",
                "agent_category",
                "agent_missing_po",
                "qa_assigned",
                "qa_category",
                "qa_delivered",
                "qa_missing_po",
                "dashboard_completed",
            ):
                add_section(section)
        return impacted_sections

    def _refresh_after_work_change(self, impacted_sections: list[str], *, reason: str) -> None:
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(*impacted_sections, force=True, reason=reason)
            return
        self._refresh_recent_submissions_label(force=True, reason=reason, ttl_ms=0)
        self._refresh_work_touch_chart(force=True, reason=reason, ttl_ms=0)
        if "agent_client_followup" in impacted_sections:
            self._refresh_client_followup(force=True, reason=reason, ttl_ms=0)
        if "agent_parts" in impacted_sections:
            self._refresh_agent_parts(force=True, reason=reason, ttl_ms=0)
            self._refresh_category_parts(force=True, reason=reason, ttl_ms=0)
        if "agent_missing_po" in impacted_sections:
            self._refresh_missing_po_followups(force=True, reason=reason, ttl_ms=0)
        if "agent_rtv" in impacted_sections and self.rtv_tab is not None:
            self._refresh_rtv_rows(force=True, reason=reason, ttl_ms=0)
        if "agent_team_client_followup" in impacted_sections and self.team_client_tab is not None:
            self._refresh_team_client_followup(force=True, reason=reason, ttl_ms=0)

    @staticmethod
    def _format_submission_stamp(raw_stamp: str) -> str:
        text = str(raw_stamp or "").strip()
        if not text:
            return ""
        dt_value = parse_iso_datetime(text)
        if dt_value is not None:
            return dt_value.strftime("%Y-%m-%d %H:%M")
        return text[:16] if len(text) >= 16 else text

    def _confirm_recent_submission_delete(self, row: dict[str, Any]) -> bool:
        work_order = DepotRules.normalize_work_order(str(row.get("work_order", "") or ""))
        touch = str(row.get("touch", "") or "").strip() or "submission"
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Icon.Warning)
        dialog.setWindowTitle("Remove Submission")
        dialog.setText(f"Remove the recent {touch} submission for {work_order or 'this work order'}?")
        dialog.setInformativeText("This only removes your recent submission. Queue state cleanup rules still apply.")
        if self.styleSheet():
            dialog.setStyleSheet(self.styleSheet())
        remove_button = dialog.addButton("Remove Submission", QMessageBox.ButtonRole.YesRole)
        dialog.addButton(QMessageBox.StandardButton.Cancel)
        dialog.setDefaultButton(QMessageBox.StandardButton.Cancel)
        dialog.exec()
        return dialog.clickedButton() == remove_button

    def _on_recent_submission_link_activated(self, link: str) -> None:
        text = str(link or "").strip()
        if not text.startswith("delete_recent:"):
            return
        submission_id = int(max(0, safe_int(text.partition(":")[2], 0)))
        if submission_id <= 0:
            return
        row = self._recent_submission_rows_by_id.get(submission_id)
        if row is None:
            try:
                row = self.tracker.get_submission_record(submission_id)
            except Exception as exc:
                self._show_themed_message(
                    QMessageBox.Icon.Warning,
                    "Recent Submission",
                    f"Could not load that submission:\n{type(exc).__name__}: {exc}",
                )
                return
        if row is None:
            self._refresh_recent_submissions_label(force=True, reason="recent_delete_missing", ttl_ms=0)
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Recent Submission",
                "That submission no longer exists.",
            )
            return
        if not self._confirm_recent_submission_delete(row):
            return
        try:
            result = self.tracker.delete_user_submission(submission_id, self.current_user)
        except Exception as exc:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Remove Failed",
                f"Could not remove submission:\n{type(exc).__name__}: {exc}",
            )
            return
        impacted_sections = self._work_touch_impacted_sections(
            str(result.get("touch", "") or ""),
            bool(result.get("client_unit", False)),
        )
        self._refresh_after_work_change(impacted_sections, reason="delete_work_submission")
        self._show_copy_notice(
            self.recent_submissions_label,
            f"Removed {str(result.get('work_order', '') or '').strip() or 'submission'}",
            duration_ms=3200,
        )

    def _refresh_recent_submissions_label(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_RECENT_VIEW_TTL_MS,
    ) -> None:
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("agent_recent", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        started = time.monotonic()
        try:
            rows = self.tracker.list_recent_user_submissions(self.current_user, limit=3)
            fallback_map = {
                DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
                for row in rows
                if DepotRules.normalize_work_order(str(row["work_order"] or ""))
            }
            category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_recent_submissions_query_failed",
                severity="warning",
                summary="Failed querying recent submissions for agent panel; showing unavailable fallback.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self._recent_submission_rows_by_id = {}
            self.recent_submissions_label.setTextFormat(Qt.TextFormat.PlainText)
            self.recent_submissions_label.setText("Recent submissions: unavailable")
            return

        if not rows:
            self._recent_submission_rows_by_id = {}
            self.recent_submissions_label.setTextFormat(Qt.TextFormat.PlainText)
            self.recent_submissions_label.setText("Latest 3 submissions:\n1. (none)\n2. (none)\n3. (none)")
            self._mark_depot_view_refreshed(
                "agent_recent",
                state_key,
                payload=[],
                reason=reason,
                duration_ms=(time.monotonic() - started) * 1000.0,
                row_count=0,
            )
            return

        rendered_rows: list[dict[str, Any]] = []
        lines: list[str] = ["Latest 3 submissions:"]
        for index, row in enumerate(rows, start=1):
            row_payload = dict(row)
            submission_id = int(max(0, safe_int(row_payload.get("id", 0), 0)))
            wo = str(row["work_order"] or "").strip()
            touch = str(row["touch"] or "").strip()
            category = category_map.get(DepotRules.normalize_work_order(wo), "") or str(row["category"] or "").strip() or "Other"
            client_marker = " | Client" if int(row["client_unit"] or 0) else ""
            stamp = self._format_submission_stamp(str(row["latest_stamp"] or ""))
            remove_link = (
                f'<a href="delete_recent:{submission_id}" '
                'style="text-decoration:none;">'
                '<span style="background-color:#D95A5A; color:#FFFFFF; font-weight:700; '
                'padding:1px 6px; border-radius:8px;">&minus;</span></a>'
            )
            lines.append(
                f'{remove_link} {index}. {escape(wo)} ({escape(touch)} | {escape(category)}{escape(client_marker)}) '
                f'[{escape(stamp)}]'
            )
            rendered_rows.append(row_payload)

        for index in range(len(rows) + 1, 4):
            lines.append(f"{index}. (none)")

        self._recent_submission_rows_by_id = {
            int(max(0, safe_int(row_payload.get("id", 0), 0))): row_payload for row_payload in rendered_rows
        }
        self.recent_submissions_label.setTextFormat(Qt.TextFormat.RichText)
        self.recent_submissions_label.setText("<br/>".join(lines))
        self._mark_depot_view_refreshed(
            "agent_recent",
            state_key,
            payload=rendered_rows,
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _build_parts_tab(self):
        layout = QVBoxLayout(self.parts_tab)
        self.parts_table = QTableWidget()
        configure_standard_table(
            self.parts_table,
            ["Work Order", "Client", "Flag", "Age", "Working", "Installed", "Category", "QA Note"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.parts_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.parts_table.cellClicked.connect(
            lambda row, col: self._on_parts_table_cell_clicked("parts", self.parts_table, row, col)
        )
        self.parts_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.parts_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.parts_table.customContextMenuRequested.connect(
            lambda pos: self._open_agent_notes_from_context(self.parts_table, pos)
        )
        self.parts_refresh_btn = QPushButton("Refresh")
        self.parts_refresh_btn.clicked.connect(lambda: self._refresh_agent_parts(force=True, reason="manual", ttl_ms=0))
        self.parts_open_notes_btn = QPushButton("Open Notes")
        self.parts_open_notes_btn.setProperty("actionRole", "pick")
        self.parts_open_notes_btn.clicked.connect(lambda: self._open_agent_notes_for_table(self.parts_table))
        self.parts_working_btn = QPushButton("Agent Is Working This")
        self.parts_working_btn.setProperty("actionRole", "apply")
        self.parts_working_btn.clicked.connect(lambda: self._toggle_selected_part_working(self.parts_table))
        self.parts_installed_btn = QPushButton("Parts Installed")
        self.parts_installed_btn.setProperty("actionRole", "apply")
        self.parts_installed_btn.clicked.connect(lambda: self._toggle_selected_part_installed(self.parts_table))
        self.parts_workorder_search = QLineEdit()
        self.parts_workorder_search.setPlaceholderText("Search work order...")
        self.parts_workorder_search.setClearButtonEnabled(True)
        self.parts_workorder_search.textChanged.connect(lambda _text: self._parts_search_timer.start())
        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.parts_workorder_search, 1)
        controls.addWidget(self.parts_refresh_btn)
        controls.addWidget(self.parts_open_notes_btn)
        controls.addWidget(self.parts_working_btn)
        controls.addWidget(self.parts_installed_btn)
        layout.addLayout(controls)
        layout.addWidget(self.parts_table)
    def _open_agent_notes_from_context(self, table: QTableWidget, pos: QPoint) -> None:
        if not _select_table_row_by_context_pos(table, pos):
            return
        self._open_agent_notes_for_table(table)

    @staticmethod
    def _flag_tooltip(flag: str, qa_comment: str, agent_comment: str, has_image: bool) -> str:
        flag_text = flag if flag else "None"
        qa_text = qa_comment if qa_comment else "(none)"
        agent_text = agent_comment if agent_comment else "(none)"
        return (
            "Double-click to copy work order.\n"
            "Right-click to open notes.\n"
            f"Flag: {flag_text}\n"
            f"QA Note: {qa_text}\n"
            f"Agent Note: {agent_text}"
        )

    @staticmethod
    def _part_age_label(created_at: str) -> str:
        return part_age_label(created_at)

    @staticmethod
    def _note_preview(note: str, max_len: int = 64) -> str:
        return note_preview(note, max_len=max_len)

    @staticmethod
    def _center_item(item: QTableWidgetItem) -> QTableWidgetItem:
        return _center_table_item(item)

    def _client_checked_icon(self) -> QIcon:
        return self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)

    def _part_installed_icon(self) -> QIcon:
        icon = QIcon.fromTheme("applications-engineering")
        if icon.isNull():
            icon = QIcon.fromTheme("tools")
        if icon.isNull():
            icon = QIcon.fromTheme("preferences-system")
        if icon.isNull():
            icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
        return icon

    def _followup_done_icon(self) -> QIcon:
        return self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)

    def _followup_clock_icon(self, color_hex: str) -> QIcon:
        color = QColor(normalize_hex(color_hex, "#21B46D"))
        pix = QPixmap(16, 16)
        pix.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pix)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        pen = QPen(color)
        pen.setWidth(2)
        painter.setPen(pen)
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.drawEllipse(2, 2, 12, 12)
        painter.drawLine(8, 8, 8, 5)
        painter.drawLine(8, 8, 11, 9)
        painter.end()
        return QIcon(pix)

    def _followup_wait_icon_by_days(self, days_since_action: int) -> tuple[QIcon, str]:
        days = int(max(0, safe_int(days_since_action, 0)))
        if days <= 0:
            return self._followup_clock_icon("#21B46D"), DepotRules.followup_stage_label(0)
        if days == 1:
            return self._followup_clock_icon("#D1A91F"), DepotRules.followup_stage_label(1)
        return self._followup_clock_icon("#D95A5A"), DepotRules.followup_stage_label(2)

    @staticmethod
    def _parse_iso_date(raw_value: str) -> date | None:
        return parse_iso_date(raw_value)

    @staticmethod
    def _parse_iso_datetime(raw_value: str) -> datetime | None:
        return parse_iso_datetime(raw_value)

    @classmethod
    def _is_working_flag_stale(cls, raw_stamp: str) -> bool:
        updated_at = cls._parse_iso_datetime(raw_stamp)
        if updated_at is None:
            return True
        return bool((datetime.now() - updated_at) >= timedelta(hours=int(max(1, cls.FLAG_WORK_REVERT_HOURS))))

    def _flag_alert_counts_from_rows(self, rows: list[sqlite3.Row]) -> tuple[int, int]:
        urgent_count = 0
        in_progress_count = 0
        for row in rows:
            flag = str(row["qa_flag"] or "").strip()
            image_path = str(row["qa_flag_image_path"] or "").strip()
            if not flag and not image_path:
                continue
            if self.tracker.is_alert_quiet(str(row["alert_quiet_until"] or "").strip()):
                continue
            working_user = DepotRules.normalize_user_id(str(row["working_user_id"] or ""))
            working_stamp = str(row["working_updated_at"] or "").strip()
            if working_user and not self._is_working_flag_stale(working_stamp):
                in_progress_count += 1
            else:
                urgent_count += 1
        return urgent_count, in_progress_count

    def _refresh_flag_alert_watchdog(self) -> None:
        if not self.isVisible():
            return
        try:
            part_rows = self.tracker.db.fetchall(
                "SELECT COALESCE(qa_flag, '') AS qa_flag, COALESCE(qa_flag_image_path, '') AS qa_flag_image_path, "
                "COALESCE(working_user_id, '') AS working_user_id, COALESCE(working_updated_at, '') AS working_updated_at, "
                "COALESCE(alert_quiet_until, '') AS alert_quiet_until "
                "FROM parts WHERE assigned_user_id=? AND is_active=1",
                (self.current_user,),
            )
            part_urgent, part_in_progress = self._flag_alert_counts_from_rows(part_rows)
            self._parts_has_urgent_flagged_rows = bool(part_urgent > 0)
            self._parts_has_in_progress_flagged_rows = bool(part_in_progress > 0)
            self._parts_has_flagged_rows = bool((part_urgent + part_in_progress) > 0)

            if self._is_tech3_user:
                cat_rows = self.tracker.db.fetchall(
                    "SELECT COALESCE(qa_flag, '') AS qa_flag, COALESCE(qa_flag_image_path, '') AS qa_flag_image_path, "
                    "COALESCE(working_user_id, '') AS working_user_id, COALESCE(working_updated_at, '') AS working_updated_at, "
                    "COALESCE(alert_quiet_until, '') AS alert_quiet_until "
                    "FROM parts WHERE is_active=1"
                )
                cat_urgent, cat_in_progress = self._flag_alert_counts_from_rows(cat_rows)
                self._cat_parts_has_urgent_flagged_rows = bool(cat_urgent > 0)
                self._cat_parts_has_in_progress_flagged_rows = bool(cat_in_progress > 0)
                self._cat_parts_has_flagged_rows = bool((cat_urgent + cat_in_progress) > 0)
            else:
                self._cat_parts_has_urgent_flagged_rows = False
                self._cat_parts_has_in_progress_flagged_rows = False
                self._cat_parts_has_flagged_rows = False
            if self.team_client_tab is not None:
                self._refresh_team_client_followup()
            self._update_tab_alert_states()
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_flag_alert_watchdog_failed",
                severity="warning",
                summary="Failed refreshing agent flag alert watchdog state.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )

    @staticmethod
    def _format_working_updated_stamp(raw_stamp: str) -> str:
        return format_working_updated_stamp(raw_stamp)

    def _toggle_selected_part_working(self, table: QTableWidget) -> None:
        part_id = _selected_part_id_from_table(table)
        if part_id is None:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
            return
        row = self.tracker.db.fetchone(
            "SELECT COALESCE(working_user_id, '') AS working_user_id "
            "FROM parts WHERE id=?",
            (int(part_id),),
        )
        if row is None:
            self._show_themed_message(QMessageBox.Icon.Warning, "Missing", "Selected part no longer exists.")
            return
        working_user = DepotRules.normalize_user_id(str(row["working_user_id"] or ""))
        if working_user and working_user != self.current_user:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "In Use",
                f"This unit is already marked as being worked by {working_user}.",
            )
            return
        next_user = "" if working_user == self.current_user else self.current_user
        try:
            self.tracker.set_part_working_user(part_id, next_user)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_set_part_working_failed",
                severity="warning",
                summary="Failed updating working-owner flag for part.",
                exc=exc,
                context={"user_id": str(self.current_user), "part_id": int(part_id), "next_user": str(next_user)},
            )
            self._show_themed_message(QMessageBox.Icon.Critical, "Save Failed", f"Could not update flag:\n{type(exc).__name__}: {exc}")
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_parts",
                "agent_category",
                "qa_assigned",
                "qa_delivered",
                reason="part_working_toggle",
            )
            return
        self._refresh_agent_parts(force=True, reason="part_working_toggle", ttl_ms=0)
        self._refresh_category_parts(force=True, reason="part_working_toggle", ttl_ms=0)

    def _toggle_selected_part_installed(self, table: QTableWidget) -> None:
        part_id = _selected_part_id_from_table(table)
        if part_id is None:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
            return
        row = self.tracker.db.fetchone(
            "SELECT COALESCE(parts_installed, 0) AS parts_installed, COALESCE(work_order, '') AS work_order "
            "FROM parts WHERE id=?",
            (int(part_id),),
        )
        if row is None:
            self._show_themed_message(QMessageBox.Icon.Warning, "Missing", "Selected part no longer exists.")
            return
        work_order = DepotRules.normalize_work_order(str(row["work_order"] or ""))
        delivered_rows = self.tracker.list_delivered_part_details(work_order)
        delivered_options: list[dict[str, str]] = []
        preselected_keys: set[str] = set()
        for detail in delivered_rows:
            installed_keys = _installed_key_set_from_text(str(detail["installed_keys"] or ""))
            merged_rows = _dedupe_part_detail_rows(
                _merged_part_detail_rows(
                    str(detail["lpn"] or ""),
                    str(detail["part_number"] or ""),
                    str(detail["part_description"] or ""),
                    str(detail["shipping_info"] or ""),
                )
            )
            for lpn_value, part_value, desc_value, ship_value in merged_rows:
                row_key = _part_detail_row_key(lpn_value, part_value, desc_value, ship_value)
                if not row_key:
                    continue
                delivered_options.append(
                    {
                        "row_key": row_key,
                        "lpn": lpn_value,
                        "part_number": part_value,
                        "part_description": desc_value,
                        "shipping_info": ship_value,
                    }
                )
                if row_key in installed_keys:
                    preselected_keys.add(row_key)
        if not delivered_options:
            self._show_themed_message(
                QMessageBox.Icon.Information,
                "Installed Parts",
                "No delivered part lines are available yet for this work order.",
            )
            return
        dialog = DeliveredInstallPickerDialog(
            work_order,
            delivered_options,
            preselected_keys,
            app_window=self.app_window,
            parent=self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        selected_keys = dialog.selected_row_keys()
        try:
            self.tracker.set_part_installed(part_id, selected_keys, self.current_user)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_set_part_installed_failed",
                severity="warning",
                summary="Failed toggling installed state for part.",
                exc=exc,
                context={
                    "user_id": str(self.current_user),
                    "part_id": int(part_id),
                    "selected_keys": list(selected_keys),
                },
            )
            self._show_themed_message(
                QMessageBox.Icon.Critical,
                "Save Failed",
                f"Could not update installed state:\n{type(exc).__name__}: {exc}",
            )
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_parts",
                "agent_category",
                "qa_assigned",
                "qa_delivered",
                reason="part_installed_toggle",
            )
            return
        self._refresh_agent_parts(force=True, reason="part_installed_toggle", ttl_ms=0)
        self._refresh_category_parts(force=True, reason="part_installed_toggle", ttl_ms=0)

    def _on_parts_table_cell_clicked(self, tab_key: str, table: QTableWidget, row: int, col: int) -> None:
        if row < 0 or col < 0:
            return
        hdr = table.horizontalHeaderItem(col)
        header = str(hdr.text() or "").strip() if hdr is not None else ""
        if header == "Working":
            table.selectRow(row)
            self._toggle_selected_part_working(table)
            return
        if header == "Installed":
            table.selectRow(row)
            self._toggle_selected_part_installed(table)
            return

    def eventFilter(self, watched, event) -> bool:  # noqa: N802
        return super().eventFilter(watched, event)

    def _open_agent_notes_for_table(self, table: QTableWidget) -> None:
        changed, _part_id = _edit_part_notes(
            self,
            self.tracker,
            role="agent",
            current_user=self.current_user,
            table=table,
        )
        if not changed:
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_parts",
                "agent_category",
                "agent_missing_po",
                "qa_assigned",
                "qa_delivered",
                "qa_missing_po",
                reason="agent_part_notes",
            )
            return
        self._refresh_agent_parts(force=True, reason="agent_part_notes", ttl_ms=0)
        self._refresh_category_parts(force=True, reason="agent_part_notes", ttl_ms=0)
        self._refresh_missing_po_followups(force=True, reason="agent_part_notes", ttl_ms=0)

    def _refresh_agent_parts(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        search_text = str(self.parts_workorder_search.text() or "").strip() if hasattr(self, "parts_workorder_search") else ""
        state_key = {"user_id": self.current_user, "search": search_text}
        if not self._should_refresh_depot_view("agent_parts", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        started = time.monotonic()
        try:
            rows = self.tracker.list_agent_active_parts(self.current_user, search_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_parts_refresh_failed",
                severity="warning",
                summary="Failed loading agent active parts.",
                exc=exc,
                context={"user_id": str(self.current_user), "search_text": str(search_text)},
            )
            self.parts_table.setRowCount(0)
            self._parts_has_flagged_rows = False
            self._parts_has_urgent_flagged_rows = False
            self._parts_has_in_progress_flagged_rows = False
            self._update_tab_alert_states()
            return

        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)

        self.parts_table.setRowCount(0)
        urgent_flagged_rows = 0
        in_progress_flagged_rows = 0
        for row_idx, r in enumerate(rows):
            self.parts_table.insertRow(row_idx)
            part_id = int(r["id"])
            work_order = str(r["work_order"] or "").strip()
            category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
            is_client = bool(int(r["client_unit"] or 0))
            age_text = self._part_age_label(str(r["created_at"] or ""))
            qa_comment = str(r["qa_comment"] or r["comments"] or "").strip()
            agent_comment = str(r["agent_comment"] or "").strip()
            flag = str(r["qa_flag"] or "").strip()
            working_user = DepotRules.normalize_user_id(str(r["working_user_id"] or ""))
            working_stamp = str(r["working_updated_at"] or "").strip()
            parts_installed = bool(int(r["parts_installed"] or 0))
            parts_installed_by = DepotRules.normalize_user_id(str(r["parts_installed_by"] or ""))
            parts_installed_at = str(r["parts_installed_at"] or "").strip()
            alert_quiet_until = str(r["alert_quiet_until"] or "").strip()
            image_abs = self.tracker.resolve_qa_flag_icon(
                str(r["qa_flag"] or "").strip(),
                str(r["qa_flag_image_path"] or ""),
            )
            has_flag = bool(str(flag).strip() or str(image_abs).strip())
            if has_flag and not self.tracker.is_alert_quiet(alert_quiet_until):
                if working_user and not self._is_working_flag_stale(working_stamp):
                    in_progress_flagged_rows += 1
                else:
                    urgent_flagged_rows += 1

            client_item = QTableWidgetItem("")
            client_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if is_client:
                client_item.setIcon(self._client_checked_icon())
                client_item.setToolTip("Client unit")
            else:
                client_item.setToolTip("Non-client unit")
            self._center_item(client_item)
            flag_item = QTableWidgetItem("" if image_abs else (flag if flag else ""))
            flag_item.setData(Qt.ItemDataRole.UserRole, part_id)
            flag_item.setToolTip(self._flag_tooltip(flag, qa_comment, agent_comment, bool(image_abs)))
            if image_abs:
                flag_item.setIcon(QIcon(image_abs))
            self._center_item(flag_item)
            working_item = QTableWidgetItem("\U0001F527" if working_user else "")
            working_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if working_user:
                working_tip = f"Agent working this unit: {working_user}"
                friendly_stamp = self._format_working_updated_stamp(working_stamp)
                if friendly_stamp:
                    working_tip += f"\nUpdated: {friendly_stamp}"
                working_item.setToolTip(working_tip)
            else:
                working_item.setToolTip("No agent is marked as working this unit.")
            self._center_item(working_item)
            installed_item = QTableWidgetItem("")
            installed_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if parts_installed:
                installed_item.setIcon(self._part_installed_icon())
                installed_tip = "Parts installed."
                if parts_installed_by:
                    installed_tip += f"\nBy: {parts_installed_by}"
                friendly_installed = self._format_working_updated_stamp(parts_installed_at)
                if friendly_installed:
                    installed_tip += f"\nAt: {friendly_installed}"
                installed_item.setToolTip(installed_tip)
            else:
                installed_item.setToolTip("Click to mark parts installed.")
            self._center_item(installed_item)
            qa_note_item = QTableWidgetItem(self._note_preview(qa_comment))
            qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")
            self._center_item(qa_note_item)
            age_item = self._center_item(QTableWidgetItem(age_text))
            work_item = self._center_item(QTableWidgetItem(work_order))
            work_item.setData(Qt.ItemDataRole.UserRole, part_id)
            category_item = self._center_item(QTableWidgetItem(category))
            self.parts_table.setItem(row_idx, 0, work_item)
            self.parts_table.setItem(row_idx, 1, client_item)
            self.parts_table.setItem(row_idx, 2, flag_item)
            self.parts_table.setItem(row_idx, 3, age_item)
            self.parts_table.setItem(row_idx, 4, working_item)
            self.parts_table.setItem(row_idx, 5, installed_item)
            self.parts_table.setItem(row_idx, 6, category_item)
            self.parts_table.setItem(row_idx, 7, qa_note_item)
        self._parts_has_flagged_rows = bool((urgent_flagged_rows + in_progress_flagged_rows) > 0)
        self._parts_has_urgent_flagged_rows = bool(urgent_flagged_rows > 0)
        self._parts_has_in_progress_flagged_rows = bool(in_progress_flagged_rows > 0)
        self._update_tab_alert_states()
        self._mark_depot_view_refreshed(
            "agent_parts",
            state_key,
            payload=[dict(row) for row in rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _build_cat_parts_tab(self):
        layout = QVBoxLayout(self.cat_parts_tab)
        filter_layout = QHBoxLayout()
        self.cat_filter = QComboBox()
        self._refresh_category_filter_options()
        self.cat_filter.currentTextChanged.connect(lambda _text: self._refresh_category_parts(reason="filter-change", ttl_ms=DEPOT_VIEW_TTL_MS))
        filter_layout.addWidget(QLabel("Category:"))
        filter_layout.addWidget(self.cat_filter, 1)
        self.cat_workorder_search = QLineEdit()
        self.cat_workorder_search.setPlaceholderText("Search work order...")
        self.cat_workorder_search.setClearButtonEnabled(True)
        self.cat_workorder_search.textChanged.connect(lambda _text: self._cat_parts_search_timer.start())
        filter_layout.addWidget(QLabel("Work Order:"))
        filter_layout.addWidget(self.cat_workorder_search, 1)
        self.cat_refresh_btn = QPushButton("Refresh")
        self.cat_refresh_btn.clicked.connect(lambda: self._refresh_category_parts(force=True, reason="manual", ttl_ms=0))
        filter_layout.addWidget(self.cat_refresh_btn, 0)
        self.cat_open_notes_btn = QPushButton("Open Notes")
        self.cat_open_notes_btn.setProperty("actionRole", "pick")
        self.cat_open_notes_btn.clicked.connect(lambda: self._open_agent_notes_for_table(self.cat_parts_table))
        filter_layout.addWidget(self.cat_open_notes_btn, 0)
        self.cat_working_btn = QPushButton("Agent Is Working This")
        self.cat_working_btn.setProperty("actionRole", "apply")
        self.cat_working_btn.clicked.connect(lambda: self._toggle_selected_part_working(self.cat_parts_table))
        filter_layout.addWidget(self.cat_working_btn, 0)
        self.cat_installed_btn = QPushButton("Parts Installed")
        self.cat_installed_btn.setProperty("actionRole", "apply")
        self.cat_installed_btn.clicked.connect(lambda: self._toggle_selected_part_installed(self.cat_parts_table))
        filter_layout.addWidget(self.cat_installed_btn, 0)
        layout.addLayout(filter_layout)

        headers = ["Work Order", "Client", "Flag", "Age", "Working", "Installed", "Category", "QA Note"]
        resize_modes: dict[int, QHeaderView.ResizeMode] = {
            0: QHeaderView.ResizeMode.ResizeToContents,
            1: QHeaderView.ResizeMode.ResizeToContents,
            2: QHeaderView.ResizeMode.ResizeToContents,
            3: QHeaderView.ResizeMode.ResizeToContents,
            4: QHeaderView.ResizeMode.ResizeToContents,
            5: QHeaderView.ResizeMode.ResizeToContents,
            6: QHeaderView.ResizeMode.ResizeToContents,
            7: QHeaderView.ResizeMode.Stretch,
        }
        if self._is_tech3_user:
            headers = ["Work Order", "Client", "Flag", "Age", "Working", "Installed", "Agent", "Category", "QA Note"]
            resize_modes = {
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.ResizeToContents,
                8: QHeaderView.ResizeMode.Stretch,
            }

        self.cat_parts_table = QTableWidget()
        configure_standard_table(
            self.cat_parts_table,
            headers,
            resize_modes=resize_modes,
            stretch_last=True,
        )
        self.cat_parts_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.cat_parts_table.cellClicked.connect(
            lambda row, col: self._on_parts_table_cell_clicked("cat_parts", self.cat_parts_table, row, col)
        )
        self.cat_parts_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.cat_parts_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.cat_parts_table.customContextMenuRequested.connect(
            lambda pos: self._open_agent_notes_from_context(self.cat_parts_table, pos)
        )
        layout.addWidget(self.cat_parts_table)

    def _refresh_category_filter_options(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "cat_filter"):
            return
        previous = self.cat_filter.currentText().strip() or "All"
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("agent_category_filter_options", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        categories: list[str] = ["All"]
        started = time.monotonic()
        try:
            categories.extend(self.tracker.active_part_category_options())
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_category_filter_query_failed",
                severity="warning",
                summary="Failed loading category filter options; continuing with defaults.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )

        self.cat_filter.blockSignals(True)
        self.cat_filter.clear()
        self.cat_filter.addItems(categories)
        if previous in categories:
            self.cat_filter.setCurrentText(previous)
        else:
            self.cat_filter.setCurrentIndex(0)
        self.cat_filter.blockSignals(False)
        self._mark_depot_view_refreshed(
            "agent_category_filter_options",
            state_key,
            payload=list(categories),
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(categories),
        )

    def _refresh_category_parts(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ):
        self._refresh_category_filter_options(force=force, reason=reason, ttl_ms=ttl_ms)
        agent_name_lookup: dict[str, str] = {}
        if self._is_tech3_user:
            try:
                for agent_user, agent_meta in self.tracker.agent_display_map().items():
                    agent_name_lookup[agent_user] = str(agent_meta[0] or "").strip()
            except Exception as exc:
                _runtime_log_event(
                    "ui.agent_category_parts_agent_lookup_failed",
                    severity="warning",
                    summary="Failed resolving agent names for Tech 3 category parts view.",
                    exc=exc,
                    context={"user_id": str(self.current_user)},
                )
        cat = self.cat_filter.currentText().strip()
        search_text = str(self.cat_workorder_search.text() or "").strip() if hasattr(self, "cat_workorder_search") else ""
        state_key = {"search": search_text, "category": cat, "tech3": self._is_tech3_user}
        if not self._should_refresh_depot_view("agent_category", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        started = time.monotonic()
        try:
            rows = self.tracker.list_category_active_parts(search_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_category_parts_refresh_failed",
                severity="warning",
                summary="Failed loading agent category parts.",
                exc=exc,
                context={"user_id": str(self.current_user), "search_text": str(search_text), "category": str(cat)},
            )
            self.cat_parts_table.setRowCount(0)
            self._cat_parts_has_flagged_rows = False
            self._cat_parts_has_urgent_flagged_rows = False
            self._cat_parts_has_in_progress_flagged_rows = False
            self._update_tab_alert_states()
            return

        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        self.cat_parts_table.setRowCount(0)
        urgent_flagged_rows = 0
        in_progress_flagged_rows = 0
        display_rows: list[sqlite3.Row] = []
        for r in rows:
            work_order = str(r["work_order"] or "").strip()
            resolved_category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
            if cat and cat != "All" and resolved_category != cat:
                continue
            display_rows.append(r)
        for row_idx, r in enumerate(display_rows):
            self.cat_parts_table.insertRow(row_idx)
            part_id = int(r["id"])
            work_order = str(r["work_order"] or "").strip()
            assigned_user = DepotRules.normalize_user_id(str(r["assigned_user_id"] or ""))
            category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
            is_client = bool(int(r["client_unit"] or 0))
            age_text = self._part_age_label(str(r["created_at"] or ""))
            qa_comment = str(r["qa_comment"] or r["comments"] or "").strip()
            agent_comment = str(r["agent_comment"] or "").strip()
            flag = str(r["qa_flag"] or "").strip()
            working_user = DepotRules.normalize_user_id(str(r["working_user_id"] or ""))
            working_stamp = str(r["working_updated_at"] or "").strip()
            parts_installed = bool(int(r["parts_installed"] or 0))
            parts_installed_by = DepotRules.normalize_user_id(str(r["parts_installed_by"] or ""))
            parts_installed_at = str(r["parts_installed_at"] or "").strip()
            alert_quiet_until = str(r["alert_quiet_until"] or "").strip()
            image_abs = self.tracker.resolve_qa_flag_icon(
                str(r["qa_flag"] or "").strip(),
                str(r["qa_flag_image_path"] or ""),
            )
            has_flag = bool(str(flag).strip() or str(image_abs).strip())
            if has_flag and not self.tracker.is_alert_quiet(alert_quiet_until):
                if working_user and not self._is_working_flag_stale(working_stamp):
                    in_progress_flagged_rows += 1
                else:
                    urgent_flagged_rows += 1

            client_item = QTableWidgetItem("")
            client_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if is_client:
                client_item.setIcon(self._client_checked_icon())
                client_item.setToolTip("Client unit")
            else:
                client_item.setToolTip("Non-client unit")
            self._center_item(client_item)
            flag_item = QTableWidgetItem("" if image_abs else (flag if flag else ""))
            flag_item.setData(Qt.ItemDataRole.UserRole, part_id)
            flag_item.setToolTip(self._flag_tooltip(flag, qa_comment, agent_comment, bool(image_abs)))
            if image_abs:
                flag_item.setIcon(QIcon(image_abs))
            self._center_item(flag_item)
            working_item = QTableWidgetItem("\U0001F527" if working_user else "")
            working_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if working_user:
                working_tip = f"Agent working this unit: {working_user}"
                friendly_stamp = self._format_working_updated_stamp(working_stamp)
                if friendly_stamp:
                    working_tip += f"\nUpdated: {friendly_stamp}"
                working_item.setToolTip(working_tip)
            else:
                working_item.setToolTip("No agent is marked as working this unit.")
            self._center_item(working_item)
            installed_item = QTableWidgetItem("")
            installed_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if parts_installed:
                installed_item.setIcon(self._part_installed_icon())
                installed_tip = "Parts installed."
                if parts_installed_by:
                    installed_tip += f"\nBy: {parts_installed_by}"
                friendly_installed = self._format_working_updated_stamp(parts_installed_at)
                if friendly_installed:
                    installed_tip += f"\nAt: {friendly_installed}"
                installed_item.setToolTip(installed_tip)
            else:
                installed_item.setToolTip("Click to mark parts installed.")
            self._center_item(installed_item)
            qa_note_item = QTableWidgetItem(self._note_preview(qa_comment))
            qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")
            self._center_item(qa_note_item)
            age_item = self._center_item(QTableWidgetItem(age_text))
            work_item = self._center_item(QTableWidgetItem(work_order))
            work_item.setData(Qt.ItemDataRole.UserRole, part_id)
            category_item = self._center_item(QTableWidgetItem(category))
            assigned_item = self._center_item(QTableWidgetItem(assigned_user if assigned_user else "-"))
            if assigned_user:
                assigned_name = str(agent_name_lookup.get(assigned_user, "") or "").strip()
                if assigned_name and assigned_name != assigned_user:
                    assigned_item.setToolTip(f"{assigned_name}")
                else:
                    assigned_item.setToolTip(assigned_user)
            else:
                assigned_item.setToolTip("Unassigned")
            self.cat_parts_table.setItem(row_idx, 0, work_item)
            self.cat_parts_table.setItem(row_idx, 1, client_item)
            self.cat_parts_table.setItem(row_idx, 2, flag_item)
            self.cat_parts_table.setItem(row_idx, 3, age_item)
            self.cat_parts_table.setItem(row_idx, 4, working_item)
            if self._is_tech3_user:
                self.cat_parts_table.setItem(row_idx, 5, installed_item)
                self.cat_parts_table.setItem(row_idx, 6, assigned_item)
                self.cat_parts_table.setItem(row_idx, 7, category_item)
                self.cat_parts_table.setItem(row_idx, 8, qa_note_item)
            else:
                self.cat_parts_table.setItem(row_idx, 5, installed_item)
                self.cat_parts_table.setItem(row_idx, 6, category_item)
                self.cat_parts_table.setItem(row_idx, 7, qa_note_item)
        self._cat_parts_has_flagged_rows = bool((urgent_flagged_rows + in_progress_flagged_rows) > 0)
        self._cat_parts_has_urgent_flagged_rows = bool(urgent_flagged_rows > 0)
        self._cat_parts_has_in_progress_flagged_rows = bool(in_progress_flagged_rows > 0)
        self._update_tab_alert_states()
        self._mark_depot_view_refreshed(
            "agent_category",
            state_key,
            payload=[dict(row) for row in display_rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(display_rows),
        )

    def _build_client_tab(self):
        layout = QVBoxLayout(self.client_tab)
        if self._is_tech3_user:
            summary = QLabel("Client follow-up queue for all agents (Client + Other daily, Part Order after 21 days).")
        else:
            summary = QLabel("Client follow-up queue for this agent (Client + Other daily, Part Order after 21 days).")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Follow-up Queue"), 0)
        controls.addStretch(1)
        self.client_refresh_btn = QPushButton("Refresh")
        self.client_refresh_btn.clicked.connect(lambda: self._refresh_client_followup(force=True, reason="manual", ttl_ms=0))
        controls.addWidget(self.client_refresh_btn, 0)
        layout.addLayout(controls)

        self.client_due_summary = QLabel("No follow-up alerts.")
        self.client_due_summary.setProperty("section", True)
        layout.addWidget(self.client_due_summary)

        self.client_followup_table = QTableWidget()
        headers = ["Due", "Work Order", "Status", "Last Update", "Age", "Notes"]
        resize_modes = {
            0: QHeaderView.ResizeMode.ResizeToContents,
            1: QHeaderView.ResizeMode.ResizeToContents,
            2: QHeaderView.ResizeMode.ResizeToContents,
            3: QHeaderView.ResizeMode.ResizeToContents,
            4: QHeaderView.ResizeMode.ResizeToContents,
            5: QHeaderView.ResizeMode.Stretch,
        }
        if self._is_tech3_user:
            headers = ["Agent", "Due", "Work Order", "Status", "Last Update", "Age", "Notes"]
            resize_modes = {
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.Stretch,
            }
        configure_standard_table(
            self.client_followup_table,
            headers,
            resize_modes=resize_modes,
            stretch_last=True,
        )
        self.client_followup_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.client_followup_table.cellClicked.connect(self._on_client_followup_cell_clicked)
        self.client_followup_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        layout.addWidget(self.client_followup_table, 1)

        self._client_due_items: list[QTableWidgetItem] = []
        self._client_due_active_ids = set()
        self._client_due_flash_on = True
        self._client_due_flash_timer = QTimer(self)
        self._client_due_flash_timer.setInterval(700)
        self._client_due_flash_timer.timeout.connect(self._on_client_due_flash_tick)
        self._client_due_flash_timer.start()
    def _build_rtv_tab(self) -> None:
        if self.rtv_tab is None:
            return
        layout = QVBoxLayout(self.rtv_tab)
        summary = QLabel("Tech 3 RTV queue with shared comment updates.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        self.agent_rtv_table = QTableWidget()
        configure_standard_table(
            self.agent_rtv_table,
            ["Work Order", "Logged At", "Logged By", "Comments"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.agent_rtv_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.agent_rtv_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.agent_rtv_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.agent_rtv_table.customContextMenuRequested.connect(
            lambda pos: self._open_agent_rtv_comment_from_context(pos)
        )

        self.agent_rtv_refresh_btn = QPushButton("Refresh")
        self.agent_rtv_refresh_btn.clicked.connect(lambda: self._refresh_rtv_rows(force=True, reason="manual", ttl_ms=0))
        self.agent_rtv_open_notes_btn = QPushButton("Open Notes / Update Comment")
        self.agent_rtv_open_notes_btn.setProperty("actionRole", "pick")
        self.agent_rtv_open_notes_btn.clicked.connect(self._open_selected_agent_rtv_comment)
        self.agent_rtv_workorder_search = QLineEdit()
        self.agent_rtv_workorder_search.setPlaceholderText("Search work order...")
        self.agent_rtv_workorder_search.setClearButtonEnabled(True)
        self.agent_rtv_workorder_search.textChanged.connect(lambda _text: self._agent_rtv_search_timer.start())

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.agent_rtv_workorder_search, 1)
        controls.addWidget(self.agent_rtv_refresh_btn)
        controls.addWidget(self.agent_rtv_open_notes_btn)
        layout.addLayout(controls)
        layout.addWidget(self.agent_rtv_table, 1)
    def _refresh_rtv_rows(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "agent_rtv_table"):
            return
        search_text = str(self.agent_rtv_workorder_search.text() or "").strip() if hasattr(self, "agent_rtv_workorder_search") else ""
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("agent_rtv", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_rtv_rows(search_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_rtv_refresh_failed",
                severity="warning",
                summary="Failed loading RTV queue for Agent window.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.agent_rtv_table.setRowCount(0)
            return
        agent_meta = self._agent_meta_lookup()
        self.agent_rtv_table.setRowCount(0)
        for row_idx, row in enumerate(rows):
            self.agent_rtv_table.insertRow(row_idx)
            work_item = self._center_item(QTableWidgetItem(str(row["work_order"] or "").strip()))
            work_item.setData(Qt.ItemDataRole.UserRole, int(row["id"] or 0))
            logged_item = self._center_item(QTableWidgetItem(self._format_working_updated_stamp(str(row["created_at"] or "").strip()) or "-"))
            logged_item.setToolTip(str(row["created_at"] or "").strip())
            normalized_user = DepotRules.normalize_user_id(str(row["user_id"] or ""))
            user_item = self._center_item(QTableWidgetItem(normalized_user or "-"))
            user_icon = _resolve_user_icon_from_agent_meta(normalized_user, agent_meta)
            if user_icon is not None:
                user_item.setIcon(user_icon)
            comment_text = str(row["comments"] or "").strip()
            comment_item = self._center_item(QTableWidgetItem(self._note_preview(comment_text)))
            comment_item.setData(Qt.ItemDataRole.UserRole + 1, comment_text)
            comment_item.setToolTip(f"Comments: {comment_text if comment_text else '(none)'}")
            self.agent_rtv_table.setItem(row_idx, 0, work_item)
            self.agent_rtv_table.setItem(row_idx, 1, logged_item)
            self.agent_rtv_table.setItem(row_idx, 2, user_item)
            self.agent_rtv_table.setItem(row_idx, 3, comment_item)
        self._mark_depot_view_refreshed(
            "agent_rtv",
            state_key,
            payload=[dict(row) for row in rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _open_agent_rtv_comment_from_context(self, pos: QPoint) -> None:
        if not hasattr(self, "agent_rtv_table"):
            return
        if not _select_table_row_by_context_pos(self.agent_rtv_table, pos):
            return
        self._open_selected_agent_rtv_comment()

    def _open_selected_agent_rtv_comment(self) -> None:
        if not hasattr(self, "agent_rtv_table"):
            return
        if not _edit_aux_queue_comment(
            self,
            self.tracker,
            table=self.agent_rtv_table,
            target_key="rtvs.comments",
            theme_kind="agent",
            title="RTV Comment",
        ):
            return
        self._refresh_rtv_rows(force=True, reason="agent_rtv_comment", ttl_ms=0)
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views("agent_rtv", "dashboard_notes", reason="agent_rtv_comment")

    def _agent_meta_lookup(self) -> dict[str, tuple[str, str]]:
        try:
            repository = getattr(self.tracker, "user_repository", None)
            if repository is not None:
                return repository.agent_display_map()
            return self.tracker.agent_display_map()
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_missing_po_agent_meta_query_failed",
                severity="warning",
                summary="Agent Missing PO tab could not resolve agent metadata.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
        return {}

    def _build_missing_po_tab(self) -> None:
        if self.missing_po_tab is None:
            return
        layout = QVBoxLayout(self.missing_po_tab)
        summary = QLabel("Missing PO rows waiting for Part Order follow up.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        self.missing_po_summary = QLabel("No Missing PO rows.")
        self.missing_po_summary.setProperty("section", True)
        layout.addWidget(self.missing_po_summary)

        self.agent_missing_po_table = QTableWidget()
        configure_standard_table(
            self.agent_missing_po_table,
            ["Work Order", "Assigned Agent", "Submitted By", "Logged At", "Age", "Category", "Client", "QA Note"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.agent_missing_po_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.agent_missing_po_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.agent_missing_po_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.agent_missing_po_table.customContextMenuRequested.connect(
            lambda pos: self._open_agent_notes_from_context(self.agent_missing_po_table, pos)
        )

        self.agent_missing_po_refresh_btn = QPushButton("Refresh")
        self.agent_missing_po_refresh_btn.clicked.connect(lambda: self._refresh_missing_po_followups(force=True, reason="manual", ttl_ms=0))
        self.agent_missing_po_open_notes_btn = QPushButton("Open Notes")
        self.agent_missing_po_open_notes_btn.setProperty("actionRole", "pick")
        self.agent_missing_po_open_notes_btn.clicked.connect(
            lambda: self._open_agent_notes_for_table(self.agent_missing_po_table)
        )
        self.agent_missing_po_reassign_btn = QPushButton("Reassign Agent")
        self.agent_missing_po_reassign_btn.setProperty("actionRole", "pick")
        self.agent_missing_po_reassign_btn.clicked.connect(self._reassign_selected_missing_po_followup)
        self.agent_missing_po_resolve_btn = QPushButton("Resolve")
        self.agent_missing_po_resolve_btn.clicked.connect(self._resolve_selected_missing_po_followup)
        self.agent_missing_po_workorder_search = QLineEdit()
        self.agent_missing_po_workorder_search.setPlaceholderText("Search work order...")
        self.agent_missing_po_workorder_search.setClearButtonEnabled(True)
        self.agent_missing_po_workorder_search.textChanged.connect(lambda _text: self._agent_missing_po_search_timer.start())

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.agent_missing_po_workorder_search, 1)
        controls.addWidget(self.agent_missing_po_refresh_btn)
        controls.addWidget(self.agent_missing_po_open_notes_btn)
        controls.addWidget(self.agent_missing_po_reassign_btn)
        controls.addWidget(self.agent_missing_po_resolve_btn)
        layout.addLayout(controls)
        layout.addWidget(self.agent_missing_po_table, 1)

    def _refresh_missing_po_followups(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        table = getattr(self, "agent_missing_po_table", None)
        if table is None:
            return

        search_text = ""
        if hasattr(self, "agent_missing_po_workorder_search"):
            search_text = str(self.agent_missing_po_workorder_search.text() or "").strip().casefold()
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("agent_missing_po", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        previous_ids = set(getattr(self, "_missing_po_followup_ids", set()))
        started = time.monotonic()
        try:
            all_rows = self.tracker.list_missing_part_order_followups()
        except Exception as exc:
            table.setRowCount(0)
            self._missing_po_followup_ids = set()
            if hasattr(self, "missing_po_summary"):
                self.missing_po_summary.setText("Missing PO unavailable. Details were logged.")
            _runtime_log_event(
                "ui.agent_missing_po_refresh_failed",
                severity="warning",
                summary="Agent Missing PO tab failed to refresh.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self._update_tab_alert_states()
            return

        rows = list(all_rows)
        if search_text:
            rows = [
                row
                for row in all_rows
                if search_text in str(row.get("work_order", "") or "").strip().casefold()
            ]

        current_ids = {int(row.get("id", 0) or 0) for row in all_rows if int(row.get("id", 0) or 0) > 0}
        self._missing_po_followup_ids = current_ids
        if current_ids.difference(previous_ids) and "missing_po" in self._tab_alert_ack_states:
            self._tab_alert_ack_states["missing_po"] = False

        fallback_map = {
            DepotRules.normalize_work_order(str(row.get("work_order", "") or "").strip()): str(row.get("category", "") or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row.get("work_order", "") or "").strip())
        }
        category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        for row in rows:
            normalized_work_order = DepotRules.normalize_work_order(str(row.get("work_order", "") or "").strip())
            row["resolved_category"] = category_map.get(normalized_work_order, "") or str(row.get("category", "") or "").strip() or "Other"

        _populate_missing_po_followup_table(
            table,
            rows=rows,
            all_rows_count=len(all_rows),
            search_text=search_text,
            summary_label=getattr(self, "missing_po_summary", None),
            agent_meta=self._agent_meta_lookup(),
            icon_host=self,
        )
        self._update_tab_alert_states()
        self._mark_depot_view_refreshed(
            "agent_missing_po",
            state_key,
            payload=list(rows),
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _refresh_after_missing_po_followup_action(self) -> None:
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_missing_po",
                "agent_parts",
                "agent_category",
                "qa_missing_po",
                "qa_assigned",
                "qa_category",
                "qa_owner",
                reason="missing_po_followup_action",
            )
            return
        self._refresh_agent_parts(force=True, reason="missing_po_followup_action", ttl_ms=0)
        self._refresh_category_parts(force=True, reason="missing_po_followup_action", ttl_ms=0)
        self._refresh_missing_po_followups(force=True, reason="missing_po_followup_action", ttl_ms=0)

    def _reassign_selected_missing_po_followup(self) -> None:
        table = getattr(self, "agent_missing_po_table", None)
        if table is None:
            return
        _reassign_missing_po_followup(
            self,
            self.tracker,
            table=table,
            current_user=self.current_user,
            role_key="agent",
            refresh_callback=self._refresh_after_missing_po_followup_action,
        )

    def _resolve_selected_missing_po_followup(self) -> None:
        table = getattr(self, "agent_missing_po_table", None)
        if table is None:
            return
        _resolve_missing_po_followup(
            self,
            self.tracker,
            table=table,
            current_user=self.current_user,
            role_key="agent",
            refresh_callback=self._refresh_after_missing_po_followup_action,
        )

    def _build_team_client_tab(self) -> None:
        if self.team_client_tab is None:
            return
        layout = QVBoxLayout(self.team_client_tab)
        summary = QLabel("Tech 3 view: team client follow-up items due now.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Team Follow-up Queue"), 0)
        controls.addStretch(1)
        self.team_client_refresh_btn = QPushButton("Refresh")
        self.team_client_refresh_btn.clicked.connect(lambda: self._refresh_team_client_followup(force=True, reason="manual", ttl_ms=0))
        controls.addWidget(self.team_client_refresh_btn, 0)
        layout.addLayout(controls)

        self.team_client_due_summary = QLabel("No team follow-up alerts.")
        self.team_client_due_summary.setProperty("section", True)
        layout.addWidget(self.team_client_due_summary)

        self.team_client_followup_table = QTableWidget()
        configure_standard_table(
            self.team_client_followup_table,
            ["Agent", "Due", "Work Order", "Status", "Last Update", "Age", "Notes"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.team_client_followup_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.team_client_followup_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        layout.addWidget(self.team_client_followup_table, 1)

    def _refresh_team_client_followup(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if self.team_client_tab is None or not hasattr(self, "team_client_followup_table"):
            return
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("agent_team_client_followup", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_team_client_followups()
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_team_client_followup_query_failed",
                severity="warning",
                summary="Failed loading team client follow-up rows.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.team_client_followup_table.setRowCount(0)
            self.team_client_due_summary.setText("Team follow-up unavailable. Details were logged.")
            self._team_client_due_count = 0
            self._update_tab_alert_states()
            return

        today = datetime.now().date()
        today_iso = today.isoformat()
        agent_meta = self._agent_meta_lookup()
        self.team_client_followup_table.setRowCount(0)
        kept_row = 0
        due_total = 0
        due_active_total = 0
        for r in rows:
            latest_touch = str(r["latest_touch"] or "").strip()
            if latest_touch not in {DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER}:
                continue
            work_order = str(r["work_order"] or "").strip()
            if not work_order:
                continue
            user_id = DepotRules.normalize_user_id(str(r["user_id"] or ""))
            last_action = DepotRules.normalize_followup_action(str(r["followup_last_action"] or ""))
            last_action_at = str(r["followup_last_action_at"] or "").strip()
            last_action_actor = DepotRules.normalize_user_id(str(r["followup_last_actor"] or ""))
            alert_quiet_until = str(r["alert_quiet_until"] or "").strip()
            quiet_active = self.tracker.is_alert_quiet(alert_quiet_until)
            no_contact_count = int(max(0, safe_int(r["followup_no_contact_count"], 0)))
            action_date = self._parse_iso_date(last_action_at)
            days_since_action = max(0, (today - action_date).days) if action_date is not None else -1
            last_update = str(r["latest_touch_date"] or "").strip()
            if not last_update:
                last_update = str(r["created_at"] or "")[:10]
            age_days = -1
            if len(last_update) >= 10:
                try:
                    age_days = max(0, (today - datetime.strptime(last_update[:10], "%Y-%m-%d").date()).days)
                except Exception:
                    age_days = -1
            age_text = f"{age_days}d" if age_days >= 0 else "-"
            notes = str(r["comments"] or "").strip()

            due = False
            due_reason = ""
            if latest_touch == DepotRules.TOUCH_OTHER:
                due = bool(last_update and last_update < today_iso)
                if due:
                    due_reason = "Daily follow-up required for Client + Other."
            elif latest_touch == DepotRules.TOUCH_PART_ORDER:
                part_order_date = str(r["last_part_order_date"] or "").strip() or last_update
                if len(part_order_date) >= 10:
                    try:
                        po_days = max(0, (today - datetime.strptime(part_order_date[:10], "%Y-%m-%d").date()).days)
                    except Exception:
                        po_days = 0
                    if po_days >= 21:
                        due = True
                        due_reason = f"Part Order follow-up due ({po_days} days)."
            if not due:
                continue
            has_action = bool(last_action)
            due_active = bool(due and not quiet_active and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED)

            self.team_client_followup_table.insertRow(kept_row)
            agent_item = self._center_item(QTableWidgetItem(user_id if user_id else "-"))
            agent_icon = _resolve_user_icon_from_agent_meta(user_id, agent_meta)
            if agent_icon is not None:
                agent_item.setIcon(agent_icon)
            due_item = self._center_item(QTableWidgetItem(""))
            if due_active:
                due_item.setText(DepotRules.followup_stage_label(0))
                due_item.setIcon(self._followup_clock_icon("#21B46D"))
            elif has_action:
                if last_action == DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED:
                    due_item.setText("")
                    due_item.setIcon(self._followup_done_icon())
                else:
                    clock_icon, stage_text = self._followup_wait_icon_by_days(days_since_action)
                    due_item.setText(stage_text)
                    due_item.setIcon(clock_icon)
            action_line = f"\nLast follow-up: {last_action}" if last_action else ""
            actor_line = f" by {last_action_actor}" if last_action_actor else ""
            when_line = f" ({self._format_working_updated_stamp(last_action_at)})" if last_action_at else ""
            attempt_line = f"\nNo-contact follow-ups: {no_contact_count}" if no_contact_count > 0 else ""
            quiet_line = (
                f"\nAlert quiet until: {self._format_working_updated_stamp(alert_quiet_until)}"
                if quiet_active
                else ""
            )
            due_item.setToolTip(f"{due_reason}{action_line}{actor_line}{when_line}{attempt_line}{quiet_line}")

            work_item = self._center_item(QTableWidgetItem(work_order))
            status_item = self._center_item(QTableWidgetItem(latest_touch))
            update_item = self._center_item(QTableWidgetItem(last_update if last_update else "-"))
            age_item = self._center_item(QTableWidgetItem(age_text))
            note_item = self._center_item(QTableWidgetItem(self._note_preview(notes)))
            note_item.setToolTip(f"Comments: {notes if notes else '(none)'}")

            self.team_client_followup_table.setItem(kept_row, 0, agent_item)
            self.team_client_followup_table.setItem(kept_row, 1, due_item)
            self.team_client_followup_table.setItem(kept_row, 2, work_item)
            self.team_client_followup_table.setItem(kept_row, 3, status_item)
            self.team_client_followup_table.setItem(kept_row, 4, update_item)
            self.team_client_followup_table.setItem(kept_row, 5, age_item)
            self.team_client_followup_table.setItem(kept_row, 6, note_item)

            due_total += 1
            if due_active:
                due_active_total += 1
            kept_row += 1

        self._team_client_due_count = int(due_active_total)
        if due_total > 0:
            self.team_client_due_summary.setText(f"Team follow-up rows: {due_total} | Due now: {due_active_total}")
        else:
            self.team_client_due_summary.setText("No team follow-up alerts.")
        self._update_tab_alert_states()
        self._mark_depot_view_refreshed(
            "agent_team_client_followup",
            state_key,
            payload={"row_count": kept_row, "due_count": due_active_total},
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=kept_row,
        )

    def _refresh_client_followup(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ):
        if not hasattr(self, "client_followup_table"):
            return
        state_key = {"user_id": self.current_user, "tech3": self._is_tech3_user}
        if not self._should_refresh_depot_view("agent_client_followup", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_team_client_followups() if self._is_tech3_user else self.tracker.list_client_followups(self.current_user)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_client_followup_query_failed",
                severity="warning",
                summary="Failed loading agent client follow-up rows.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.client_followup_table.setRowCount(0)
            self.client_due_summary.setText("Client follow-up unavailable. Details were logged.")
            self._client_due_items = []
            self._client_due_active_ids = set()
            self._client_due_ack_ids = set()
            self._update_tab_alert_states()
            return

        today = datetime.now().date()
        today_iso = today.isoformat()
        agent_meta = self._agent_meta_lookup()
        self.client_followup_table.setRowCount(0)
        previous_active_ids = set(getattr(self, "_client_due_active_ids", set()))
        due_items: list[QTableWidgetItem] = []
        due_count = 0
        due_active_ids: set[int] = set()
        due_quiet_count = 0
        kept_row = 0
        for r in rows:
            client_part_id = int(r["id"])
            latest_touch = str(r["latest_touch"] or "").strip()
            if latest_touch not in {DepotRules.TOUCH_PART_ORDER, DepotRules.TOUCH_OTHER}:
                continue
            work_order = str(r["work_order"] or "").strip()
            if not work_order:
                continue
            last_action = DepotRules.normalize_followup_action(str(r["followup_last_action"] or ""))
            last_action_at = str(r["followup_last_action_at"] or "").strip()
            last_action_actor = DepotRules.normalize_user_id(str(r["followup_last_actor"] or ""))
            alert_quiet_until = str(r["alert_quiet_until"] or "").strip()
            quiet_active = self.tracker.is_alert_quiet(alert_quiet_until)
            no_contact_count = int(max(0, safe_int(r["followup_no_contact_count"], 0)))
            action_date = self._parse_iso_date(last_action_at)
            days_since_action = (
                max(0, (today - action_date).days)
                if action_date is not None
                else -1
            )
            last_update = str(r["latest_touch_date"] or "").strip()
            if not last_update:
                last_update = str(r["created_at"] or "")[:10]
            age_days = -1
            if len(last_update) >= 10:
                try:
                    age_days = max(0, (today - datetime.strptime(last_update[:10], "%Y-%m-%d").date()).days)
                except Exception:
                    age_days = -1
            age_text = f"{age_days}d" if age_days >= 0 else "-"
            notes = str(r["comments"] or "").strip()

            due = False
            due_reason = ""
            if latest_touch == DepotRules.TOUCH_OTHER:
                # "Other" requires daily follow-up after the submission day.
                due = bool(last_update and last_update < today_iso)
                if due:
                    due_reason = "Daily follow-up required for Client + Other."
            elif latest_touch == DepotRules.TOUCH_PART_ORDER:
                part_order_date = str(r["last_part_order_date"] or "").strip() or last_update
                if len(part_order_date) >= 10:
                    try:
                        po_days = max(0, (today - datetime.strptime(part_order_date[:10], "%Y-%m-%d").date()).days)
                    except Exception:
                        po_days = 0
                    if po_days >= 21:
                        due = True
                        due_reason = f"Part Order follow-up due ({po_days} days)."

            has_action = bool(last_action)
            due_active = bool(due and not quiet_active and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED)

            self.client_followup_table.insertRow(kept_row)
            due_item = self._center_item(QTableWidgetItem(""))
            due_item.setData(Qt.ItemDataRole.UserRole, client_part_id)
            due_item.setData(Qt.ItemDataRole.UserRole + 1, 1 if due else 0)
            due_item.setData(Qt.ItemDataRole.UserRole + 2, 1 if has_action else 0)
            due_item.setData(Qt.ItemDataRole.UserRole + 3, 1 if quiet_active else 0)
            if due_active:
                if has_action and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED:
                    clock_icon, stage_text = self._followup_wait_icon_by_days(days_since_action)
                    due_item.setText(stage_text)
                    due_item.setIcon(clock_icon)
                else:
                    due_item.setText(DepotRules.followup_stage_label(0))
                    due_item.setIcon(self._followup_clock_icon("#21B46D"))
            elif due and has_action:
                if last_action == DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED:
                    due_item.setText("")
                    due_item.setIcon(self._followup_done_icon())
                else:
                    clock_icon, stage_text = self._followup_wait_icon_by_days(days_since_action)
                    due_item.setText(stage_text)
                    due_item.setIcon(clock_icon)
            if due:
                action_line = f"\nLast follow-up: {last_action}" if last_action else ""
                actor_line = f" by {last_action_actor}" if last_action_actor else ""
                when_line = f" ({self._format_working_updated_stamp(last_action_at)})" if last_action_at else ""
                attempt_line = f"\nNo-contact follow-ups: {no_contact_count}" if no_contact_count > 0 else ""
                quiet_line = (
                    f"\nAlert quiet until: {self._format_working_updated_stamp(alert_quiet_until)}"
                    if quiet_active
                    else ""
                )
                if has_action and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED and days_since_action >= 0:
                    stage_name = DepotRules.followup_stage_label(0 if days_since_action <= 0 else (1 if days_since_action == 1 else 2))
                    wait_line = f"\nWaiting Stage: {stage_name}"
                else:
                    wait_line = ""
                if has_action and last_action:
                    due_item.setToolTip(
                        f"{due_reason}{wait_line}{action_line}{actor_line}{when_line}{attempt_line}{quiet_line}\n"
                        "Click this cell to log another follow-up action."
                    )
                else:
                    due_item.setToolTip(
                        f"{due_reason}{action_line}{actor_line}{when_line}{attempt_line}{quiet_line}\n"
                        "Click this cell to log follow-up action."
                    )
            else:
                due_item.setToolTip("No follow-up due.")
            work_item = self._center_item(QTableWidgetItem(work_order))
            status_item = self._center_item(QTableWidgetItem(latest_touch))
            update_item = self._center_item(QTableWidgetItem(last_update if last_update else "-"))
            age_item = self._center_item(QTableWidgetItem(age_text))
            note_item = self._center_item(QTableWidgetItem(self._note_preview(notes)))
            note_item.setToolTip(f"Comments: {notes if notes else '(none)'}")

            if self._is_tech3_user:
                user_id = DepotRules.normalize_user_id(str(r["user_id"] or ""))
                agent_item = self._center_item(QTableWidgetItem(user_id if user_id else "-"))
                user_icon = _resolve_user_icon_from_agent_meta(user_id, agent_meta)
                if user_icon is not None:
                    agent_item.setIcon(user_icon)
                self.client_followup_table.setItem(kept_row, 0, agent_item)
                self.client_followup_table.setItem(kept_row, 1, due_item)
                self.client_followup_table.setItem(kept_row, 2, work_item)
                self.client_followup_table.setItem(kept_row, 3, status_item)
                self.client_followup_table.setItem(kept_row, 4, update_item)
                self.client_followup_table.setItem(kept_row, 5, age_item)
                self.client_followup_table.setItem(kept_row, 6, note_item)
            else:
                self.client_followup_table.setItem(kept_row, 0, due_item)
                self.client_followup_table.setItem(kept_row, 1, work_item)
                self.client_followup_table.setItem(kept_row, 2, status_item)
                self.client_followup_table.setItem(kept_row, 3, update_item)
                self.client_followup_table.setItem(kept_row, 4, age_item)
                self.client_followup_table.setItem(kept_row, 5, note_item)
            if due_active:
                due_active_ids.add(client_part_id)
                if int(client_part_id) not in self._client_due_ack_ids:
                    due_items.append(due_item)
                due_count += 1
            elif due and quiet_active:
                due_quiet_count += 1
            kept_row += 1

        self._client_due_ack_ids.intersection_update(due_active_ids)
        self._client_due_active_ids = due_active_ids
        if due_active_ids.difference(previous_active_ids):
            self._tab_alert_ack_states["client"] = False
        self._client_due_items = due_items
        self._client_due_flash_on = True
        self._apply_client_due_flash_visuals()
        self._update_tab_alert_states()
        if due_count > 0:
            suffix = f" | Quieted: {due_quiet_count}" if due_quiet_count > 0 else ""
            self.client_due_summary.setText(f"Follow-up due now: {due_count}{suffix}")
        elif due_quiet_count > 0:
            self.client_due_summary.setText(f"Follow-up alerts quieted until tomorrow morning: {due_quiet_count}")
        else:
            self.client_due_summary.setText("No follow-up alerts.")
        self._mark_depot_view_refreshed(
            "agent_client_followup",
            state_key,
            payload={"row_count": kept_row, "due_count": due_count},
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=kept_row,
        )

    def _on_client_followup_cell_clicked(self, row: int, col: int) -> None:
        due_col = 1 if self._is_tech3_user else 0
        work_col = 2 if self._is_tech3_user else 1
        if col != due_col:
            return
        if row < 0 or row >= int(self.client_followup_table.rowCount()):
            return
        due_item = self.client_followup_table.item(row, due_col)
        work_item = self.client_followup_table.item(row, work_col)
        if due_item is None or work_item is None:
            return
        is_due_row = safe_int(due_item.data(Qt.ItemDataRole.UserRole + 1), 0) > 0
        has_logged_action = safe_int(due_item.data(Qt.ItemDataRole.UserRole + 2), 0) > 0
        quiet_active = safe_int(due_item.data(Qt.ItemDataRole.UserRole + 3), 0) > 0
        if not is_due_row and not has_logged_action:
            return
        client_part_id = safe_int(due_item.data(Qt.ItemDataRole.UserRole), 0)
        if client_part_id <= 0:
            return
        if is_due_row and not quiet_active and client_part_id in self._client_due_active_ids:
            self._client_due_ack_ids.add(int(client_part_id))
            due_item.setBackground(QColor(0, 0, 0, 0))
            self._client_due_items = [
                item
                for item in self._client_due_items
                if item is not None and safe_int(item.data(Qt.ItemDataRole.UserRole), 0) != int(client_part_id)
            ]
            self._apply_client_due_flash_visuals()
            self._update_tab_alert_states()
        work_order = str(work_item.text() or "").strip() or "(unknown)"
        action, ok = show_flowgrid_themed_input_item(
            self,
            self.app_window,
            "agent",
            "Client Follow-up",
            (
                f"Work Order: {work_order}\n"
                "Select follow-up outcome:"
            ),
            list(DepotRules.CLIENT_FOLLOWUP_ACTIONS),
            0,
            False,
        )
        if not ok:
            return
        action_text = DepotRules.normalize_followup_action(str(action or ""))
        if not action_text:
            return
        try:
            no_contact_count = self.tracker.mark_client_followup_action(client_part_id, action_text, self.current_user)
        except Exception as exc:
            _runtime_log_event(
                "ui.agent_client_followup_mark_failed",
                severity="warning",
                summary="Failed recording client follow-up action.",
                exc=exc,
                context={
                    "user_id": str(self.current_user),
                    "client_part_id": int(client_part_id),
                    "work_order": str(work_order),
                    "action": str(action_text),
                },
            )
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Follow-up",
                f"Could not save follow-up update:\n{type(exc).__name__}: {exc}",
            )
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_client_followup",
                "agent_team_client_followup",
                "qa_client_followup",
                "dashboard_notes",
                reason="client_followup_action",
            )
        else:
            self._refresh_client_followup(force=True, reason="client_followup_action", ttl_ms=0)
            if self.team_client_tab is not None:
                self._refresh_team_client_followup(force=True, reason="client_followup_action", ttl_ms=0)
        if action_text in DepotRules.CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS and int(no_contact_count) == 3:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "No Contact Alert",
                "Please ship unit back to store due to no contact from client.",
            )

    def _apply_client_due_flash_visuals(self) -> None:
        if not hasattr(self, "_client_due_items"):
            return
        if not self._client_due_items:
            if hasattr(self, "client_followup_table"):
                for row_idx in range(int(self.client_followup_table.rowCount())):
                    item = self.client_followup_table.item(row_idx, 0)
                    if item is not None:
                        item.setBackground(QColor(0, 0, 0, 0))
            return
        on_color = QColor("#D95A5A")
        on_color.setAlpha(105)
        off_color = QColor(0, 0, 0, 0)
        for item in list(self._client_due_items):
            if item is None:
                continue
            item.setBackground(on_color if self._client_due_flash_on else off_color)

    def _on_client_due_flash_tick(self) -> None:
        if not hasattr(self, "_client_due_items"):
            return
        if not self._client_due_items:
            self._client_due_flash_on = True
            return
        self._client_due_flash_on = not bool(getattr(self, "_client_due_flash_on", False))
        self._apply_client_due_flash_visuals()

__all__ = ["DeliveredInstallPickerDialog", "DepotAgentWindow"]
