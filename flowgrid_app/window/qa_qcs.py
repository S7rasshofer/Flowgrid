from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import csv
import re
import time

from PySide6.QtCore import QDate, QPoint, Qt, QTimer
from PySide6.QtGui import QColor, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QStyle,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from flowgrid_app import PermissionDeniedError, PermissionService
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import normalize_hex, safe_int

from .agent import DepotAgentWindow
from .common import AlertPulseTabBar, format_working_updated_stamp, note_preview, parse_iso_date, part_age_label
from .constants import DEPOT_RECENT_VIEW_TTL_MS, DEPOT_SEARCH_REFRESH_DEBOUNCE_MS, DEPOT_VIEW_TTL_MS
from .popup_support import (
    DepotFramelessToolWindow,
    _ensure_shell_window_available,
    _visible_flowgrid_shell_window,
    show_flowgrid_themed_input_item,
    show_flowgrid_themed_save_file_name,
)
from .query_support import (
    _dedupe_part_detail_rows,
    _installed_key_set_from_text,
    _merged_part_detail_rows,
    _part_detail_row_key,
)
from .shared_actions import (
    _copy_work_order_with_notice,
    _edit_aux_queue_comment,
    _edit_part_notes,
    _populate_missing_po_followup_table,
    _reassign_missing_po_followup,
    _resolve_missing_po_followup,
)
from .table_support import _center_table_item, _select_table_row_by_context_pos, configure_standard_table

DepotTracker = Any

class DepotQAWindow(DepotFramelessToolWindow):
    def __init__(self, tracker: DepotTracker, current_user: str, app_window: "QuickInputsWindow" | None = None):
        super().__init__(app_window, window_title="QA/WCS", theme_kind="qa", size=(820, 550))
        self.tracker = tracker
        self.current_user = DepotRules.normalize_user_id(current_user)
        permission_service = getattr(self.tracker, "permission_service", None)
        if permission_service is not None:
            permission_service.require_qa_access(self.current_user)
        elif not self.tracker.can_open_qa_window(self.current_user):
            raise PermissionDeniedError(PermissionService.QA_ACCESS_DENIED_MESSAGE)
        self._is_admin_user = self.tracker.is_admin_user(self.current_user)
        self._can_view_missing_po_tab = self.tracker.can_access_missing_po_followups(self.current_user)
        self._always_on_top_config_key = "qa_window_always_on_top"
        self._window_always_on_top = self._load_window_always_on_top_preference(self._always_on_top_config_key, default=True)
        self.set_window_always_on_top(self._window_always_on_top)

        self.qa_tabs = QTabWidget(self)
        self._qa_tab_bar = AlertPulseTabBar(self.qa_tabs)
        self.qa_tabs.setTabBar(self._qa_tab_bar)
        self.root_layout.addWidget(self.qa_tabs)

        self.submit_tab = QWidget()
        self.assigned_tab = QWidget()
        self.delivered_tab = QWidget()
        self.qa_cat_parts_tab = QWidget()
        self.qa_client_tab = QWidget()
        self.qa_rtv_tab = QWidget()
        self.client_jo_tab = QWidget()
        if self._can_view_missing_po_tab:
            self.missing_po_followup_tab = QWidget()

        self.qa_tabs.addTab(self.submit_tab, "Submit")
        self.qa_tabs.addTab(self.assigned_tab, "Active Parts")
        self.qa_tabs.addTab(self.delivered_tab, "Parts Delivered")
        self.qa_tabs.addTab(self.qa_cat_parts_tab, "Cat Parts")
        self.qa_tabs.addTab(self.qa_client_tab, "Client Est.")
        self.qa_tabs.addTab(self.qa_rtv_tab, "RTV")
        self.qa_tabs.addTab(self.client_jo_tab, "JO")
        if self._can_view_missing_po_tab:
            self.qa_tabs.addTab(self.missing_po_followup_tab, "Missing PO")

        self._qa_tab_indices: dict[str, int] = {}
        self._qa_tab_titles: dict[str, str] = {}
        self._qa_tab_alert_states: dict[str, bool] = {}
        self._qa_tab_alert_ack_states: dict[str, bool] = {}
        self._qa_missing_po_followup_ids: set[int] = set()
        self._qa_tab_indices["cat_parts"] = int(self.qa_tabs.indexOf(self.qa_cat_parts_tab))
        self._qa_tab_titles["cat_parts"] = "Cat Parts"
        self._qa_tab_alert_states["cat_parts"] = False
        self._qa_tab_alert_ack_states["cat_parts"] = False
        self._qa_tab_indices["client"] = int(self.qa_tabs.indexOf(self.qa_client_tab))
        self._qa_tab_titles["client"] = "Client Est."
        self._qa_tab_alert_states["client"] = False
        self._qa_tab_alert_ack_states["client"] = False
        self._qa_tab_indices["rtv"] = int(self.qa_tabs.indexOf(self.qa_rtv_tab))
        self._qa_tab_titles["rtv"] = "RTV"
        self._qa_tab_alert_states["rtv"] = False
        self._qa_tab_alert_ack_states["rtv"] = False
        self._qa_tab_indices["client_jo"] = int(self.qa_tabs.indexOf(self.client_jo_tab))
        self._qa_tab_titles["client_jo"] = "JO"
        self._qa_tab_alert_states["client_jo"] = False
        self._qa_tab_alert_ack_states["client_jo"] = False
        if self._can_view_missing_po_tab:
            self._qa_tab_indices["missing_po"] = int(self.qa_tabs.indexOf(self.missing_po_followup_tab))
            self._qa_tab_titles["missing_po"] = "Missing PO"
            self._qa_tab_alert_states["missing_po"] = False
            self._qa_tab_alert_ack_states["missing_po"] = False
        self._qa_tab_flash_on = True
        self._qa_tab_flash_timer = QTimer(self)
        self._qa_tab_flash_timer.setInterval(700)
        self._qa_tab_flash_timer.timeout.connect(self._on_qa_tab_alert_flash_tick)
        self._qa_tab_flash_timer.start()
        self._qa_tabs_ready = False
        self.qa_tabs.currentChanged.connect(self._on_qa_tab_changed)
        self._qa_window_initialized = False
        self._qa_owner_preview_timer = QTimer(self)
        self._qa_owner_preview_timer.setSingleShot(True)
        self._qa_owner_preview_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_owner_preview_timer.timeout.connect(lambda: self._refresh_repair_owner_preview(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_assigned_search_timer = QTimer(self)
        self._qa_assigned_search_timer.setSingleShot(True)
        self._qa_assigned_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_assigned_search_timer.timeout.connect(lambda: self._refresh_assigned_parts(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_delivered_search_timer = QTimer(self)
        self._qa_delivered_search_timer.setSingleShot(True)
        self._qa_delivered_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_delivered_search_timer.timeout.connect(lambda: self._refresh_delivered_parts(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_category_search_timer = QTimer(self)
        self._qa_category_search_timer.setSingleShot(True)
        self._qa_category_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_category_search_timer.timeout.connect(lambda: self._refresh_qa_category_parts(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_rtv_search_timer = QTimer(self)
        self._qa_rtv_search_timer.setSingleShot(True)
        self._qa_rtv_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_rtv_search_timer.timeout.connect(lambda: self._refresh_qa_rtv_rows(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_client_jo_search_timer = QTimer(self)
        self._qa_client_jo_search_timer.setSingleShot(True)
        self._qa_client_jo_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_client_jo_search_timer.timeout.connect(lambda: self._refresh_qa_client_jo_rows(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))
        self._qa_missing_po_search_timer = QTimer(self)
        self._qa_missing_po_search_timer.setSingleShot(True)
        self._qa_missing_po_search_timer.setInterval(DEPOT_SEARCH_REFRESH_DEBOUNCE_MS)
        self._qa_missing_po_search_timer.timeout.connect(lambda: self._refresh_missing_po_followups(reason="search", ttl_ms=DEPOT_VIEW_TTL_MS))

        self._build_qa_submit_tab()
        self._build_qa_assigned_tab()
        self._build_qa_delivered_tab()
        self._build_qa_cat_parts_tab()
        self._build_qa_client_followup_tab()
        self._build_qa_rtv_tab()
        self._build_qa_client_jo_tab()
        if self._can_view_missing_po_tab:
            self._build_qa_missing_po_followup_tab()
        self._qa_tabs_ready = True

        self.recent_submissions_label = QLabel()
        self.recent_submissions_label.setWordWrap(True)
        self.recent_submissions_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.root_layout.addWidget(self.recent_submissions_label)
        self._apply_read_only_ui_state()

        if self.app_window is not None:
            self.apply_theme_styles()

    def _apply_read_only_ui_state(self) -> None:
        if not self.is_read_only_mode():
            return
        self._disable_widgets_for_read_only(
            [
                getattr(self, "qa_work_order", None),
                getattr(self, "qa_client_check", None),
                getattr(self, "qa_comments", None),
                getattr(self, "qa_flag_combo", None),
                getattr(self, "qa_bulk_parts_input", None),
                getattr(self, "qa_bulk_import_btn", None),
                getattr(self, "qa_assigned_open_notes_btn", None),
                getattr(self, "qa_delivered_open_notes_btn", None),
                getattr(self, "qa_cat_open_notes_btn", None),
                getattr(self, "qa_rtv_open_notes_btn", None),
                getattr(self, "qa_client_jo_open_notes_btn", None),
                getattr(self, "qa_missing_po_reassign_btn", None),
                getattr(self, "qa_missing_po_resolve_btn", None),
                getattr(self, "qa_missing_po_open_notes_btn", None),
            ],
            "QA data updates",
        )

    def apply_theme_styles(self) -> None:
        super().apply_theme_styles()
        self._apply_qa_tab_alert_visuals()

    def showEvent(self, event) -> None:  # noqa: N802
        super().showEvent(event)
        force_refresh = not bool(self._qa_window_initialized)
        self._refresh_qa_visible_views(force=force_refresh, reason="window-show")
        self._qa_window_initialized = True

    def _qa_tab_normal_text_color(self) -> QColor:
        if self.app_window is not None:
            return QColor(normalize_hex(self.app_window.palette_data.get("label_text", "#FFFFFF"), "#FFFFFF"))
        return QColor("#FFFFFF")

    def _qa_tab_key_for_index(self, index: int) -> str:
        for key, idx in self._qa_tab_indices.items():
            if int(idx) == int(index):
                return key
        return ""

    def _acknowledge_qa_tab_alert(self, key: str) -> None:
        if key not in self._qa_tab_alert_states:
            return
        if not bool(self._qa_tab_alert_states.get(key, False)):
            return
        if bool(self._qa_tab_alert_ack_states.get(key, False)):
            return
        self._qa_tab_alert_ack_states[key] = True
        self._apply_qa_tab_alert_visuals()

    def _refresh_qa_tab_for_key(
        self,
        key: str,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if key == "cat_parts":
            self._refresh_qa_category_parts(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "client":
            self._refresh_qa_client_followup(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "rtv":
            self._refresh_qa_rtv_rows(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "client_jo":
            self._refresh_qa_client_jo_rows(force=force, reason=reason, ttl_ms=ttl_ms)
        elif key == "missing_po" and self._can_view_missing_po_tab:
            self._refresh_missing_po_followups(force=force, reason=reason, ttl_ms=ttl_ms)

    def _refresh_qa_visible_views(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        self._refresh_recent_submissions_label(force=force, reason=reason, ttl_ms=DEPOT_RECENT_VIEW_TTL_MS)
        current_index = int(self.qa_tabs.currentIndex()) if hasattr(self, "qa_tabs") else -1
        current_widget = self.qa_tabs.widget(current_index) if current_index >= 0 else None
        if current_widget is self.submit_tab:
            self._refresh_repair_owner_preview(force=force, reason=reason, ttl_ms=ttl_ms)
            return
        if current_widget is self.assigned_tab:
            self._refresh_assigned_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            return
        if current_widget is self.delivered_tab:
            self._refresh_delivered_parts(force=force, reason=reason, ttl_ms=ttl_ms)
            return
        self._refresh_qa_tab_for_key(
            self._qa_tab_key_for_index(current_index),
            force=force,
            reason=reason,
            ttl_ms=ttl_ms,
        )

    def _on_qa_tab_changed(self, index: int) -> None:
        if not bool(getattr(self, "_qa_tabs_ready", False)):
            return
        current_widget = self.qa_tabs.widget(int(index)) if hasattr(self, "qa_tabs") else None
        if current_widget is self.submit_tab:
            self._refresh_repair_owner_preview(reason="tab-change", ttl_ms=DEPOT_VIEW_TTL_MS)
            return
        if current_widget is self.assigned_tab:
            self._refresh_assigned_parts(reason="tab-change", ttl_ms=DEPOT_VIEW_TTL_MS)
            return
        if current_widget is self.delivered_tab:
            self._refresh_delivered_parts(reason="tab-change", ttl_ms=DEPOT_VIEW_TTL_MS)
            return
        key = self._qa_tab_key_for_index(index)
        if key == "missing_po":
            self._acknowledge_qa_tab_alert(key)
        self._refresh_qa_tab_for_key(key, reason="tab-change", ttl_ms=DEPOT_VIEW_TTL_MS)

    def _set_qa_tab_alert(self, key: str, enabled: bool, acknowledged: bool | None = None) -> None:
        if key not in self._qa_tab_alert_states:
            return
        enabled_now = bool(enabled)
        self._qa_tab_alert_states[key] = enabled_now
        if acknowledged is None:
            acknowledged = bool(enabled_now and self._qa_tab_alert_ack_states.get(key, False))
        self._qa_tab_alert_ack_states[key] = bool(enabled_now and acknowledged)
        self._apply_qa_tab_alert_visuals()

    def _update_qa_tab_alert_states(self) -> None:
        self._set_qa_tab_alert("cat_parts", False, acknowledged=True)
        client_alert = bool(
            any(
                int(part_id) not in getattr(self, "_qa_client_due_ack_ids", set())
                for part_id in getattr(self, "_qa_client_due_active_ids", set())
            )
        )
        self._set_qa_tab_alert("client", client_alert)
        self._set_qa_tab_alert("rtv", False, acknowledged=True)
        if "missing_po" not in self._qa_tab_alert_states:
            return
        missing_po_alert = bool(getattr(self, "_qa_missing_po_followup_ids", set()))
        missing_po_ack = bool(self._qa_tab_alert_ack_states.get("missing_po", False))
        if missing_po_alert and int(self._qa_tab_indices.get("missing_po", -1)) == int(self.qa_tabs.currentIndex()):
            missing_po_ack = True
        self._set_qa_tab_alert("missing_po", missing_po_alert, acknowledged=missing_po_ack)

    def _apply_qa_tab_alert_visuals(self) -> None:
        if not hasattr(self, "qa_tabs"):
            return
        tab_bar = self.qa_tabs.tabBar()
        normal_color = self._qa_tab_normal_text_color()
        alert_indices: set[int] = set()
        ack_indices: set[int] = set()
        for key, idx in self._qa_tab_indices.items():
            if idx < 0 or idx >= int(self.qa_tabs.count()):
                continue
            base_text = self._qa_tab_titles.get(key, self.qa_tabs.tabText(idx))
            self.qa_tabs.setTabText(idx, base_text)
            if not bool(self._qa_tab_alert_states.get(key, False)):
                continue
            alert_indices.add(int(idx))
            if bool(self._qa_tab_alert_ack_states.get(key, False)):
                ack_indices.add(int(idx))
        if isinstance(tab_bar, AlertPulseTabBar):
            tab_bar.set_alert_visual_state(alert_indices, ack_indices, bool(self._qa_tab_flash_on), normal_color)
            return
        flashing_color = QColor("#F4BCBC")
        acknowledged_color = QColor("#E6C177")
        for key, idx in self._qa_tab_indices.items():
            if idx < 0 or idx >= int(self.qa_tabs.count()):
                continue
            if idx not in alert_indices:
                tab_bar.setTabTextColor(idx, normal_color)
                continue
            if idx in ack_indices:
                tab_bar.setTabTextColor(idx, acknowledged_color)
            else:
                tab_bar.setTabTextColor(idx, flashing_color if self._qa_tab_flash_on else normal_color)

    def _on_qa_tab_alert_flash_tick(self) -> None:
        if not any(bool(value) for value in self._qa_tab_alert_states.values()):
            self._qa_tab_flash_on = True
            self._apply_qa_tab_alert_visuals()
            return
        self._qa_tab_flash_on = not bool(self._qa_tab_flash_on)
        self._apply_qa_tab_alert_visuals()

    def _build_qa_submit_tab(self):
        layout = QFormLayout(self.submit_tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setHorizontalSpacing(6)
        layout.setVerticalSpacing(3)
        layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.qa_work_order = QLineEdit()
        self.qa_repair_owner = QLineEdit()
        self.qa_repair_owner.setReadOnly(True)
        self.qa_repair_owner.setPlaceholderText("Latest Part Order repair owner")
        self.qa_repair_owner_refresh_btn = QPushButton("Refresh")
        self.qa_repair_owner_refresh_btn.setProperty("actionRole", "pick")
        self.qa_client_check = QCheckBox("Client")
        self.qa_comments = QLineEdit()
        self.qa_flag_combo = QComboBox()
        self._populate_flags()
        self.qa_bulk_parts_wrap = QWidget()
        self.qa_bulk_parts_layout = QVBoxLayout(self.qa_bulk_parts_wrap)
        self.qa_bulk_parts_layout.setContentsMargins(0, 0, 0, 0)
        self.qa_bulk_parts_layout.setSpacing(3)
        self.qa_bulk_parts_headers = QWidget(self.qa_bulk_parts_wrap)
        self.qa_bulk_parts_headers.setProperty("muted", True)
        self.qa_bulk_parts_headers_layout = QGridLayout(self.qa_bulk_parts_headers)
        self.qa_bulk_parts_headers_layout.setContentsMargins(6, 0, 6, 0)
        self.qa_bulk_parts_headers_layout.setHorizontalSpacing(10)
        self.qa_bulk_parts_headers_layout.setVerticalSpacing(0)
        for col_idx, header_text in enumerate(("LPN", "Part Description", "Part #", "Shipping Info")):
            header_label = QLabel(header_text, self.qa_bulk_parts_headers)
            header_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.qa_bulk_parts_headers_layout.addWidget(header_label, 0, col_idx)
        self.qa_bulk_parts_headers_layout.setColumnStretch(0, 2)
        self.qa_bulk_parts_headers_layout.setColumnStretch(1, 2)
        self.qa_bulk_parts_headers_layout.setColumnStretch(2, 4)
        self.qa_bulk_parts_headers_layout.setColumnStretch(3, 4)
        self.qa_bulk_parts_input = QTextEdit()
        self.qa_bulk_parts_input.setAcceptRichText(False)
        self.qa_bulk_parts_input.setMinimumHeight(116)
        self.qa_bulk_parts_input.setPlaceholderText(
            "Paste tab-separated rows here:\n"
            "LPN<TAB>Part Description<TAB>Part #<TAB>Shipping Info\n"
            "Only rows with shipping info containing 'delivered' are imported."
        )
        self.qa_bulk_parts_input.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_bulk_parts_input.customContextMenuRequested.connect(self._show_qa_bulk_parts_context_menu)
        self.qa_bulk_parts_layout.addWidget(self.qa_bulk_parts_headers)
        self.qa_bulk_parts_layout.addWidget(self.qa_bulk_parts_input)
        self.qa_bulk_import_btn = QPushButton("Import Delivered Rows")
        self.qa_bulk_import_btn.setProperty("actionRole", "pick")
        self.qa_bulk_import_btn.clicked.connect(self._submit_qa_bulk_parts)
        self.qa_bulk_import_status = QLabel("")
        self.qa_bulk_import_status.setWordWrap(True)
        self.qa_bulk_import_status.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)

        repair_owner_row_wrap = QWidget(self.submit_tab)
        repair_owner_row = QHBoxLayout(repair_owner_row_wrap)
        repair_owner_row.setContentsMargins(0, 0, 0, 0)
        repair_owner_row.setSpacing(6)
        repair_owner_row.addWidget(self.qa_repair_owner, 1)
        repair_owner_row.addWidget(self.qa_repair_owner_refresh_btn, 0)

        layout.addRow("Work Order", self.qa_work_order)
        layout.addRow("Repair Owner", repair_owner_row_wrap)
        layout.addRow("Client", self.qa_client_check)
        layout.addRow("Flag", self.qa_flag_combo)
        layout.addRow("QA Comment", self.qa_comments)
        layout.addRow("Parts Paste", self.qa_bulk_parts_wrap)
        layout.addRow("", self.qa_bulk_import_btn)
        layout.addRow("", self.qa_bulk_import_status)

        self.qa_work_order.textChanged.connect(lambda _text: self._qa_owner_preview_timer.start())
        self.qa_repair_owner_refresh_btn.clicked.connect(
            lambda _checked=False: self._refresh_repair_owner_preview(force=True, reason="manual", ttl_ms=0)
        )
        self.qa_work_order.returnPressed.connect(self._submit_qa_part)

    def _refresh_repair_owner_preview(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_repair_owner"):
            return
        work_order = DepotRules.normalize_work_order(str(self.qa_work_order.text() or "").strip()) if hasattr(self, "qa_work_order") else ""
        state_key = {"work_order": work_order}
        if not self._should_refresh_depot_view("qa_owner", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            if not work_order:
                self.qa_repair_owner.clear()
                self.qa_repair_owner.setPlaceholderText("Latest Part Order repair owner")
                self._mark_depot_view_refreshed(
                    "qa_owner",
                    state_key,
                    payload="",
                    reason=reason,
                    duration_ms=(time.monotonic() - started) * 1000.0,
                    row_count=0,
                )
                return
            source_submission = self.tracker.get_latest_part_order_submission(work_order)
            if source_submission is None:
                self.qa_repair_owner.setText("No Part Order submission")
                self._mark_depot_view_refreshed(
                    "qa_owner",
                    state_key,
                    payload="No Part Order submission",
                    reason=reason,
                    duration_ms=(time.monotonic() - started) * 1000.0,
                    row_count=0,
                )
                return
            owner_user_id = DepotRules.normalize_user_id(str(source_submission.get("user_id", "") or ""))
            self.qa_repair_owner.setText(owner_user_id if owner_user_id else "Repair owner unavailable")
            self._mark_depot_view_refreshed(
                "qa_owner",
                state_key,
                payload=owner_user_id,
                reason=reason,
                duration_ms=(time.monotonic() - started) * 1000.0,
                row_count=1,
            )
        except Exception as exc:
            self.qa_repair_owner.setText("Repair owner lookup failed")
            _runtime_log_event(
                "ui.qa_repair_owner_preview_failed",
                severity="warning",
                summary="QA repair-owner preview refresh failed.",
                exc=exc,
                context={
                    "user_id": str(self.current_user),
                    "work_order": work_order,
                    "reason": str(reason or ""),
                },
            )
            if str(reason or "").strip().lower() == "manual":
                self._show_themed_message(
                    QMessageBox.Icon.Warning,
                    "Refresh Failed",
                    "Repair owner could not be refreshed. Details were logged for support.",
                )

    def _confirm_qa_flag_retention(self, work_order: str, selected_flag: str) -> str | None:
        selected_flag_text = str(selected_flag or "").strip()
        if selected_flag_text:
            return selected_flag_text

        existing_flag = self.tracker.get_active_part_qa_flag(work_order)
        if not existing_flag:
            return ""

        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Icon.Question)
        dialog.setWindowTitle("Confirm QA Flag")
        dialog.setText(
            "This work order already has an existing QA flag. "
            "Submitting with no flag selected will clear the existing QA flag."
        )
        dialog.setInformativeText(
            f"Existing QA flag: {existing_flag}\n\nDo you want to keep the existing flag?"
        )
        keep_button = dialog.addButton("Keep Existing Flag", QMessageBox.ButtonRole.YesRole)
        clear_button = dialog.addButton("Clear Flag", QMessageBox.ButtonRole.NoRole)
        dialog.addButton(QMessageBox.StandardButton.Cancel)
        dialog.setDefaultButton(keep_button)
        dialog.exec()

        clicked = dialog.clickedButton()
        if clicked == keep_button:
            return existing_flag
        if clicked == clear_button:
            return ""
        return None

    def _qa_agent_choice_items(self, work_order: str) -> tuple[list[str], dict[str, str], int]:
        repository = getattr(self.tracker, "user_repository", None)
        if repository is not None:
            return repository.part_owner_choice_items(work_order)
        return self.tracker.part_owner_choice_items(work_order)

    def _prompt_qa_missing_part_order_agent(self, work_order: str) -> str:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        try:
            agent_items, item_lookup, current_index = self._qa_agent_choice_items(normalized_work_order)
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_missing_part_order_agent_prompt_failed",
                severity="warning",
                summary="QA missing-Part-Order submission could not load the agent list.",
                exc=exc,
                context={"user_id": str(self.current_user), "work_order": normalized_work_order},
            )
            self._show_themed_message(
                QMessageBox.Icon.Critical,
                "Agent Selection Failed",
                f"Could not load the agent list:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
            )
            return ""
        if not agent_items:
            _runtime_log_event(
                "ui.qa_missing_part_order_agent_prompt_failed_no_agents",
                severity="warning",
                summary="QA missing-Part-Order submission could not continue because no agents are configured.",
                context={"user_id": str(self.current_user), "work_order": normalized_work_order},
            )
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Parts Submission Blocked",
                "No agents are configured. Add an agent or create the Part Order submission before continuing.",
            )
            return ""

        selection, ok = show_flowgrid_themed_input_item(
            self,
            self.app_window,
            "qa",
            "Assign Repair Owner",
            (
                f"Work Order: {normalized_work_order}\n"
                "No Part Order submission exists for this work order.\n"
                "Select the agent who should own this repair.\n"
                "This parts submission will still be saved and logged for admin follow up."
            ),
            agent_items,
            current_index,
            False,
        )
        if not ok:
            return ""
        return item_lookup.get(str(selection or "").strip(), "")

    def _resolve_qa_submission_context(self, work_order: str) -> dict[str, str] | None:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return None

        category = self._resolve_category_for_qa_submission(normalized_work_order)
        if not category:
            return None

        source_submission = self.tracker.get_latest_part_order_submission(normalized_work_order)
        if source_submission is not None:
            return {
                "work_order": normalized_work_order,
                "category": str(category or "").strip(),
                "assigned_user_id": "",
            }

        assigned_user_id = self._prompt_qa_missing_part_order_agent(normalized_work_order)
        if not assigned_user_id:
            return None
        return {
            "work_order": normalized_work_order,
            "category": str(category or "").strip(),
            "assigned_user_id": str(assigned_user_id or "").strip(),
        }

    def _resolve_category_for_qa_submission(self, work_order: str) -> str:
        normalized_work_order = DepotRules.normalize_work_order(work_order)
        if not normalized_work_order:
            return ""
        source_submission = self.tracker.get_latest_part_order_submission(normalized_work_order)
        resolved = self.tracker.resolve_work_order_category(
            normalized_work_order,
            str(source_submission.get("category", "") or "").strip() if source_submission is not None else "",
        )
        if resolved:
            return resolved

        category, ok = show_flowgrid_themed_input_item(
            self,
            self.app_window,
            "qa",
            "Select Category",
            (
                f"Work Order: {normalized_work_order}\n"
                "No category has been recorded yet for this work order.\n"
                "Select the category to continue the parts submission."
            ),
            list(DepotRules.CATEGORY_OPTIONS),
            0,
            False,
        )
        if not ok:
            return ""
        category_text = str(category or "").strip()
        if not category_text:
            return ""
        updated_category = self.tracker.update_work_order_category(normalized_work_order, category_text)
        return updated_category or category_text

    def _populate_flags(self) -> None:
        previous = self.qa_flag_combo.currentText().strip() if hasattr(self, "qa_flag_combo") else "None"
        options = self.tracker.get_qa_flag_options(include_none=True)
        self.qa_flag_combo.blockSignals(True)
        self.qa_flag_combo.clear()
        self.qa_flag_combo.addItems(options)
        idx = self.qa_flag_combo.findText(previous)
        self.qa_flag_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.qa_flag_combo.blockSignals(False)

    def _submit_qa_part(self):
        if self._warn_if_read_only("QA submission"):
            return
        wo = self.qa_work_order.text().strip()
        bulk_text = str(self.qa_bulk_parts_input.toPlainText() or "").strip() if hasattr(self, "qa_bulk_parts_input") else ""
        if not wo:
            if bulk_text:
                self._submit_qa_bulk_parts()
                return
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Work order required.")
            return
        if bulk_text:
            # Enter on submit should include both form submission and delivered import in one action.
            self._submit_qa_bulk_parts()
            return
        client = self.qa_client_check.isChecked()
        comments = self.qa_comments.text().strip()
        submission_context = self._resolve_qa_submission_context(wo)
        if submission_context is None:
            return
        category = str(submission_context.get("category", "") or "").strip()
        assigned_user_id = DepotRules.normalize_user_id(str(submission_context.get("assigned_user_id", "") or ""))
        selected_flag = str(self.qa_flag_combo.currentText() if hasattr(self, "qa_flag_combo") else "").strip()
        if selected_flag.lower() == "none":
            selected_flag = ""
        selected_flag = self._confirm_qa_flag_retention(wo, selected_flag)
        if selected_flag is None:
            return

        try:
            if assigned_user_id:
                self.tracker.submit_part_missing_part_order(
                    self.current_user,
                    wo,
                    category,
                    client,
                    assigned_user_id,
                    comments,
                    selected_flag,
                )
            else:
                self.tracker.submit_part(
                    self.current_user,
                    wo,
                    category,
                    client,
                    comments,
                    selected_flag,
                )
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_part_submit_failed",
                severity="warning",
                summary="QA part submission failed.",
                exc=exc,
                context={"user_id": str(self.current_user), "work_order": str(wo)},
            )
            self._show_themed_message(
                QMessageBox.Icon.Critical,
                "Save Failed",
                f"Could not submit parts:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
            )
            return
        self.qa_work_order.clear()
        self.qa_comments.clear()
        if hasattr(self, "qa_flag_combo"):
            self.qa_flag_combo.setCurrentIndex(0)
        if hasattr(self, "qa_bulk_import_status"):
            self.qa_bulk_import_status.clear()
        self._refresh_repair_owner_preview()
        self.qa_work_order.setFocus()
        self._refresh_after_qa_submit()

    def _refresh_after_qa_submit(self) -> None:
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "qa_recent",
                "qa_assigned",
                "qa_delivered",
                "qa_category",
                "qa_missing_po",
                "qa_owner",
                "agent_parts",
                "agent_category",
                "agent_missing_po",
                "dashboard_completed",
                reason="qa_submit",
            )
            return
        self._refresh_recent_submissions_label(force=True, reason="qa_submit", ttl_ms=0)
        self._refresh_assigned_parts(force=True, reason="qa_submit", ttl_ms=0)
        self._refresh_delivered_parts(force=True, reason="qa_submit", ttl_ms=0)
        self._refresh_qa_category_parts(force=True, reason="qa_submit", ttl_ms=0)
        self._refresh_missing_po_followups(force=True, reason="qa_submit", ttl_ms=0)
        self._refresh_repair_owner_preview(force=True, reason="qa_submit", ttl_ms=0)

    def closeEvent(self, event) -> None:  # noqa: N802
        super().closeEvent(event)
        if event.isAccepted() and _visible_flowgrid_shell_window() is None:
            _ensure_shell_window_available(self.app_window)

    def _show_qa_bulk_parts_context_menu(self, pos: QPoint) -> None:
        if not hasattr(self, "qa_bulk_parts_input"):
            return
        menu = self.qa_bulk_parts_input.createStandardContextMenu()
        menu.addSeparator()
        paste_import_action = menu.addAction("Paste Clipboard and Import Delivered Rows")
        chosen = menu.exec(self.qa_bulk_parts_input.mapToGlobal(pos))
        if chosen == paste_import_action and self._append_clipboard_to_qa_bulk_parts():
            self._submit_qa_bulk_parts()
        menu.deleteLater()

    def _append_clipboard_to_qa_bulk_parts(self) -> bool:
        if not hasattr(self, "qa_bulk_parts_input"):
            return False
        clipboard = QApplication.clipboard()
        clipboard_text = str(clipboard.text() or "")
        normalized_clipboard = clipboard_text.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
        if not normalized_clipboard.strip():
            self._show_themed_message(QMessageBox.Icon.Warning, "Paste", "Clipboard is empty.")
            return False
        existing_text = str(self.qa_bulk_parts_input.toPlainText() or "")
        if existing_text.strip():
            merged = f"{existing_text.rstrip()}\n{normalized_clipboard}"
        else:
            merged = normalized_clipboard
        self.qa_bulk_parts_input.setPlainText(merged)
        return True

    def _submit_qa_bulk_parts(self) -> None:
        if self._warn_if_read_only("QA bulk import"):
            return
        if not hasattr(self, "qa_bulk_parts_input"):
            return
        raw_text = str(self.qa_bulk_parts_input.toPlainText() or "")
        if not raw_text.strip():
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Bulk Import",
                "Paste tab-separated rows before importing.",
            )
            return

        form_work_order = DepotRules.normalize_work_order(str(self.qa_work_order.text() or "").strip())
        if not form_work_order:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Bulk Import",
                "Work order required. Set the Work Order field, then import delivered rows.",
            )
            return

        client = self.qa_client_check.isChecked()
        selected_flag = str(self.qa_flag_combo.currentText() if hasattr(self, "qa_flag_combo") else "").strip()
        if selected_flag.lower() == "none":
            selected_flag = ""
        selected_flag = self._confirm_qa_flag_retention(form_work_order, selected_flag)
        if selected_flag is None:
            return
        base_comment = str(self.qa_comments.text() or "").strip()

        parsed_row_order: list[str] = []
        parsed_row_map: dict[str, tuple[int, str, str, str, str]] = {}
        duplicate_rows_in_paste = 0
        skipped_not_delivered = 0
        skipped_missing_lpn = 0
        skipped_bad_format = 0

        for line_no, raw_line in enumerate(raw_text.splitlines(), start=1):
            line = str(raw_line or "").strip()
            if not line:
                continue
            columns = [str(col or "").strip() for col in raw_line.split("\t")]
            if len(columns) < 4:
                columns = [str(col or "").strip() for col in re.split(r"\s{2,}", line) if str(col or "").strip()]
            if len(columns) < 4:
                skipped_bad_format += 1
                continue

            lpn = DepotRules.normalize_work_order(columns[0])
            part_number = str(columns[1] or "").strip()
            part_description = str(columns[2] or "").strip()
            shipping_info = " ".join(str(col or "").strip() for col in columns[3:] if str(col or "").strip())
            shipping_info_normalized = shipping_info.strip().casefold()

            if not lpn:
                skipped_missing_lpn += 1
                continue
            if not re.match(r"^delivered\b", shipping_info_normalized):
                skipped_not_delivered += 1
                continue
            row_key = _part_detail_row_key(lpn, part_number, part_description, shipping_info)
            if not row_key:
                skipped_bad_format += 1
                continue
            if row_key in parsed_row_map:
                duplicate_rows_in_paste += 1
            else:
                parsed_row_order.append(row_key)
            parsed_row_map[row_key] = (line_no, lpn, part_number, part_description, shipping_info)

        parsed_rows: list[tuple[int, str, str, str, str]] = [parsed_row_map[key] for key in parsed_row_order]

        inserted_count = 0
        updated_existing = 0
        failed_count = 0
        existing_active = self.tracker.find_active_parts_by_work_orders([form_work_order])
        submission_context: dict[str, str] | None = None

        if parsed_rows:
            try:
                submission_context = self._resolve_qa_submission_context(form_work_order)
                if submission_context is None:
                    return
                category = str(submission_context.get("category", "") or "").strip()
                assigned_user_id = DepotRules.normalize_user_id(str(submission_context.get("assigned_user_id", "") or ""))
                if assigned_user_id:
                    created_part_id = self.tracker.submit_part_missing_part_order(
                        self.current_user,
                        form_work_order,
                        category,
                        client,
                        assigned_user_id,
                        base_comment,
                        selected_flag,
                        True,
                    )
                else:
                    created_part_id = self.tracker.submit_part(
                        self.current_user,
                        form_work_order,
                        category,
                        client,
                        base_comment,
                        selected_flag,
                        True,
                    )
                for _line_no, lpn, part_number, part_description, shipping_info in parsed_rows:
                    self.tracker.upsert_part_detail(
                        created_part_id,
                        lpn,
                        part_number,
                        part_description,
                        shipping_info,
                        True,
                    )
                if form_work_order in existing_active:
                    updated_existing += 1
                else:
                    inserted_count += 1
            except Exception as exc:
                failed_count += 1
                _runtime_log_event(
                    "ui.qa_bulk_part_submit_failed",
                    severity="warning",
                    summary="QA bulk import failed for one row; continued with remaining rows.",
                    exc=exc,
                    context={
                        "user_id": str(self.current_user),
                        "line_no": int(parsed_rows[0][0]) if parsed_rows else -1,
                        "work_order": str(form_work_order),
                    },
                )

        total_lines = len([line for line in raw_text.splitlines() if str(line or "").strip()])
        status_text = (
            f"Bulk import: inserted {inserted_count}, updated {updated_existing}. "
            f"Not delivered {skipped_not_delivered}, duplicate rows in paste {duplicate_rows_in_paste}, missing LPN {skipped_missing_lpn}, "
            f"bad format {skipped_bad_format}, failed {failed_count}, lines {total_lines}."
        )
        if submission_context is not None and str(submission_context.get("assigned_user_id", "") or "").strip():
            status_text += " Logged for Missing PO."
        if hasattr(self, "qa_bulk_import_status"):
            self.qa_bulk_import_status.setText(status_text)

        if (inserted_count + updated_existing) > 0:
            self.qa_work_order.clear()
            self.qa_comments.clear()
            if hasattr(self, "qa_flag_combo"):
                self.qa_flag_combo.setCurrentIndex(0)
            self._refresh_repair_owner_preview()
            self.qa_work_order.setFocus()
            self._refresh_after_qa_submit()

    def _refresh_recent_submissions_label(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_RECENT_VIEW_TTL_MS,
    ) -> None:
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("qa_recent", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.db.fetchall(
                "SELECT work_order, assigned_user_id, category, client_unit, created_at "
                "FROM parts WHERE user_id=? ORDER BY created_at DESC LIMIT 3",
                (self.current_user,),
            )
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_recent_submissions_query_failed",
                severity="warning",
                summary="Failed querying recent submissions for QA panel; showing unavailable fallback.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.recent_submissions_label.setText("Recent submissions: unavailable")
            return

        if not rows:
            self.recent_submissions_label.setText("Latest 3 submissions:\n1. (none)\n2. (none)\n3. (none)")
            self._mark_depot_view_refreshed(
                "qa_recent",
                state_key,
                payload=[],
                reason=reason,
                duration_ms=(time.monotonic() - started) * 1000.0,
                row_count=0,
            )
            return

        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        lines: list[str] = ["Latest 3 submissions:"]
        for index, row in enumerate(rows, start=1):
            wo = str(row["work_order"] or "").strip()
            repair_owner = str(row["assigned_user_id"] or "").strip()
            category = category_map.get(DepotRules.normalize_work_order(wo), "") or str(row["category"] or "").strip() or "Other"
            client_marker = " \u2713" if int(row["client_unit"] or 0) else ""
            created = str(row["created_at"] or "")
            stamp = created[11:16] if len(created) >= 16 else created
            lines.append(f"{index}. {wo} -> {repair_owner} ({category}{client_marker}) [{stamp}]")

        for index in range(len(rows) + 1, 4):
            lines.append(f"{index}. (none)")

        self.recent_submissions_label.setText("\n".join(lines))
        self._mark_depot_view_refreshed(
            "qa_recent",
            state_key,
            payload=[dict(row) for row in rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _build_qa_assigned_tab(self):
        layout = QVBoxLayout(self.assigned_tab)
        self.qa_assigned_table = QTableWidget()
        configure_standard_table(
            self.qa_assigned_table,
            ["Work Order", "Client", "Flag", "Age", "Working", "Repair Owner", "Category", "QA Note", "Agent Note"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.ResizeToContents,
                8: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.qa_assigned_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.qa_assigned_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_assigned_table.customContextMenuRequested.connect(
            lambda pos: self._open_qa_notes_from_context(self.qa_assigned_table, pos)
        )
        self.qa_assigned_refresh = QPushButton("Refresh")
        self.qa_assigned_refresh.clicked.connect(lambda: self._refresh_assigned_parts(force=True, reason="manual", ttl_ms=0))
        self.qa_assigned_open_notes_btn = QPushButton("Open Notes / Flag")
        self.qa_assigned_open_notes_btn.setProperty("actionRole", "pick")
        self.qa_assigned_open_notes_btn.clicked.connect(
            lambda: self._open_selected_qa_notes_for_table(self.qa_assigned_table)
        )
        self.qa_assigned_workorder_search = QLineEdit()
        self.qa_assigned_workorder_search.setPlaceholderText("Search work order...")
        self.qa_assigned_workorder_search.setClearButtonEnabled(True)
        self.qa_assigned_workorder_search.textChanged.connect(lambda _text: self._qa_assigned_search_timer.start())
        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.qa_assigned_workorder_search, 1)
        controls.addWidget(self.qa_assigned_refresh)
        controls.addWidget(self.qa_assigned_open_notes_btn)
        layout.addLayout(controls)
        layout.addWidget(self.qa_assigned_table)

    def _build_qa_delivered_tab(self):
        layout = QVBoxLayout(self.delivered_tab)
        self.qa_delivered_table = QTableWidget()
        configure_standard_table(
            self.qa_delivered_table,
            [
                "Work Order",
                "Installed",
                "Age",
                "Repair Owner",
                "Category",
                "LPN",
                "Part Description",
                "Part #",
                "Shipping Info",
                "QA Note",
            ],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.ResizeToContents,
                8: QHeaderView.ResizeMode.ResizeToContents,
                9: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.qa_delivered_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.qa_delivered_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_delivered_table.customContextMenuRequested.connect(
            lambda pos: self._open_qa_notes_from_context(self.qa_delivered_table, pos)
        )

        self.qa_delivered_refresh = QPushButton("Refresh")
        self.qa_delivered_refresh.clicked.connect(lambda: self._refresh_delivered_parts(force=True, reason="manual", ttl_ms=0))
        self.qa_delivered_open_notes_btn = QPushButton("Open Notes / Flag")
        self.qa_delivered_open_notes_btn.setProperty("actionRole", "pick")
        self.qa_delivered_open_notes_btn.clicked.connect(
            lambda: self._open_selected_qa_notes_for_table(self.qa_delivered_table)
        )
        self.qa_delivered_export_btn = QPushButton("Export CSV")
        self.qa_delivered_export_btn.clicked.connect(self._export_qa_delivered_csv)
        self.qa_delivered_workorder_search = QLineEdit()
        self.qa_delivered_workorder_search.setPlaceholderText("Search work order...")
        self.qa_delivered_workorder_search.setClearButtonEnabled(True)
        self.qa_delivered_workorder_search.textChanged.connect(lambda _text: self._qa_delivered_search_timer.start())

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.qa_delivered_workorder_search, 1)
        controls.addWidget(self.qa_delivered_refresh)
        controls.addWidget(self.qa_delivered_open_notes_btn)
        controls.addWidget(self.qa_delivered_export_btn)
        layout.addLayout(controls)
        layout.addWidget(self.qa_delivered_table)

    def _build_qa_cat_parts_tab(self) -> None:
        layout = QVBoxLayout(self.qa_cat_parts_tab)
        filter_layout = QHBoxLayout()
        self.qa_cat_filter = QComboBox()
        self._refresh_qa_category_filter_options()
        self.qa_cat_filter.currentTextChanged.connect(lambda _text: self._refresh_qa_category_parts(reason="filter-change", ttl_ms=DEPOT_VIEW_TTL_MS))
        filter_layout.addWidget(QLabel("Category:"))
        filter_layout.addWidget(self.qa_cat_filter, 1)
        self.qa_cat_workorder_search = QLineEdit()
        self.qa_cat_workorder_search.setPlaceholderText("Search work order...")
        self.qa_cat_workorder_search.setClearButtonEnabled(True)
        self.qa_cat_workorder_search.textChanged.connect(lambda _text: self._qa_category_search_timer.start())
        filter_layout.addWidget(QLabel("Work Order:"))
        filter_layout.addWidget(self.qa_cat_workorder_search, 1)
        self.qa_cat_refresh_btn = QPushButton("Refresh")
        self.qa_cat_refresh_btn.clicked.connect(lambda: self._refresh_qa_category_parts(force=True, reason="manual", ttl_ms=0))
        filter_layout.addWidget(self.qa_cat_refresh_btn, 0)
        self.qa_cat_open_notes_btn = QPushButton("Open Notes / Flag")
        self.qa_cat_open_notes_btn.setProperty("actionRole", "pick")
        self.qa_cat_open_notes_btn.clicked.connect(lambda: self._open_selected_qa_notes_for_table(self.qa_cat_parts_table))
        filter_layout.addWidget(self.qa_cat_open_notes_btn, 0)
        layout.addLayout(filter_layout)

        self.qa_cat_parts_table = QTableWidget()
        configure_standard_table(
            self.qa_cat_parts_table,
            ["Work Order", "Client", "Flag", "Age", "Working", "Installed", "Agent", "Category", "QA Note"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
                6: QHeaderView.ResizeMode.ResizeToContents,
                7: QHeaderView.ResizeMode.ResizeToContents,
                8: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.qa_cat_parts_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qa_cat_parts_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.qa_cat_parts_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_cat_parts_table.customContextMenuRequested.connect(
            lambda pos: self._open_qa_notes_from_context(self.qa_cat_parts_table, pos)
        )
        layout.addWidget(self.qa_cat_parts_table)

    def _refresh_qa_category_filter_options(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_cat_filter"):
            return
        previous = self.qa_cat_filter.currentText().strip() or "All"
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("qa_category_filter_options", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        categories: list[str] = ["All"]
        started = time.monotonic()
        try:
            categories.extend(self.tracker.active_part_category_options())
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_category_filter_query_failed",
                severity="warning",
                summary="Failed loading QA category filter options; continuing with defaults.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
        self.qa_cat_filter.blockSignals(True)
        self.qa_cat_filter.clear()
        self.qa_cat_filter.addItems(categories)
        if previous in categories:
            self.qa_cat_filter.setCurrentText(previous)
        else:
            self.qa_cat_filter.setCurrentIndex(0)
        self.qa_cat_filter.blockSignals(False)
        self._mark_depot_view_refreshed(
            "qa_category_filter_options",
            state_key,
            payload=list(categories),
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(categories),
        )

    def _refresh_qa_category_parts(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_cat_parts_table"):
            return
        self._refresh_qa_category_filter_options(force=force, reason=reason, ttl_ms=ttl_ms)
        search_text = str(self.qa_cat_workorder_search.text() or "").strip() if hasattr(self, "qa_cat_workorder_search") else ""
        category_filter = str(self.qa_cat_filter.currentText() or "").strip() if hasattr(self, "qa_cat_filter") else "All"
        state_key = {"search": search_text, "category": category_filter}
        if not self._should_refresh_depot_view("qa_category", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        agent_meta = self._qa_agent_meta_lookup()
        try:
            rows = self.tracker.list_category_active_parts(search_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_category_parts_refresh_failed",
                severity="warning",
                summary="Failed loading QA category parts.",
                exc=exc,
                context={"user_id": str(self.current_user), "search_text": str(search_text), "category": str(category_filter)},
            )
            self.qa_cat_parts_table.setRowCount(0)
            return
        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        self.qa_cat_parts_table.setRowCount(0)
        row_idx = 0
        for r in rows:
            work_order = str(r["work_order"] or "").strip()
            category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
            if category_filter and category_filter != "All" and category != category_filter:
                continue
            self.qa_cat_parts_table.insertRow(row_idx)
            part_id = int(r["id"])
            assigned = DepotRules.normalize_user_id(str(r["assigned_user_id"] or ""))
            assigned_name, assigned_icon = agent_meta.get(assigned, ("", ""))
            age_text = part_age_label(str(r["created_at"] or ""))
            qa_comment = str(r["qa_comment"] or r["comments"] or "").strip()
            agent_comment = str(r["agent_comment"] or "").strip()
            flag = str(r["qa_flag"] or "").strip()
            working_user = DepotRules.normalize_user_id(str(r["working_user_id"] or ""))
            working_stamp = str(r["working_updated_at"] or "").strip()
            parts_installed = bool(int(r["parts_installed"] or 0))
            parts_installed_by = DepotRules.normalize_user_id(str(r["parts_installed_by"] or ""))
            parts_installed_at = str(r["parts_installed_at"] or "").strip()
            image_abs = self.tracker.resolve_qa_flag_icon(
                str(r["qa_flag"] or "").strip(),
                str(r["qa_flag_image_path"] or ""),
            )

            work_item = _center_table_item(QTableWidgetItem(work_order))
            work_item.setData(Qt.ItemDataRole.UserRole, part_id)
            client_item = _center_table_item(QTableWidgetItem(""))
            client_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if int(r["client_unit"] or 0):
                client_item.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton))
            flag_item = _center_table_item(QTableWidgetItem("" if image_abs else (flag if flag else "")))
            flag_item.setData(Qt.ItemDataRole.UserRole, part_id)
            flag_item.setToolTip(DepotAgentWindow._flag_tooltip(flag, qa_comment, agent_comment, bool(image_abs)))
            if image_abs:
                flag_item.setIcon(QIcon(image_abs))
            working_item = _center_table_item(QTableWidgetItem("\U0001F527" if working_user else ""))
            if working_user:
                working_tip = f"Agent working this unit: {working_user}"
                friendly_stamp = format_working_updated_stamp(working_stamp)
                if friendly_stamp:
                    working_tip += f"\nUpdated: {friendly_stamp}"
                working_item.setToolTip(working_tip)
            installed_item = _center_table_item(QTableWidgetItem(""))
            if parts_installed:
                installed_icon = QIcon.fromTheme("applications-engineering")
                if installed_icon.isNull():
                    installed_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
                installed_item.setIcon(installed_icon)
                tip = "Parts installed."
                if parts_installed_by:
                    tip += f"\nBy: {parts_installed_by}"
                friendly_stamp = format_working_updated_stamp(parts_installed_at)
                if friendly_stamp:
                    tip += f"\nAt: {friendly_stamp}"
                installed_item.setToolTip(tip)
            assigned_text = f"{assigned} - {assigned_name}" if assigned and assigned_name else (assigned or "-")
            assigned_item = _center_table_item(QTableWidgetItem(assigned_text))
            if assigned_icon and Path(assigned_icon).exists():
                assigned_item.setIcon(QIcon(assigned_icon))
            category_item = _center_table_item(QTableWidgetItem(category))
            age_item = _center_table_item(QTableWidgetItem(age_text))
            note_item = _center_table_item(QTableWidgetItem(note_preview(qa_comment)))
            note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")

            self.qa_cat_parts_table.setItem(row_idx, 0, work_item)
            self.qa_cat_parts_table.setItem(row_idx, 1, client_item)
            self.qa_cat_parts_table.setItem(row_idx, 2, flag_item)
            self.qa_cat_parts_table.setItem(row_idx, 3, age_item)
            self.qa_cat_parts_table.setItem(row_idx, 4, working_item)
            self.qa_cat_parts_table.setItem(row_idx, 5, installed_item)
            self.qa_cat_parts_table.setItem(row_idx, 6, assigned_item)
            self.qa_cat_parts_table.setItem(row_idx, 7, category_item)
            self.qa_cat_parts_table.setItem(row_idx, 8, note_item)
            row_idx += 1
        self._mark_depot_view_refreshed(
            "qa_category",
            state_key,
            payload={"row_count": row_idx},
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=row_idx,
        )

    def _build_qa_client_followup_tab(self) -> None:
        layout = QVBoxLayout(self.qa_client_tab)
        summary = QLabel("Client follow-up queue for all agents (Client + Other, submitted).")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Follow-up Queue"), 0)
        controls.addStretch(1)
        self.qa_client_refresh_btn = QPushButton("Refresh")
        self.qa_client_refresh_btn.clicked.connect(lambda: self._refresh_qa_client_followup(force=True, reason="manual", ttl_ms=0))
        controls.addWidget(self.qa_client_refresh_btn, 0)
        layout.addLayout(controls)

        self.qa_client_due_summary = QLabel("No follow-up alerts.")
        self.qa_client_due_summary.setProperty("section", True)
        layout.addWidget(self.qa_client_due_summary)

        self.qa_client_followup_table = QTableWidget()
        configure_standard_table(
            self.qa_client_followup_table,
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
        self.qa_client_followup_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qa_client_followup_table.cellClicked.connect(self._on_qa_client_followup_cell_clicked)
        self.qa_client_followup_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        layout.addWidget(self.qa_client_followup_table, 1)
        self._qa_client_due_items = []
        self._qa_client_due_active_ids = set()
        self._qa_client_due_ack_ids = set()
        self._qa_client_due_flash_on = True
        self._qa_client_due_flash_timer = QTimer(self)
        self._qa_client_due_flash_timer.setInterval(700)
        self._qa_client_due_flash_timer.timeout.connect(self._on_qa_client_due_flash_tick)
        self._qa_client_due_flash_timer.start()
    def _refresh_qa_client_followup(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_client_followup_table"):
            return
        state_key = {"user_id": self.current_user}
        if not self._should_refresh_depot_view("qa_client_followup", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_team_client_followups()
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_client_followup_query_failed",
                severity="warning",
                summary="Failed loading QA client follow-up rows.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.qa_client_followup_table.setRowCount(0)
            self.qa_client_due_summary.setText("Client follow-up unavailable. Details were logged.")
            self._qa_client_due_items = []
            self._qa_client_due_active_ids = set()
            self._qa_client_due_ack_ids = set()
            self._update_qa_tab_alert_states()
            return

        today = datetime.now().date()
        today_iso = today.isoformat()
        self.qa_client_followup_table.setRowCount(0)
        previous_active_ids = set(getattr(self, "_qa_client_due_active_ids", set()))
        due_items: list[QTableWidgetItem] = []
        due_count = 0
        due_quiet_count = 0
        due_active_ids: set[int] = set()
        kept_row = 0
        for r in rows:
            client_part_id = int(r["id"])
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
            action_date = parse_iso_date(last_action_at)
            days_since_action = max(0, (today - action_date).days) if action_date is not None else -1
            last_update = str(r["latest_touch_date"] or "").strip() or str(r["created_at"] or "")[:10]
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

            has_action = bool(last_action)
            due_active = bool(due and not quiet_active and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED)

            self.qa_client_followup_table.insertRow(kept_row)
            agent_item = _center_table_item(QTableWidgetItem(user_id if user_id else "-"))
            due_item = _center_table_item(QTableWidgetItem(""))
            due_item.setData(Qt.ItemDataRole.UserRole, client_part_id)
            due_item.setData(Qt.ItemDataRole.UserRole + 1, 1 if due else 0)
            due_item.setData(Qt.ItemDataRole.UserRole + 2, 1 if has_action else 0)
            due_item.setData(Qt.ItemDataRole.UserRole + 3, 1 if quiet_active else 0)
            if due_active:
                if has_action and last_action != DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED:
                    clock_icon, stage_text = DepotAgentWindow._followup_wait_icon_by_days(self, days_since_action)
                    due_item.setText(stage_text)
                    due_item.setIcon(clock_icon)
                else:
                    due_item.setText(DepotRules.followup_stage_label(0))
                    due_item.setIcon(DepotAgentWindow._followup_clock_icon(self, "#21B46D"))
            elif due and has_action:
                if last_action == DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED:
                    due_item.setIcon(DepotAgentWindow._followup_done_icon(self))
                else:
                    clock_icon, stage_text = DepotAgentWindow._followup_wait_icon_by_days(self, days_since_action)
                    due_item.setText(stage_text)
                    due_item.setIcon(clock_icon)
            quiet_line = f"\nAlert quiet until: {format_working_updated_stamp(alert_quiet_until)}" if quiet_active else ""
            action_line = f"\nLast follow-up: {last_action}" if last_action else ""
            actor_line = f" by {last_action_actor}" if last_action_actor else ""
            when_line = f" ({format_working_updated_stamp(last_action_at)})" if last_action_at else ""
            attempt_line = f"\nNo-contact follow-ups: {no_contact_count}" if no_contact_count > 0 else ""
            due_item.setToolTip(f"{due_reason}{action_line}{actor_line}{when_line}{attempt_line}{quiet_line}")

            work_item = _center_table_item(QTableWidgetItem(work_order))
            status_item = _center_table_item(QTableWidgetItem(latest_touch))
            update_item = _center_table_item(QTableWidgetItem(last_update if last_update else "-"))
            age_item = _center_table_item(QTableWidgetItem(age_text))
            note_item = _center_table_item(QTableWidgetItem(note_preview(notes)))
            note_item.setToolTip(f"Comments: {notes if notes else '(none)'}")

            self.qa_client_followup_table.setItem(kept_row, 0, agent_item)
            self.qa_client_followup_table.setItem(kept_row, 1, due_item)
            self.qa_client_followup_table.setItem(kept_row, 2, work_item)
            self.qa_client_followup_table.setItem(kept_row, 3, status_item)
            self.qa_client_followup_table.setItem(kept_row, 4, update_item)
            self.qa_client_followup_table.setItem(kept_row, 5, age_item)
            self.qa_client_followup_table.setItem(kept_row, 6, note_item)
            if due_active:
                due_active_ids.add(client_part_id)
                if int(client_part_id) not in self._qa_client_due_ack_ids:
                    due_items.append(due_item)
                due_count += 1
            elif due and quiet_active:
                due_quiet_count += 1
            kept_row += 1

        self._qa_client_due_ack_ids.intersection_update(due_active_ids)
        self._qa_client_due_active_ids = due_active_ids
        if due_active_ids.difference(previous_active_ids):
            self._qa_tab_alert_ack_states["client"] = False
        self._qa_client_due_items = due_items
        self._qa_client_due_flash_on = True
        self._apply_qa_client_due_flash_visuals()
        self._update_qa_tab_alert_states()
        if due_count > 0:
            suffix = f" | Quieted: {due_quiet_count}" if due_quiet_count > 0 else ""
            self.qa_client_due_summary.setText(f"Follow-up due now: {due_count}{suffix}")
        elif due_quiet_count > 0:
            self.qa_client_due_summary.setText(f"Follow-up alerts quieted until tomorrow morning: {due_quiet_count}")
        else:
            self.qa_client_due_summary.setText("No follow-up alerts.")
        self._mark_depot_view_refreshed(
            "qa_client_followup",
            state_key,
            payload={"row_count": kept_row, "due_count": due_count},
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=kept_row,
        )

    def _on_qa_client_followup_cell_clicked(self, row: int, col: int) -> None:
        if col != 1:
            return
        if row < 0 or row >= int(self.qa_client_followup_table.rowCount()):
            return
        due_item = self.qa_client_followup_table.item(row, 1)
        work_item = self.qa_client_followup_table.item(row, 2)
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
        if is_due_row and not quiet_active and client_part_id in self._qa_client_due_active_ids:
            self._qa_client_due_ack_ids.add(int(client_part_id))
            due_item.setBackground(QColor(0, 0, 0, 0))
            self._qa_client_due_items = [
                item
                for item in self._qa_client_due_items
                if item is not None and safe_int(item.data(Qt.ItemDataRole.UserRole), 0) != int(client_part_id)
            ]
            self._apply_qa_client_due_flash_visuals()
            self._update_qa_tab_alert_states()
        work_order = str(work_item.text() or "").strip() or "(unknown)"
        action, ok = show_flowgrid_themed_input_item(
            self,
            self.app_window,
            "qa",
            "Client Follow-up",
            f"Work Order: {work_order}\nSelect follow-up outcome:",
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
                "ui.qa_client_followup_mark_failed",
                severity="warning",
                summary="Failed recording QA client follow-up action.",
                exc=exc,
                context={"user_id": str(self.current_user), "client_part_id": int(client_part_id), "work_order": str(work_order), "action": str(action_text)},
            )
            self._show_themed_message(QMessageBox.Icon.Warning, "Follow-up", f"Could not save follow-up update:\n{type(exc).__name__}: {exc}")
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_client_followup",
                "agent_team_client_followup",
                "qa_client_followup",
                "dashboard_notes",
                reason="qa_client_followup_action",
            )
        else:
            self._refresh_qa_client_followup(force=True, reason="qa_client_followup_action", ttl_ms=0)
        if action_text in DepotRules.CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS and int(no_contact_count) == 3:
            self._show_themed_message(QMessageBox.Icon.Warning, "No Contact Alert", "Please ship unit back to store due to no contact from client.")

    def _apply_qa_client_due_flash_visuals(self) -> None:
        if not hasattr(self, "_qa_client_due_items"):
            return
        if not self._qa_client_due_items:
            if hasattr(self, "qa_client_followup_table"):
                for row_idx in range(int(self.qa_client_followup_table.rowCount())):
                    item = self.qa_client_followup_table.item(row_idx, 1)
                    if item is not None:
                        item.setBackground(QColor(0, 0, 0, 0))
            return
        on_color = QColor("#D95A5A")
        on_color.setAlpha(105)
        off_color = QColor(0, 0, 0, 0)
        for item in list(self._qa_client_due_items):
            if item is None:
                continue
            item.setBackground(on_color if self._qa_client_due_flash_on else off_color)

    def _on_qa_client_due_flash_tick(self) -> None:
        if not hasattr(self, "_qa_client_due_items"):
            return
        if not self._qa_client_due_items:
            self._qa_client_due_flash_on = True
            return
        self._qa_client_due_flash_on = not bool(getattr(self, "_qa_client_due_flash_on", False))
        self._apply_qa_client_due_flash_visuals()

    def _build_qa_rtv_tab(self) -> None:
        layout = QVBoxLayout(self.qa_rtv_tab)
        summary = QLabel("RTV queue mirrored from the Agent Tech 3 view.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        self.qa_rtv_table = QTableWidget()
        configure_standard_table(
            self.qa_rtv_table,
            ["Work Order", "Logged At", "Logged By", "Comments"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.qa_rtv_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qa_rtv_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.qa_rtv_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_rtv_table.customContextMenuRequested.connect(
            lambda pos: self._open_qa_rtv_comment_from_context(pos)
        )

        self.qa_rtv_refresh_btn = QPushButton("Refresh")
        self.qa_rtv_refresh_btn.clicked.connect(lambda: self._refresh_qa_rtv_rows(force=True, reason="manual", ttl_ms=0))
        self.qa_rtv_open_notes_btn = QPushButton("Open Notes / Update Comment")
        self.qa_rtv_open_notes_btn.setProperty("actionRole", "pick")
        self.qa_rtv_open_notes_btn.clicked.connect(self._open_selected_qa_rtv_comment)
        self.qa_rtv_workorder_search = QLineEdit()
        self.qa_rtv_workorder_search.setPlaceholderText("Search work order...")
        self.qa_rtv_workorder_search.setClearButtonEnabled(True)
        self.qa_rtv_workorder_search.textChanged.connect(lambda _text: self._qa_rtv_search_timer.start())

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.qa_rtv_workorder_search, 1)
        controls.addWidget(self.qa_rtv_refresh_btn)
        controls.addWidget(self.qa_rtv_open_notes_btn)
        layout.addLayout(controls)
        layout.addWidget(self.qa_rtv_table)

    def _refresh_qa_rtv_rows(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_rtv_table"):
            return
        search_text = str(self.qa_rtv_workorder_search.text() or "").strip() if hasattr(self, "qa_rtv_workorder_search") else ""
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("qa_rtv", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_rtv_rows(search_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_rtv_refresh_failed",
                severity="warning",
                summary="Failed loading QA RTV queue.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self.qa_rtv_table.setRowCount(0)
            return
        self.qa_rtv_table.setRowCount(0)
        for row_idx, row in enumerate(rows):
            self.qa_rtv_table.insertRow(row_idx)
            work_item = _center_table_item(QTableWidgetItem(str(row["work_order"] or "").strip()))
            work_item.setData(Qt.ItemDataRole.UserRole, int(row["id"] or 0))
            logged_item = _center_table_item(QTableWidgetItem(format_working_updated_stamp(str(row["created_at"] or "").strip()) or "-"))
            logged_item.setToolTip(str(row["created_at"] or "").strip())
            user_item = _center_table_item(QTableWidgetItem(DepotRules.normalize_user_id(str(row["user_id"] or "")) or "-"))
            comment_text = str(row["comments"] or "").strip()
            comment_item = _center_table_item(QTableWidgetItem(note_preview(comment_text)))
            comment_item.setData(Qt.ItemDataRole.UserRole + 1, comment_text)
            comment_item.setToolTip(f"Comments: {comment_text if comment_text else '(none)'}")
            self.qa_rtv_table.setItem(row_idx, 0, work_item)
            self.qa_rtv_table.setItem(row_idx, 1, logged_item)
            self.qa_rtv_table.setItem(row_idx, 2, user_item)
            self.qa_rtv_table.setItem(row_idx, 3, comment_item)
        self._mark_depot_view_refreshed(
            "qa_rtv",
            state_key,
            payload=[dict(row) for row in rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _open_qa_rtv_comment_from_context(self, pos: QPoint) -> None:
        if not hasattr(self, "qa_rtv_table"):
            return
        if not _select_table_row_by_context_pos(self.qa_rtv_table, pos):
            return
        self._open_selected_qa_rtv_comment()

    def _open_selected_qa_rtv_comment(self) -> None:
        if self._warn_if_read_only("RTV comment updates"):
            return
        if not hasattr(self, "qa_rtv_table"):
            return
        if not _edit_aux_queue_comment(
            self,
            self.tracker,
            table=self.qa_rtv_table,
            target_key="rtvs.comments",
            theme_kind="qa",
            title="RTV Comment",
        ):
            return
        self._refresh_qa_rtv_rows(force=True, reason="qa_rtv_comment", ttl_ms=0)
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views("qa_rtv", "agent_rtv", "dashboard_notes", reason="qa_rtv_comment")

    def _build_qa_client_jo_tab(self) -> None:
        layout = QVBoxLayout(self.client_jo_tab)
        summary = QLabel("Junk Out review queue with serial numbers and client filters.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        self.qa_client_jo_table = QTableWidget()
        configure_standard_table(
            self.qa_client_jo_table,
            ["Work Order", "Client", "SN", "Agent/User", "Logged At", "Category", "Notes"],
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
        self.qa_client_jo_table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qa_client_jo_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))

        self.qa_client_jo_refresh = QPushButton("Refresh")
        self.qa_client_jo_refresh.clicked.connect(lambda: self._refresh_qa_client_jo_rows(force=True, reason="manual", ttl_ms=0))
        self.qa_client_jo_workorder_search = QLineEdit()
        self.qa_client_jo_workorder_search.setPlaceholderText("Search work order...")
        self.qa_client_jo_workorder_search.setClearButtonEnabled(True)
        self.qa_client_jo_workorder_search.textChanged.connect(lambda _text: self._qa_client_jo_search_timer.start())
        self.qa_client_jo_client_filter = QComboBox()
        self.qa_client_jo_client_filter.addItem("All", "all")
        self.qa_client_jo_client_filter.addItem("Client", "client")
        self.qa_client_jo_client_filter.addItem("Non-client", "non_client")
        self.qa_client_jo_client_filter.currentIndexChanged.connect(
            lambda _index: self._refresh_qa_client_jo_rows(reason="filter-change", ttl_ms=DEPOT_VIEW_TTL_MS)
        )
        self.qa_client_jo_start_date = QDateEdit()
        self.qa_client_jo_start_date.setCalendarPopup(True)
        self.qa_client_jo_start_date.setDisplayFormat("yyyy-MM-dd")
        self.qa_client_jo_start_date.setDate(QDate.currentDate().addDays(-30))
        self.qa_client_jo_start_date.dateChanged.connect(
            lambda _date: self._refresh_qa_client_jo_rows(reason="date-filter", ttl_ms=DEPOT_VIEW_TTL_MS)
        )
        self.qa_client_jo_end_date = QDateEdit()
        self.qa_client_jo_end_date.setCalendarPopup(True)
        self.qa_client_jo_end_date.setDisplayFormat("yyyy-MM-dd")
        self.qa_client_jo_end_date.setDate(QDate.currentDate())
        self.qa_client_jo_end_date.dateChanged.connect(
            lambda _date: self._refresh_qa_client_jo_rows(reason="date-filter", ttl_ms=DEPOT_VIEW_TTL_MS)
        )

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.qa_client_jo_workorder_search, 1)
        controls.addWidget(QLabel("Client:"))
        controls.addWidget(self.qa_client_jo_client_filter, 0)
        controls.addWidget(QLabel("From:"))
        controls.addWidget(self.qa_client_jo_start_date, 0)
        controls.addWidget(QLabel("To:"))
        controls.addWidget(self.qa_client_jo_end_date, 0)
        controls.addWidget(self.qa_client_jo_refresh)
        layout.addLayout(controls)
        layout.addWidget(self.qa_client_jo_table)

    def _refresh_qa_client_jo_rows(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_client_jo_table"):
            return
        search_text = str(self.qa_client_jo_workorder_search.text() or "").strip() if hasattr(self, "qa_client_jo_workorder_search") else ""
        client_filter = (
            str(self.qa_client_jo_client_filter.currentData() or "all")
            if hasattr(self, "qa_client_jo_client_filter")
            else "all"
        )
        start_date = (
            self.qa_client_jo_start_date.date().toString("yyyy-MM-dd")
            if hasattr(self, "qa_client_jo_start_date")
            else ""
        )
        end_date = (
            self.qa_client_jo_end_date.date().toString("yyyy-MM-dd")
            if hasattr(self, "qa_client_jo_end_date")
            else ""
        )
        state_key = {"search": search_text, "client_filter": client_filter, "start_date": start_date, "end_date": end_date}
        if not self._should_refresh_depot_view("qa_client_jo", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_junk_out_rows(
                search_text,
                client_filter=client_filter,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_client_jo_refresh_failed",
                severity="warning",
                summary="Failed loading QA JO queue.",
                exc=exc,
                context={"user_id": str(self.current_user), "state_key": state_key},
            )
            self.qa_client_jo_table.setRowCount(0)
            return
        self.qa_client_jo_table.setRowCount(0)
        for row_idx, row in enumerate(rows):
            self.qa_client_jo_table.insertRow(row_idx)
            work_item = _center_table_item(QTableWidgetItem(str(row["work_order"] or "").strip()))
            work_item.setData(Qt.ItemDataRole.UserRole, int(row["id"] or 0))
            client_item = _center_table_item(QTableWidgetItem("Yes" if int(max(0, safe_int(row["client_unit"], 0))) else "No"))
            serial_item = _center_table_item(QTableWidgetItem(str(row["serial_number"] or "").strip() or "-"))
            user_item = _center_table_item(QTableWidgetItem(DepotRules.normalize_user_id(str(row["user_id"] or "")) or "-"))
            logged_raw = str(row["updated_at"] or "").strip() or str(row["created_at"] or "").strip()
            logged_item = _center_table_item(QTableWidgetItem(format_working_updated_stamp(logged_raw) or "-"))
            logged_item.setToolTip(logged_raw)
            category_item = _center_table_item(QTableWidgetItem(str(row["category"] or "").strip() or "-"))
            comment_text = str(row["comments"] or "").strip()
            comment_item = _center_table_item(QTableWidgetItem(note_preview(comment_text)))
            comment_item.setData(Qt.ItemDataRole.UserRole + 1, comment_text)
            comment_item.setToolTip(f"Notes: {comment_text if comment_text else '(none)'}")
            self.qa_client_jo_table.setItem(row_idx, 0, work_item)
            self.qa_client_jo_table.setItem(row_idx, 1, client_item)
            self.qa_client_jo_table.setItem(row_idx, 2, serial_item)
            self.qa_client_jo_table.setItem(row_idx, 3, user_item)
            self.qa_client_jo_table.setItem(row_idx, 4, logged_item)
            self.qa_client_jo_table.setItem(row_idx, 5, category_item)
            self.qa_client_jo_table.setItem(row_idx, 6, comment_item)
        self._mark_depot_view_refreshed(
            "qa_client_jo",
            state_key,
            payload=[dict(row) for row in rows],
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _open_qa_client_jo_comment_from_context(self, pos: QPoint) -> None:
        if not hasattr(self, "qa_client_jo_table"):
            return
        if not _select_table_row_by_context_pos(self.qa_client_jo_table, pos):
            return
        self._open_selected_qa_client_jo_comment()

    def _open_selected_qa_client_jo_comment(self) -> None:
        if self._warn_if_read_only("Client JO comment updates"):
            return
        if not hasattr(self, "qa_client_jo_table"):
            return
        if not _edit_aux_queue_comment(
            self,
            self.tracker,
            table=self.qa_client_jo_table,
            target_key="client_jo.comments",
            theme_kind="qa",
            title="Client JO Comment",
        ):
            return
        self._refresh_qa_client_jo_rows(force=True, reason="qa_client_jo_comment", ttl_ms=0)
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views("qa_client_jo", "dashboard_notes", reason="qa_client_jo_comment")

    def _build_qa_missing_po_followup_tab(self):
        layout = QVBoxLayout(self.missing_po_followup_tab)
        summary = QLabel("Missing PO rows waiting for Part Order follow up.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        self.qa_missing_po_summary = QLabel("No Missing PO rows.")
        self.qa_missing_po_summary.setProperty("section", True)
        layout.addWidget(self.qa_missing_po_summary)

        self.qa_missing_po_table = QTableWidget()
        configure_standard_table(
            self.qa_missing_po_table,
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
        self.qa_missing_po_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.qa_missing_po_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.qa_missing_po_table.customContextMenuRequested.connect(
            lambda pos: self._open_qa_notes_from_context(self.qa_missing_po_table, pos)
        )

        self.qa_missing_po_refresh = QPushButton("Refresh")
        self.qa_missing_po_refresh.clicked.connect(lambda: self._refresh_missing_po_followups(force=True, reason="manual", ttl_ms=0))
        self.qa_missing_po_reassign_btn = QPushButton("Reassign Agent")
        self.qa_missing_po_reassign_btn.setProperty("actionRole", "pick")
        self.qa_missing_po_reassign_btn.clicked.connect(self._reassign_selected_missing_po_followup)
        self.qa_missing_po_resolve_btn = QPushButton("Resolve")
        self.qa_missing_po_resolve_btn.clicked.connect(self._resolve_selected_missing_po_followup)
        self.qa_missing_po_open_notes_btn = QPushButton("Open Notes / Flag")
        self.qa_missing_po_open_notes_btn.setProperty("actionRole", "pick")
        self.qa_missing_po_open_notes_btn.clicked.connect(
            lambda: self._open_selected_qa_notes_for_table(self.qa_missing_po_table)
        )
        self.qa_missing_po_workorder_search = QLineEdit()
        self.qa_missing_po_workorder_search.setPlaceholderText("Search work order...")
        self.qa_missing_po_workorder_search.setClearButtonEnabled(True)
        self.qa_missing_po_workorder_search.textChanged.connect(lambda _text: self._qa_missing_po_search_timer.start())

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Work Order:"))
        controls.addWidget(self.qa_missing_po_workorder_search, 1)
        controls.addWidget(self.qa_missing_po_refresh)
        controls.addWidget(self.qa_missing_po_reassign_btn)
        controls.addWidget(self.qa_missing_po_resolve_btn)
        controls.addWidget(self.qa_missing_po_open_notes_btn)
        layout.addLayout(controls)
        layout.addWidget(self.qa_missing_po_table)

    def _refresh_missing_po_followups(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ) -> None:
        if not hasattr(self, "qa_missing_po_table"):
            return

        search_text = ""
        if hasattr(self, "qa_missing_po_workorder_search"):
            search_text = str(self.qa_missing_po_workorder_search.text() or "").strip().casefold()
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("qa_missing_po", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return

        previous_ids = set(getattr(self, "_qa_missing_po_followup_ids", set()))
        started = time.monotonic()
        try:
            all_rows = self.tracker.list_missing_part_order_followups()
        except Exception as exc:
            self.qa_missing_po_table.setRowCount(0)
            self._qa_missing_po_followup_ids = set()
            if hasattr(self, "qa_missing_po_summary"):
                self.qa_missing_po_summary.setText("Missing PO unavailable. Details were logged.")
            _runtime_log_event(
                "ui.qa_missing_part_order_followups_refresh_failed",
                severity="warning",
                summary="QA Missing PO tab failed to refresh.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
            self._update_qa_tab_alert_states()
            return

        rows = list(all_rows)
        if search_text:
            rows = [
                row
                for row in all_rows
                if search_text in str(row.get("work_order", "") or "").strip().casefold()
            ]

        current_ids = {int(row.get("id", 0) or 0) for row in all_rows if int(row.get("id", 0) or 0) > 0}
        self._qa_missing_po_followup_ids = current_ids
        if current_ids.difference(previous_ids) and "missing_po" in self._qa_tab_alert_ack_states:
            self._qa_tab_alert_ack_states["missing_po"] = False

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
            self.qa_missing_po_table,
            rows=rows,
            all_rows_count=len(all_rows),
            search_text=search_text,
            summary_label=getattr(self, "qa_missing_po_summary", None),
            agent_meta=self._qa_agent_meta_lookup(),
            icon_host=self,
        )
        self._update_qa_tab_alert_states()
        self._mark_depot_view_refreshed(
            "qa_missing_po",
            state_key,
            payload=list(rows),
            reason=reason,
            duration_ms=(time.monotonic() - started) * 1000.0,
            row_count=len(rows),
        )

    def _refresh_after_qa_followup_action(self) -> None:
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "agent_client_followup",
                "agent_team_client_followup",
                "qa_client_followup",
                "dashboard_notes",
                reason="qa_followup_action",
            )
            return
        self._refresh_qa_client_followup(force=True, reason="qa_followup_action", ttl_ms=0)

    def _reassign_selected_missing_po_followup(self) -> None:
        if self._warn_if_read_only("Missing PO reassignment"):
            return
        if not hasattr(self, "qa_missing_po_table"):
            return
        _reassign_missing_po_followup(
            self,
            self.tracker,
            table=self.qa_missing_po_table,
            current_user=self.current_user,
            role_key="qa",
            refresh_callback=self._refresh_after_qa_followup_action,
        )

    def _resolve_selected_missing_po_followup(self) -> None:
        if self._warn_if_read_only("Missing PO resolution"):
            return
        if not hasattr(self, "qa_missing_po_table"):
            return
        _resolve_missing_po_followup(
            self,
            self.tracker,
            table=self.qa_missing_po_table,
            current_user=self.current_user,
            role_key="qa",
            refresh_callback=self._refresh_after_qa_followup_action,
        )

    def _open_qa_notes_from_context(self, table: QTableWidget, pos: QPoint) -> None:
        if not _select_table_row_by_context_pos(table, pos):
            return
        self._open_selected_qa_notes_for_table(table)

    def _open_selected_qa_notes_for_table(self, table: QTableWidget) -> None:
        if self._warn_if_read_only("Notes and flag updates"):
            return
        changed, _part_id = _edit_part_notes(self, self.tracker, role="qa", table=table)
        if not changed:
            return
        if self.app_window is not None:
            self.app_window._refresh_shared_linked_views(
                "qa_assigned",
                "qa_delivered",
                "qa_category",
                "qa_missing_po",
                "agent_parts",
                "agent_category",
                "agent_missing_po",
                reason="qa_part_notes",
            )
            return
        self._refresh_assigned_parts(force=True, reason="qa_part_notes", ttl_ms=0)
        self._refresh_delivered_parts(force=True, reason="qa_part_notes", ttl_ms=0)
        self._refresh_qa_category_parts(force=True, reason="qa_part_notes", ttl_ms=0)
        self._refresh_missing_po_followups(force=True, reason="qa_part_notes", ttl_ms=0)

    def _qa_agent_meta_lookup(self) -> dict[str, tuple[str, str]]:
        try:
            repository = getattr(self.tracker, "user_repository", None)
            if repository is not None:
                return repository.agent_display_map()
            return self.tracker.agent_display_map()
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_agent_meta_query_failed",
                severity="warning",
                summary="QA tabs could not resolve agent metadata.",
                exc=exc,
                context={"user_id": str(self.current_user)},
            )
        return {}

    def _refresh_assigned_parts(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ):
        search_text = ""
        if hasattr(self, "qa_assigned_workorder_search"):
            search_text = str(self.qa_assigned_workorder_search.text() or "").strip()
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("qa_assigned", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_qa_assigned_parts(search_text)
            agent_meta = self._qa_agent_meta_lookup()
            fallback_map = {
                DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
                for row in rows
                if DepotRules.normalize_work_order(str(row["work_order"] or ""))
            }
            category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)

            self.qa_assigned_table.setRowCount(0)
            for row_idx, r in enumerate(rows):
                self.qa_assigned_table.insertRow(row_idx)
                part_id = int(r["id"])
                work_order = str(r["work_order"] or "").strip()
                assigned = DepotRules.normalize_user_id(str(r["assigned_user_id"] or ""))
                category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
                age_text = part_age_label(str(r["created_at"] or ""))
                qa_comment = str(r["qa_comment"] or r["comments"] or "").strip()
                agent_comment = str(r["agent_comment"] or "").strip()
                flag = str(r["qa_flag"] or "").strip()
                working_user = DepotRules.normalize_user_id(str(r["working_user_id"] or ""))
                working_stamp = str(r["working_updated_at"] or "").strip()
                image_abs = self.tracker.resolve_qa_flag_icon(
                    str(r["qa_flag"] or "").strip(),
                    str(r["qa_flag_image_path"] or ""),
                )
                assigned_name, assigned_icon = agent_meta.get(assigned, ("", ""))

                client_item = QTableWidgetItem("")
                client_item.setData(Qt.ItemDataRole.UserRole, part_id)
                if int(r["client_unit"] or 0):
                    client_item.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton))

                flag_item = QTableWidgetItem("" if image_abs else (flag if flag else ""))
                flag_item.setData(Qt.ItemDataRole.UserRole, part_id)
                flag_item.setToolTip(DepotAgentWindow._flag_tooltip(flag, qa_comment, agent_comment, bool(image_abs)))
                if image_abs:
                    flag_item.setIcon(QIcon(image_abs))

                working_item = QTableWidgetItem("\U0001F527" if working_user else "")
                working_item.setData(Qt.ItemDataRole.UserRole, part_id)
                if working_user:
                    working_tip = f"Agent working this unit: {working_user}"
                    friendly_stamp = format_working_updated_stamp(working_stamp)
                    if friendly_stamp:
                        working_tip += f"\nUpdated: {friendly_stamp}"
                    working_item.setToolTip(working_tip)
                else:
                    working_item.setToolTip("No agent is marked as working this unit.")

                assigned_text = "-"
                if assigned:
                    assigned_text = f"{assigned} - {assigned_name}" if assigned_name else assigned
                assigned_item = QTableWidgetItem(assigned_text)
                if assigned_icon and Path(assigned_icon).exists():
                    assigned_item.setIcon(QIcon(assigned_icon))

                qa_note_item = QTableWidgetItem(note_preview(qa_comment))
                qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")
                agent_note_item = QTableWidgetItem(note_preview(agent_comment))
                agent_note_item.setToolTip(f"Agent Note: {agent_comment if agent_comment else '(none)'}")
                work_item = QTableWidgetItem(work_order)
                work_item.setData(Qt.ItemDataRole.UserRole, part_id)

                self.qa_assigned_table.setItem(row_idx, 0, _center_table_item(work_item))
                self.qa_assigned_table.setItem(row_idx, 1, _center_table_item(client_item))
                self.qa_assigned_table.setItem(row_idx, 2, _center_table_item(flag_item))
                self.qa_assigned_table.setItem(row_idx, 3, _center_table_item(QTableWidgetItem(age_text)))
                self.qa_assigned_table.setItem(row_idx, 4, _center_table_item(working_item))
                self.qa_assigned_table.setItem(row_idx, 5, _center_table_item(assigned_item))
                self.qa_assigned_table.setItem(row_idx, 6, _center_table_item(QTableWidgetItem(category)))
                self.qa_assigned_table.setItem(row_idx, 7, _center_table_item(qa_note_item))
                self.qa_assigned_table.setItem(row_idx, 8, _center_table_item(agent_note_item))
            self._mark_depot_view_refreshed(
                "qa_assigned",
                state_key,
                payload=[dict(row) for row in rows],
                reason=reason,
                duration_ms=(time.monotonic() - started) * 1000.0,
                row_count=len(rows),
            )
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_assigned_refresh_failed",
                severity="error",
                summary="QA assigned parts refresh failed.",
                exc=exc,
                context={"search": search_text, "reason": reason},
            )
            raise

    def _refresh_delivered_parts(
        self,
        *,
        force: bool = False,
        reason: str = "",
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
    ):
        if not hasattr(self, "qa_delivered_table"):
            return

        search_text = ""
        if hasattr(self, "qa_delivered_workorder_search"):
            search_text = str(self.qa_delivered_workorder_search.text() or "").strip()
        state_key = {"search": search_text}
        if not self._should_refresh_depot_view("qa_delivered", state_key, force=force, ttl_ms=ttl_ms, reason=reason):
            return
        started = time.monotonic()
        try:
            rows = self.tracker.list_qa_delivered_parts(search_text)
            agent_meta = self._qa_agent_meta_lookup()
            fallback_map = {
                DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
                for row in rows
                if DepotRules.normalize_work_order(str(row["work_order"] or ""))
            }
            category_map = self.tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
            detail_map = self.tracker.list_delivered_part_details_bulk(list(fallback_map.keys()))
            self.qa_delivered_table.setRowCount(0)
            installed_icon = QIcon.fromTheme("applications-engineering")
            if installed_icon.isNull():
                installed_icon = QIcon.fromTheme("tools")
            if installed_icon.isNull():
                installed_icon = QIcon.fromTheme("preferences-system")
            if installed_icon.isNull():
                installed_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
            for r in rows:
                part_id = int(r["id"])
                work_order = str(r["work_order"] or "").strip()
                assigned = DepotRules.normalize_user_id(str(r["assigned_user_id"] or ""))
                assigned_name, assigned_icon = agent_meta.get(assigned, ("", ""))
                assigned_text = f"{assigned} - {assigned_name}" if assigned and assigned_name else (assigned or "-")
                category = category_map.get(DepotRules.normalize_work_order(work_order), "") or str(r["category"] or "").strip() or "Other"
                age_text = part_age_label(str(r["created_at"] or ""))
                qa_comment = str(r["qa_comment"] or r["comments"] or "").strip()
                parts_installed = bool(int(r["parts_installed"] or 0))
                parts_installed_by = DepotRules.normalize_user_id(str(r["parts_installed_by"] or ""))
                parts_installed_at = str(r["parts_installed_at"] or "").strip()
                detail_rows = detail_map.get(DepotRules.normalize_work_order(work_order), [])
                detail_line_items: list[tuple[str, str, str, str, bool]] = []
                for detail in detail_rows:
                    merged_rows = _dedupe_part_detail_rows(
                        _merged_part_detail_rows(
                            str(detail["lpn"] or ""),
                            str(detail["part_number"] or ""),
                            str(detail["part_description"] or ""),
                            str(detail["shipping_info"] or ""),
                        )
                    )
                    installed_key_set = _installed_key_set_from_text(str(detail["installed_keys"] or ""))
                    for row in merged_rows:
                        lpn_value, part_value, desc_value, ship_value = row
                        key = _part_detail_row_key(lpn_value, part_value, desc_value, ship_value)
                        row_installed = bool(key and key in installed_key_set)
                        if not installed_key_set and parts_installed:
                            # Legacy fallback before per-line install snapshots existed.
                            row_installed = True
                        detail_line_items.append((lpn_value, part_value, desc_value, ship_value, row_installed))
                if not detail_line_items:
                    detail_line_items.append(("", "", "", "", bool(parts_installed)))

                for lpn, part_number, part_description, shipping_info, row_installed in detail_line_items:
                    insert_row_idx = self.qa_delivered_table.rowCount()
                    self.qa_delivered_table.insertRow(insert_row_idx)

                    installed_item = QTableWidgetItem("")
                    installed_item.setData(Qt.ItemDataRole.UserRole, part_id)
                    if row_installed:
                        installed_item.setIcon(installed_icon)
                        tip = "Parts installed."
                        if parts_installed_by:
                            tip += f"\nBy: {parts_installed_by}"
                        friendly_stamp = format_working_updated_stamp(parts_installed_at)
                        if friendly_stamp:
                            tip += f"\nAt: {friendly_stamp}"
                        installed_item.setToolTip(tip)
                    else:
                        installed_item.setToolTip("Waiting for agent install update.")

                    assigned_item = QTableWidgetItem(assigned_text)
                    if assigned_icon and Path(assigned_icon).exists():
                        assigned_item.setIcon(QIcon(assigned_icon))
                    qa_note_item = QTableWidgetItem(note_preview(qa_comment))
                    qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")
                    work_item = QTableWidgetItem(work_order)
                    work_item.setData(Qt.ItemDataRole.UserRole, part_id)

                    self.qa_delivered_table.setItem(insert_row_idx, 0, _center_table_item(work_item))
                    self.qa_delivered_table.setItem(insert_row_idx, 1, _center_table_item(installed_item))
                    self.qa_delivered_table.setItem(insert_row_idx, 2, _center_table_item(QTableWidgetItem(age_text)))
                    self.qa_delivered_table.setItem(insert_row_idx, 3, _center_table_item(assigned_item))
                    self.qa_delivered_table.setItem(insert_row_idx, 4, _center_table_item(QTableWidgetItem(category)))
                    self.qa_delivered_table.setItem(insert_row_idx, 5, _center_table_item(QTableWidgetItem(lpn)))
                    self.qa_delivered_table.setItem(insert_row_idx, 6, _center_table_item(QTableWidgetItem(part_number)))
                    self.qa_delivered_table.setItem(insert_row_idx, 7, _center_table_item(QTableWidgetItem(part_description)))
                    self.qa_delivered_table.setItem(insert_row_idx, 8, _center_table_item(QTableWidgetItem(shipping_info)))
                    self.qa_delivered_table.setItem(insert_row_idx, 9, _center_table_item(qa_note_item))
            self._mark_depot_view_refreshed(
                "qa_delivered",
                state_key,
                payload={"row_count": int(self.qa_delivered_table.rowCount())},
                reason=reason,
                duration_ms=(time.monotonic() - started) * 1000.0,
                row_count=int(self.qa_delivered_table.rowCount()),
            )
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_delivered_refresh_failed",
                severity="error",
                summary="QA delivered parts refresh failed.",
                exc=exc,
                context={"search": search_text, "reason": reason},
            )
            raise

    def _export_qa_delivered_csv(self) -> None:
        if not hasattr(self, "qa_delivered_table"):
            return
        table = self.qa_delivered_table
        if table.rowCount() <= 0 or table.columnCount() <= 0:
            self._show_themed_message(QMessageBox.Icon.Information, "Export CSV", "No delivered rows to export.")
            return

        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        default_name = f"qa_parts_delivered_{stamp}.csv"
        if self.app_window is not None and self.app_window.config_path.parent.exists():
            start_dir = self.app_window.config_path.parent
        else:
            start_dir = Path.home()

        out_path, _ = show_flowgrid_themed_save_file_name(
            self,
            self.app_window,
            "qa",
            "Export Delivered Parts",
            str(start_dir / default_name),
            "CSV Files (*.csv);;All Files (*.*)",
        )
        if not out_path:
            return

        headers: list[str] = []
        for col in range(table.columnCount()):
            hdr = table.horizontalHeaderItem(col)
            headers.append(hdr.text() if hdr is not None else f"col_{col}")

        try:
            with open(out_path, "w", newline="", encoding="utf-8-sig") as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                for row in range(table.rowCount()):
                    values: list[str] = []
                    for col in range(table.columnCount()):
                        item = table.item(row, col)
                        value = item.text() if item is not None else ""
                        if headers[col].strip().casefold() == "installed":
                            value = "Yes" if (item is not None and not item.icon().isNull()) else ""
                        values.append(value)
                    writer.writerow(values)
            self._show_themed_message(QMessageBox.Icon.Information, "Export CSV", f"Exported:\n{out_path}")
        except Exception as exc:
            _runtime_log_event(
                "ui.qa_delivered_export_csv_failed",
                severity="error",
                summary="QA delivered parts CSV export failed.",
                exc=exc,
                context={"path": str(out_path)},
            )
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Export CSV",
                f"Failed to export CSV:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
            )

__all__ = ["DepotQAWindow"]
