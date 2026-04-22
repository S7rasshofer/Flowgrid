from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon, QPainter
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QStyle,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import DEFAULT_THEME_SURFACE, contrast_ratio, normalize_hex, readable_text, rgba_css, safe_int, shift

from .common import format_working_updated_stamp, note_preview, part_age_label
from .popup_support import DepotFramelessToolWindow, _paint_flowgrid_popup_background, show_flowgrid_themed_input_item, show_flowgrid_themed_input_text
from .table_support import (
    _center_table_item,
    _resolve_user_icon_from_agent_meta,
    _selected_part_id_from_table,
    _table_column_index_by_header,
)


FALLBACK_QA_FLAG_OPTIONS: tuple[str, ...] = (
    "None",
    "Follow Up",
    "Need Parts",
    "Escalation",
    "Client Callback",
    "Return Visit",
    "Safety",
    "Other",
)


class PartNotesDialog(QDialog):
    def __init__(
        self,
        role: str,
        part_data: dict[str, Any],
        tracker: Any | None = None,
        app_window: Any | None = None,
        current_user: str = "",
        parent: QWidget | None = None,
    ):
        super().__init__(parent)
        self.setObjectName("PartNotesDialog")
        self.role = "qa" if str(role).strip().lower() == "qa" else "agent"
        self.app_window = app_window
        self._style_kind = "qa" if self.role == "qa" else "agent"
        self.part_data = part_data
        self.tracker = tracker
        self.current_user = DepotRules.normalize_user_id(current_user)
        self._working_original_user = DepotRules.normalize_user_id(str(part_data.get("working_user_id", "") or ""))

        work_order = str(part_data.get("work_order", "") or "")
        self.setWindowTitle(f"Notes - {work_order}")
        self.setModal(True)
        self.resize(560, 320)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        resolved_category = ""
        if self.tracker is not None:
            try:
                resolved_category = self.tracker.resolve_work_order_category(
                    work_order,
                    str(part_data.get("category", "") or "").strip(),
                )
            except Exception as exc:
                _runtime_log_event(
                    "ui.part_notes_category_resolve_failed",
                    severity="warning",
                    summary="Failed resolving work-order category for notes dialog.",
                    exc=exc,
                    context={
                        "work_order": str(work_order),
                        "role": str(self.role),
                    },
                )
        summary = QLabel(
            f"Work Order: {work_order}    Category: {resolved_category or '-'}    "
            f"Client: {'Yes' if bool(part_data.get('client_unit', False)) else 'No'}"
        )
        summary.setWordWrap(True)
        layout.addWidget(summary)

        other_label = QLabel("Agent Note (Other User)" if self.role == "qa" else "QA Note (Other User)")
        other_label.setProperty("section", True)
        layout.addWidget(other_label)
        self.other_note_value = QLabel()
        self.other_note_value.setWordWrap(True)
        self.other_note_value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        other_text = str(part_data.get("agent_comment" if self.role == "qa" else "qa_comment", "") or "").strip()
        self.other_note_value.setText(other_text if other_text else "(none)")
        layout.addWidget(self.other_note_value)

        own_label = QLabel("QA Note (Your Note)" if self.role == "qa" else "Agent Note (Your Note)")
        own_label.setProperty("section", True)
        layout.addWidget(own_label)
        self.own_note_input = QTextEdit()
        self.own_note_input.setFixedHeight(86)
        own_value = str(part_data.get("qa_comment" if self.role == "qa" else "agent_comment", "") or "").strip()
        self.own_note_input.setPlainText(own_value)
        layout.addWidget(self.own_note_input)

        if self.role == "qa":
            qa_flag_row = QHBoxLayout()
            qa_flag_row.addWidget(QLabel("Flag"), 0)
            self.qa_flag_combo = QComboBox()
            flag_options = (
                self.tracker.get_qa_flag_options(include_none=True)
                if self.tracker is not None
                else list(FALLBACK_QA_FLAG_OPTIONS)
            )
            for option in flag_options:
                self.qa_flag_combo.addItem(option)
            current_flag = str(part_data.get("qa_flag", "") or "").strip()
            current_display = current_flag if current_flag else "None"
            idx = self.qa_flag_combo.findText(current_display)
            self.qa_flag_combo.setCurrentIndex(idx if idx >= 0 else 0)
            qa_flag_row.addWidget(self.qa_flag_combo, 1)
            layout.addLayout(qa_flag_row)
        else:
            self.work_toggle_check = QCheckBox("Working this unit (\U0001F527)")
            if self.current_user:
                if self._working_original_user and self._working_original_user != self.current_user:
                    self.work_toggle_check.setChecked(True)
                    self.work_toggle_check.setEnabled(False)
                    lock_notice = QLabel(f"Currently marked by {self._working_original_user}.")
                    lock_notice.setProperty("muted", True)
                    layout.addWidget(lock_notice)
                else:
                    self.work_toggle_check.setChecked(self._working_original_user == self.current_user)
            else:
                self.work_toggle_check.setEnabled(False)
            layout.addWidget(self.work_toggle_check)

            flag_text = str(part_data.get("qa_flag", "") or "").strip()
            flag_value = flag_text if flag_text else "None"
            layout.addWidget(QLabel(f"Flag: {flag_value}"))

        buttons = QHBoxLayout()
        self.save_btn = QPushButton("Save")
        self.cancel_btn = QPushButton("Cancel")
        self.save_btn.setProperty("actionRole", "save")
        self.cancel_btn.setProperty("actionRole", "reset")
        buttons.addWidget(self.save_btn)
        buttons.addWidget(self.cancel_btn)
        buttons.addStretch(1)
        layout.addLayout(buttons)

        self.save_btn.clicked.connect(self.accept)
        self.cancel_btn.clicked.connect(self.reject)

        if app_window is not None:
            base_css = app_window._popup_theme_stylesheet(self._style_kind, force_opaque_root=True)
            resolved = app_window._resolved_popup_theme(self._style_kind)
            dialog_bg = normalize_hex(
                resolved.get("background", app_window.palette_data.get("surface", DEFAULT_THEME_SURFACE)),
                app_window.palette_data.get("surface", DEFAULT_THEME_SURFACE),
            )
            dialog_text = normalize_hex(
                resolved.get("text", app_window.palette_data.get("label_text", "#FFFFFF")),
                app_window.palette_data.get("label_text", "#FFFFFF"),
            )
            if contrast_ratio(dialog_bg, dialog_text) < 4.0:
                dialog_text = readable_text(dialog_bg)
            field_bg = normalize_hex(
                resolved.get("field_bg", app_window.palette_data.get("input_bg", "#FFFFFF")),
                app_window.palette_data.get("input_bg", "#FFFFFF"),
            )
            field_text = dialog_text if contrast_ratio(field_bg, dialog_text) >= 4.0 else readable_text(field_bg)
            field_border = shift(field_bg, -0.38)
            self.setStyleSheet(
                base_css
                + (
                    "QDialog#PartNotesDialog {"
                    "background-color: transparent;"
                    f"color: {dialog_text};"
                    "}"
                    "QDialog#PartNotesDialog QLabel {"
                    f"color: {dialog_text};"
                    "background-color: transparent;"
                    "font-weight: 700;"
                    "}"
                    "QDialog#PartNotesDialog QTextEdit, QDialog#PartNotesDialog QLineEdit, QDialog#PartNotesDialog QComboBox {"
                    f"background-color: {rgba_css(field_bg, 0.95)};"
                    f"color: {field_text};"
                    f"border: 1px solid {field_border};"
                    "}"
                )
            )

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        if self.app_window is not None:
            _paint_flowgrid_popup_background(self, painter, self.app_window, self._style_kind)
        super().paintEvent(event)

    def values(self) -> dict[str, str]:
        own_note = self.own_note_input.toPlainText().strip()
        out: dict[str, str] = {
            "own_note": own_note,
            "qa_flag": "",
            "qa_flag_image_path": "",
            "working_user_id": "__UNCHANGED__",
        }
        if self.role == "qa":
            selected_flag = str(self.qa_flag_combo.currentText() if hasattr(self, "qa_flag_combo") else "").strip()
            if selected_flag.lower() == "none":
                selected_flag = ""
            out["qa_flag"] = selected_flag
            out["qa_flag_image_path"] = ""
        elif hasattr(self, "work_toggle_check") and self.work_toggle_check.isEnabled():
            out["working_user_id"] = self.current_user if self.work_toggle_check.isChecked() else ""
        return out


def _edit_aux_queue_comment(
    owner: DepotFramelessToolWindow,
    tracker: Any,
    *,
    table: QTableWidget,
    target_key: str,
    theme_kind: str,
    title: str,
) -> bool:
    row = table.currentRow()
    if row < 0:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
        return False
    row_id_item = table.item(row, 0)
    if row_id_item is None:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
        return False
    row_id = safe_int(row_id_item.data(Qt.ItemDataRole.UserRole), 0)
    if row_id <= 0:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Selected row no longer exists.")
        return False
    work_order_item = table.item(row, max(0, _table_column_index_by_header(table, "Work Order")))
    comments_item = table.item(row, max(0, _table_column_index_by_header(table, "Comments")))
    work_order = str(work_order_item.text() or "").strip() if work_order_item is not None else ""
    current_text = str(comments_item.text() or "").strip() if comments_item is not None else ""
    if comments_item is not None:
        stored_text = str(comments_item.data(Qt.ItemDataRole.UserRole + 1) or "").strip()
        if stored_text:
            current_text = stored_text
    note_text, ok = show_flowgrid_themed_input_text(
        owner,
        owner.app_window,
        theme_kind,
        title,
        f"Work Order: {work_order or '-'}\nUpdate comment:",
        current_text if current_text != "(none)" else "",
    )
    if not ok:
        return False
    try:
        tracker.update_dashboard_note_value(target_key, int(row_id), str(note_text or "").strip())
    except Exception as exc:
        _runtime_log_event(
            "ui.aux_queue_comment_save_failed",
            severity="warning",
            summary="Failed saving queue comment update.",
            exc=exc,
            context={"target_key": str(target_key), "row_id": int(row_id), "work_order": str(work_order)},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Warning,
            "Save Failed",
            f"Could not save comment:\n{type(exc).__name__}: {exc}",
        )
        return False
    return True


def _copy_work_order_with_notice(
    owner: DepotFramelessToolWindow,
    item: QTableWidgetItem | None,
    *,
    header_text: str = "Work Order",
    duration_ms: int = 4200,
) -> str:
    if item is None:
        return ""
    table = item.tableWidget()
    if table is None:
        return ""
    work_col = _table_column_index_by_header(table, header_text)
    if work_col < 0:
        return ""
    work_item = table.item(item.row(), work_col)
    work_order = str(work_item.text() or "").strip() if work_item is not None else ""
    if work_order:
        QApplication.clipboard().setText(work_order)
        owner._show_copy_notice(table, f"Copied Work Order: {work_order}", duration_ms=duration_ms)
    return work_order


def _edit_part_notes(
    owner: DepotFramelessToolWindow,
    tracker: Any,
    *,
    role: str,
    current_user: str = "",
    table: QTableWidget | None = None,
    part_id: int | None = None,
) -> tuple[bool, int | None]:
    target_part_id = int(part_id) if part_id is not None else _selected_part_id_from_table(table) if table is not None else None
    if target_part_id is None:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
        return False, None
    try:
        part_data = tracker.get_part_note_context(target_part_id)
    except Exception as exc:
        _runtime_log_event(
            "ui.part_notes_context_load_failed",
            severity="warning",
            summary="Failed loading part-note context.",
            exc=exc,
            context={"role": str(role), "part_id": int(target_part_id), "current_user": str(current_user or "")},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Critical,
            "Load Failed",
            f"Could not load notes:\n{type(exc).__name__}: {exc}",
        )
        return False, int(target_part_id)
    if part_data is None:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Missing", "Selected part no longer exists.")
        return False, int(target_part_id)

    dialog = PartNotesDialog(
        role,
        part_data,
        tracker=tracker,
        app_window=owner.app_window,
        current_user=current_user,
        parent=owner,
    )
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return False, int(target_part_id)

    values = dialog.values()
    try:
        if str(role).strip().lower() == "qa":
            tracker.update_part_qa_fields(
                int(target_part_id),
                values.get("own_note", ""),
                values.get("qa_flag", ""),
            )
        else:
            tracker.update_part_agent_comment(int(target_part_id), values.get("own_note", ""))
            working_user_id = str(values.get("working_user_id", "__UNCHANGED__"))
            if working_user_id != "__UNCHANGED__":
                tracker.set_part_working_user(int(target_part_id), working_user_id)
    except Exception as exc:
        _runtime_log_event(
            "ui.part_notes_save_failed",
            severity="warning",
            summary="Failed saving part-note changes.",
            exc=exc,
            context={"role": str(role), "part_id": int(target_part_id), "current_user": str(current_user or "")},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Critical,
            "Save Failed",
            f"Could not save notes:\n{type(exc).__name__}: {exc}",
        )
        return False, int(target_part_id)
    return True, int(target_part_id)


def _populate_missing_po_followup_table(
    table: QTableWidget,
    *,
    rows: list[dict[str, Any]],
    all_rows_count: int,
    search_text: str,
    summary_label: QLabel | None,
    agent_meta: dict[str, tuple[str, str]],
    icon_host: QWidget,
) -> None:
    client_icon = icon_host.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)
    table.setRowCount(0)
    for row_idx, row in enumerate(rows):
        table.insertRow(row_idx)
        part_id = int(row.get("id", 0) or 0)
        work_order = str(row.get("work_order", "") or "").strip()
        assigned_user = DepotRules.normalize_user_id(str(row.get("assigned_user_id", "") or ""))
        assigned_name, assigned_icon = agent_meta.get(assigned_user, ("", ""))
        assigned_text = f"{assigned_user} - {assigned_name}" if assigned_user and assigned_name else (assigned_user or "-")
        submitted_by = DepotRules.normalize_user_id(str(row.get("logged_by", "") or row.get("user_id", "") or ""))
        logged_at = str(row.get("logged_at", "") or row.get("created_at", "") or "").strip()
        logged_display = format_working_updated_stamp(logged_at) if logged_at else "-"
        age_text = part_age_label(logged_at)
        category = str(row.get("resolved_category", "") or row.get("category", "") or "").strip() or "Other"
        qa_comment = str(row.get("qa_comment", "") or "").strip()

        work_item = _center_table_item(QTableWidgetItem(work_order))
        work_item.setData(Qt.ItemDataRole.UserRole, part_id)
        assigned_item = _center_table_item(QTableWidgetItem(assigned_text))
        if assigned_icon and Path(assigned_icon).exists():
            assigned_item.setIcon(QIcon(assigned_icon))
        submitted_item = _center_table_item(QTableWidgetItem(submitted_by or "-"))
        submitted_icon = _resolve_user_icon_from_agent_meta(submitted_by, agent_meta)
        if submitted_icon is not None:
            submitted_item.setIcon(submitted_icon)
        logged_item = _center_table_item(QTableWidgetItem(logged_display))
        if logged_at:
            logged_item.setToolTip(logged_at.replace("T", " "))
        age_item = _center_table_item(QTableWidgetItem(age_text))
        category_item = _center_table_item(QTableWidgetItem(category))
        client_item = _center_table_item(QTableWidgetItem(""))
        client_item.setData(Qt.ItemDataRole.UserRole, part_id)
        if int(row.get("client_unit", 0) or 0):
            client_item.setIcon(client_icon)
            client_item.setToolTip("Client unit")
        else:
            client_item.setToolTip("Non-client unit")
        qa_note_item = _center_table_item(QTableWidgetItem(note_preview(qa_comment)))
        qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")

        table.setItem(row_idx, 0, work_item)
        table.setItem(row_idx, 1, assigned_item)
        table.setItem(row_idx, 2, submitted_item)
        table.setItem(row_idx, 3, logged_item)
        table.setItem(row_idx, 4, age_item)
        table.setItem(row_idx, 5, category_item)
        table.setItem(row_idx, 6, client_item)
        table.setItem(row_idx, 7, qa_note_item)

    if summary_label is not None:
        if search_text:
            summary_label.setText(
                f"Missing PO rows: {all_rows_count} | Showing: {len(rows)}" if all_rows_count else "No Missing PO rows."
            )
        else:
            summary_label.setText(
                f"Missing PO rows: {all_rows_count}" if all_rows_count else "No Missing PO rows."
            )


def _reassign_missing_po_followup(
    owner: DepotFramelessToolWindow,
    tracker: Any,
    *,
    table: QTableWidget,
    current_user: str,
    role_key: str,
    refresh_callback: Callable[[], None],
) -> None:
    if table.currentRow() < 0:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
        return
    part_id = _selected_part_id_from_table(table)
    if part_id is None:
        return

    work_order = tracker.get_part_work_order(part_id)
    if not work_order:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Missing", "Selected part no longer exists.")
        return
    try:
        agent_items, item_lookup, current_index = tracker.part_owner_choice_items(work_order)
    except Exception as exc:
        _runtime_log_event(
            f"ui.{role_key}_missing_po_reassign_agent_query_failed",
            severity="warning",
            summary=f"{role_key.title()} Missing PO reassignment could not load the agent list.",
            exc=exc,
            context={"user_id": str(current_user), "part_id": int(part_id), "work_order": work_order},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Critical,
            "Reassign Agent",
            f"Could not load the agent list:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
        )
        return
    if not agent_items:
        owner._show_themed_message(
            QMessageBox.Icon.Warning,
            "Reassign Agent",
            "No agents are configured. Add an agent before reassigning this Missing PO item.",
        )
        return

    selection, ok = show_flowgrid_themed_input_item(
        owner,
        owner.app_window,
        owner._theme_kind,
        "Reassign Agent",
        (
            f"Work Order: {work_order}\n"
            "Select the agent who should own this Missing PO item."
        ),
        agent_items,
        current_index,
        False,
    )
    if not ok:
        return
    assigned_user_id = item_lookup.get(str(selection or "").strip(), "")
    if not assigned_user_id:
        return

    try:
        tracker.reassign_part_owner(part_id, assigned_user_id)
    except Exception as exc:
        _runtime_log_event(
            f"ui.{role_key}_missing_po_reassign_failed",
            severity="warning",
            summary=f"{role_key.title()} Missing PO reassignment failed.",
            exc=exc,
            context={"user_id": str(current_user), "part_id": int(part_id), "work_order": work_order},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Critical,
            "Save Failed",
            f"Could not reassign this Missing PO item:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
        )
        return
    owner._show_copy_notice(table, f"Reassigned to {assigned_user_id}", duration_ms=3200)
    refresh_callback()


def _resolve_missing_po_followup(
    owner: DepotFramelessToolWindow,
    tracker: Any,
    *,
    table: QTableWidget,
    current_user: str,
    role_key: str,
    refresh_callback: Callable[[], None],
) -> None:
    if table.currentRow() < 0:
        owner._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a row first.")
        return
    part_id = _selected_part_id_from_table(table)
    if part_id is None:
        return
    try:
        tracker.resolve_missing_part_order_followup(part_id, current_user)
    except Exception as exc:
        _runtime_log_event(
            f"ui.{role_key}_missing_po_resolve_failed",
            severity="warning",
            summary=f"{role_key.title()} Missing PO resolve action failed.",
            exc=exc,
            context={"user_id": str(current_user), "part_id": int(part_id)},
        )
        owner._show_themed_message(
            QMessageBox.Icon.Critical,
            "Resolve Failed",
            f"Could not resolve this Missing PO item:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
        )
        return
    owner._show_copy_notice(table, "Missing PO resolved", duration_ms=3200)
    refresh_callback()


__all__ = [
    "PartNotesDialog",
    "_copy_work_order_with_notice",
    "_edit_aux_queue_comment",
    "_edit_part_notes",
    "_populate_missing_po_followup_table",
    "_reassign_missing_po_followup",
    "_resolve_missing_po_followup",
]
