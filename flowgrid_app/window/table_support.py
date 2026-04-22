from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QPoint, QSize, Qt, QRect
from PySide6.QtGui import QIcon, QPainter, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QHeaderView,
    QStyle,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableWidget,
    QTableWidgetItem,
)


class FlowgridIconCenteredDelegate(QStyledItemDelegate):
    def paint(self, painter, option, index):
        icon_data = index.data(Qt.ItemDataRole.DecorationRole)
        text = str(index.data(Qt.ItemDataRole.DisplayRole) or "")
        if icon_data and not text.strip():
            opt = QStyleOptionViewItem(option)
            self.initStyleOption(opt, index)
            style = QApplication.style() if opt.widget is None else opt.widget.style()
            painter.save()
            style.drawControl(QStyle.CE_ItemViewItem, opt, painter)
            painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
            icon = QIcon(icon_data) if isinstance(icon_data, QIcon) else QIcon(QPixmap(icon_data))
            available = min(opt.rect.width(), opt.rect.height()) - 6
            icon_size = max(16, min(available, 32))
            target_rect = QRect(0, 0, icon_size, icon_size)
            target_rect.moveCenter(opt.rect.center())
            painter.drawPixmap(target_rect, icon.pixmap(QSize(icon_size, icon_size)))
            painter.restore()
            return
        super().paint(painter, option, index)


def configure_standard_table(
    table: QTableWidget,
    headers: list[str] | tuple[str, ...],
    *,
    resize_modes: dict[int, QHeaderView.ResizeMode] | None = None,
    stretch_last: bool = True,
) -> None:
    table.setColumnCount(len(headers))
    table.setHorizontalHeaderLabels([str(h) for h in headers])
    table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    table.verticalHeader().setVisible(False)
    table.verticalHeader().setDefaultSectionSize(30)
    table.setAlternatingRowColors(True)
    table.setShowGrid(True)
    table.setGridStyle(Qt.PenStyle.SolidLine)
    table.setWordWrap(False)
    table.setTextElideMode(Qt.TextElideMode.ElideRight)
    table.setIconSize(QSize(26, 26))
    table.setItemDelegate(FlowgridIconCenteredDelegate(table))
    table.horizontalHeader().setStretchLastSection(bool(stretch_last))
    table.horizontalHeader().setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
    if resize_modes:
        for col_idx, mode in resize_modes.items():
            if 0 <= int(col_idx) < table.columnCount():
                table.horizontalHeader().setSectionResizeMode(int(col_idx), mode)


def _center_table_item(item: QTableWidgetItem) -> QTableWidgetItem:
    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
    return item


def _resolve_user_icon_from_agent_meta(
    user_id: str,
    agent_meta: dict[str, tuple[str, str]] | None,
) -> QIcon | None:
    if not user_id or not agent_meta:
        return None
    _, icon_path = agent_meta.get(user_id, ("", ""))
    if not icon_path:
        return None
    if not Path(icon_path).exists():
        return None
    icon = QIcon(icon_path)
    return icon if not icon.isNull() else None


def _center_table_icon_item(icon: QIcon | None, text: str = "") -> QTableWidgetItem:
    item = QTableWidgetItem(icon or QIcon(), str(text or ""))
    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
    return item


def _table_column_index_by_header(table: QTableWidget, header_text: str) -> int:
    for col in range(int(table.columnCount())):
        hdr = table.horizontalHeaderItem(col)
        if hdr is not None and str(hdr.text() or "").strip() == header_text:
            return col
    return -1


def _selected_part_id_from_table(table: QTableWidget) -> int | None:
    row = table.currentRow()
    if row < 0:
        return None
    for col in range(int(table.columnCount())):
        cell_item = table.item(row, col)
        if cell_item is None:
            continue
        raw_id = cell_item.data(Qt.ItemDataRole.UserRole)
        try:
            return int(raw_id)
        except Exception:
            continue
    return None


def _copy_work_order_from_table_item(
    item: QTableWidgetItem | None,
    *,
    header_text: str = "Work Order",
) -> tuple[QTableWidget | None, str]:
    if item is None:
        return None, ""
    table = item.tableWidget()
    if table is None:
        return None, ""
    work_col = _table_column_index_by_header(table, header_text)
    if work_col < 0:
        return table, ""
    work_item = table.item(item.row(), work_col)
    if work_item is None:
        return table, ""
    work_order = str(work_item.text() or "").strip()
    if work_order:
        QApplication.clipboard().setText(work_order)
    return table, work_order


def _select_table_row_by_context_pos(
    table: QTableWidget | None,
    pos: QPoint,
    *,
    header_text: str = "Work Order",
) -> bool:
    if table is None:
        return False
    row = int(table.rowAt(int(pos.y())))
    if row < 0:
        return False
    work_col = _table_column_index_by_header(table, header_text)
    if work_col < 0:
        work_col = 0
    table.setCurrentCell(row, work_col)
    table.selectRow(row)
    return True


__all__ = [
    "FlowgridIconCenteredDelegate",
    "_center_table_icon_item",
    "_center_table_item",
    "_copy_work_order_from_table_item",
    "_resolve_user_icon_from_agent_meta",
    "_select_table_row_by_context_pos",
    "_selected_part_id_from_table",
    "_table_column_index_by_header",
    "configure_standard_table",
]
