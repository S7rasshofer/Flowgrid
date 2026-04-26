from __future__ import annotations

import csv
from datetime import datetime
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

from flowgrid_app import PermissionDeniedError, PermissionService
from flowgrid_app.depot_async import DepotLoadResult
from flowgrid_app.depot_rules import DepotRules
from flowgrid_app.icon_io import _load_icon_image_file
from flowgrid_app.paths import ASSETS_DIR_NAME
from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import clamp, contrast_ratio, normalize_hex, readable_text, rgba_css, safe_int, shift
from flowgrid_app.workflow_core import DepotTracker, QA_FLAG_SEVERITY_OPTIONS, TRACKER_DASHBOARD_TABLES

from .agent import DepotAgentWindow
from .common import TouchDistributionBar, format_working_updated_stamp
from .popup_support import (
    DepotFramelessToolWindow,
    FlowgridThemedDialog,
    UI_SPACING_FRAMELESS_TOOL,
    _ensure_shell_window_available,
    _visible_flowgrid_shell_window,
    show_flowgrid_themed_open_file_name,
    show_flowgrid_themed_save_file_name,
)
from .table_support import _center_table_item, configure_standard_table

class IconCropDialog(FlowgridThemedDialog):
    def __init__(self, image: QImage, app_window: "QuickInputsWindow" | None = None, parent: QWidget | None = None):
        super().__init__(parent, app_window, "admin")
        self._source_image = image.copy()
        self._result_image = QImage()

        self.setWindowTitle("Icon Creator")
        self.setModal(True)
        self.resize(920, 560)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        hint = QLabel(
            "Drag to move the image inside the icon frame. "
            "Use mouse wheel or slider to zoom. The live preview shows the final icon."
        )
        hint.setWordWrap(True)
        layout.addWidget(hint)

        body = QHBoxLayout()
        body.setSpacing(8)
        self.arrange_preview = IconArrangePreview(self._source_image, self)
        body.addWidget(self.arrange_preview, 1)

        preview_col = QVBoxLayout()
        preview_col.setSpacing(6)
        preview_col.addWidget(QLabel("Final Icon Preview"))
        self.output_preview = QLabel()
        self.output_preview.setFixedSize(130, 130)
        self.output_preview.setFrameShape(QFrame.Shape.Box)
        preview_col.addWidget(self.output_preview, 0, Qt.AlignmentFlag.AlignCenter)
        preview_col.addStretch(1)
        body.addLayout(preview_col, 0)
        layout.addLayout(body, 1)

        zoom_row = QHBoxLayout()
        zoom_row.addWidget(QLabel("Zoom"), 0)
        self.zoom_slider = QSlider(Qt.Orientation.Horizontal)
        self.zoom_slider.setRange(100, 400)
        self.zoom_slider.setSingleStep(5)
        self.zoom_slider.setPageStep(20)
        self.zoom_slider.setValue(100)
        zoom_row.addWidget(self.zoom_slider, 1)
        layout.addLayout(zoom_row)

        actions = QHBoxLayout()
        self.crop_btn = QPushButton("Apply")
        self.cancel_btn = QPushButton("Cancel")
        self.crop_btn.setProperty("actionRole", "save")
        self.cancel_btn.setProperty("actionRole", "reset")
        actions.addWidget(self.crop_btn)
        actions.addWidget(self.cancel_btn)
        layout.addLayout(actions)

        self.zoom_slider.valueChanged.connect(self._on_zoom_slider_changed)
        self.arrange_preview.zoom_changed.connect(self._on_preview_zoom_changed)
        self.arrange_preview.view_changed.connect(self._update_output_preview)
        self.crop_btn.clicked.connect(self._accept_crop)
        self.cancel_btn.clicked.connect(self.reject)

        self.apply_theme_styles(force_opaque_root=True)
        self._update_output_preview()

    @staticmethod
    def _set_preview_label_image(target: QLabel, image: QImage) -> None:
        if image.isNull():
            target.clear()
            return
        pix = QPixmap.fromImage(image)
        if pix.isNull():
            target.clear()
            return
        fitted = pix.scaled(
            target.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        target.setPixmap(fitted)

    def _on_zoom_slider_changed(self, value: int) -> None:
        self.arrange_preview.set_zoom(float(value) / 100.0)

    def _on_preview_zoom_changed(self, value: float) -> None:
        slider_value = int(round(float(value) * 100.0))
        if self.zoom_slider.value() == slider_value:
            return
        self.zoom_slider.blockSignals(True)
        self.zoom_slider.setValue(slider_value)
        self.zoom_slider.blockSignals(False)

    def _update_output_preview(self) -> None:
        preview = self.arrange_preview.render_cropped_image(180)
        self._set_preview_label_image(self.output_preview, preview)

    def _accept_crop(self) -> None:
        arranged = self.arrange_preview.render_cropped_image(256)
        if arranged.isNull():
            return
        self._result_image = arranged
        self.accept()

    def result_image(self) -> QImage:
        return self._result_image.copy()

class IconArrangePreview(QWidget):
    zoom_changed = Signal(float)
    view_changed = Signal()

    def __init__(self, image: QImage, parent: QWidget | None = None):
        super().__init__(parent)
        self._image = image.copy()
        self._frame_rect = QRect()
        self._zoom = 1.0
        self._pan = QPointF(0.0, 0.0)
        self._drag_active = False
        self._last_pos = QPoint()
        self.setMinimumSize(360, 250)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setCursor(Qt.CursorShape.OpenHandCursor)
        self.set_source_image(image, reset_view=True)

    def set_source_image(self, image: QImage, *, reset_view: bool = True) -> None:
        self._image = image.copy()
        if reset_view:
            self._zoom = 1.0
            self._pan = QPointF(0.0, 0.0)
            self.zoom_changed.emit(self._zoom)
        self._clamp_pan()
        self.view_changed.emit()
        self.update()

    def _calc_frame_rect(self) -> QRect:
        bounds = self.rect().adjusted(10, 10, -10, -10)
        if bounds.width() <= 0 or bounds.height() <= 0:
            return QRect()
        side = max(1, min(bounds.width(), bounds.height()))
        x = bounds.left() + max(0, (bounds.width() - side) // 2)
        y = bounds.top() + max(0, (bounds.height() - side) // 2)
        return QRect(x, y, side, side)

    def zoom(self) -> float:
        return float(self._zoom)

    def set_zoom(self, value: float) -> None:
        new_zoom = float(clamp(float(value), 1.0, 4.0))
        if abs(new_zoom - self._zoom) < 0.001:
            return
        self._zoom = new_zoom
        self._clamp_pan()
        self.zoom_changed.emit(self._zoom)
        self.view_changed.emit()
        self.update()

    def _clamp_pan(self, frame: QRectF | None = None) -> None:
        if self._image.isNull():
            self._pan = QPointF(0.0, 0.0)
            return
        draw_frame = QRectF(frame) if frame is not None else QRectF(self._frame_rect if self._frame_rect.isValid() else self._calc_frame_rect())
        if draw_frame.width() <= 0 or draw_frame.height() <= 0:
            self._pan = QPointF(0.0, 0.0)
            return
        base_scale = max(
            draw_frame.width() / max(1, self._image.width()),
            draw_frame.height() / max(1, self._image.height()),
        )
        scale = base_scale * self._zoom
        scaled_w = float(self._image.width()) * scale
        scaled_h = float(self._image.height()) * scale
        max_x = max(0.0, (scaled_w - draw_frame.width()) / 2.0)
        max_y = max(0.0, (scaled_h - draw_frame.height()) / 2.0)
        self._pan.setX(float(clamp(self._pan.x(), -max_x, max_x)))
        self._pan.setY(float(clamp(self._pan.y(), -max_y, max_y)))

    def _draw_image_in_frame(self, painter: QPainter, frame: QRectF) -> None:
        if self._image.isNull() or frame.width() <= 0 or frame.height() <= 0:
            return
        self._clamp_pan(frame)
        base_scale = max(
            frame.width() / max(1, self._image.width()),
            frame.height() / max(1, self._image.height()),
        )
        scale = base_scale * self._zoom
        scaled_w = float(self._image.width()) * scale
        scaled_h = float(self._image.height()) * scale
        center = frame.center()
        target = QRectF(
            center.x() - (scaled_w / 2.0) + self._pan.x(),
            center.y() - (scaled_h / 2.0) + self._pan.y(),
            scaled_w,
            scaled_h,
        )

        clip = QPainterPath()
        clip.addRoundedRect(frame, 6, 6)
        painter.save()
        painter.setClipPath(clip)
        painter.drawImage(target, self._image)
        painter.restore()
        painter.setPen(QPen(QColor(42, 224, 203), 2, Qt.PenStyle.SolidLine))
        painter.drawRoundedRect(frame, 6, 6)

    def render_cropped_image(self, target_size: int = 256) -> QImage:
        if self._image.isNull():
            return QImage()
        side = max(64, int(target_size))
        output = QImage(side, side, QImage.Format.Format_ARGB32_Premultiplied)
        output.fill(Qt.GlobalColor.transparent)
        painter = QPainter(output)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        self._draw_image_in_frame(painter, QRectF(0.0, 0.0, float(side), float(side)))
        painter.end()
        return output

    def mousePressEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if event.button() != Qt.MouseButton.LeftButton:
            return super().mousePressEvent(event)
        point = event.position().toPoint()
        if not self._frame_rect.isValid() or not self._frame_rect.contains(point):
            return super().mousePressEvent(event)
        self._drag_active = True
        self._last_pos = point
        self.setCursor(Qt.CursorShape.ClosedHandCursor)
        event.accept()

    def mouseMoveEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if not self._drag_active:
            return super().mouseMoveEvent(event)
        point = event.position().toPoint()
        delta = point - self._last_pos
        self._last_pos = point
        self._pan = QPointF(self._pan.x() + delta.x(), self._pan.y() + delta.y())
        self._clamp_pan()
        self.view_changed.emit()
        self.update()
        event.accept()

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        if not self._drag_active:
            return super().mouseReleaseEvent(event)
        self._drag_active = False
        self.setCursor(Qt.CursorShape.OpenHandCursor)
        event.accept()

    def wheelEvent(self, event) -> None:  # noqa: N802
        point = event.position().toPoint()
        if self._frame_rect.isValid() and self._frame_rect.contains(point):
            delta_y = event.angleDelta().y()
            if delta_y != 0:
                factor = 1.1 if delta_y > 0 else (1.0 / 1.1)
                self.set_zoom(self._zoom * factor)
                event.accept()
                return
        super().wheelEvent(event)

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        painter.fillRect(self.rect(), QColor(10, 10, 10, 220))
        if self._image.isNull():
            return
        self._frame_rect = self._calc_frame_rect()
        if not self._frame_rect.isValid():
            return
        self._draw_image_in_frame(painter, QRectF(self._frame_rect))

class DroppableImagePathLineEdit(QLineEdit):
    def __init__(
        self,
        parent: QWidget | None = None,
        *,
        path_handler: Callable[[str], str] | None = None,
    ) -> None:
        super().__init__(parent)
        self._path_handler = path_handler
        self.setAcceptDrops(True)

    @staticmethod
    def _first_local_file_path(event) -> str:
        mime = event.mimeData() if event is not None else None
        if mime is None or not mime.hasUrls():
            return ""
        for url in mime.urls():
            if url.isLocalFile():
                local_path = str(url.toLocalFile() or "").strip()
                if local_path:
                    return local_path
        return ""

    def dragEnterEvent(self, event) -> None:  # noqa: N802
        path = self._first_local_file_path(event)
        if path and Path(path).exists() and Path(path).is_file():
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event) -> None:  # noqa: N802
        path = self._first_local_file_path(event)
        if path and Path(path).exists() and Path(path).is_file():
            event.acceptProposedAction()
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event) -> None:  # noqa: N802
        path = self._first_local_file_path(event)
        if not path:
            super().dropEvent(event)
            return
        accepted_path = str(path).strip()
        if self._path_handler is not None:
            try:
                accepted_path = str(self._path_handler(accepted_path) or "").strip()
            except Exception:
                accepted_path = ""
        if accepted_path:
            self.setText(accepted_path)
        event.acceptProposedAction()

class DepotAdminDialog(DepotFramelessToolWindow):
    def __init__(self, tracker: DepotTracker, current_user: str, app_window: "QuickInputsWindow" | None = None):
        super().__init__(app_window, window_title="User Setup", theme_kind="admin", size=(760, 520))
        self.tracker = tracker
        self.current_user = DepotRules.normalize_user_id(current_user)
        permission_service = getattr(self.tracker, "permission_service", None)
        if permission_service is not None:
            permission_service.require_admin_access(self.current_user)
        elif not self.tracker.is_admin_user(self.current_user):
            raise PermissionDeniedError(PermissionService.ADMIN_ACCESS_DENIED_MESSAGE)
        self._always_on_top_config_key = "admin_window_always_on_top"
        self._window_always_on_top = self._load_window_always_on_top_preference(self._always_on_top_config_key, default=False)
        self.set_window_always_on_top(self._window_always_on_top)
        self._users_cache: dict[str, dict[str, Any]] = {}
        self._roles_cache: dict[str, dict[str, Any]] = {}
        self._qa_flag_cache: dict[str, dict[str, Any]] = {}
        self._selected_role_name = ""

        self.whoami_label = QLabel(f"Current User: {self.current_user}")
        self.root_layout.addWidget(self.whoami_label)

        self.admin_tabs = QTabWidget(self)
        self.admin_tabs.setObjectName("AdminTabs")
        self.root_layout.addWidget(self.admin_tabs, 1)

        self.users_tab = QWidget()
        self.roles_tab = QWidget()
        self.qa_tab = QWidget()
        self.users_tab.setObjectName("AdminUsersTab")
        self.roles_tab.setObjectName("AdminRolesTab")
        self.qa_tab.setObjectName("AdminQaTab")
        self.admin_tabs.addTab(self.users_tab, "Users")
        self.admin_tabs.addTab(self.roles_tab, "Roles")
        self.admin_tabs.addTab(self.qa_tab, "Action Flags")

        self._build_users_tab()
        self._build_roles_tab()
        self._build_qa_tab()

        if self.app_window is not None:
            self.apply_theme_styles()

        self.refresh_roles()
        self.refresh_users()
        self.refresh_qa_flags()
        self._apply_read_only_ui_state()

    def _apply_read_only_ui_state(self) -> None:
        if not self.is_read_only_mode():
            return
        self._disable_widgets_for_read_only(
            [
                getattr(self, "user_id_input", None),
                getattr(self, "user_name_input", None),
                getattr(self, "user_location_input", None),
                getattr(self, "user_role_combo", None),
                getattr(self, "user_access_combo", None),
                getattr(self, "user_icon_input", None),
                getattr(self, "user_icon_browse", None),
                getattr(self, "user_save_btn", None),
                getattr(self, "user_remove_btn", None),
                getattr(self, "role_name_input", None),
                getattr(self, "role_behavior_combo", None),
                getattr(self, "role_save_btn", None),
                getattr(self, "role_remove_btn", None),
                getattr(self, "qa_flag_name_input", None),
                getattr(self, "qa_flag_severity_combo", None),
                getattr(self, "qa_flag_icon_input", None),
                getattr(self, "qa_flag_icon_browse", None),
                getattr(self, "qa_flag_save_btn", None),
                getattr(self, "qa_flag_remove_btn", None),
            ],
            "User Setup changes",
        )

    def apply_theme_styles(self) -> None:
        if self.app_window is None:
            return
        super().apply_theme_styles()

    def closeEvent(self, event) -> None:  # noqa: N802
        super().closeEvent(event)
        if event.isAccepted() and _visible_flowgrid_shell_window() is None:
            _ensure_shell_window_available(self.app_window)

    def _build_users_tab(self) -> None:
        layout = QVBoxLayout(self.users_tab)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        form = QFormLayout()
        self.user_id_input = QLineEdit()
        self.user_name_input = QLineEdit()
        self.user_location_input = QLineEdit()
        self.user_role_combo = QComboBox()
        self.user_role_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContentsOnFirstShow)
        self.user_access_combo = QComboBox()
        self.user_access_combo.addItem("None", DepotRules.ADMIN_ACCESS_NONE)
        self.user_access_combo.addItem("Reporting", DepotRules.ADMIN_ACCESS_REPORTING)
        self.user_access_combo.addItem("Admin", DepotRules.ADMIN_ACCESS_ADMIN)

        self.user_icon_input = DroppableImagePathLineEdit(
            self,
            path_handler=lambda path: self._accept_dropped_icon_path(
                path,
                role_key="user",
                failure_event_key="ui.user_setup_user_icon_open_failed",
                failure_summary="User icon drop failed because the image could not be decoded.",
            ),
        )
        self.user_icon_browse = QPushButton("Browse")
        self.user_icon_browse.setProperty("actionRole", "pick")
        icon_row = QHBoxLayout()
        icon_row.setContentsMargins(0, 0, 0, 0)
        icon_row.setSpacing(4)
        icon_row.addWidget(self.user_icon_input, 1)
        icon_row.addWidget(self.user_icon_browse, 0)
        icon_wrap = QWidget()
        icon_wrap.setLayout(icon_row)

        form.addRow("User ID", self.user_id_input)
        form.addRow("Name", self.user_name_input)
        form.addRow("Location", self.user_location_input)
        form.addRow("Role", self.user_role_combo)
        form.addRow("Access", self.user_access_combo)
        form.addRow("Icon", icon_wrap)
        layout.addLayout(form)

        btn_row = QHBoxLayout()
        self.user_save_btn = QPushButton("Add / Update")
        self.user_remove_btn = QPushButton("Remove")
        self.user_clear_btn = QPushButton("Clear")
        self.user_save_btn.setProperty("actionRole", "save")
        self.user_remove_btn.setProperty("actionRole", "reset")
        self.user_clear_btn.setProperty("actionRole", "pick")
        btn_row.addWidget(self.user_save_btn)
        btn_row.addWidget(self.user_remove_btn)
        btn_row.addWidget(self.user_clear_btn)
        layout.addLayout(btn_row)

        self.users_table = QTableWidget()
        configure_standard_table(
            self.users_table,
            ["User ID", "Name", "Location", "Role", "Access", "Icon"],
            resize_modes={
                0: QHeaderView.ResizeMode.ResizeToContents,
                1: QHeaderView.ResizeMode.Stretch,
                2: QHeaderView.ResizeMode.ResizeToContents,
                3: QHeaderView.ResizeMode.ResizeToContents,
                4: QHeaderView.ResizeMode.ResizeToContents,
                5: QHeaderView.ResizeMode.ResizeToContents,
            },
            stretch_last=True,
        )
        layout.addWidget(self.users_table, 1)

        self.user_icon_browse.clicked.connect(self._browse_user_icon)
        self.user_save_btn.clicked.connect(self._save_user)
        self.user_remove_btn.clicked.connect(self._remove_selected_user)
        self.user_clear_btn.clicked.connect(self._clear_user_form)
        self.users_table.itemSelectionChanged.connect(self._on_user_selected)
        self.users_table.cellDoubleClicked.connect(self._on_user_double_clicked_row)
        self._reload_user_role_combo()

    def _build_roles_tab(self) -> None:
        layout = QVBoxLayout(self.roles_tab)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        form = QFormLayout()
        self.role_name_input = QLineEdit()
        self.role_behavior_combo = QComboBox()
        for role_slot in (
            DepotRules.ROLE_SLOT_NONE,
            DepotRules.ROLE_SLOT_QA,
            DepotRules.ROLE_SLOT_TECH1,
            DepotRules.ROLE_SLOT_TECH2,
            DepotRules.ROLE_SLOT_TECH3,
            DepotRules.ROLE_SLOT_MP,
        ):
            self.role_behavior_combo.addItem(DepotRules.role_slot_label(role_slot), role_slot)
        form.addRow("Role Name", self.role_name_input)
        form.addRow("Behavior", self.role_behavior_combo)
        layout.addLayout(form)

        btn_row = QHBoxLayout()
        self.role_save_btn = QPushButton("Add / Update")
        self.role_remove_btn = QPushButton("Remove")
        self.role_clear_btn = QPushButton("Clear")
        self.role_save_btn.setProperty("actionRole", "save")
        self.role_remove_btn.setProperty("actionRole", "reset")
        self.role_clear_btn.setProperty("actionRole", "pick")
        btn_row.addWidget(self.role_save_btn)
        btn_row.addWidget(self.role_remove_btn)
        btn_row.addWidget(self.role_clear_btn)
        layout.addLayout(btn_row)

        self.roles_table = QTableWidget()
        configure_standard_table(
            self.roles_table,
            ["Role", "Behavior"],
            resize_modes={
                0: QHeaderView.ResizeMode.Stretch,
                1: QHeaderView.ResizeMode.ResizeToContents,
            },
            stretch_last=True,
        )
        layout.addWidget(self.roles_table, 1)

        self.role_save_btn.clicked.connect(self._save_role)
        self.role_remove_btn.clicked.connect(self._remove_selected_role)
        self.role_clear_btn.clicked.connect(self._clear_role_form)
        self.roles_table.itemSelectionChanged.connect(self._on_role_selected)
        self.roles_table.cellDoubleClicked.connect(self._on_role_double_clicked_row)

    def _build_qa_tab(self) -> None:
        layout = QVBoxLayout(self.qa_tab)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        form = QFormLayout()
        self.qa_flag_name_input = QLineEdit()
        self.qa_flag_severity_combo = QComboBox()
        for value in QA_FLAG_SEVERITY_OPTIONS:
            self.qa_flag_severity_combo.addItem(value)

        self.qa_flag_icon_input = DroppableImagePathLineEdit(
            self,
            path_handler=lambda path: self._accept_dropped_icon_path(
                path,
                role_key="qa_flag",
                failure_event_key="ui.admin_qa_flag_icon_open_failed",
                failure_summary="Action flag icon drop failed because the image could not be decoded.",
            ),
        )
        self.qa_flag_icon_browse = QPushButton("Browse")
        self.qa_flag_icon_browse.setProperty("actionRole", "pick")
        icon_row = QHBoxLayout()
        icon_row.setContentsMargins(0, 0, 0, 0)
        icon_row.setSpacing(4)
        icon_row.addWidget(self.qa_flag_icon_input, 1)
        icon_row.addWidget(self.qa_flag_icon_browse, 0)
        icon_wrap = QWidget()
        icon_wrap.setLayout(icon_row)

        form.addRow("Flag Name", self.qa_flag_name_input)
        form.addRow("Severity", self.qa_flag_severity_combo)
        form.addRow("Icon", icon_wrap)
        layout.addLayout(form)

        btn_row = QHBoxLayout()
        self.qa_flag_save_btn = QPushButton("Add / Update")
        self.qa_flag_remove_btn = QPushButton("Remove")
        self.qa_flag_clear_btn = QPushButton("Clear")
        self.qa_flag_save_btn.setProperty("actionRole", "save")
        self.qa_flag_remove_btn.setProperty("actionRole", "reset")
        self.qa_flag_clear_btn.setProperty("actionRole", "pick")
        btn_row.addWidget(self.qa_flag_save_btn)
        btn_row.addWidget(self.qa_flag_remove_btn)
        btn_row.addWidget(self.qa_flag_clear_btn)
        layout.addLayout(btn_row)

        self.qa_flags_table = QTableWidget()
        configure_standard_table(
            self.qa_flags_table,
            ["Flag Name", "Severity", "Icon"],
            resize_modes={
                0: QHeaderView.ResizeMode.Stretch,
                1: QHeaderView.ResizeMode.ResizeToContents,
                2: QHeaderView.ResizeMode.ResizeToContents,
            },
            stretch_last=True,
        )
        layout.addWidget(self.qa_flags_table, 1)

        self.qa_flag_icon_browse.clicked.connect(self._browse_qa_flag_icon)
        self.qa_flag_save_btn.clicked.connect(self._save_qa_flag)
        self.qa_flag_remove_btn.clicked.connect(self._remove_selected_qa_flag)
        self.qa_flag_clear_btn.clicked.connect(self._clear_qa_flag_form)
        self.qa_flags_table.itemSelectionChanged.connect(self._on_qa_flag_selected)
        self.qa_flags_table.cellDoubleClicked.connect(self._on_qa_flag_double_clicked_row)

    def _reload_user_role_combo(self) -> None:
        current_text = str(self.user_role_combo.currentText() or "").strip() if hasattr(self, "user_role_combo") else ""
        self.user_role_combo.blockSignals(True)
        self.user_role_combo.clear()
        self.user_role_combo.addItem("", "")
        for row in self._roles_cache.values():
            role_name = str(row.get("role_name", "") or "").strip()
            if role_name:
                self.user_role_combo.addItem(role_name, role_name)
        if current_text:
            index = self.user_role_combo.findData(current_text)
            if index < 0:
                index = self.user_role_combo.findText(current_text)
            self.user_role_combo.setCurrentIndex(index if index >= 0 else 0)
        else:
            self.user_role_combo.setCurrentIndex(0)
        self.user_role_combo.blockSignals(False)

    def refresh_roles(self) -> None:
        self._roles_cache.clear()
        self.roles_table.setRowCount(0)
        repository = getattr(self.tracker, "user_repository", None)
        rows = repository.list_role_definitions() if repository is not None else self.tracker.list_role_definitions()
        for row in rows:
            role_name = str(row.get("role_name", "") or "").strip()
            if not role_name:
                continue
            role_slot = DepotRules.normalize_role_slot(
                row.get("role_slot", ""),
                default=DepotRules.ROLE_SLOT_NONE,
            )
            row_idx = self.roles_table.rowCount()
            self.roles_table.insertRow(row_idx)
            role_item = QTableWidgetItem(role_name)
            role_item.setData(Qt.ItemDataRole.UserRole, role_name)
            behavior_item = QTableWidgetItem(DepotRules.role_slot_label(role_slot))
            behavior_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            self.roles_table.setItem(row_idx, 0, role_item)
            self.roles_table.setItem(row_idx, 1, behavior_item)
            self._roles_cache[role_name] = {
                "role_slot": role_slot,
                "sort_order": int(row.get("sort_order", 0) or 0),
            }
        self._reload_user_role_combo()

    def refresh_users(self) -> None:
        self._users_cache.clear()
        self.users_table.setRowCount(0)
        repository = getattr(self.tracker, "user_repository", None)
        rows = repository.list_setup_users() if repository is not None else self.tracker.list_setup_users()
        for row in rows:
            user_id = DepotRules.normalize_user_id(str(row.get("user_id", "") or ""))
            name = str(row.get("name", "") or "").strip()
            location = str(row.get("location", "") or "").strip()
            icon_path = str(row.get("icon_path", "") or "").strip()
            role_name = str(row.get("role_name", "") or "").strip()
            role_slot = DepotRules.normalize_role_slot(
                row.get("role_slot", ""),
                default=DepotRules.ROLE_SLOT_NONE,
            )
            access_level = DepotRules.normalize_admin_access_level(
                row.get("access_level", ""),
                default=DepotRules.ADMIN_ACCESS_NONE,
            )
            row_idx = self.users_table.rowCount()
            self.users_table.insertRow(row_idx)
            user_item = QTableWidgetItem(user_id)
            user_item.setData(Qt.ItemDataRole.UserRole, user_id)
            name_item = QTableWidgetItem(name)
            location_item = QTableWidgetItem(location or "-")
            role_item = QTableWidgetItem(role_name or "-")
            access_item = QTableWidgetItem(DepotRules.admin_access_label(access_level) if access_level else "-")
            icon_item = QTableWidgetItem(Path(icon_path).name if icon_path else "-")
            user_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            role_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            access_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            icon_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            if icon_path and Path(icon_path).exists():
                user_item.setIcon(QIcon(icon_path))
                icon_item.setIcon(QIcon(icon_path))
            self.users_table.setItem(row_idx, 0, user_item)
            self.users_table.setItem(row_idx, 1, name_item)
            self.users_table.setItem(row_idx, 2, location_item)
            self.users_table.setItem(row_idx, 3, role_item)
            self.users_table.setItem(row_idx, 4, access_item)
            self.users_table.setItem(row_idx, 5, icon_item)
            self._users_cache[user_id] = {
                "name": name,
                "location": location,
                "role_name": role_name,
                "role_slot": role_slot,
                "access_level": access_level,
                "icon_path": icon_path,
            }

    def refresh_agents(self) -> None:
        self.refresh_users()

    def refresh_qa_flags(self) -> None:
        self._qa_flag_cache.clear()
        self.qa_flags_table.setRowCount(0)
        for row in self.tracker.list_qa_flags():
            flag_name = str(row.get("flag_name", "") or "").strip()
            severity = str(row.get("severity", "Medium") or "Medium").strip()
            icon_path = str(row.get("icon_path", "") or "").strip()
            row_idx = self.qa_flags_table.rowCount()
            self.qa_flags_table.insertRow(row_idx)
            flag_item = QTableWidgetItem(flag_name)
            flag_item.setData(Qt.ItemDataRole.UserRole, flag_name)
            severity_item = QTableWidgetItem(severity)
            icon_item = QTableWidgetItem(Path(icon_path).name if icon_path else "-")
            flag_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            icon_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            if icon_path and Path(icon_path).exists():
                flag_item.setIcon(QIcon(icon_path))
                icon_item.setIcon(QIcon(icon_path))
            self.qa_flags_table.setItem(row_idx, 0, flag_item)
            self.qa_flags_table.setItem(row_idx, 1, severity_item)
            self.qa_flags_table.setItem(row_idx, 2, icon_item)
            self._qa_flag_cache[flag_name] = {
                "severity": severity,
                "icon_path": icon_path,
            }
        self._notify_qa_flag_list_changed()

    def refresh_admins(self) -> None:
        self.refresh_users()

    def _notify_user_list_changed(self) -> None:
        if self.app_window is None:
            return
        self.app_window._apply_depot_access_controls()
        self.app_window._refresh_shared_linked_views(
            "agent_missing_po",
            "dashboard_completed",
            "qa_assigned",
            "qa_delivered",
            "qa_missing_po",
            "qa_owner",
            reason="user_setup_users_changed",
        )

    def _notify_qa_flag_list_changed(self) -> None:
        if self.app_window is None:
            return
        self.app_window._refresh_shared_linked_views(
            "agent_category",
            "agent_parts",
            "dashboard_completed",
            "qa_assigned",
            "qa_delivered",
            "qa_flags",
            "qa_missing_po",
            reason="admin_qa_flags_changed",
        )

    def _clear_user_form(self) -> None:
        self.user_id_input.clear()
        self.user_name_input.clear()
        self.user_location_input.clear()
        self.user_role_combo.setCurrentIndex(0)
        self.user_access_combo.setCurrentIndex(0)
        self.user_icon_input.clear()

    def _clear_role_form(self) -> None:
        self._selected_role_name = ""
        self.role_name_input.clear()
        self.role_behavior_combo.setCurrentIndex(0)

    def _clear_qa_flag_form(self) -> None:
        self.qa_flag_name_input.clear()
        self.qa_flag_severity_combo.setCurrentIndex(1 if self.qa_flag_severity_combo.count() > 1 else 0)
        self.qa_flag_icon_input.clear()

    def _load_user_into_form(self, user_id: str) -> bool:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return False
        data = self._users_cache.get(normalized)
        if not data:
            return False
        self.user_id_input.setText(normalized)
        self.user_name_input.setText(str(data.get("name", "")))
        self.user_location_input.setText(str(data.get("location", "")))
        role_name = str(data.get("role_name", "") or "").strip()
        role_index = self.user_role_combo.findData(role_name)
        if role_index < 0:
            role_index = self.user_role_combo.findText(role_name)
        self.user_role_combo.setCurrentIndex(role_index if role_index >= 0 else 0)
        access_level = DepotRules.normalize_admin_access_level(
            data.get("access_level", ""),
            default=DepotRules.ADMIN_ACCESS_NONE,
        )
        access_index = self.user_access_combo.findData(access_level)
        self.user_access_combo.setCurrentIndex(access_index if access_index >= 0 else 0)
        self.user_icon_input.setText(str(data.get("icon_path", "")))
        return True

    def _load_role_into_form(self, role_name: str) -> bool:
        normalized_name = str(role_name or "").strip()
        if not normalized_name:
            return False
        details = self._roles_cache.get(normalized_name)
        if not details:
            return False
        self._selected_role_name = normalized_name
        self.role_name_input.setText(normalized_name)
        role_slot = DepotRules.normalize_role_slot(
            details.get("role_slot", ""),
            default=DepotRules.ROLE_SLOT_NONE,
        )
        role_index = self.role_behavior_combo.findData(role_slot)
        self.role_behavior_combo.setCurrentIndex(role_index if role_index >= 0 else 0)
        return True

    def _select_role_item(self, role_name: str) -> None:
        normalized_name = str(role_name or "").strip()
        if not normalized_name:
            return
        for idx in range(self.roles_table.rowCount()):
            item = self.roles_table.item(idx, 0)
            if item is None:
                continue
            if str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip().casefold() == normalized_name.casefold():
                self.roles_table.selectRow(idx)
                return

    def _select_user_item(self, user_id: str) -> None:
        normalized = DepotRules.normalize_user_id(user_id)
        if not normalized:
            return
        for idx in range(self.users_table.rowCount()):
            item = self.users_table.item(idx, 0)
            if item is None:
                continue
            row_user = DepotRules.normalize_user_id(str(item.data(Qt.ItemDataRole.UserRole) or item.text() or ""))
            if row_user == normalized:
                self.users_table.selectRow(idx)
                return

    def _on_user_selected(self) -> None:
        row = self.users_table.currentRow()
        if row < 0:
            return
        item = self.users_table.item(row, 0)
        if item is None:
            return
        user_id = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "")
        if not self._load_user_into_form(user_id):
            return

    def _on_user_double_clicked_row(self, row: int, _column: int) -> None:
        if row < 0:
            return
        item = self.users_table.item(row, 0)
        if item is None:
            return
        user_id = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "")
        if not self._load_user_into_form(user_id):
            return
        self.user_name_input.setFocus()
        self.user_name_input.selectAll()

    def _on_role_selected(self) -> None:
        row = self.roles_table.currentRow()
        if row < 0:
            return
        item = self.roles_table.item(row, 0)
        if item is None:
            return
        role_name = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip()
        self._load_role_into_form(role_name)

    def _on_role_double_clicked_row(self, row: int, _column: int) -> None:
        if row < 0:
            return
        item = self.roles_table.item(row, 0)
        if item is None:
            return
        role_name = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip()
        if not self._load_role_into_form(role_name):
            return
        self.role_name_input.setFocus()
        self.role_name_input.selectAll()

    def _on_qa_flag_selected(self) -> None:
        row = self.qa_flags_table.currentRow()
        if row < 0:
            return
        item = self.qa_flags_table.item(row, 0)
        if item is None:
            return
        flag_name = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip()
        if not flag_name:
            return
        details = self._qa_flag_cache.get(flag_name, {})
        self.qa_flag_name_input.setText(flag_name)
        severity = str(details.get("severity", "Medium") or "Medium")
        idx = self.qa_flag_severity_combo.findText(severity)
        self.qa_flag_severity_combo.setCurrentIndex(idx if idx >= 0 else 1)
        self.qa_flag_icon_input.setText(str(details.get("icon_path", "") or ""))

    def _on_qa_flag_double_clicked_row(self, row: int, _column: int) -> None:
        if row < 0:
            return
        self.qa_flags_table.selectRow(row)
        self._on_qa_flag_selected()

    @staticmethod
    def _image_file_dialog_filter() -> str:
        patterns: list[str] = []
        for fmt in QImageReader.supportedImageFormats():
            try:
                ext = bytes(fmt).decode("ascii", errors="ignore").strip().lower()
            except Exception:
                ext = ""
            if ext:
                patterns.append(f"*.{ext}")
        if patterns:
            deduped = " ".join(sorted(set(patterns)))
            return f"Images ({deduped});;All Files (*.*)"
        return "All Files (*.*)"

    @staticmethod
    def _read_icon_image(path: str) -> tuple[QImage, str]:
        reader = QImageReader(path)
        reader.setAutoTransform(True)
        image = reader.read()
        if not image.isNull():
            return image, ""
        error_text = str(reader.errorString() or "").strip()
        fallback_pixmap = QPixmap(path)
        if not fallback_pixmap.isNull():
            return fallback_pixmap.toImage(), ""
        if not error_text:
            error_text = "Unsupported or corrupted image format."
        return QImage(), error_text

    @staticmethod
    def _initial_image_browse_dir(current_path: str = "") -> str:
        raw_path = str(current_path or "").strip()
        if raw_path:
            candidate = Path(raw_path)
            if candidate.exists():
                if candidate.is_file():
                    return str(candidate.parent)
                if candidate.is_dir():
                    return str(candidate)
            parent_dir = candidate.parent if str(candidate.parent) else Path.home()
            if parent_dir.exists():
                return str(parent_dir)
        return str(Path.home())

    def _select_native_image_file(self, dialog_title: str, current_path: str = "") -> str:
        selected_path, _ = QFileDialog.getOpenFileName(
            self,
            str(dialog_title or "").strip() or "Select Image",
            self._initial_image_browse_dir(current_path),
            self._image_file_dialog_filter(),
        )
        return str(selected_path or "").strip()

    def _accept_dropped_icon_path(
        self,
        icon_path: str,
        *,
        role_key: str,
        failure_event_key: str,
        failure_summary: str,
    ) -> str:
        normalized_path = str(icon_path or "").strip()
        if not normalized_path:
            return ""
        return self._edit_icon_with_popup(
            normalized_path,
            role_key=role_key,
            failure_event_key=failure_event_key,
            failure_summary=failure_summary,
        )

    def _browse_qa_flag_icon(self) -> None:
        selected = self._select_icon_path_with_editor(
            "Select QA Flag Icon",
            role_key="qa_flag",
            failure_event_key="ui.admin_qa_flag_icon_open_failed",
            failure_summary="Action flag icon selection failed because the image could not be decoded.",
            current_path=str(self.qa_flag_icon_input.text() or ""),
        )
        if selected:
            self.qa_flag_icon_input.setText(selected)

    def _write_cropped_temp_icon(self, image: QImage, role_key: str = "agent") -> str:
        if image.isNull():
            return ""
        safe_key = re.sub(r"[^A-Za-z0-9_-]+", "_", str(role_key or "agent")).strip("_") or "agent"
        temp_dir = self.tracker.db.db_path.parent / ASSETS_DIR_NAME / "_icon_tmp" / safe_key
        try:
            temp_dir.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            _runtime_log_event(
                "ui.admin_crop_temp_dir_create_failed",
                severity="warning",
                summary="Failed creating temporary crop icon directory.",
                exc=exc,
                context={"temp_dir": str(temp_dir)},
            )
            return ""
        stamp = int(time.time() * 1000)
        temp_path = temp_dir / f"crop_{stamp}.png"
        try:
            if image.save(str(temp_path), "PNG"):
                return str(temp_path)
        except Exception as exc:
            _runtime_log_event(
                "ui.admin_crop_temp_write_failed",
                severity="warning",
                summary="Failed writing cropped icon temp file.",
                exc=exc,
                context={"temp_path": str(temp_path)},
            )
            return ""
        return ""

    def _edit_icon_with_popup(
        self,
        icon_path: str,
        *,
        role_key: str,
        failure_event_key: str,
        failure_summary: str,
    ) -> str:
        image, load_error = self._read_icon_image(icon_path)
        if image.isNull():
            _runtime_log_event(
                failure_event_key,
                severity="warning",
                summary=failure_summary,
                context={
                    "icon_path": str(icon_path),
                    "error": load_error,
                },
            )
            details = f"\n\nDetails: {load_error}" if load_error else ""
            self._show_themed_message(QMessageBox.Icon.Warning, "Invalid Image", f"Could not open that image file.{details}")
            return ""
        dialog = IconCropDialog(image, app_window=self.app_window, parent=self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return ""
        result_image = dialog.result_image()
        if result_image.isNull():
            return ""
        written = self._write_cropped_temp_icon(result_image, role_key=role_key)
        if not written:
            self._show_themed_message(QMessageBox.Icon.Warning, "Save Failed", "Failed to prepare cropped icon.")
        return written

    def _select_icon_path_with_editor(
        self,
        dialog_title: str,
        *,
        role_key: str,
        failure_event_key: str,
        failure_summary: str,
        current_path: str = "",
    ) -> str:
        icon_path = self._select_native_image_file(dialog_title, current_path=current_path)
        if not icon_path:
            return ""
        return self._edit_icon_with_popup(
            icon_path,
            role_key=role_key,
            failure_event_key=failure_event_key,
            failure_summary=failure_summary,
        )

    def _browse_user_icon(self) -> None:
        selected = self._select_icon_path_with_editor(
            "Select User Icon",
            role_key="user",
            failure_event_key="ui.user_setup_user_icon_open_failed",
            failure_summary="User icon selection failed because the image could not be decoded.",
            current_path=str(self.user_icon_input.text() or ""),
        )
        if selected:
            self.user_icon_input.setText(selected)

    def _save_user(self) -> None:
        if self._warn_if_read_only("User updates"):
            return
        user_id = DepotRules.normalize_user_id(self.user_id_input.text())
        name = str(self.user_name_input.text() or "").strip()
        location = str(self.user_location_input.text() or "").strip()
        role_name = str(self.user_role_combo.currentData() or self.user_role_combo.currentText() or "").strip()
        access_level = DepotRules.normalize_admin_access_level(
            self.user_access_combo.currentData(),
            default=DepotRules.ADMIN_ACCESS_NONE,
        )
        icon_path = str(self.user_icon_input.text() or "").strip()
        if not user_id or not name:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "User ID and Name are required.")
            return
        if not role_name:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Validation",
                "Role is required.",
            )
            return
        existing_row = self._users_cache.get(user_id, {})
        existing_access = DepotRules.normalize_admin_access_level(
            existing_row.get("access_level", ""),
            default=DepotRules.ADMIN_ACCESS_NONE,
        )
        if user_id == self.current_user and existing_access == DepotRules.ADMIN_ACCESS_ADMIN and access_level != DepotRules.ADMIN_ACCESS_ADMIN:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Validation",
                "Cannot remove your own admin access from User Setup.",
            )
            return
        if icon_path and not Path(icon_path).exists():
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Icon path does not exist. Browse and select a valid icon.")
            return
        repository = getattr(self.tracker, "user_repository", None)
        try:
            saved = (
                repository.upsert_setup_user(user_id, name, role_name, location, access_level, icon_path)
                if repository is not None
                else self.tracker.upsert_setup_user(user_id, name, role_name, location, access_level, icon_path)
            )
        except ValueError as exc:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", str(exc))
            return
        self.user_icon_input.setText(str(saved.get("icon_path", "") or ""))
        self.refresh_users()
        self._select_user_item(user_id)
        self._load_user_into_form(user_id)
        self._notify_user_list_changed()
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"User {user_id} updated.")

    def _remove_selected_user(self) -> None:
        if self._warn_if_read_only("User removal"):
            return
        row = self.users_table.currentRow()
        if row < 0:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a user to remove.")
            return
        item = self.users_table.item(row, 0)
        if item is None:
            return
        user_id = DepotRules.normalize_user_id(str(item.data(Qt.ItemDataRole.UserRole) or item.text() or ""))
        if not user_id:
            return
        row_data = self._users_cache.get(user_id, {})
        access_level = DepotRules.normalize_admin_access_level(
            row_data.get("access_level", ""),
            default=DepotRules.ADMIN_ACCESS_NONE,
        )
        if user_id == self.current_user and access_level == DepotRules.ADMIN_ACCESS_ADMIN:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Validation",
                "Cannot remove your own admin access from User Setup.",
            )
            return
        repository = getattr(self.tracker, "user_repository", None)
        if repository is not None:
            repository.delete_setup_user(user_id)
        else:
            self.tracker.delete_setup_user(user_id)
        self.refresh_users()
        self._clear_user_form()
        self._notify_user_list_changed()
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"User {user_id} removed.")

    def _save_role(self) -> None:
        if self._warn_if_read_only("Role updates"):
            return
        role_name = str(self.role_name_input.text() or "").strip()
        role_slot = DepotRules.normalize_role_slot(
            self.role_behavior_combo.currentData(),
            default=DepotRules.ROLE_SLOT_NONE,
        )
        if not role_name:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Role name is required.")
            return
        repository = getattr(self.tracker, "user_repository", None)
        try:
            saved = (
                repository.upsert_role_definition(role_name, role_slot, original_role_name=self._selected_role_name)
                if repository is not None
                else self.tracker.upsert_role_definition(role_name, role_slot, original_role_name=self._selected_role_name)
            )
        except ValueError as exc:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", str(exc))
            return
        self.refresh_roles()
        self.refresh_users()
        self._select_role_item(str(saved.get("role_name", "") or role_name))
        self._notify_user_list_changed()
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"Role {role_name} updated.")

    def _remove_selected_role(self) -> None:
        if self._warn_if_read_only("Role removal"):
            return
        row = self.roles_table.currentRow()
        if row < 0:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a role to remove.")
            return
        item = self.roles_table.item(row, 0)
        if item is None:
            return
        role_name = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip()
        if not role_name:
            return
        repository = getattr(self.tracker, "user_repository", None)
        try:
            if repository is not None:
                repository.delete_role_definition(role_name)
            else:
                self.tracker.delete_role_definition(role_name)
        except ValueError as exc:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", str(exc))
            return
        self.refresh_roles()
        self.refresh_users()
        self._clear_role_form()
        self._notify_user_list_changed()
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"Role {role_name} removed.")

    def _save_qa_flag(self) -> None:
        if self._warn_if_read_only("Action-flag updates"):
            return
        flag_name = str(self.qa_flag_name_input.text() or "").strip()
        severity = str(self.qa_flag_severity_combo.currentText() or "Medium").strip()
        icon_path = str(self.qa_flag_icon_input.text() or "").strip()
        if not flag_name:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Flag name is required.")
            return
        if icon_path and not Path(icon_path).exists():
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Flag icon path does not exist.")
            return
        selected_row = self.qa_flags_table.currentRow()
        selected_name = ""
        if selected_row >= 0:
            selected_item = self.qa_flags_table.item(selected_row, 0)
            if selected_item is not None:
                selected_name = str(selected_item.data(Qt.ItemDataRole.UserRole) or selected_item.text() or "").strip()
        if selected_name and selected_name != flag_name and flag_name in self._qa_flag_cache:
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Validation",
                "A QA flag with that name already exists.",
            )
            return
        self.tracker.upsert_qa_flag(flag_name, severity, icon_path)
        if selected_name and selected_name != flag_name:
            self.tracker.delete_qa_flag(selected_name)
        self.refresh_qa_flags()
        for row_idx in range(self.qa_flags_table.rowCount()):
            cell = self.qa_flags_table.item(row_idx, 0)
            if cell is None:
                continue
            name = str(cell.data(Qt.ItemDataRole.UserRole) or cell.text() or "").strip()
            if name == flag_name:
                self.qa_flags_table.selectRow(row_idx)
                break
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"QA flag '{flag_name}' updated.")

    def _remove_selected_qa_flag(self) -> None:
        if self._warn_if_read_only("Action-flag removal"):
            return
        row = self.qa_flags_table.currentRow()
        if row < 0:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a QA flag to remove.")
            return
        item = self.qa_flags_table.item(row, 0)
        if item is None:
            return
        flag_name = str(item.data(Qt.ItemDataRole.UserRole) or item.text() or "").strip()
        if not flag_name:
            return
        self.tracker.delete_qa_flag(flag_name)
        self.refresh_qa_flags()
        self._clear_qa_flag_form()
        self._show_themed_message(QMessageBox.Icon.Information, "Saved", f"QA flag '{flag_name}' removed.")

class DashboardTrendChart(QWidget):
    """Stacked daily chart for submission touch rows with distinct-unit markers."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._rows: list[dict[str, Any]] = []
        self._series_colors: dict[str, str] = {
            "complete": "#21B46D",
            "junk": "#D95A5A",
            "part_order": "#D3A327",
            "rtv": "#4F86D9",
            "triaged": "#20AFA8",
            "other": "#5B708A",
        }
        self.setMinimumHeight(250)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def clear_series(self) -> None:
        self._rows = []
        self.update()

    def set_series(self, rows: list[dict[str, Any]]) -> None:
        cleaned: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            entry_date = str(row.get("entry_date", "") or "").strip()
            if not entry_date:
                continue
            cleaned.append(
                {
                    "entry_date": entry_date,
                    "total_rows": int(max(0, safe_int(row.get("total_rows", 0), 0))),
                    "units": int(max(0, safe_int(row.get("units", 0), 0))),
                    "complete": int(max(0, safe_int(row.get("complete", 0), 0))),
                    "junk": int(max(0, safe_int(row.get("junk", 0), 0))),
                    "part_order": int(max(0, safe_int(row.get("part_order", 0), 0))),
                    "rtv": int(max(0, safe_int(row.get("rtv", 0), 0))),
                    "triaged": int(max(0, safe_int(row.get("triaged", 0), 0))),
                    "other": int(max(0, safe_int(row.get("other", 0), 0))),
                }
            )
        self._rows = cleaned
        self.update()

    @staticmethod
    def _label_for_date(raw_value: str) -> str:
        text = str(raw_value or "").strip()
        if len(text) >= 10:
            try:
                parsed = datetime.strptime(text[:10], "%Y-%m-%d")
                return parsed.strftime("%m-%d")
            except Exception:
                return text[:10]
        return text

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        bounds = self.rect().adjusted(0, 0, -1, -1)
        if bounds.width() <= 8 or bounds.height() <= 8:
            return

        panel_path = QPainterPath()
        panel_path.addRoundedRect(QRectF(bounds), 8.0, 8.0)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(12, 18, 24, 110))
        painter.drawPath(panel_path)
        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.setPen(QPen(QColor(255, 255, 255, 72), 1))
        painter.drawPath(panel_path)

        if not self._rows:
            painter.setPen(QColor(210, 220, 230, 170))
            painter.drawText(bounds, Qt.AlignmentFlag.AlignCenter, "No submission activity for the selected range.")
            return

        left_pad = 46
        right_pad = 12
        top_pad = 24
        bottom_pad = 44
        chart_rect = QRectF(
            float(bounds.left() + left_pad),
            float(bounds.top() + top_pad),
            float(max(10, bounds.width() - left_pad - right_pad)),
            float(max(10, bounds.height() - top_pad - bottom_pad)),
        )
        if chart_rect.width() <= 12 or chart_rect.height() <= 12:
            return

        max_value = 0
        for row in self._rows:
            max_value = max(
                max_value,
                int(row.get("total_rows", 0)),
                int(row.get("complete", 0)),
                int(row.get("junk", 0)),
                int(row.get("part_order", 0)),
                int(row.get("rtv", 0)),
                int(row.get("triaged", 0)),
                int(row.get("other", 0)),
            )
        max_value = max(1, max_value)

        guide_steps = 4
        painter.setPen(QPen(QColor(255, 255, 255, 42), 1))
        for idx in range(guide_steps + 1):
            ratio = float(idx) / float(guide_steps)
            y = chart_rect.bottom() - (chart_rect.height() * ratio)
            painter.drawLine(QPointF(chart_rect.left(), y), QPointF(chart_rect.right(), y))
            label_value = int(round(max_value * ratio))
            painter.setPen(QColor(225, 232, 240, 150))
            painter.drawText(
                QRectF(float(bounds.left() + 4), y - 10.0, float(left_pad - 8), 20.0),
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                str(label_value),
            )
            painter.setPen(QPen(QColor(255, 255, 255, 42), 1))

        row_count = len(self._rows)
        slot_width = chart_rect.width() / max(1, row_count)
        bar_width = max(6.0, min(22.0, slot_width * 0.62))
        complete_color = QColor(self._series_colors["complete"])
        junk_color = QColor(self._series_colors["junk"])
        part_order_color = QColor(self._series_colors["part_order"])
        rtv_color = QColor(self._series_colors["rtv"])
        triaged_color = QColor(self._series_colors["triaged"])
        other_color = QColor(self._series_colors["other"])
        label_step = max(1, math.ceil(row_count / 14.0))

        for idx, row in enumerate(self._rows):
            center_x = chart_rect.left() + (slot_width * idx) + (slot_width / 2.0)
            total_rows_value = int(row.get("total_rows", 0))
            units_value = int(row.get("units", 0))
            complete_value = int(row.get("complete", 0))
            junk_value = int(row.get("junk", 0))
            part_order_value = int(row.get("part_order", 0))
            rtv_value = int(row.get("rtv", 0))
            triaged_value = int(row.get("triaged", 0))
            other_value = int(row.get("other", 0))
            if total_rows_value <= 0:
                continue

            bar_left = center_x - (bar_width / 2.0)
            current_bottom = chart_rect.bottom()
            segments = (
                ("complete", complete_value, complete_color),
                ("junk", junk_value, junk_color),
                ("part_order", part_order_value, part_order_color),
                ("rtv", rtv_value, rtv_color),
                ("triaged", triaged_value, triaged_color),
                ("other", other_value, other_color),
            )
            positive_segment_indexes = [seg_idx for seg_idx, (_label, value, _color) in enumerate(segments) if int(max(0, value)) > 0]
            last_positive_index = positive_segment_indexes[-1] if positive_segment_indexes else -1
            painter.setPen(Qt.PenStyle.NoPen)
            for seg_idx, (_label, value, color) in enumerate(segments):
                safe_value = int(max(0, value))
                if safe_value <= 0:
                    continue
                seg_height = (float(safe_value) / float(max_value)) * chart_rect.height()
                segment_rect = QRectF(
                    bar_left,
                    current_bottom - seg_height,
                    bar_width,
                    seg_height,
                )
                painter.setBrush(color)
                if seg_idx == last_positive_index:
                    painter.drawRoundedRect(segment_rect, 3.0, 3.0)
                else:
                    painter.drawRect(segment_rect)
                current_bottom -= seg_height

            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.setPen(QPen(QColor(255, 255, 255, 38), 1))
            total_height = (float(total_rows_value) / float(max_value)) * chart_rect.height()
            total_rect = QRectF(
                bar_left,
                chart_rect.bottom() - total_height,
                bar_width,
                total_height,
            )
            painter.drawRoundedRect(total_rect, 3.0, 3.0)

            painter.setPen(QColor(230, 236, 243, 190))
            units_text_rect = QRectF(center_x - max(26.0, slot_width / 2.0), total_rect.top() - 18.0, max(52.0, slot_width), 14.0)
            painter.drawText(units_text_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignBottom, f"U:{units_value}")

            if idx % label_step == 0 or idx == row_count - 1:
                painter.setPen(QColor(225, 232, 240, 165))
                label_rect = QRectF(center_x - max(22.0, slot_width / 2.0), chart_rect.bottom() + 6.0, max(44.0, slot_width), 18.0)
                painter.drawText(label_rect, Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop, self._label_for_date(str(row.get("entry_date", ""))))

        legend_items = (
            ("Complete", complete_color),
            ("JO", junk_color),
            ("PO", part_order_color),
            ("RTV", rtv_color),
            ("Tri", triaged_color),
            ("Other", other_color),
        )
        legend_x = chart_rect.left()
        legend_y = float(bounds.top() + 4)
        for label, color in legend_items:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(color)
            painter.drawRoundedRect(QRectF(legend_x, legend_y, 12.0, 12.0), 2.0, 2.0)
            painter.setPen(QColor(230, 236, 243, 190))
            text_width = 58.0 if label in ("PO", "RTV", "Tri") else 76.0
            if label == "Other":
                text_width = 70.0
            painter.drawText(QRectF(legend_x + 16.0, legend_y - 2.0, text_width, 18.0), Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, label)
            legend_x += text_width + 18.0

class DepotDashboardDialog(DepotFramelessToolWindow):
    """Tracker data viewer using shared frameless chrome plus filtered submissions metrics."""

    TIMEFRAME_OPTIONS: tuple[tuple[str, str], ...] = (
        ("current_week", "Current Week"),
        ("today", "Today"),
        ("yesterday", "Yesterday"),
        ("last_7_days", "Last 7 Days"),
        ("last_30_days", "Last 30 Days"),
        ("this_month", "This Month"),
        ("all_time", "All Time"),
        ("custom", "Custom"),
    )

    TOUCH_ORDER: tuple[str, ...] = (
        DepotRules.TOUCH_COMPLETE,
        DepotRules.TOUCH_JUNK,
        DepotRules.TOUCH_PART_ORDER,
        DepotRules.TOUCH_RTV,
        "Triaged",
        DepotRules.TOUCH_OTHER,
    )

    TOUCH_COLORS: dict[str, str] = {
        DepotRules.TOUCH_COMPLETE: "#21B46D",
        DepotRules.TOUCH_JUNK: "#D95A5A",
        DepotRules.TOUCH_PART_ORDER: "#D3A327",
        DepotRules.TOUCH_RTV: "#4F86D9",
        "Triaged": "#20AFA8",
        DepotRules.TOUCH_OTHER: "#8E97A8",
    }

    def __init__(self, app_window: "QuickInputsWindow") -> None:
        super().__init__(
            app_window,
            window_title="Data Dashboard",
            theme_kind="dashboard",
            size=(1040, 620),
            minimum_size=(860, 500),
        )
        self.app_window = app_window
        permission_service = getattr(getattr(self.app_window, "app_context", None), "permission_service", None)
        if permission_service is not None:
            permission_service.require_dashboard_access(getattr(self.app_window, "current_user", ""))
        elif not self.app_window.depot_tracker.can_access_dashboard(getattr(self.app_window, "current_user", "")):
            raise PermissionDeniedError(PermissionService.DASHBOARD_ACCESS_DENIED_MESSAGE)
        self._always_on_top_config_key = "dashboard_window_always_on_top"
        self._window_always_on_top = self._load_window_always_on_top_preference(self._always_on_top_config_key, default=False)
        self.set_window_always_on_top(self._window_always_on_top)
        self._date_sync_in_progress = False
        self._dashboard_has_loaded = False
        self._dashboard_loading = False
        self._completed_loaded = False
        self._completed_loading = False
        self._notes_loaded = False
        self._notes_loading = False
        self._completed_search_timer = QTimer(self)
        self._completed_search_timer.setSingleShot(True)
        self._completed_search_timer.setInterval(250)
        self._completed_search_timer.timeout.connect(lambda: self.refresh_completed_parts(reason="search"))
        body = QWidget(self)
        self.root_layout.addWidget(body, 1)
        layout = QVBoxLayout(body)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(UI_SPACING_FRAMELESS_TOOL)

        subtitle = QLabel("Live table view from Tracker Hub data.")
        subtitle.setProperty("muted", True)
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(6)
        controls.addWidget(QLabel("Table:"), 0)
        self.table_combo = QComboBox()
        for table_name, label_text in TRACKER_DASHBOARD_TABLES:
            self.table_combo.addItem(label_text, table_name)
        self.table_combo.setMaxVisibleItems(max(8, len(TRACKER_DASHBOARD_TABLES)))
        self.table_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContentsOnFirstShow)
        self.table_combo.setMinimumWidth(220)
        self.table_combo.setMaximumWidth(340)
        controls.addWidget(self.table_combo, 0)
        controls.addSpacing(8)
        controls.addWidget(QLabel("Rows:"), 0)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(50, 5000)
        self.limit_spin.setSingleStep(50)
        self.limit_spin.setValue(300)
        self.limit_spin.setMinimumWidth(110)
        controls.addWidget(self.limit_spin, 0)
        controls.addStretch(1)
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setProperty("actionRole", "pick")
        self.export_btn = QPushButton("Export CSV")
        self.export_btn.setProperty("actionRole", "save")
        controls.addWidget(self.refresh_btn, 0)
        controls.addWidget(self.export_btn, 0)
        layout.addLayout(controls)

        self.dashboard_filters_wrap = QWidget(self)
        dashboard_filters = QHBoxLayout(self.dashboard_filters_wrap)
        dashboard_filters.setContentsMargins(0, 0, 0, 0)
        dashboard_filters.setSpacing(6)
        dashboard_filters.addWidget(QLabel("Category:"), 0)
        self.category_filter_combo = QComboBox()
        self.category_filter_combo.addItem("All Categories", "")
        self.category_filter_combo.setMinimumWidth(180)
        dashboard_filters.addWidget(self.category_filter_combo, 0)
        dashboard_filters.addStretch(1)
        layout.addWidget(self.dashboard_filters_wrap)

        self.submission_filters_wrap = QWidget(self)
        submission_filters = QHBoxLayout(self.submission_filters_wrap)
        submission_filters.setContentsMargins(0, 0, 0, 0)
        submission_filters.setSpacing(6)
        submission_filters.addWidget(QLabel("Range:"), 0)
        self.timeframe_combo = QComboBox()
        for key, label in self.TIMEFRAME_OPTIONS:
            self.timeframe_combo.addItem(label, key)
        submission_filters.addWidget(self.timeframe_combo, 0)
        submission_filters.addWidget(QLabel("From:"), 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        submission_filters.addWidget(self.start_date_edit, 0)
        submission_filters.addWidget(QLabel("To:"), 0)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        submission_filters.addWidget(self.end_date_edit, 0)
        submission_filters.addWidget(QLabel("User:"), 0)
        self.user_filter_combo = QComboBox()
        self.user_filter_combo.addItem("All Users", "")
        self.user_filter_combo.setMinimumWidth(160)
        submission_filters.addWidget(self.user_filter_combo, 0)
        submission_filters.addStretch(1)
        layout.addWidget(self.submission_filters_wrap)

        self.touch_summary_label = QLabel("")
        self.touch_summary_label.setProperty("muted", True)
        self.touch_summary_label.setWordWrap(True)
        layout.addWidget(self.touch_summary_label)

        self.touch_bar = TouchDistributionBar(self)
        layout.addWidget(self.touch_bar)

        self.touch_legend_label = QLabel("")
        self.touch_legend_label.setWordWrap(True)
        layout.addWidget(self.touch_legend_label)

        self.empty_hint = QLabel("")
        self.empty_hint.setProperty("muted", True)
        self.empty_hint.setWordWrap(True)
        self.empty_hint.hide()
        layout.addWidget(self.empty_hint)

        self.results_tabs = QTabWidget(self)
        self.list_tab = QWidget(self.results_tabs)
        self.table_tab = QWidget(self.results_tabs)
        self.completed_tab = QWidget(self.results_tabs)
        self.notes_tab = QWidget(self.results_tabs)
        self.results_tabs.addTab(self.list_tab, "List")
        self.results_tabs.addTab(self.table_tab, "Table")
        self.results_tabs.addTab(self.completed_tab, "Completed")
        self.results_tabs.addTab(self.notes_tab, "Notes")
        layout.addWidget(self.results_tabs, 1)

        list_layout = QVBoxLayout(self.list_tab)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setSpacing(4)
        self.table = QTableWidget()
        configure_standard_table(self.table, [])
        list_layout.addWidget(self.table, 1)

        table_layout = QVBoxLayout(self.table_tab)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(4)
        self.table_trend_chart = DashboardTrendChart(self.table_tab)
        table_layout.addWidget(self.table_trend_chart, 1)
        self.table_placeholder_label = QLabel("Chart view is currently reserved for Submissions.")
        self.table_placeholder_label.setWordWrap(True)
        self.table_placeholder_label.setProperty("muted", True)
        self.table_placeholder_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        table_layout.addWidget(self.table_placeholder_label, 0)

        self._build_completed_tab()

        notes_layout = QVBoxLayout(self.notes_tab)
        notes_layout.setContentsMargins(0, 0, 0, 0)
        notes_layout.setSpacing(4)

        notes_intro = QLabel("Testing utility: edit note fields on existing rows.")
        notes_intro.setWordWrap(True)
        notes_intro.setProperty("muted", True)
        notes_layout.addWidget(notes_intro)

        notes_controls = QHBoxLayout()
        notes_controls.setContentsMargins(0, 0, 0, 0)
        notes_controls.setSpacing(6)
        notes_controls.addWidget(QLabel("Field:"), 0)
        self.notes_target_combo = QComboBox()
        self.notes_target_combo.setMinimumWidth(240)
        notes_controls.addWidget(self.notes_target_combo, 0)
        notes_controls.addWidget(QLabel("Rows:"), 0)
        self.notes_limit_spin = QSpinBox()
        self.notes_limit_spin.setRange(25, 5000)
        self.notes_limit_spin.setSingleStep(25)
        self.notes_limit_spin.setValue(200)
        self.notes_limit_spin.setMinimumWidth(100)
        notes_controls.addWidget(self.notes_limit_spin, 0)
        notes_controls.addWidget(QLabel("Work Order:"), 0)
        self.notes_work_order_filter = QLineEdit()
        self.notes_work_order_filter.setPlaceholderText("Optional filter")
        self.notes_work_order_filter.setMinimumWidth(180)
        notes_controls.addWidget(self.notes_work_order_filter, 0)
        self.notes_refresh_btn = QPushButton("Load")
        self.notes_refresh_btn.setProperty("actionRole", "pick")
        notes_controls.addWidget(self.notes_refresh_btn, 0)
        notes_controls.addStretch(1)
        notes_layout.addLayout(notes_controls)

        self.notes_table = QTableWidget()
        notes_headers = ["id", "created_at", "user_id", "work_order", "note_preview"]
        note_resize_modes = {
            0: QHeaderView.ResizeMode.ResizeToContents,
            1: QHeaderView.ResizeMode.ResizeToContents,
            2: QHeaderView.ResizeMode.ResizeToContents,
            3: QHeaderView.ResizeMode.ResizeToContents,
            4: QHeaderView.ResizeMode.Stretch,
        }
        configure_standard_table(self.notes_table, notes_headers, resize_modes=note_resize_modes, stretch_last=True)
        notes_layout.addWidget(self.notes_table, 1)

        self.notes_selected_label = QLabel("Select a row to edit.")
        self.notes_selected_label.setWordWrap(True)
        self.notes_selected_label.setProperty("muted", True)
        notes_layout.addWidget(self.notes_selected_label)

        self.notes_editor = QTextEdit()
        self.notes_editor.setPlaceholderText("Selected note text...")
        self.notes_editor.setMinimumHeight(110)
        self.notes_editor.setEnabled(False)
        notes_layout.addWidget(self.notes_editor)

        notes_actions = QHBoxLayout()
        notes_actions.setContentsMargins(0, 0, 0, 0)
        notes_actions.setSpacing(6)
        self.notes_save_btn = QPushButton("Save Note")
        self.notes_save_btn.setProperty("actionRole", "save")
        self.notes_save_btn.setEnabled(False)
        notes_actions.addWidget(self.notes_save_btn, 0)
        self.notes_status_label = QLabel("")
        self.notes_status_label.setWordWrap(True)
        self.notes_status_label.setProperty("muted", True)
        notes_actions.addWidget(self.notes_status_label, 1)
        notes_layout.addLayout(notes_actions)

        self._notes_selected_row_id: int | None = None

        self.table_combo.currentIndexChanged.connect(self._on_table_changed)
        self.limit_spin.valueChanged.connect(lambda _value: self.refresh_dashboard(reason="limit-change"))
        self.refresh_btn.clicked.connect(lambda _checked=False: self.refresh_dashboard(force=True, reason="manual"))
        self.export_btn.clicked.connect(self.export_csv)
        self.timeframe_combo.currentIndexChanged.connect(self._on_timeframe_changed)
        self.start_date_edit.dateChanged.connect(self._on_custom_date_changed)
        self.end_date_edit.dateChanged.connect(self._on_custom_date_changed)
        self.user_filter_combo.currentIndexChanged.connect(lambda _index: self.refresh_dashboard(reason="user-filter"))
        self.category_filter_combo.currentIndexChanged.connect(self._on_dashboard_category_changed)
        self.notes_target_combo.currentIndexChanged.connect(lambda _index: self._on_notes_filter_changed())
        self.notes_limit_spin.valueChanged.connect(lambda _value: self._on_notes_filter_changed())
        self.notes_work_order_filter.returnPressed.connect(lambda: self.refresh_notes_rows(force=True, reason="notes-filter"))
        self.notes_refresh_btn.clicked.connect(lambda _checked=False: self.refresh_notes_rows(force=True, reason="manual"))
        self.notes_table.itemSelectionChanged.connect(self._on_notes_selection_changed)
        self.notes_save_btn.clicked.connect(self._save_selected_note)
        self.results_tabs.currentChanged.connect(self._on_results_tab_changed)

        self._set_timeframe_key("current_week")
        self._populate_submission_user_filter()
        self._populate_dashboard_category_filter()
        self._populate_notes_targets()
        self.apply_theme_styles()
        self.refresh_combo_popup_width()
        QTimer.singleShot(0, lambda: self.refresh_dashboard(reason="window-open"))
        self._apply_read_only_ui_state()

    def _apply_read_only_ui_state(self) -> None:
        if not self.is_read_only_mode():
            return
        self._disable_widgets_for_read_only(
            [
                getattr(self, "notes_editor", None),
                getattr(self, "notes_save_btn", None),
                getattr(self, "completed_open_notes_btn", None),
            ],
            "Dashboard note updates",
        )

    def closeEvent(self, event) -> None:  # noqa: N802
        try:
            self.app_window.cancel_depot_reads(
                "dashboard.close",
                "dashboard_metrics",
                "dashboard_completed",
                "dashboard_notes",
            )
        except Exception as exc:
            _runtime_log_event(
                "ui.depot_dashboard_async_cancel_failed",
                severity="warning",
                summary="Dashboard failed cancelling background reads while closing.",
                exc=exc,
            )
        self.app_window.config.setdefault("popup_positions", {})["depot_dashboard"] = {
            "x": int(self.x()),
            "y": int(self.y()),
        }
        self.app_window.queue_save_config()
        super().closeEvent(event)
        if event.isAccepted() and _visible_flowgrid_shell_window() is None:
            _ensure_shell_window_available(self.app_window)

    def apply_theme_styles(self) -> None:
        if self.app_window is None:
            return
        super().apply_theme_styles()
        muted = rgba_css(self.app_window.palette_data["label_text"], 0.80)
        self.setStyleSheet(
            self.styleSheet()
            + (
                "QLabel[muted='true'] {"
                f"color: {muted};"
                "font-weight: 700;"
                "}"
                "QLabel[section='true'] {"
                "font-size: 14px;"
                "font-weight: 800;"
                "}"
            )
        )

    def _on_table_changed(self) -> None:
        self.refresh_combo_popup_width()
        self.refresh_dashboard(reason="table-change")

    def _on_dashboard_category_changed(self) -> None:
        self.refresh_combo_popup_width()
        self.refresh_dashboard(reason="category-filter")
        if self._completed_loaded or self.results_tabs.currentWidget() is self.completed_tab:
            self.refresh_completed_parts(force=True, reason="category-filter")
        if self._notes_loaded or self.results_tabs.currentWidget() is self.notes_tab:
            self.refresh_notes_rows(force=True, reason="category-filter")

    def _on_timeframe_changed(self) -> None:
        key = str(self.timeframe_combo.currentData() or "").strip()
        if key:
            self._set_timeframe_key(key)
        self.refresh_dashboard(reason="timeframe-change")

    def _on_custom_date_changed(self) -> None:
        if self._date_sync_in_progress:
            return
        self._date_sync_in_progress = True
        try:
            if self.start_date_edit.date() > self.end_date_edit.date():
                self.end_date_edit.setDate(self.start_date_edit.date())
            if str(self.timeframe_combo.currentData() or "") != "custom":
                idx = self.timeframe_combo.findData("custom")
                if idx >= 0:
                    self.timeframe_combo.setCurrentIndex(idx)
        finally:
            self._date_sync_in_progress = False
        self.refresh_dashboard(reason="date-change")

    def _on_results_tab_changed(self, _index: int) -> None:
        current = self.results_tabs.currentWidget()
        if current is self.completed_tab and not self._completed_loaded:
            self.refresh_completed_parts(reason="tab-show")
        elif current is self.notes_tab and not self._notes_loaded:
            self.refresh_notes_rows(reason="tab-show")

    def _on_notes_filter_changed(self) -> None:
        self._notes_loaded = False
        if self.results_tabs.currentWidget() is self.notes_tab:
            self.refresh_notes_rows(reason="notes-filter")

    def _set_timeframe_key(self, key: str) -> None:
        today = QDate.currentDate()
        start = today
        end = today
        if key == "current_week":
            start = today.addDays(1 - int(today.dayOfWeek()))
            end = start.addDays(6)
        elif key == "today":
            start = today
            end = today
        elif key == "yesterday":
            start = today.addDays(-1)
            end = start
        elif key == "last_7_days":
            end = today
            start = today.addDays(-6)
        elif key == "last_30_days":
            end = today
            start = today.addDays(-29)
        elif key == "this_month":
            start = QDate(today.year(), today.month(), 1)
            end = start.addMonths(1).addDays(-1)
        elif key == "all_time":
            start = today.addDays(-3650)
            end = today
        elif key == "custom":
            return
        self._date_sync_in_progress = True
        try:
            self.start_date_edit.setDate(start)
            self.end_date_edit.setDate(end)
        finally:
            self._date_sync_in_progress = False

    def _current_submission_filters(self) -> tuple[str | None, str | None, str | None]:
        key = str(self.timeframe_combo.currentData() or "current_week").strip()
        start_date: str | None = None
        end_date: str | None = None
        if key != "all_time":
            start_qdate = self.start_date_edit.date()
            end_qdate = self.end_date_edit.date()
            if start_qdate > end_qdate:
                start_qdate, end_qdate = end_qdate, start_qdate
            start_date = start_qdate.toString("yyyy-MM-dd")
            end_date = end_qdate.toString("yyyy-MM-dd")
        selected_user = str(self.user_filter_combo.currentData() or "").strip()
        return start_date, end_date, (selected_user if selected_user else None)

    def _latest_workload_mix_enabled(self, user_id: str | None) -> bool:
        timeframe_key = str(self.timeframe_combo.currentData() or "").strip()
        return bool(str(user_id or "").strip() and timeframe_key == "last_30_days")

    def _current_dashboard_category_filter(self) -> str | None:
        selected_category = str(self.category_filter_combo.currentData() or "").strip()
        return selected_category if selected_category else None

    def _apply_dashboard_category_filter_options(self, categories: list[str], selected_category: str = "") -> None:
        selected_category = str(selected_category or "").strip()
        self.category_filter_combo.blockSignals(True)
        try:
            self.category_filter_combo.clear()
            self.category_filter_combo.addItem("All Categories", "")
            seen = {""}
            for category_text in categories:
                normalized_text = str(category_text or "").strip()
                if not normalized_text:
                    continue
                key = normalized_text.casefold()
                if key in seen:
                    continue
                seen.add(key)
                self.category_filter_combo.addItem(normalized_text, normalized_text)
            if selected_category:
                selected_index = self.category_filter_combo.findData(selected_category)
                if selected_index < 0:
                    self.category_filter_combo.addItem(selected_category, selected_category)
                    selected_index = self.category_filter_combo.findData(selected_category)
                if selected_index >= 0:
                    self.category_filter_combo.setCurrentIndex(selected_index)
        finally:
            self.category_filter_combo.blockSignals(False)

    def _populate_dashboard_category_filter(self) -> None:
        selected_category = str(self.category_filter_combo.currentData() or "").strip()
        self._apply_dashboard_category_filter_options(list(DepotRules.CATEGORY_OPTIONS), selected_category)

    def _apply_submission_user_filter_options(self, users: list[str], selected_user: str = "") -> None:
        selected_user = str(selected_user or "").strip()
        self.user_filter_combo.blockSignals(True)
        try:
            self.user_filter_combo.clear()
            self.user_filter_combo.addItem("All Users", "")
            seen = {""}
            for user_id in users:
                normalized_user = DepotRules.normalize_user_id(str(user_id or ""))
                if not normalized_user or normalized_user in seen:
                    continue
                seen.add(normalized_user)
                self.user_filter_combo.addItem(normalized_user, normalized_user)
            if selected_user:
                idx = self.user_filter_combo.findData(selected_user)
                if idx < 0:
                    self.user_filter_combo.addItem(selected_user, selected_user)
                    idx = self.user_filter_combo.findData(selected_user)
                if idx >= 0:
                    self.user_filter_combo.setCurrentIndex(idx)
        finally:
            self.user_filter_combo.blockSignals(False)

    def _populate_submission_user_filter(self) -> None:
        selected_user = str(self.user_filter_combo.currentData() or "").strip()
        self._apply_submission_user_filter_options([], selected_user)

    @staticmethod
    def _note_preview_text(value: str, max_len: int = 120) -> str:
        compact = " ".join(str(value or "").replace("\r", "\n").splitlines()).strip()
        if len(compact) <= int(max_len):
            return compact
        safe_max = max(8, int(max_len))
        return compact[: safe_max - 3].rstrip() + "..."

    def _populate_notes_targets(self) -> None:
        selected_key = str(self.notes_target_combo.currentData() or "").strip()
        options: list[tuple[str, str]] = []
        try:
            options = self.app_window.depot_tracker.dashboard_note_target_options()
        except Exception as exc:
            _runtime_log_event(
                "ui.depot_dashboard_notes_target_load_failed",
                severity="warning",
                summary="Dashboard notes editor failed loading editable target list.",
                exc=exc,
            )

        self.notes_target_combo.blockSignals(True)
        self.notes_target_combo.clear()
        for key, label in options:
            self.notes_target_combo.addItem(str(label), str(key))
        if selected_key:
            idx = self.notes_target_combo.findData(selected_key)
            if idx >= 0:
                self.notes_target_combo.setCurrentIndex(idx)
        self.notes_target_combo.blockSignals(False)
        self.notes_refresh_btn.setEnabled(bool(options))
        self.notes_limit_spin.setEnabled(bool(options))
        self.notes_work_order_filter.setEnabled(bool(options))
        self.notes_save_btn.setEnabled(False)

    def _build_completed_tab(self) -> None:
        layout = QVBoxLayout(self.completed_tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        summary = QLabel("Completed parts queue moved from QA. Shows closed work orders with part history.")
        summary.setWordWrap(True)
        summary.setProperty("muted", True)
        layout.addWidget(summary)

        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(6)
        controls.addWidget(QLabel("Work Order:"), 0)
        self.completed_workorder_search = QLineEdit()
        self.completed_workorder_search.setPlaceholderText("Search work order...")
        self.completed_workorder_search.setClearButtonEnabled(True)
        controls.addWidget(self.completed_workorder_search, 1)
        self.completed_refresh_btn = QPushButton("Refresh")
        self.completed_refresh_btn.setProperty("actionRole", "pick")
        self.completed_open_notes_btn = QPushButton("Open Notes / Flag")
        self.completed_open_notes_btn.setProperty("actionRole", "pick")
        controls.addWidget(self.completed_refresh_btn, 0)
        controls.addWidget(self.completed_open_notes_btn, 0)
        layout.addLayout(controls)

        self.completed_table = QTableWidget()
        configure_standard_table(
            self.completed_table,
            ["Client", "Flag", "Age", "Working", "Work Order", "Repair Owner", "Category", "Outcome", "Closed At", "QA Note", "Agent Note"],
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
                9: QHeaderView.ResizeMode.ResizeToContents,
                10: QHeaderView.ResizeMode.Stretch,
            },
            stretch_last=True,
        )
        self.completed_table.itemDoubleClicked.connect(lambda item: _copy_work_order_with_notice(self, item))
        self.completed_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.completed_table.customContextMenuRequested.connect(self._open_completed_notes_from_context)
        layout.addWidget(self.completed_table, 1)
        self.completed_status_label = QLabel("")
        self.completed_status_label.setWordWrap(True)
        self.completed_status_label.setProperty("muted", True)
        layout.addWidget(self.completed_status_label, 0)

        self.completed_workorder_search.textChanged.connect(lambda _text: self._completed_search_timer.start())
        self.completed_refresh_btn.clicked.connect(lambda _checked=False: self.refresh_completed_parts(force=True, reason="manual"))
        self.completed_open_notes_btn.clicked.connect(self._open_selected_completed_notes)

    def _open_completed_notes_from_context(self, pos: QPoint) -> None:
        if not _select_table_row_by_context_pos(self.completed_table, pos):
            return
        self._open_selected_completed_notes()

    def _open_selected_completed_notes(self) -> None:
        if self._warn_if_read_only("Completed-part note updates"):
            return
        saved, _part_id = _edit_part_notes(
            self,
            self.app_window.depot_tracker,
            role="qa",
            table=self.completed_table,
        )
        if not saved:
            return
        self.app_window._refresh_shared_linked_views("dashboard_completed", force=True, reason="dashboard_completed_note")

    @staticmethod
    def _load_dashboard_completed_payload(
        tracker: DepotTracker,
        *,
        search_text: str,
        category_filter: str | None,
    ) -> dict[str, Any]:
        rows = tracker.list_completed_parts(search_text, category_filter=category_filter)
        fallback_map = {
            DepotRules.normalize_work_order(str(row["work_order"] or "")): str(row["category"] or "").strip()
            for row in rows
            if DepotRules.normalize_work_order(str(row["work_order"] or ""))
        }
        category_map = tracker.resolve_work_order_categories_bulk(list(fallback_map.keys()), fallback_map)
        agent_meta = tracker.agent_display_map()
        flag_icon_map = {
            str(row.get("flag_name", "") or "").strip(): str(row.get("icon_path", "") or "").strip()
            for row in tracker.list_qa_flags()
            if str(row.get("flag_name", "") or "").strip()
        }
        rendered_rows: list[dict[str, Any]] = []
        for row in rows:
            row_payload = {str(key): row[key] for key in row.keys()}
            work_order = DepotRules.normalize_work_order(str(row_payload.get("work_order", "") or ""))
            row_payload["resolved_category"] = (
                category_map.get(work_order, "") or str(row_payload.get("category", "") or "").strip() or "Other"
            )
            qa_flag = str(row_payload.get("qa_flag", "") or "").strip()
            legacy_icon = str(row_payload.get("qa_flag_image_path", "") or "").strip()
            row_payload["resolved_qa_flag_icon"] = flag_icon_map.get(qa_flag, "")
            if not row_payload["resolved_qa_flag_icon"] and legacy_icon:
                row_payload["resolved_qa_flag_icon"] = tracker.resolve_part_flag_image_path(legacy_icon)
            rendered_rows.append(row_payload)
        return {
            "rows": rendered_rows,
            "agent_meta": {
                str(user_id): {"name": str(meta[0] or ""), "icon_path": str(meta[1] or "")}
                for user_id, meta in agent_meta.items()
            },
            "search_text": str(search_text or ""),
            "category_filter": str(category_filter or ""),
        }

    def refresh_completed_parts(self, *, force: bool = False, reason: str = "") -> None:
        if not hasattr(self, "completed_table"):
            return

        search_text = ""
        if hasattr(self, "completed_workorder_search"):
            search_text = str(self.completed_workorder_search.text() or "").strip()
        category_filter = self._current_dashboard_category_filter()
        state_key = {
            "search_text": search_text,
            "category_filter": category_filter or "",
        }
        self._completed_loading = True
        self.completed_refresh_btn.setEnabled(False)
        if hasattr(self, "completed_status_label"):
            self.completed_status_label.setText("Loading completed rows...")
        if self.completed_table.rowCount() <= 0:
            self.completed_table.setRowCount(0)

        def _loader(worker_tracker: DepotTracker) -> dict[str, Any]:
            return DepotDashboardDialog._load_dashboard_completed_payload(
                worker_tracker,
                search_text=search_text,
                category_filter=category_filter,
            )

        request = self.app_window.start_depot_read(
            "dashboard_completed",
            state_key,
            reason=reason or "completed-refresh",
            force=force,
            loader=_loader,
            on_success=self._apply_completed_parts_result,
            on_error=self._handle_completed_parts_error,
        )
        if request is None:
            self._completed_loading = False
            self.completed_refresh_btn.setEnabled(True)
            if hasattr(self, "completed_status_label"):
                self.completed_status_label.setText("Could not start completed-row load. Details were logged for support.")
            return

    def _apply_completed_parts_result(self, result: DepotLoadResult) -> None:
        self._completed_loading = False
        self._completed_loaded = True
        if hasattr(self, "completed_refresh_btn"):
            self.completed_refresh_btn.setEnabled(True)
        payload = result.payload if isinstance(result.payload, dict) else {}
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            rows = []
        agent_meta_raw = payload.get("agent_meta", {})
        agent_meta = agent_meta_raw if isinstance(agent_meta_raw, dict) else {}
        valid_rows = [row for row in rows if isinstance(row, dict)]
        self.completed_table.setRowCount(len(valid_rows))
        if hasattr(self, "completed_status_label"):
            self.completed_status_label.setText(f"Loaded {len(valid_rows)} completed row(s).")
        for row_idx, r in enumerate(valid_rows):
            part_id = int(max(0, safe_int(r.get("id", 0), 0)))
            work_order = str(r.get("work_order", "") or "").strip()
            assigned = DepotRules.normalize_user_id(str(r.get("assigned_user_id", "") or ""))
            category = str(r.get("resolved_category", "") or r.get("category", "") or "").strip() or "Other"
            age_text = DepotAgentWindow._part_age_label(str(r.get("created_at", "") or ""))
            qa_comment = str(r.get("qa_comment", "") or r.get("comments", "") or "").strip()
            agent_comment = str(r.get("agent_comment", "") or "").strip()
            flag = str(r.get("qa_flag", "") or "").strip()
            working_user = DepotRules.normalize_user_id(str(r.get("working_user_id", "") or ""))
            working_stamp = str(r.get("working_updated_at", "") or "").strip()
            outcome_text = str(r.get("latest_touch", "") or "").strip()
            closed_at_raw = str(r.get("latest_touch_at", "") or "").strip()
            closed_at_text = self._normalize_dashboard_datetime(closed_at_raw) if closed_at_raw else "-"

            image_abs = str(r.get("resolved_qa_flag_icon", "") or "")
            assigned_meta = agent_meta.get(assigned, {})
            assigned_name = str(assigned_meta.get("name", "") or "") if isinstance(assigned_meta, dict) else ""
            assigned_icon = str(assigned_meta.get("icon_path", "") or "") if isinstance(assigned_meta, dict) else ""

            client_item = QTableWidgetItem("")
            client_item.setData(Qt.ItemDataRole.UserRole, part_id)
            if int(max(0, safe_int(r.get("client_unit", 0), 0))):
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
                friendly_stamp = DepotAgentWindow._format_working_updated_stamp(working_stamp)
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

            work_item = QTableWidgetItem(work_order)
            work_item.setData(Qt.ItemDataRole.UserRole, part_id)
            qa_note_item = QTableWidgetItem(DepotAgentWindow._note_preview(qa_comment))
            qa_note_item.setToolTip(f"QA Note: {qa_comment if qa_comment else '(none)'}")
            agent_note_item = QTableWidgetItem(DepotAgentWindow._note_preview(agent_comment))
            agent_note_item.setToolTip(f"Agent Note: {agent_comment if agent_comment else '(none)'}")

            self.completed_table.setItem(row_idx, 0, _center_table_item(client_item))
            self.completed_table.setItem(row_idx, 1, _center_table_item(flag_item))
            self.completed_table.setItem(row_idx, 2, _center_table_item(QTableWidgetItem(age_text)))
            self.completed_table.setItem(row_idx, 3, _center_table_item(working_item))
            self.completed_table.setItem(row_idx, 4, _center_table_item(work_item))
            self.completed_table.setItem(row_idx, 5, _center_table_item(assigned_item))
            self.completed_table.setItem(row_idx, 6, _center_table_item(QTableWidgetItem(category)))
            self.completed_table.setItem(row_idx, 7, _center_table_item(QTableWidgetItem(outcome_text)))
            self.completed_table.setItem(row_idx, 8, _center_table_item(QTableWidgetItem(closed_at_text)))
            self.completed_table.setItem(row_idx, 9, _center_table_item(qa_note_item))
            self.completed_table.setItem(row_idx, 10, _center_table_item(agent_note_item))

    def _handle_completed_parts_error(self, result: DepotLoadResult) -> None:
        self._completed_loading = False
        if hasattr(self, "completed_refresh_btn"):
            self.completed_refresh_btn.setEnabled(True)
        if hasattr(self, "completed_status_label"):
            self.completed_status_label.setText(
                f"Load failed: {result.error_type or 'Error'}"
                + (f": {result.error_message}" if result.error_message else "")
            )
        _runtime_log_event(
            "ui.depot_dashboard_completed_query_failed",
            severity="warning",
            summary="Dashboard completed queue query failed.",
            context={
                "error_type": result.error_type,
                "error_message": result.error_message,
                "view": result.request.view_key,
                "reason": result.request.reason,
                "state_key": result.request.state_key[:500],
                "duration_ms": int(max(0.0, result.duration_ms)),
            },
        )

    def _configure_notes_rows_table(self) -> None:
        headers = ["id", "created_at", "user_id", "work_order", "note_preview"]
        resize_modes = {
            0: QHeaderView.ResizeMode.ResizeToContents,
            1: QHeaderView.ResizeMode.ResizeToContents,
            2: QHeaderView.ResizeMode.ResizeToContents,
            3: QHeaderView.ResizeMode.ResizeToContents,
            4: QHeaderView.ResizeMode.Stretch,
        }
        configure_standard_table(self.notes_table, headers, resize_modes=resize_modes, stretch_last=True)

    @staticmethod
    def _load_dashboard_notes_payload(
        tracker: DepotTracker,
        *,
        target_key: str,
        rows_limit: int,
        work_order_filter: str,
        category_filter: str | None,
    ) -> dict[str, Any]:
        rows = tracker.fetch_dashboard_note_rows(
            target_key,
            limit=rows_limit,
            work_order_filter=work_order_filter,
            category_filter=category_filter,
        )
        return {
            "target_key": target_key,
            "rows": rows,
            "rows_limit": int(rows_limit),
            "work_order_filter": work_order_filter,
            "category_filter": category_filter or "",
        }

    def refresh_notes_rows(self, *, force: bool = False, reason: str = "") -> None:
        self._configure_notes_rows_table()
        target_key = str(self.notes_target_combo.currentData() or "").strip()
        if not target_key:
            self.notes_table.setRowCount(0)
            self._notes_selected_row_id = None
            self.notes_editor.clear()
            self.notes_editor.setEnabled(False)
            self.notes_save_btn.setEnabled(False)
            self.notes_selected_label.setText("No editable note fields are currently available.")
            self.notes_status_label.setText("")
            self._notes_loading = False
            self._notes_loaded = False
            return

        rows_limit = int(self.notes_limit_spin.value())
        work_order_filter = str(self.notes_work_order_filter.text() or "").strip()
        category_filter = self._current_dashboard_category_filter()
        state_key = {
            "target_key": target_key,
            "rows_limit": rows_limit,
            "work_order_filter": work_order_filter,
            "category_filter": category_filter or "",
        }
        self._notes_loading = True
        self.notes_refresh_btn.setEnabled(False)
        self._notes_selected_row_id = None
        self.notes_editor.clear()
        self.notes_editor.setEnabled(False)
        self.notes_save_btn.setEnabled(False)
        self.notes_selected_label.setText("Select a row to edit.")
        self.notes_status_label.setText("Loading note rows...")

        def _loader(worker_tracker: DepotTracker) -> dict[str, Any]:
            return DepotDashboardDialog._load_dashboard_notes_payload(
                worker_tracker,
                target_key=target_key,
                rows_limit=rows_limit,
                work_order_filter=work_order_filter,
                category_filter=category_filter,
            )

        request = self.app_window.start_depot_read(
            "dashboard_notes",
            state_key,
            reason=reason or "notes-refresh",
            force=force,
            loader=_loader,
            on_success=self._apply_notes_rows_result,
            on_error=self._handle_notes_rows_error,
        )
        if request is None:
            self._notes_loading = False
            self.notes_refresh_btn.setEnabled(True)
            self.notes_status_label.setText("Could not start note load. Details were logged for support.")

    def _apply_notes_rows_result(self, result: DepotLoadResult) -> None:
        self._configure_notes_rows_table()
        self._notes_loading = False
        self._notes_loaded = True
        self.notes_refresh_btn.setEnabled(True)
        payload = result.payload if isinstance(result.payload, dict) else {}
        rows_raw = payload.get("rows", [])
        rows = rows_raw if isinstance(rows_raw, list) else []
        self.notes_table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            row_id = int(max(0, safe_int(row.get("id"), 0)))
            created_text = self._normalize_dashboard_datetime(str(row.get("created_at", "") or ""))
            user_text = str(row.get("user_id", "") or "").strip()
            work_order_text = str(row.get("work_order", "") or "").strip()
            note_text = str(row.get("note_text", "") or "").strip()
            preview_text = self._note_preview_text(note_text)

            id_item = QTableWidgetItem(str(row_id))
            id_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            id_item.setData(Qt.ItemDataRole.UserRole, note_text)
            self.notes_table.setItem(row_idx, 0, id_item)

            created_item = QTableWidgetItem(created_text)
            created_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            self.notes_table.setItem(row_idx, 1, created_item)

            user_item = QTableWidgetItem(user_text)
            user_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            self.notes_table.setItem(row_idx, 2, user_item)

            work_order_item = QTableWidgetItem(work_order_text)
            work_order_item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter))
            self.notes_table.setItem(row_idx, 3, work_order_item)

            preview_item = QTableWidgetItem(preview_text)
            preview_item.setToolTip(note_text if note_text else "(empty)")
            self.notes_table.setItem(row_idx, 4, preview_item)

        self.notes_table.clearSelection()
        self._notes_selected_row_id = None
        self.notes_editor.clear()
        self.notes_editor.setEnabled(False)
        self.notes_save_btn.setEnabled(False)
        self.notes_selected_label.setText("Select a row to edit.")
        self.notes_status_label.setText(f"Loaded {len(rows)} row(s).")

    def _handle_notes_rows_error(self, result: DepotLoadResult) -> None:
        self._notes_loading = False
        self.notes_refresh_btn.setEnabled(True)
        _runtime_log_event(
            "ui.depot_dashboard_notes_query_failed",
            severity="warning",
            summary="Dashboard notes editor query failed.",
            context={
                "error_type": result.error_type,
                "error_message": result.error_message,
                "view": result.request.view_key,
                "reason": result.request.reason,
                "state_key": result.request.state_key[:500],
                "duration_ms": int(max(0.0, result.duration_ms)),
            },
        )
        self.notes_selected_label.setText("Could not load note rows. Details were logged for support.")
        self.notes_status_label.setText(
            f"Load failed: {result.error_type or 'Error'}"
            + (f": {result.error_message}" if result.error_message else "")
        )

    def _on_notes_selection_changed(self) -> None:
        selected_rows = self.notes_table.selectionModel().selectedRows() if self.notes_table.selectionModel() is not None else []
        if not selected_rows:
            self._notes_selected_row_id = None
            self.notes_editor.clear()
            self.notes_editor.setEnabled(False)
            self.notes_save_btn.setEnabled(False)
            self.notes_selected_label.setText("Select a row to edit.")
            return

        row_idx = int(selected_rows[0].row())
        id_item = self.notes_table.item(row_idx, 0)
        if id_item is None:
            self._notes_selected_row_id = None
            self.notes_editor.clear()
            self.notes_editor.setEnabled(False)
            self.notes_save_btn.setEnabled(False)
            self.notes_selected_label.setText("Select a row to edit.")
            return

        row_id = int(max(0, safe_int(id_item.text(), 0)))
        note_text = str(id_item.data(Qt.ItemDataRole.UserRole) or "")
        work_order_item = self.notes_table.item(row_idx, 3)
        user_item = self.notes_table.item(row_idx, 2)
        work_order_text = work_order_item.text().strip() if work_order_item is not None else ""
        user_text = user_item.text().strip() if user_item is not None else ""

        self._notes_selected_row_id = row_id if row_id > 0 else None
        self.notes_editor.setEnabled(self._notes_selected_row_id is not None and not self.is_read_only_mode())
        self.notes_editor.setPlainText(note_text)
        self.notes_save_btn.setEnabled(self._notes_selected_row_id is not None and not self.is_read_only_mode())
        if self._notes_selected_row_id is not None:
            self.notes_selected_label.setText(
                f"Editing row #{self._notes_selected_row_id} | Work Order: {work_order_text or '(none)'} | User: {user_text or '(none)'}"
            )
        else:
            self.notes_selected_label.setText("Select a row to edit.")
        self.notes_status_label.setText("")

    def _save_selected_note(self) -> None:
        if self._warn_if_read_only("Dashboard note updates"):
            return
        target_key = str(self.notes_target_combo.currentData() or "").strip()
        row_id = self._notes_selected_row_id
        if not target_key or row_id is None:
            self._show_themed_message(QMessageBox.Icon.Warning, "Validation", "Select a note row first.")
            return

        note_text = str(self.notes_editor.toPlainText() or "").strip()
        try:
            self.app_window.depot_tracker.update_dashboard_note_value(target_key, row_id, note_text)
        except Exception as exc:
            _runtime_log_event(
                "ui.depot_dashboard_notes_save_failed",
                severity="error",
                summary="Dashboard notes editor failed saving a note field.",
                exc=exc,
                context={"target_key": target_key, "row_id": row_id},
            )
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Save failed",
                f"Could not save note:\n{type(exc).__name__}: {exc}",
            )
            return

        selected_row = self.notes_table.currentRow()
        if selected_row >= 0:
            id_item = self.notes_table.item(selected_row, 0)
            preview_item = self.notes_table.item(selected_row, 4)
            if id_item is not None:
                id_item.setData(Qt.ItemDataRole.UserRole, note_text)
            if preview_item is not None:
                preview_item.setText(self._note_preview_text(note_text))
                preview_item.setToolTip(note_text if note_text else "(empty)")
        self.notes_status_label.setText(f"Saved row #{row_id} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
        self.refresh_completed_parts(force=True, reason="notes-save")

    def _touch_color(self, touch: str) -> str:
        normalized = str(touch or "").strip()
        return normalize_hex(self.TOUCH_COLORS.get(normalized, "#6F7C91"), "#6F7C91")

    @staticmethod
    def _format_dashboard_average(value: Any) -> str:
        return f"{float(value or 0.0):.2f}"

    def _refresh_touch_distribution(
        self,
        start_date: str | None,
        end_date: str | None,
        user_id: str | None,
        category_filter: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        latest_workload_mix = self._latest_workload_mix_enabled(user_id)
        if metrics is None:
            try:
                metrics = self.app_window.depot_tracker.get_dashboard_metrics(
                    start_date=start_date,
                    end_date=end_date,
                    user_id=user_id,
                    category=category_filter,
                    include_latest_workload_mix=latest_workload_mix,
                )
            except Exception as exc:
                _runtime_log_event(
                    "ui.depot_dashboard_metrics_query_failed",
                    severity="error",
                    summary="Dashboard metrics query failed.",
                    exc=exc,
                    context={
                        "start_date": start_date,
                        "end_date": end_date,
                        "user_id": user_id,
                        "category_filter": category_filter or "",
                    },
                )
                self.touch_bar.set_segments([])
                self.touch_summary_label.setText("Touch metrics unavailable. Details were logged for support.")
                self.touch_legend_label.setText("")
                return

        by_touch_raw = metrics.get("latest_by_touch" if latest_workload_mix else "by_touch", {})
        by_touch: dict[str, int] = {}
        if isinstance(by_touch_raw, dict):
            for key, value in by_touch_raw.items():
                touch_name = str(key or "").strip()
                if not touch_name:
                    continue
                by_touch[touch_name] = int(max(0, safe_int(value, 0)))

        ordered_keys: list[str] = []
        for key in self.TOUCH_ORDER:
            if key in by_touch:
                ordered_keys.append(key)
        for key in sorted(by_touch.keys()):
            if key not in ordered_keys:
                ordered_keys.append(key)

        segments = [
            (touch, int(by_touch.get(touch, 0)), self._touch_color(touch))
            for touch in ordered_keys
            if int(by_touch.get(touch, 0)) > 0
        ]
        self.touch_bar.set_segments(segments)

        total_submissions = int(max(0, safe_int(metrics.get("total_submissions", 0), 0)))
        total_units = int(max(0, safe_int(metrics.get("total_units", 0), 0)))
        complete_count = int(max(0, safe_int(metrics.get("complete_count", 0), 0)))
        junk_count = int(max(0, safe_int(metrics.get("junk_count", 0), 0)))
        part_order_count = int(max(0, safe_int(metrics.get("part_order_count", 0), 0)))
        rtv_count = int(max(0, safe_int(metrics.get("rtv_count", 0), 0)))
        triaged_count = int(max(0, safe_int(metrics.get("triaged_count", 0), 0)))
        other_touch_count = int(max(0, safe_int(metrics.get("other_touch_count", 0), 0)))
        latest_complete_count = int(max(0, safe_int(metrics.get("latest_complete_count", 0), 0)))
        latest_junk_count = int(max(0, safe_int(metrics.get("latest_junk_count", 0), 0)))
        latest_part_order_count = int(max(0, safe_int(metrics.get("latest_part_order_count", 0), 0)))
        latest_rtv_count = int(max(0, safe_int(metrics.get("latest_rtv_count", 0), 0)))
        latest_triaged_count = int(max(0, safe_int(metrics.get("latest_triaged_count", 0), 0)))
        latest_other_touch_count = int(max(0, safe_int(metrics.get("latest_other_touch_count", 0), 0)))
        day_span = int(max(1, safe_int(metrics.get("day_span", 1), 1)))
        avg_submission_rows = self._format_dashboard_average(metrics.get("avg_submission_rows_per_day", 0.0))
        avg_units = self._format_dashboard_average(metrics.get("avg_units_per_day", 0.0))
        avg_complete = self._format_dashboard_average(metrics.get("avg_complete_per_day", 0.0))
        avg_junk = self._format_dashboard_average(metrics.get("avg_junk_per_day", 0.0))
        date_label = "All Time" if start_date is None or end_date is None else f"{start_date} to {end_date}"
        user_label = user_id if user_id else "All Users"
        category_label = category_filter if category_filter else "All Categories"
        if latest_workload_mix:
            self.touch_summary_label.setText(
                "Last 30 Days single-agent view. Top bar shows latest work-order status mix while totals remain raw submission rows. "
                f"Units touched distinct: {total_units} ({avg_units}/day) | "
                f"Submission rows: {total_submissions} ({avg_submission_rows}/day) | "
                f"Raw rows -> Com: {complete_count} ({avg_complete}/day) | JO: {junk_count} ({avg_junk}/day) | "
                f"PO: {part_order_count} | RTV: {rtv_count} | Triaged: {triaged_count} | Other: {other_touch_count} | "
                f"Latest WO mix -> Com: {latest_complete_count} | JO: {latest_junk_count} | PO: {latest_part_order_count} | "
                f"RTV: {latest_rtv_count} | Triaged: {latest_triaged_count} | Other: {latest_other_touch_count} | "
                f"Range: {date_label} | User: {user_label} | Category: {category_label} | Days: {day_span}"
            )
        else:
            self.touch_summary_label.setText(
                f"Range touch mix for submission rows. Units touched distinct: {total_units} ({avg_units}/day) | "
                f"Submission rows: {total_submissions} ({avg_submission_rows}/day) | "
                f"Complete rows: {complete_count} ({avg_complete}/day) | "
                f"JO rows: {junk_count} ({avg_junk}/day) | "
                f"PO rows: {part_order_count} | RTV rows: {rtv_count} | Triaged rows: {triaged_count} | Other rows: {other_touch_count} | "
                f"Range: {date_label} | User: {user_label} | Category: {category_label} | Days: {day_span}"
            )
        if segments:
            legend_chunks = [
                f"<span style='color:{color}; font-weight:700'>{DepotRules.chart_touch_label(touch)}</span>: {count}"
                for touch, count, color in segments
            ]
            legend_prefix = "Latest WO mix | " if latest_workload_mix else ""
            self.touch_legend_label.setText(legend_prefix + " | ".join(legend_chunks))
            self.touch_legend_label.setTextFormat(Qt.TextFormat.RichText)
        else:
            if latest_workload_mix:
                self.touch_legend_label.setText("No latest work-order activity in the selected filter range.")
            else:
                self.touch_legend_label.setText("No touch activity in the selected filter range.")
            self.touch_legend_label.setTextFormat(Qt.TextFormat.PlainText)

    @staticmethod
    def _normalize_dashboard_datetime(raw_value: Any) -> str:
        text = str(raw_value or "").strip()
        if not text:
            return ""
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                try:
                    parsed = parsed.astimezone()
                except Exception:
                    pass
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        return text.replace("T", " ")

    def _format_dashboard_cell_text(self, col_name: str, raw_value: Any) -> str:
        if raw_value is None:
            return ""
        text = str(raw_value).strip()
        if not text:
            return ""
        normalized_name = str(col_name or "").strip().lower()
        if normalized_name.endswith("_at"):
            return self._normalize_dashboard_datetime(text)
        if normalized_name.endswith("_date") and "T" in text:
            normalized = self._normalize_dashboard_datetime(text)
            return normalized[:10] if normalized else text
        return text

    def _refresh_table_placeholder(
        self,
        table_name: str,
        row_count: int,
        start_date: str | None,
        end_date: str | None,
        user_id: str | None,
        category_filter: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        if not hasattr(self, "table_placeholder_label"):
            return
        category_label = category_filter if category_filter else "All Categories"
        if table_name != "submissions":
            if hasattr(self, "table_trend_chart"):
                self.table_trend_chart.clear_series()
                self.table_trend_chart.setVisible(False)
            self.table_placeholder_label.setText(
                f"Chart view is currently reserved for Submissions.\n"
                f"Current source: {table_name} | Category: {category_label} | Rows loaded: {int(row_count)}"
            )
            return
        range_label = "All Time" if start_date is None or end_date is None else f"{start_date} to {end_date}"
        user_label = user_id if user_id else "All Users"
        latest_workload_mix = self._latest_workload_mix_enabled(user_id)
        if metrics is None:
            try:
                metrics = self.app_window.depot_tracker.get_dashboard_metrics(
                    start_date=start_date,
                    end_date=end_date,
                    user_id=user_id,
                    category=category_filter,
                    include_latest_workload_mix=latest_workload_mix,
                )
            except Exception as exc:
                _runtime_log_event(
                    "ui.depot_dashboard_placeholder_metrics_query_failed",
                    severity="warning",
                    summary="Dashboard placeholder metrics query failed.",
                    exc=exc,
                    context={
                        "table_name": table_name,
                        "start_date": start_date,
                        "end_date": end_date,
                        "user_id": user_id,
                        "category_filter": category_filter or "",
                    },
                )
                metrics = {}
        total_units = int(max(0, safe_int(metrics.get("total_units", 0), 0)))
        total_submissions = int(max(0, safe_int(metrics.get("total_submissions", 0), 0)))
        complete_count = int(max(0, safe_int(metrics.get("complete_count", 0), 0)))
        junk_count = int(max(0, safe_int(metrics.get("junk_count", 0), 0)))
        part_order_count = int(max(0, safe_int(metrics.get("part_order_count", 0), 0)))
        rtv_count = int(max(0, safe_int(metrics.get("rtv_count", 0), 0)))
        triaged_count = int(max(0, safe_int(metrics.get("triaged_count", 0), 0)))
        other_touch_count = int(max(0, safe_int(metrics.get("other_touch_count", 0), 0)))
        avg_complete = self._format_dashboard_average(metrics.get("avg_complete_per_day", 0.0))
        avg_junk = self._format_dashboard_average(metrics.get("avg_junk_per_day", 0.0))
        trend_daily_raw = metrics.get("trend_daily", [])
        trend_daily = trend_daily_raw if isinstance(trend_daily_raw, list) else []
        if hasattr(self, "table_trend_chart"):
            self.table_trend_chart.set_series(trend_daily)
            self.table_trend_chart.setVisible(True)
        header_text = "Daily submission touch trend. Bar height = submission rows. Top label = distinct units touched."
        if latest_workload_mix:
            header_text += " Touch mix above uses latest work-order status in this single-agent Last 30 Days view; the table and trend stay on raw submission rows."
        self.table_placeholder_label.setText(
            f"{header_text}\n"
            f"Range: {range_label} | User: {user_label} | Category: {category_label} | Rows loaded: {int(row_count)}\n"
            f"Units touched distinct: {total_units} | Submission rows: {total_submissions} | "
            f"Complete rows: {complete_count} ({avg_complete}/day) | JO rows: {junk_count} ({avg_junk}/day) | "
            f"PO rows: {part_order_count} | RTV rows: {rtv_count} | Triaged rows: {triaged_count} | Other rows: {other_touch_count}"
        )

    def refresh_combo_popup_width(self) -> None:
        combos: list[QComboBox] = [self.table_combo, self.timeframe_combo, self.user_filter_combo, self.category_filter_combo]
        if hasattr(self, "notes_target_combo"):
            combos.append(self.notes_target_combo)
        for combo in combos:
            item_count = int(combo.count())
            if item_count <= 0:
                continue
            fm = combo.fontMetrics()
            widest_text = 0
            for idx in range(item_count):
                widest_text = max(widest_text, fm.horizontalAdvance(combo.itemText(idx)))
            popup_width = int(max(220, widest_text + 56))
            view = combo.view()
            if view is None:
                continue
            view.setMinimumWidth(popup_width)
            view.setTextElideMode(Qt.TextElideMode.ElideNone)
            view.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

    @staticmethod
    def _dashboard_default_headers(tracker: DepotTracker, table_name: str) -> list[str]:
        normalized_table = str(table_name or "").strip()
        if normalized_table == "submissions":
            return [
                "id",
                "created_at",
                "user_id",
                "work_order",
                "touch",
                "client_unit",
                "entry_date",
                "part_order_count",
            ]
        info_rows = tracker.db.fetchall(f"PRAGMA table_info({normalized_table})")
        return [str(row["name"]) for row in info_rows if str(row["name"] or "").strip()]

    @staticmethod
    def _load_dashboard_payload(
        tracker: DepotTracker,
        *,
        table_name: str,
        limit: int,
        start_date: str | None,
        end_date: str | None,
        user_id: str | None,
        category_filter: str | None,
        latest_workload_mix: bool,
    ) -> dict[str, Any]:
        normalized_table = str(table_name or "").strip()
        allowed_tables = {name for name, _label in TRACKER_DASHBOARD_TABLES}
        if normalized_table not in allowed_tables:
            raise ValueError("Invalid dashboard table selection.")

        rows_raw = tracker.fetch_dashboard_table_rows(
            normalized_table,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            user_id=user_id,
            category_filter=category_filter,
        )
        rows: list[dict[str, Any]] = []
        headers: list[str] = []
        for row in rows_raw:
            row_dict = {str(name): row[name] for name in row.keys()}
            if not headers:
                headers = list(row_dict.keys())
            rows.append(row_dict)
        if not headers:
            headers = DepotDashboardDialog._dashboard_default_headers(tracker, normalized_table)

        category_options = tracker.dashboard_category_options()
        user_rows = tracker.db.fetchall(
            "SELECT DISTINCT TRIM(COALESCE(user_id, '')) AS user_id "
            "FROM submissions WHERE TRIM(COALESCE(user_id, '')) <> '' "
            "ORDER BY user_id COLLATE NOCASE ASC"
        )
        user_options = [str(row["user_id"] or "").strip() for row in user_rows if str(row["user_id"] or "").strip()]

        metrics: dict[str, Any] = {}
        if normalized_table == "submissions":
            metrics = tracker.get_dashboard_metrics(
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
                category=category_filter,
                include_latest_workload_mix=latest_workload_mix,
            )

        return {
            "table_name": normalized_table,
            "submissions_mode": normalized_table == "submissions",
            "limit": int(limit),
            "start_date": start_date,
            "end_date": end_date,
            "user_id": user_id or "",
            "category_filter": category_filter or "",
            "latest_workload_mix": bool(latest_workload_mix),
            "headers": headers,
            "rows": rows,
            "category_options": category_options,
            "user_options": user_options,
            "metrics": metrics,
        }

    def refresh_dashboard(self, *, force: bool = False, reason: str = "") -> None:
        table_name = str(self.table_combo.currentData() or "").strip()
        if not table_name:
            return

        allowed_tables = {name for name, _label in TRACKER_DASHBOARD_TABLES}
        if table_name not in allowed_tables:
            return

        submissions_mode = table_name == "submissions"
        self.submission_filters_wrap.setVisible(submissions_mode)
        self.touch_summary_label.setVisible(submissions_mode)
        self.touch_bar.setVisible(submissions_mode)
        self.touch_legend_label.setVisible(submissions_mode)
        self._populate_dashboard_category_filter()
        if submissions_mode:
            self._populate_submission_user_filter()
        self.refresh_combo_popup_width()

        limit = int(self.limit_spin.value())
        start_date: str | None = None
        end_date: str | None = None
        user_id: str | None = None
        category_filter = self._current_dashboard_category_filter()

        if submissions_mode:
            start_date, end_date, user_id = self._current_submission_filters()

        latest_workload_mix = self._latest_workload_mix_enabled(user_id)
        state_key = {
            "table_name": table_name,
            "limit": limit,
            "start_date": start_date,
            "end_date": end_date,
            "user_id": user_id or "",
            "category_filter": category_filter or "",
            "latest_workload_mix": latest_workload_mix,
        }
        self._dashboard_loading = True
        self.refresh_btn.setEnabled(False)
        if self.table.rowCount() <= 0:
            self.empty_hint.setText("Loading dashboard data...")
            self.empty_hint.show()
        if submissions_mode:
            self.touch_bar.set_segments([])
            self.touch_summary_label.setText("Loading dashboard metrics...")
            self.touch_legend_label.setText("")
        else:
            self.touch_bar.set_segments([])
            self.touch_summary_label.setText("")
            self.touch_legend_label.setText("")

        def _loader(worker_tracker: DepotTracker) -> dict[str, Any]:
            return DepotDashboardDialog._load_dashboard_payload(
                worker_tracker,
                table_name=table_name,
                limit=limit,
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
                category_filter=category_filter,
                latest_workload_mix=latest_workload_mix,
            )

        request = self.app_window.start_depot_read(
            "dashboard_metrics",
            state_key,
            reason=reason or "dashboard-refresh",
            force=force,
            loader=_loader,
            on_success=self._apply_dashboard_result,
            on_error=self._handle_dashboard_error,
        )
        if request is None:
            self._dashboard_loading = False
            self.refresh_btn.setEnabled(True)
            self.empty_hint.setText("Could not start dashboard load. Details were logged for support.")
            self.empty_hint.show()

    def _apply_dashboard_result(self, result: DepotLoadResult) -> None:
        self._dashboard_loading = False
        self._dashboard_has_loaded = True
        self.refresh_btn.setEnabled(True)
        payload = result.payload if isinstance(result.payload, dict) else {}
        table_name = str(payload.get("table_name", "") or "").strip()
        if not table_name:
            table_name = str(self.table_combo.currentData() or "").strip()
        submissions_mode = bool(payload.get("submissions_mode", table_name == "submissions"))
        start_date = str(payload.get("start_date") or "").strip() or None
        end_date = str(payload.get("end_date") or "").strip() or None
        user_id = str(payload.get("user_id") or "").strip() or None
        category_filter = str(payload.get("category_filter") or "").strip() or None
        category_options_raw = payload.get("category_options", [])
        user_options_raw = payload.get("user_options", [])
        category_options = category_options_raw if isinstance(category_options_raw, list) else []
        user_options = user_options_raw if isinstance(user_options_raw, list) else []
        self.submission_filters_wrap.setVisible(submissions_mode)
        self.touch_summary_label.setVisible(submissions_mode)
        self.touch_bar.setVisible(submissions_mode)
        self.touch_legend_label.setVisible(submissions_mode)
        self._apply_dashboard_category_filter_options([str(item) for item in category_options], category_filter or "")
        if submissions_mode:
            self._apply_submission_user_filter_options([str(item) for item in user_options], user_id or "")
        self.refresh_combo_popup_width()

        headers_raw = payload.get("headers", [])
        headers = [str(name) for name in headers_raw] if isinstance(headers_raw, list) else []
        rows_raw = payload.get("rows", [])
        rows = [row for row in rows_raw if isinstance(row, dict)] if isinstance(rows_raw, list) else []
        resize_modes: dict[int, QHeaderView.ResizeMode] = {}
        if headers:
            for idx in range(len(headers)):
                resize_modes[idx] = QHeaderView.ResizeMode.ResizeToContents
            resize_modes[len(headers) - 1] = QHeaderView.ResizeMode.Stretch
        configure_standard_table(self.table, headers, resize_modes=resize_modes, stretch_last=True)

        self.table.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            for col_idx, col_name in enumerate(headers):
                raw_value = row.get(col_name)
                text = self._format_dashboard_cell_text(col_name, raw_value)
                item = QTableWidgetItem(text)
                item.setToolTip(text)
                self.table.setItem(row_idx, col_idx, item)

        metrics_raw = payload.get("metrics", {})
        metrics = metrics_raw if isinstance(metrics_raw, dict) else {}
        self._refresh_table_placeholder(
            table_name,
            len(rows),
            start_date,
            end_date,
            user_id,
            category_filter,
            metrics=metrics,
        )

        if submissions_mode:
            self._refresh_touch_distribution(start_date, end_date, user_id, category_filter, metrics=metrics)
        else:
            self.touch_bar.set_segments([])
            self.touch_summary_label.setText("")
            self.touch_legend_label.setText("")

        if rows:
            self.empty_hint.hide()
        else:
            self.empty_hint.setText("No rows in this table for the current row limit/filter.")
            self.empty_hint.show()

        if self._completed_loaded or self.results_tabs.currentWidget() is self.completed_tab:
            self.refresh_completed_parts(reason="dashboard-refresh")
        if self._notes_loaded or self.results_tabs.currentWidget() is self.notes_tab:
            self.refresh_notes_rows(reason="dashboard-refresh")

    def _handle_dashboard_error(self, result: DepotLoadResult) -> None:
        self._dashboard_loading = False
        self.refresh_btn.setEnabled(True)
        _runtime_log_event(
            "ui.depot_dashboard_query_failed",
            severity="warning",
            summary="Dashboard table query failed.",
            context={
                "error_type": result.error_type,
                "error_message": result.error_message,
                "view": result.request.view_key,
                "reason": result.request.reason,
                "state_key": result.request.state_key[:500],
                "duration_ms": int(max(0.0, result.duration_ms)),
            },
        )
        self.empty_hint.setText("Could not load dashboard data. Details were logged for support.")
        self.empty_hint.show()
        self.touch_bar.set_segments([])
        self.touch_summary_label.setText("Dashboard metrics unavailable. Details were logged for support.")
        self.touch_legend_label.setText("")

    def export_csv(self) -> None:
        table = self.table
        if table.columnCount() <= 0:
            self._show_themed_message(QMessageBox.Icon.Information, "Export CSV", "No data to export.")
            return

        table_name = str(self.table_combo.currentData() or "table").strip()
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        default_name = f"{table_name}_{stamp}.csv"
        start_dir = self.app_window.config_path.parent if self.app_window.config_path.parent.exists() else Path.home()
        out_path, _ = show_flowgrid_themed_save_file_name(
            self,
            self.app_window,
            "dashboard",
            "Export Dashboard Table",
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
                        values.append(item.text() if item is not None else "")
                    writer.writerow(values)
            self._show_themed_message(QMessageBox.Icon.Information, "Export CSV", f"Exported:\n{out_path}")
        except Exception as exc:
            _runtime_log_event(
                "ui.depot_dashboard_export_csv_failed",
                severity="error",
                summary="Dashboard CSV export failed.",
                exc=exc,
                context={"path": str(out_path), "table": table_name},
            )
            self._show_themed_message(
                QMessageBox.Icon.Warning,
                "Export CSV",
                f"Failed to export CSV:\n{type(exc).__name__}: {exc}\n\nDetails were logged for support.",
            )

__all__ = [
    "DashboardTrendChart",
    "DepotAdminDialog",
    "DepotDashboardDialog",
    "DroppableImagePathLineEdit",
    "IconArrangePreview",
    "IconCropDialog",
]
