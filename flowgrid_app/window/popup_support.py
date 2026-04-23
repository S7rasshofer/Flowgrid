from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from PySide6.QtCore import QEvent, QPoint, QRect, Qt, QTimer
from PySide6.QtGui import QColor, QGuiApplication, QMouseEvent, QPainter, QPixmap
from PySide6.QtWidgets import (
    QAbstractButton,
    QApplication,
    QComboBox,
    QColorDialog,
    QDateEdit,
    QDialog,
    QFileDialog,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QScrollBar,
    QSlider,
    QSpinBox,
    QStyle,
    QTabBar,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import DEFAULT_THEME_SURFACE, clamp, normalize_hex

from .constants import DEPOT_VIEW_TTL_MS


FLOWGRID_SHELL_WINDOW_PROPERTY = "flowgrid_shell_window"

UI_MARGIN_FRAMELESS_TOOL = 6
UI_SPACING_FRAMELESS_TOOL = 6
UI_MARGIN_STANDARD_DIALOG = 10
UI_SPACING_STANDARD_DIALOG = 8
HEADER_CLOSE_BUTTON_WIDTH = 26
HEADER_CLOSE_BUTTON_HEIGHT = 22

_FLOWGRID_SHELL_FACTORY: Callable[[], QWidget] | None = None


def configure_flowgrid_shell_factory(factory: Callable[[], QWidget] | None) -> None:
    global _FLOWGRID_SHELL_FACTORY
    _FLOWGRID_SHELL_FACTORY = factory


def mark_flowgrid_shell_window(shell: QWidget) -> None:
    shell.setProperty(FLOWGRID_SHELL_WINDOW_PROPERTY, True)
    setattr(shell, "_flowgrid_shell_window", True)


def _drag_target_widget(root: QWidget, local_pos: QPoint) -> QWidget:
    child = root.childAt(local_pos)
    return child if isinstance(child, QWidget) else root


def _is_drag_blocked_widget(widget: QWidget | None) -> bool:
    blocked_types = (
        QLineEdit,
        QTextEdit,
        QAbstractButton,
        QComboBox,
        QSpinBox,
        QDateEdit,
        QSlider,
        QListWidget,
        QScrollArea,
        QScrollBar,
        QTabBar,
    )
    current = widget
    while current is not None:
        if isinstance(current, blocked_types):
            return True
        current = current.parentWidget()
    return False


def show_flowgrid_themed_message(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    icon: QMessageBox.Icon,
    title: str,
    text: str,
) -> None:
    resolved_app_window = _resolve_flowgrid_popup_app_window(parent, app_window)
    runtime_options = getattr(resolved_app_window, "runtime_options", None)
    message_sink = getattr(runtime_options, "message_sink", None)
    if callable(message_sink):
        try:
            raw_icon_value = getattr(icon, "value", icon)
            message_sink(
                {
                    "theme_kind": str(theme_kind or "").strip() or "main",
                    "icon": int(raw_icon_value),
                    "title": str(title or ""),
                    "text": str(text or ""),
                }
            )
            return
        except Exception as exc:
            _runtime_log_event(
                "ui.themed_message_sink_failed",
                severity="warning",
                summary="Diagnostic themed message sink failed; falling back to modal dialog.",
                exc=exc,
                context={"theme_kind": str(theme_kind), "title": str(title)},
            )
    dialog = FlowgridThemedMessageDialog(parent, resolved_app_window, theme_kind, icon, title, text)
    dialog.exec()


def _resolve_flowgrid_popup_app_window(
    parent: QWidget | None,
    app_window: Any | None,
) -> Any | None:
    if app_window is not None:
        return app_window
    probe = parent
    while probe is not None:
        candidate = getattr(probe, "app_window", None)
        if candidate is not None:
            return candidate
        probe = probe.parentWidget()
    return None


def _apply_flowgrid_popup_stylesheet(
    widget: QWidget,
    app_window: Any | None,
    theme_kind: str,
    *,
    force_opaque_root: bool = True,
    event_key: str = "ui.themed_popup_stylesheet_failed",
    title: str = "",
) -> None:
    if app_window is None:
        return
    try:
        widget.setStyleSheet(app_window._popup_theme_stylesheet(theme_kind, force_opaque_root=force_opaque_root))
    except Exception as exc:
        _runtime_log_event(
            event_key,
            severity="warning",
            summary="Failed applying themed popup stylesheet.",
            exc=exc,
            context={"theme_kind": str(theme_kind), "title": str(title or widget.windowTitle())},
        )


def _paint_flowgrid_popup_background(
    widget: QWidget,
    painter: QPainter,
    app_window: Any | None,
    theme_kind: str,
) -> None:
    if app_window is None:
        return

    target_rect = widget.rect()
    if target_rect.width() <= 0 or target_rect.height() <= 0:
        return

    target_size = target_rect.size()
    parent_widget = widget.parentWidget()
    reference_size = target_size
    if parent_widget is not None:
        parent_size = parent_widget.size()
        if parent_size.width() > 0 and parent_size.height() > 0:
            reference_size = parent_size

    resolved_kind = str(theme_kind or "").strip() or "main"
    if resolved_kind == "main":
        base_bg = normalize_hex(
            app_window.palette_data.get("control_bg", app_window.palette_data.get("surface", DEFAULT_THEME_SURFACE)),
            DEFAULT_THEME_SURFACE,
        )
        requested_transparent = bool(app_window.config.get("theme_page_transparent_primary_bg", False))
        transparent = app_window._effective_popup_transparency("main")
        bg = app_window.render_background_pixmap(reference_size, kind="main")
    else:
        resolved = app_window._resolved_popup_theme(resolved_kind)
        base_bg = normalize_hex(
            resolved.get("background", app_window.palette_data.get("control_bg", DEFAULT_THEME_SURFACE)),
            app_window.palette_data.get("control_bg", DEFAULT_THEME_SURFACE),
        )
        requested_transparent = bool(resolved.get("transparent", False))
        transparent = app_window._effective_popup_transparency(resolved_kind)
        bg = app_window.render_background_pixmap(reference_size, kind=resolved_kind)

    bg_color = QColor(base_bg)
    if transparent:
        bg_color.setAlpha(220)
    elif requested_transparent:
        bg_color.setAlpha(max(13, int(round(255 * 0.05))))
    else:
        bg_color.setAlpha(244)
    painter.fillRect(target_rect, bg_color)

    if not bg.isNull():
        src = QRect(0, 0, bg.width(), bg.height())
        if bg.width() >= target_size.width() and bg.height() >= target_size.height():
            src = QRect(
                max(0, (bg.width() - target_size.width()) // 2),
                max(0, (bg.height() - target_size.height()) // 2),
                int(target_size.width()),
                int(target_size.height()),
            )
        painter.setOpacity(0.84 if transparent else (0.22 if requested_transparent else 0.94))
        painter.drawPixmap(target_rect, bg, src)
        painter.setOpacity(1.0)

    overlay = QColor(app_window.palette_data.get("shell_overlay", base_bg))
    overlay.setAlpha(56 if transparent else (12 if requested_transparent else 28))
    painter.fillRect(target_rect, overlay)


class FlowgridThemedDialog(QDialog):
    def __init__(
        self,
        parent: QWidget | None,
        app_window: Any | None,
        theme_kind: str,
    ) -> None:
        super().__init__(parent)
        self._app_window = _resolve_flowgrid_popup_app_window(parent, app_window)
        self._theme_kind = str(theme_kind or "").strip() or "main"
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

    def apply_theme_styles(self, *, force_opaque_root: bool = True) -> None:
        _apply_flowgrid_popup_stylesheet(
            self,
            self._app_window,
            self._theme_kind,
            force_opaque_root=force_opaque_root,
            event_key="ui.themed_dialog_stylesheet_failed",
        )

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        _paint_flowgrid_popup_background(self, painter, self._app_window, self._theme_kind)
        super().paintEvent(event)


class FlowgridThemedFileDialog(QFileDialog):
    def __init__(
        self,
        parent: QWidget | None,
        app_window: Any | None,
        theme_kind: str,
    ) -> None:
        super().__init__(parent)
        self._app_window = _resolve_flowgrid_popup_app_window(parent, app_window)
        self._theme_kind = str(theme_kind or "").strip() or "main"
        self.setOption(QFileDialog.Option.DontUseNativeDialog, True)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        _apply_flowgrid_popup_stylesheet(
            self,
            self._app_window,
            self._theme_kind,
            force_opaque_root=True,
            event_key="ui.themed_file_dialog_stylesheet_failed",
        )

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        _paint_flowgrid_popup_background(self, painter, self._app_window, self._theme_kind)
        super().paintEvent(event)


class FlowgridThemedColorDialog(QColorDialog):
    def __init__(
        self,
        parent: QWidget | None,
        app_window: Any | None,
        theme_kind: str,
    ) -> None:
        super().__init__(parent)
        self._app_window = _resolve_flowgrid_popup_app_window(parent, app_window)
        self._theme_kind = str(theme_kind or "").strip() or "main"
        self.setOption(QColorDialog.ColorDialogOption.DontUseNativeDialog, True)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        _apply_flowgrid_popup_stylesheet(
            self,
            self._app_window,
            self._theme_kind,
            force_opaque_root=True,
            event_key="ui.themed_color_dialog_stylesheet_failed",
        )

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        _paint_flowgrid_popup_background(self, painter, self._app_window, self._theme_kind)
        super().paintEvent(event)


class FlowgridThemedMessageDialog(FlowgridThemedDialog):
    def __init__(
        self,
        parent: QWidget | None,
        app_window: Any | None,
        theme_kind: str,
        icon: QMessageBox.Icon,
        title: str,
        text: str,
    ) -> None:
        super().__init__(parent, app_window, theme_kind)
        self.setWindowTitle(str(title or "").strip() or "Message")
        self.setModal(True)
        self.setMinimumWidth(360)
        self.setMaximumWidth(620)
        self.apply_theme_styles(force_opaque_root=True)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(UI_MARGIN_STANDARD_DIALOG, UI_MARGIN_STANDARD_DIALOG, UI_MARGIN_STANDARD_DIALOG, UI_MARGIN_STANDARD_DIALOG)
        layout.setSpacing(UI_SPACING_STANDARD_DIALOG)

        content_row = QHBoxLayout()
        content_row.setContentsMargins(0, 0, 0, 0)
        content_row.setSpacing(UI_SPACING_STANDARD_DIALOG)

        icon_label = QLabel()
        icon_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignHCenter)
        icon_label.setPixmap(self._message_icon_pixmap(icon))
        icon_label.setMinimumWidth(40)
        content_row.addWidget(icon_label, 0)

        text_label = QLabel(str(text or ""))
        text_label.setWordWrap(True)
        text_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        content_row.addWidget(text_label, 1)
        layout.addLayout(content_row)

        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(UI_SPACING_STANDARD_DIALOG)
        button_row.addStretch(1)
        ok_button = QPushButton("OK")
        ok_button.setProperty("actionRole", "pick")
        ok_button.clicked.connect(self.accept)
        ok_button.setDefault(True)
        ok_button.setAutoDefault(True)
        button_row.addWidget(ok_button, 0)
        layout.addLayout(button_row)

    def _message_icon_pixmap(self, icon: QMessageBox.Icon) -> QPixmap:
        style = self.style() if self.style() is not None else QApplication.style()
        if style is None:
            return QPixmap()
        mapping = {
            QMessageBox.Icon.Information: QStyle.StandardPixmap.SP_MessageBoxInformation,
            QMessageBox.Icon.Warning: QStyle.StandardPixmap.SP_MessageBoxWarning,
            QMessageBox.Icon.Critical: QStyle.StandardPixmap.SP_MessageBoxCritical,
            QMessageBox.Icon.Question: QStyle.StandardPixmap.SP_MessageBoxQuestion,
        }
        standard = mapping.get(icon, QStyle.StandardPixmap.SP_MessageBoxInformation)
        return style.standardIcon(standard).pixmap(32, 32)


class FlowgridThemedInputDialog(QInputDialog):
    def __init__(
        self,
        parent: QWidget | None,
        app_window: Any | None,
        theme_kind: str,
    ) -> None:
        super().__init__(parent)
        self._app_window = _resolve_flowgrid_popup_app_window(parent, app_window)
        self._theme_kind = str(theme_kind or "").strip() or "main"
        self.setObjectName("FlowgridThemedInputDialog")
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        _paint_flowgrid_popup_background(self, painter, self._app_window, self._theme_kind)
        super().paintEvent(event)


def _configure_flowgrid_file_dialog(
    dialog: FlowgridThemedFileDialog,
    title: str,
    directory: str,
    file_filter: str,
) -> None:
    dialog.setWindowTitle(str(title or "").strip() or "Select File")
    initial_path = str(directory or "").strip()
    if initial_path:
        candidate = Path(initial_path)
        if candidate.exists() and candidate.is_dir():
            dialog.setDirectory(str(candidate))
        else:
            parent_dir = candidate.parent if str(candidate.parent) else Path.home()
            if parent_dir.exists():
                dialog.setDirectory(str(parent_dir))
            if candidate.name:
                dialog.selectFile(candidate.name)
    filters = [item for item in str(file_filter or "").split(";;") if str(item or "").strip()]
    if filters:
        dialog.setNameFilters(filters)
        dialog.selectNameFilter(filters[0])


def show_flowgrid_themed_open_file_name(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    directory: str,
    file_filter: str,
) -> tuple[str, str]:
    dialog = FlowgridThemedFileDialog(parent, app_window, theme_kind)
    _configure_flowgrid_file_dialog(dialog, title, directory, file_filter)
    dialog.setFileMode(QFileDialog.FileMode.ExistingFile)
    dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptOpen)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return "", dialog.selectedNameFilter()
    files = dialog.selectedFiles()
    return (str(files[0]), dialog.selectedNameFilter()) if files else ("", dialog.selectedNameFilter())


def show_flowgrid_themed_open_file_names(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    directory: str,
    file_filter: str,
) -> tuple[list[str], str]:
    dialog = FlowgridThemedFileDialog(parent, app_window, theme_kind)
    _configure_flowgrid_file_dialog(dialog, title, directory, file_filter)
    dialog.setFileMode(QFileDialog.FileMode.ExistingFiles)
    dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptOpen)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return [], dialog.selectedNameFilter()
    return [str(path) for path in dialog.selectedFiles()], dialog.selectedNameFilter()


def show_flowgrid_themed_save_file_name(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    directory: str,
    file_filter: str,
) -> tuple[str, str]:
    dialog = FlowgridThemedFileDialog(parent, app_window, theme_kind)
    _configure_flowgrid_file_dialog(dialog, title, directory, file_filter)
    dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptSave)
    dialog.setFileMode(QFileDialog.FileMode.AnyFile)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return "", dialog.selectedNameFilter()
    files = dialog.selectedFiles()
    return (str(files[0]), dialog.selectedNameFilter()) if files else ("", dialog.selectedNameFilter())


def show_flowgrid_themed_existing_directory(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    directory: str,
) -> str:
    dialog = FlowgridThemedFileDialog(parent, app_window, theme_kind)
    _configure_flowgrid_file_dialog(dialog, title, directory, "")
    dialog.setFileMode(QFileDialog.FileMode.Directory)
    dialog.setOption(QFileDialog.Option.ShowDirsOnly, True)
    dialog.setAcceptMode(QFileDialog.AcceptMode.AcceptOpen)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return ""
    files = dialog.selectedFiles()
    return str(files[0]) if files else ""


def show_flowgrid_themed_color(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    current: QColor,
) -> QColor:
    dialog = FlowgridThemedColorDialog(parent, app_window, theme_kind)
    dialog.setWindowTitle(str(title or "").strip() or "Pick Color")
    dialog.setCurrentColor(current)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return QColor()
    return dialog.currentColor()


def _build_flowgrid_themed_input_dialog(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
) -> QInputDialog:
    if app_window is None and parent is not None:
        app_window = getattr(parent, "app_window", None)
    dialog = FlowgridThemedInputDialog(parent, app_window, theme_kind)
    dialog.setWindowTitle(str(title or "").strip() or "Input")
    dialog.setModal(True)
    dialog.setOption(QInputDialog.InputDialogOption.UseListViewForComboBoxItems, False)
    dialog.setMinimumWidth(340)
    dialog.setMaximumWidth(520)
    resolved_theme = str(theme_kind or "").strip() or "main"
    if app_window is not None:
        try:
            dialog.setStyleSheet(app_window._popup_theme_stylesheet(resolved_theme, force_opaque_root=True))
        except Exception as exc:
            _runtime_log_event(
                "ui.themed_input_dialog_stylesheet_failed",
                severity="warning",
                summary="Failed applying themed stylesheet to input dialog.",
                exc=exc,
                context={"theme_kind": str(resolved_theme), "title": str(title)},
            )
    return dialog


def show_flowgrid_themed_input_text(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    label: str,
    default_text: str = "",
) -> tuple[str, bool]:
    dialog = _build_flowgrid_themed_input_dialog(parent, app_window, theme_kind, title)
    dialog.setInputMode(QInputDialog.InputMode.TextInput)
    dialog.setLabelText(str(label or ""))
    dialog.setTextValue(str(default_text or ""))
    accepted = dialog.exec() == QDialog.DialogCode.Accepted
    return str(dialog.textValue() or ""), bool(accepted)


def show_flowgrid_themed_input_int(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    label: str,
    value: int,
    min_value: int,
    max_value: int,
    step: int = 1,
) -> tuple[int, bool]:
    dialog = _build_flowgrid_themed_input_dialog(parent, app_window, theme_kind, title)
    dialog.setInputMode(QInputDialog.InputMode.IntInput)
    dialog.setLabelText(str(label or ""))
    dialog.setIntRange(int(min_value), int(max_value))
    dialog.setIntStep(max(1, int(step)))
    dialog.setIntValue(int(value))
    accepted = dialog.exec() == QDialog.DialogCode.Accepted
    return int(dialog.intValue()), bool(accepted)


def show_flowgrid_themed_input_item(
    parent: QWidget | None,
    app_window: Any | None,
    theme_kind: str,
    title: str,
    label: str,
    items: list[str] | tuple[str, ...],
    current_index: int = 0,
    editable: bool = False,
) -> tuple[str, bool]:
    dialog = _build_flowgrid_themed_input_dialog(parent, app_window, theme_kind, title)
    values = [str(item or "").strip() for item in items if str(item or "").strip()]
    if not values:
        return "", False
    index = int(clamp(int(current_index), 0, len(values) - 1))
    dialog.setInputMode(QInputDialog.InputMode.TextInput)
    dialog.setLabelText(str(label or ""))
    dialog.setComboBoxItems(values)
    dialog.setComboBoxEditable(bool(editable))
    dialog.setTextValue(values[index])
    dialog.setMinimumHeight(148)
    dialog.setMaximumHeight(230)
    accepted = dialog.exec() == QDialog.DialogCode.Accepted
    return str(dialog.textValue() or "").strip(), bool(accepted)


def restore_flowgrid_popup_position(
    widget: QWidget,
    config: dict[str, Any] | None,
    popup_key: str,
    *,
    queue_save: Callable[[], None] | None = None,
) -> bool:
    normalized_key = str(popup_key or "").strip()
    if not normalized_key or not isinstance(config, dict):
        return False

    popup_positions = config.get("popup_positions", {})
    if not isinstance(popup_positions, dict):
        return False

    popup_pos = popup_positions.get(normalized_key)
    if not isinstance(popup_pos, dict) or "x" not in popup_pos or "y" not in popup_pos:
        return False

    try:
        x = int(popup_pos.get("x", 0))
        y = int(popup_pos.get("y", 0))
    except Exception as exc:
        _runtime_log_event(
            "ui.popup_restore_position_invalid",
            severity="warning",
            summary="Stored popup position was invalid; skipping restore for this popup.",
            exc=exc,
            context={"popup_key": normalized_key, "window_title": str(widget.windowTitle())},
        )
        return False

    win_w = max(120, int(widget.width()))
    win_h = max(120, int(widget.height()))
    target_rect = QRect(int(x), int(y), win_w, win_h)

    visible_geometry: QRect | None = None
    for screen in QGuiApplication.screens():
        try:
            geometry = screen.availableGeometry()
        except Exception as exc:
            _runtime_log_event(
                "ui.popup_restore_screen_geometry_failed",
                severity="warning",
                summary="Failed reading screen geometry while restoring popup position; checking next screen.",
                exc=exc,
                context={"popup_key": normalized_key, "window_title": str(widget.windowTitle())},
            )
            continue
        if geometry.intersects(target_rect):
            visible_geometry = geometry
            break

    if visible_geometry is None:
        primary = QGuiApplication.primaryScreen()
        if primary is not None:
            try:
                visible_geometry = primary.availableGeometry()
            except Exception as exc:
                _runtime_log_event(
                    "ui.popup_restore_primary_geometry_failed",
                    severity="warning",
                    summary="Failed reading primary-screen geometry while restoring popup position; using stored coordinates.",
                    exc=exc,
                    context={"popup_key": normalized_key, "window_title": str(widget.windowTitle())},
                )

    if visible_geometry is None:
        widget.move(int(x), int(y))
        return True

    max_x = int(visible_geometry.right() - win_w + 1)
    max_y = int(visible_geometry.bottom() - win_h + 1)
    if max_x < int(visible_geometry.left()):
        max_x = int(visible_geometry.left())
    if max_y < int(visible_geometry.top()):
        max_y = int(visible_geometry.top())

    clamped_x = int(clamp(x, int(visible_geometry.left()), max_x))
    clamped_y = int(clamp(y, int(visible_geometry.top()), max_y))
    widget.move(clamped_x, clamped_y)

    if clamped_x != x or clamped_y != y:
        popup_positions[normalized_key] = {"x": clamped_x, "y": clamped_y}
        if callable(queue_save):
            queue_save()

    return True


class DepotFramelessToolWindow(QDialog):
    """Frameless depot panels: shared chrome, drag-to-move, themed background, themed message boxes."""

    def __init__(
        self,
        app_window: Any | None,
        *,
        window_title: str,
        theme_kind: str,
        size: tuple[int, int] = (780, 560),
        minimum_size: tuple[int, int] | None = None,
    ) -> None:
        super().__init__()
        self.app_window = app_window
        self._theme_kind = theme_kind
        self._drag_offset: QPoint | None = None

        self.setWindowTitle(window_title)
        self.setModal(False)
        self.setWindowModality(Qt.WindowModality.NonModal)
        self.setWindowFlags(Qt.WindowType.Window | Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.resize(int(size[0]), int(size[1]))
        if minimum_size is not None:
            self.setMinimumSize(int(minimum_size[0]), int(minimum_size[1]))
        self._copy_notice_label: QLabel | None = None
        self._copy_notice_host: QWidget | None = None

        self.root_layout = QVBoxLayout(self)
        self.root_layout.setContentsMargins(
            UI_MARGIN_FRAMELESS_TOOL,
            UI_MARGIN_FRAMELESS_TOOL,
            UI_MARGIN_FRAMELESS_TOOL,
            UI_MARGIN_FRAMELESS_TOOL,
        )
        self.root_layout.setSpacing(UI_SPACING_FRAMELESS_TOOL)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(UI_SPACING_FRAMELESS_TOOL)
        self.header_title = QLabel(window_title)
        self.header_title.setProperty("section", True)
        self.header_mode_badge = QLabel(self._read_only_badge_text())
        self.header_mode_badge.setObjectName("DepotReadOnlyBadge")
        self.header_mode_badge.setVisible(bool(self.header_mode_badge.text()))
        self.header_mode_badge.setStyleSheet(
            "QLabel#DepotReadOnlyBadge {"
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
        self.header_close_btn = QPushButton("x")
        self.header_close_btn.setFixedSize(HEADER_CLOSE_BUTTON_WIDTH, HEADER_CLOSE_BUTTON_HEIGHT)
        self.header_close_btn.setObjectName("DepotFramelessCloseButton")
        self.header_close_btn.setAutoDefault(False)
        self.header_close_btn.setDefault(False)
        self.header_close_btn.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.header_close_btn.clicked.connect(self.close)
        header_row.addWidget(self.header_title)
        header_row.addWidget(self.header_mode_badge, 0, Qt.AlignmentFlag.AlignVCenter)
        header_row.addStretch(1)
        header_row.addWidget(self.header_close_btn)
        self.root_layout.addLayout(header_row)
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)

    def is_read_only_mode(self) -> bool:
        if self.app_window is None:
            return False
        runtime_options = getattr(self.app_window, "runtime_options", None)
        if runtime_options is not None:
            return bool(getattr(runtime_options, "read_only_db", False))
        depot_db = getattr(self.app_window, "depot_db", None)
        return bool(getattr(depot_db, "read_only", False))

    def _read_only_badge_text(self) -> str:
        if not self.is_read_only_mode():
            return ""
        if self.app_window is None:
            return "READ ONLY"
        runtime_options = getattr(self.app_window, "runtime_options", None)
        channel_label = str(getattr(runtime_options, "channel_label", "") or "").strip()
        if channel_label:
            return f"{channel_label.upper()} READ ONLY"
        return "READ ONLY"

    def _read_only_message_text(self, action_label: str = "") -> str:
        action = str(action_label or "That action").strip() or "That action"
        return f"{action} is unavailable while this channel is running in read-only mode."

    def _show_read_only_message(self, action_label: str = "") -> None:
        self._show_themed_message(QMessageBox.Icon.Information, "Read Only", self._read_only_message_text(action_label))

    def _warn_if_read_only(self, action_label: str = "") -> bool:
        if not self.is_read_only_mode():
            return False
        _runtime_log_event(
            "ui.read_only_action_blocked",
            severity="info",
            summary="A read-only channel blocked a write-capable UI action.",
            context={
                "window_title": str(self.windowTitle()),
                "action_label": str(action_label or "").strip(),
            },
        )
        self._show_read_only_message(action_label)
        return True

    def _disable_widgets_for_read_only(self, widgets: list[QWidget], action_label: str) -> None:
        if not self.is_read_only_mode():
            return
        tooltip = self._read_only_message_text(action_label)
        for widget in widgets:
            if widget is None:
                continue
            widget.setEnabled(False)
            widget.setToolTip(tooltip)

    def apply_theme_styles(self) -> None:
        if self.app_window is None:
            return
        self.setStyleSheet(self.app_window._popup_theme_stylesheet(self._theme_kind))

    def _refresh_coordinator(self) -> Any | None:
        if self.app_window is None:
            return None
        return getattr(self.app_window, "depot_refresh_coordinator", None)

    def _should_refresh_depot_view(
        self,
        view_key: str,
        state_key: Any,
        *,
        force: bool = False,
        ttl_ms: int = DEPOT_VIEW_TTL_MS,
        reason: str = "",
    ) -> bool:
        coordinator = self._refresh_coordinator()
        if coordinator is None:
            return True
        return coordinator.should_refresh_view(
            view_key,
            state_key,
            force=force,
            ttl_ms=ttl_ms,
            reason=reason,
        )

    def _mark_depot_view_refreshed(
        self,
        view_key: str,
        state_key: Any,
        *,
        payload: Any = None,
        reason: str = "",
        duration_ms: float | None = None,
        row_count: int | None = None,
    ) -> None:
        coordinator = self._refresh_coordinator()
        if coordinator is None:
            return
        coordinator.mark_view_refreshed(
            view_key,
            state_key,
            payload=payload,
            reason=reason,
            duration_ms=duration_ms,
            row_count=row_count,
        )

    def set_window_always_on_top(self, enabled: bool) -> None:
        keep_on_top = bool(enabled)
        was_visible = self.isVisible()
        self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint, keep_on_top)
        if was_visible:
            self.show()
            if keep_on_top:
                self.raise_()

    def _load_window_always_on_top_preference(self, config_key: str, default: bool = True) -> bool:
        if self.app_window is None:
            return bool(default)
        resolved_key = str(config_key or "").strip()
        popup_key = self.app_window._popup_window_on_top_config_key(self._theme_kind)
        if popup_key:
            resolved_key = popup_key
        if resolved_key:
            self._always_on_top_config_key = resolved_key
        return bool(self.app_window.config.get(resolved_key, default))

    def _apply_window_always_on_top_preference(self, config_key: str, enabled: bool) -> bool:
        keep_on_top = bool(enabled)
        if self.app_window is not None:
            popup_key = self.app_window._popup_window_on_top_config_key(self._theme_kind)
            resolved_key = popup_key or str(config_key or "").strip()
            if popup_key:
                keep_on_top = self.app_window._apply_popup_window_on_top_preference(self._theme_kind, keep_on_top)
            else:
                self.set_window_always_on_top(keep_on_top)
                if resolved_key:
                    self.app_window.config[resolved_key] = keep_on_top
            if resolved_key:
                self._always_on_top_config_key = resolved_key
            self.app_window.queue_save_config()
        else:
            self.set_window_always_on_top(keep_on_top)
        return keep_on_top

    def _show_themed_message(self, icon: QMessageBox.Icon, title: str, text: str) -> None:
        show_flowgrid_themed_message(self, self.app_window, self._theme_kind, icon, title, text)

    def _clear_copy_notice(self) -> None:
        label = self._copy_notice_label
        self._copy_notice_label = None
        self._copy_notice_host = None
        if label is None:
            return
        try:
            label.hide()
            label.deleteLater()
        except Exception as exc:
            _runtime_log_event(
                "ui.copy_notice_clear_failed",
                severity="warning",
                summary="Failed clearing copy-notice overlay.",
                exc=exc,
                context={"window_title": str(self.windowTitle())},
            )

    def _show_copy_notice(self, anchor: QWidget | None, text: str, *, duration_ms: int = 4200) -> None:
        self._clear_copy_notice()
        host = anchor
        if host is not None and hasattr(host, "viewport"):
            try:
                vp = host.viewport()  # type: ignore[attr-defined]
                if isinstance(vp, QWidget):
                    host = vp
            except Exception:
                host = anchor
        if not isinstance(host, QWidget):
            host = self
        message = str(text or "").strip()
        if not message:
            return
        notice = QLabel(message, host)
        notice.setObjectName("FlowgridCopyNotice")
        notice.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        notice.setAlignment(Qt.AlignmentFlag.AlignCenter)
        notice.setWordWrap(False)
        notice.setStyleSheet(
            "QLabel#FlowgridCopyNotice {"
            "background-color: rgba(8, 16, 28, 228);"
            "color: #FFFFFF;"
            "border: 1px solid rgba(74, 196, 186, 230);"
            "border-radius: 8px;"
            "padding: 4px 10px;"
            "font-weight: 800;"
            "}"
        )
        hint = notice.sizeHint()
        max_w = max(170, int(host.width()) - 16)
        width = int(clamp(int(hint.width()) + 20, 180, max_w))
        height = int(max(28, int(hint.height()) + 10))
        x = int(max(8, int(host.width()) - width - 8))
        y = int(max(8, int(host.height()) - height - 8))
        notice.setGeometry(x, y, width, height)
        notice.show()
        notice.raise_()
        self._copy_notice_label = notice
        self._copy_notice_host = host
        QTimer.singleShot(max(1200, int(duration_ms)), self._clear_copy_notice)

    def eventFilter(self, watched, event) -> bool:  # noqa: N802
        if (
            self.isVisible()
            and isinstance(watched, QWidget)
            and (watched is self or self.isAncestorOf(watched))
            and isinstance(event, QMouseEvent)
        ):
            if event.type() == QEvent.Type.MouseButtonPress and event.button() == Qt.MouseButton.LeftButton:
                if not _is_drag_blocked_widget(watched):
                    self._drag_offset = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
                    event.accept()
                    return True
            elif event.type() == QEvent.Type.MouseMove and self._drag_offset is not None:
                if event.buttons() & Qt.MouseButton.LeftButton:
                    self.move(event.globalPosition().toPoint() - self._drag_offset)
                    event.accept()
                    return True
            elif event.type() == QEvent.Type.MouseButtonRelease and self._drag_offset is not None:
                self._drag_offset = None
                event.accept()
                return True
        return super().eventFilter(watched, event)

    def mousePressEvent(self, event: QMouseEvent) -> None:  # noqa: N802
        target = _drag_target_widget(self, event.position().toPoint())
        if event.button() == Qt.MouseButton.LeftButton and not _is_drag_blocked_widget(target):
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

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform)
        if self.app_window is not None:
            _paint_flowgrid_popup_background(self, painter, self.app_window, self._theme_kind)
        super().paintEvent(event)

    def closeEvent(self, event) -> None:  # noqa: N802
        self._drag_offset = None
        self._clear_copy_notice()
        super().closeEvent(event)

    def keyPressEvent(self, event) -> None:  # noqa: N802
        key = event.key()
        if key in (int(Qt.Key.Key_Return), int(Qt.Key.Key_Enter)):
            event.accept()
            return
        super().keyPressEvent(event)


def _iter_flowgrid_shell_windows() -> list[QWidget]:
    app = QApplication.instance()
    if app is None:
        return []
    shells: list[QWidget] = []
    for widget in app.topLevelWidgets():
        if not isinstance(widget, QWidget):
            continue
        if bool(widget.property(FLOWGRID_SHELL_WINDOW_PROPERTY)) or bool(getattr(widget, "_flowgrid_shell_window", False)):
            shells.append(widget)
    return shells


def _visible_flowgrid_shell_window() -> QWidget | None:
    for shell in _iter_flowgrid_shell_windows():
        try:
            if shell.isVisible():
                return shell
        except Exception:
            continue
    return None


def _ensure_shell_window_available(preferred_shell: QWidget | None) -> QWidget | None:
    visible_shell = _visible_flowgrid_shell_window()
    if visible_shell is not None:
        try:
            if visible_shell.isMinimized():
                visible_shell.showNormal()
            visible_shell.raise_()
            visible_shell.activateWindow()
        except Exception as exc:
            _runtime_log_event(
                "ui.shell_window_activate_failed",
                severity="warning",
                summary="Failed activating the existing Flowgrid shell window.",
                exc=exc,
            )
        return visible_shell

    candidate_shells: list[QWidget] = []
    if preferred_shell is not None:
        candidate_shells.append(preferred_shell)
    for shell in _iter_flowgrid_shell_windows():
        if shell not in candidate_shells:
            candidate_shells.append(shell)

    for shell in candidate_shells:
        try:
            if shell.isMinimized():
                shell.showNormal()
            else:
                shell.show()
            shell.raise_()
            shell.activateWindow()
            return shell
        except Exception as exc:
            _runtime_log_event(
                "ui.shell_window_restore_failed",
                severity="warning",
                summary="Failed restoring an existing Flowgrid shell window; trying the next candidate.",
                exc=exc,
            )

    factory = _FLOWGRID_SHELL_FACTORY
    if factory is None:
        _runtime_log_event(
            "ui.shell_window_factory_missing",
            severity="error",
            summary="No Flowgrid shell factory is configured for shell-window recovery.",
        )
        return None

    try:
        shell = factory()
        mark_flowgrid_shell_window(shell)
        shell.show()
        shell.raise_()
        shell.activateWindow()
        return shell
    except Exception as exc:
        _runtime_log_event(
            "ui.shell_window_create_failed",
            severity="error",
            summary="Failed creating a replacement Flowgrid shell window.",
            exc=exc,
        )
        return None


__all__ = [
    "DepotFramelessToolWindow",
    "FLOWGRID_SHELL_WINDOW_PROPERTY",
    "FlowgridThemedDialog",
    "_apply_flowgrid_popup_stylesheet",
    "_ensure_shell_window_available",
    "_paint_flowgrid_popup_background",
    "_resolve_flowgrid_popup_app_window",
    "_visible_flowgrid_shell_window",
    "configure_flowgrid_shell_factory",
    "mark_flowgrid_shell_window",
    "restore_flowgrid_popup_position",
    "show_flowgrid_themed_color",
    "show_flowgrid_themed_existing_directory",
    "show_flowgrid_themed_input_int",
    "show_flowgrid_themed_input_item",
    "show_flowgrid_themed_input_text",
    "show_flowgrid_themed_message",
    "show_flowgrid_themed_open_file_name",
    "show_flowgrid_themed_open_file_names",
    "show_flowgrid_themed_save_file_name",
]
