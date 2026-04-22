from __future__ import annotations

from datetime import date, datetime

from PySide6.QtCore import QRect, QRectF, Qt
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPalette, QPen
from PySide6.QtWidgets import QStyle, QStyleOptionTab, QStylePainter, QTabBar, QSizePolicy, QWidget

from flowgrid_app.runtime_logging import _runtime_log_event
from flowgrid_app.ui_utils import normalize_hex


def parse_iso_date(raw_value: str) -> date | None:
    stamp = str(raw_value or "").strip()
    if not stamp:
        return None
    try:
        return datetime.fromisoformat(stamp.replace("Z", "+00:00")).date()
    except Exception:
        pass
    if len(stamp) >= 10:
        try:
            return datetime.strptime(stamp[:10], "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def parse_iso_datetime(raw_value: str) -> datetime | None:
    stamp = str(raw_value or "").strip()
    if not stamp:
        return None
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone().replace(tzinfo=None)
        return parsed
    except Exception:
        pass
    candidate = stamp.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(candidate[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    return None


def part_age_label(created_at: str) -> str:
    raw = str(created_at or "").strip()
    if len(raw) < 10:
        return "-"
    try:
        created_day = datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except Exception:
        return "-"
    age_days = max(0, (datetime.utcnow().date() - created_day).days)
    return f"{age_days}d"


def note_preview(note: str, max_len: int = 64) -> str:
    cleaned = " ".join(str(note or "").split()).strip()
    if not cleaned:
        return "(none)"
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[: max(0, max_len - 3)].rstrip() + "..."


def format_working_updated_stamp(raw_stamp: str) -> str:
    stamp = str(raw_stamp or "").strip()
    if not stamp:
        return ""
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        return parsed.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return stamp.replace("T", " ")


class AlertPulseTabBar(QTabBar):
    """Tab bar with per-tab alert pulse/bloom rendering without changing tab width."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._alert_indices: set[int] = set()
        self._ack_indices: set[int] = set()
        self._flash_on = True
        self._paint_error_logged = False
        self._normal_text_color = QColor("#FFFFFF")
        self._alert_color = QColor("#D95A5A")
        self._ack_color = QColor("#D1A91F")
        self.setDrawBase(False)

    def set_alert_visual_state(
        self,
        alert_indices: set[int],
        ack_indices: set[int],
        flash_on: bool,
        normal_text_color: QColor,
    ) -> None:
        self._alert_indices = {int(idx) for idx in alert_indices if idx >= 0}
        self._ack_indices = {int(idx) for idx in ack_indices if idx >= 0}
        self._flash_on = bool(flash_on)
        self._normal_text_color = QColor(normal_text_color)
        self.update()

    def _paint_bloom(self, painter: QPainter, tab_rect: QRect, pulse_on: bool, acknowledged: bool) -> None:
        if tab_rect.isNull():
            return
        base = QColor(self._ack_color if acknowledged else self._alert_color)
        if acknowledged:
            inner_alpha = 126
            mid_alpha = 82
            outer_alpha = 46
        elif pulse_on:
            inner_alpha = 210
            mid_alpha = 148
            outer_alpha = 92
        else:
            inner_alpha = 22
            mid_alpha = 10
            outer_alpha = 0

        outer_rect = QRectF(tab_rect.adjusted(-14, -8, 14, 9))
        mid_rect = QRectF(tab_rect.adjusted(-8, -5, 8, 6))
        inner_rect = QRectF(tab_rect.adjusted(-3, -2, 3, 3))

        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.setPen(Qt.PenStyle.NoPen)

        for rect, alpha in ((outer_rect, outer_alpha), (mid_rect, mid_alpha), (inner_rect, inner_alpha)):
            path = QPainterPath()
            path.addRoundedRect(rect, 9.0, 9.0)
            fill = QColor(base)
            fill.setAlpha(int(max(0, alpha)))
            painter.fillPath(path, fill)
        painter.restore()

    def paintEvent(self, event) -> None:  # noqa: N802
        try:
            painter = QStylePainter(self)
            for idx in range(int(self.count())):
                opt = QStyleOptionTab()
                self.initStyleOption(opt, idx)
                alert_enabled = idx in self._alert_indices
                acknowledged = idx in self._ack_indices
                pulse_on = bool(alert_enabled and not acknowledged and self._flash_on)

                palette = QPalette(opt.palette)
                if alert_enabled:
                    if pulse_on:
                        text_color = QColor("#FFFFFF")
                    elif acknowledged:
                        text_color = QColor("#FFF9E5")
                    else:
                        text_color = QColor("#FFF2F2")
                else:
                    text_color = QColor(self._normal_text_color)
                palette.setColor(QPalette.ColorRole.ButtonText, text_color)
                palette.setColor(QPalette.ColorRole.WindowText, text_color)
                opt.palette = palette

                painter.drawControl(QStyle.ControlElement.CE_TabBarTabShape, opt)
                if alert_enabled:
                    self._paint_bloom(painter, self.tabRect(idx), pulse_on, acknowledged)
                painter.drawControl(QStyle.ControlElement.CE_TabBarTabLabel, opt)
        except Exception as exc:
            if not self._paint_error_logged:
                self._paint_error_logged = True
                _runtime_log_event(
                    "ui.alert_tabbar_paint_failed",
                    severity="error",
                    summary="Alert pulse tab bar paint failed; falling back to default tab rendering.",
                    exc=exc,
                    context={"tab_count": int(self.count())},
                )
            super().paintEvent(event)


class TouchDistributionBar(QWidget):
    """Simple stacked bar showing filtered submission touch counts by color."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._segments: list[tuple[str, int, str]] = []
        self._total = 0
        self.setMinimumHeight(24)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

    def set_segments(self, segments: list[tuple[str, int, str]]) -> None:
        cleaned: list[tuple[str, int, str]] = []
        total = 0
        for label, count, color in segments:
            safe_count = int(max(0, int(count)))
            if safe_count <= 0:
                continue
            safe_color = normalize_hex(str(color or "#7BDAEA"), "#7BDAEA")
            cleaned.append((str(label), safe_count, safe_color))
            total += safe_count
        self._segments = cleaned
        self._total = total
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        bounds = self.rect().adjusted(0, 0, -1, -1)
        if bounds.width() <= 2 or bounds.height() <= 2:
            return

        bar_path = QPainterPath()
        bar_path.addRoundedRect(QRectF(bounds), 6.0, 6.0)

        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(12, 18, 24, 110))
        painter.drawPath(bar_path)

        if self._total > 0 and self._segments:
            painter.save()
            painter.setClipPath(bar_path)
            full_width = float(bounds.width())
            x = float(bounds.left())
            remaining_width = full_width
            remaining_total = int(self._total)
            for idx, (_label, count, color_hex) in enumerate(self._segments):
                if idx == len(self._segments) - 1:
                    segment_width = remaining_width
                else:
                    ratio = float(count) / max(1.0, float(remaining_total))
                    segment_width = max(1.0, round(remaining_width * ratio, 2))
                segment_rect = QRectF(x, float(bounds.top()), segment_width, float(bounds.height()))
                painter.setBrush(QColor(color_hex))
                painter.drawRect(segment_rect)
                x += segment_width
                remaining_width -= segment_width
                remaining_total -= count
            painter.restore()

        painter.setBrush(Qt.BrushStyle.NoBrush)
        painter.setPen(QPen(QColor(255, 255, 255, 72), 1))
        painter.drawPath(bar_path)


__all__ = [
    "AlertPulseTabBar",
    "TouchDistributionBar",
    "format_working_updated_stamp",
    "note_preview",
    "parse_iso_date",
    "parse_iso_datetime",
    "part_age_label",
]
