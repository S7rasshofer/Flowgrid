from __future__ import annotations

from typing import Any, Callable


class WindowManager:
    _LEGACY_ATTRS = {
        "agent": "active_agent_window",
        "qa": "active_qa_window",
        "admin": "admin_dialog",
        "dashboard": "depot_dashboard_dialog",
    }

    def __init__(self, shell: Any):
        self.shell = shell
        self._windows: dict[str, Any] = {}

    def get_window(self, key: str) -> Any | None:
        return self._windows.get(str(key or "").strip().lower())

    def set_window(self, key: str, window: Any | None) -> None:
        normalized = str(key or "").strip().lower()
        legacy_attr = self._LEGACY_ATTRS.get(normalized)
        if not normalized:
            return
        if window is None:
            self._windows.pop(normalized, None)
        else:
            self._windows[normalized] = window
        if legacy_attr:
            setattr(self.shell, legacy_attr, window)

    def clear_window(self, key: str, window: Any | None = None) -> None:
        normalized = str(key or "").strip().lower()
        current = self._windows.get(normalized)
        if window is not None and current is not None and current is not window:
            return
        self.set_window(normalized, None)

    def _attach_window(self, key: str, window: Any) -> Any:
        try:
            from PySide6.QtCore import Qt

            window.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        except Exception:
            pass
        try:
            window.destroyed.connect(lambda *_args, _key=key, _window=window: self.clear_window(_key, _window))
        except Exception:
            pass
        self.set_window(key, window)
        return window

    def ensure_window(self, key: str, factory: Callable[[], Any]) -> Any:
        existing = self.get_window(key)
        if existing is not None:
            return existing
        window = factory()
        return self._attach_window(key, window)

    def show_window(self, key: str, factory: Callable[[], Any], prepare: Callable[[Any], None] | None = None) -> Any:
        window = self.ensure_window(key, factory)
        if prepare is not None:
            prepare(window)
        window.show()
        window.raise_()
        window.activateWindow()
        return window

    def show_controlled_window(
        self,
        key: str,
        factory: Callable[[], Any],
        *,
        can_open: Callable[[], bool] | None = None,
        on_denied: Callable[[], None] | None = None,
        prepare: Callable[[Any], None] | None = None,
    ) -> Any | None:
        if can_open is not None and not bool(can_open()):
            if on_denied is not None:
                on_denied()
            return None
        return self.show_window(key, factory, prepare=prepare)

    def close_all(self) -> None:
        for key, window in list(self._windows.items()):
            try:
                if window is not None:
                    window.close()
            finally:
                self.clear_window(key, window)
