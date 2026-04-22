from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(slots=True)
class RuntimeOptions:
    read_only_db: bool = False
    skip_shortcut_sync: bool = False
    skip_startup_repairs: bool = False
    skip_shared_icon_reconcile: bool = False
    message_sink: Callable[[dict[str, Any]], None] | None = None


@dataclass(slots=True)
class AppContext:
    current_user: str
    config: dict[str, Any]
    db: Any
    tracker: Any
    user_repository: Any | None = None
    permission_service: Any | None = None
    shell: Any | None = None
    runtime_options: RuntimeOptions = field(default_factory=RuntimeOptions)
    extra: dict[str, Any] = field(default_factory=dict)
