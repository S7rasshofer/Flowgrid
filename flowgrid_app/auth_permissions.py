from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RoleSnapshot:
    user_id: str
    role_name: str
    role_slot: str
    access_level: str
    is_admin: bool
    agent_tier: int
    is_tech3: bool
    is_mp: bool
    can_open_agent_window: bool
    can_open_qa_window: bool
    can_access_qa: bool
    can_access_hidden_tabs: bool
    can_access_dashboard: bool


class PermissionDeniedError(RuntimeError):
    pass


class PermissionService:
    AGENT_ACCESS_DENIED_MESSAGE = "Only administrators and tech-role users can access the Agent window."
    QA_ACCESS_DENIED_MESSAGE = "Only administrators and QA-role users can access the QA/WCS window."
    ADMIN_ACCESS_DENIED_MESSAGE = "Only administrators can access the User Setup window."
    DASHBOARD_ACCESS_DENIED_MESSAGE = "Only administrators and reporting users can access the dashboard."

    def __init__(self, user_repository: Any):
        self.user_repository = user_repository

    def role_snapshot(self, user_id: str) -> RoleSnapshot:
        return self.user_repository.get_role_snapshot(user_id)

    def can_access_qa(self, user_id: str) -> bool:
        return bool(self.role_snapshot(user_id).can_access_qa)

    def can_open_agent_window(self, user_id: str) -> bool:
        return bool(self.role_snapshot(user_id).can_open_agent_window)

    def can_access_admin(self, user_id: str) -> bool:
        return bool(self.role_snapshot(user_id).is_admin)

    def can_access_hidden_tabs(self, user_id: str) -> bool:
        return bool(self.role_snapshot(user_id).can_access_hidden_tabs)

    def can_access_dashboard(self, user_id: str) -> bool:
        return bool(self.role_snapshot(user_id).can_access_dashboard)

    def require_agent_access(self, user_id: str) -> None:
        if not self.can_open_agent_window(user_id):
            raise PermissionDeniedError(self.AGENT_ACCESS_DENIED_MESSAGE)

    def require_qa_access(self, user_id: str) -> None:
        if not self.can_access_qa(user_id):
            raise PermissionDeniedError(self.QA_ACCESS_DENIED_MESSAGE)

    def require_admin_access(self, user_id: str) -> None:
        if not self.can_access_admin(user_id):
            raise PermissionDeniedError(self.ADMIN_ACCESS_DENIED_MESSAGE)

    def require_dashboard_access(self, user_id: str) -> None:
        if not self.can_access_dashboard(user_id):
            raise PermissionDeniedError(self.DASHBOARD_ACCESS_DENIED_MESSAGE)
