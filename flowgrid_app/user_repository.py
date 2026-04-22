from __future__ import annotations

from typing import Any

from .auth_permissions import RoleSnapshot


class UserRepository:
    def __init__(self, tracker: Any, rules: Any):
        self.tracker = tracker
        self.rules = rules

    def _empty_role_snapshot(self) -> RoleSnapshot:
        return RoleSnapshot(
            user_id="",
            role_name="",
            role_slot=self.rules.ROLE_SLOT_NONE,
            access_level=self.rules.ADMIN_ACCESS_NONE,
            is_admin=False,
            agent_tier=0,
            is_tech3=False,
            is_mp=False,
            can_open_agent_window=False,
            can_open_qa_window=False,
            can_access_qa=False,
            can_access_hidden_tabs=False,
            can_access_dashboard=False,
        )

    def _role_definition_maps(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        by_name: dict[str, dict[str, Any]] = {}
        preferred_by_slot: dict[str, dict[str, Any]] = {}
        for row in self.tracker.list_role_definitions():
            role_name = str(row.get("role_name", "") or "").strip()
            role_slot = self.rules.normalize_role_slot(
                row.get("role_slot", ""),
                default=self.rules.ROLE_SLOT_NONE,
            )
            if not role_name:
                continue
            entry = {
                "role_name": role_name,
                "role_slot": role_slot,
                "sort_order": int(row.get("sort_order", 0) or 0),
            }
            by_name[role_name.casefold()] = entry
            preferred_by_slot.setdefault(role_slot, entry)
        return by_name, preferred_by_slot

    def _resolve_role_assignment(
        self,
        stored_role_name: str,
        agent_tier: int,
    ) -> tuple[str, str]:
        normalized_role_name = str(stored_role_name or "").strip()
        by_name, preferred_by_slot = self._role_definition_maps()
        if normalized_role_name:
            matched = by_name.get(normalized_role_name.casefold())
            if matched is not None:
                return str(matched.get("role_name", "") or ""), str(matched.get("role_slot", "") or "")
        if int(agent_tier) > 0:
            role_slot = self.rules.role_slot_from_agent_tier(agent_tier, default=self.rules.ROLE_SLOT_NONE)
            fallback = preferred_by_slot.get(role_slot)
            if fallback is not None:
                return str(fallback.get("role_name", "") or ""), str(fallback.get("role_slot", "") or "")
        return "", self.rules.ROLE_SLOT_NONE

    def get_role_snapshot(self, user_id: str) -> RoleSnapshot:
        normalized = self.rules.normalize_user_id(user_id)
        if not normalized:
            return self._empty_role_snapshot()

        access_level = self.rules.ADMIN_ACCESS_NONE
        stored_role_name = ""
        is_admin = normalized == "KIDDS"
        if is_admin:
            access_level = self.rules.ADMIN_ACCESS_ADMIN
        else:
            row = self.tracker.db.fetchone(
                "SELECT COALESCE(access_level, '') AS access_level, COALESCE(position, '') AS position "
                "FROM admin_users WHERE user_id=? LIMIT 1",
                (normalized,),
            )
            if row is not None:
                stored_role_name = str(row["position"] or "").strip()
                access_level = self.rules.normalize_admin_access_level(
                    row["access_level"],
                    default=self.rules.ADMIN_ACCESS_NONE,
                )
                is_admin = access_level == self.rules.ADMIN_ACCESS_ADMIN

        raw_agent_tier = 0
        row = self.tracker.db.fetchone("SELECT tier FROM agents WHERE user_id=? LIMIT 1", (normalized,))
        if row is not None:
            raw_agent_tier = int(self.rules.normalize_agent_tier(row["tier"], default=1))

        role_name, role_slot = self._resolve_role_assignment(stored_role_name, raw_agent_tier)
        agent_tier = int(self.rules.role_slot_to_agent_tier(role_slot, default=0))
        is_tech3 = role_slot == self.rules.ROLE_SLOT_TECH3
        is_mp = role_slot == self.rules.ROLE_SLOT_MP
        can_open_agent_window = bool(
            is_admin
            or role_slot
            in {
                self.rules.ROLE_SLOT_TECH1,
                self.rules.ROLE_SLOT_TECH2,
                self.rules.ROLE_SLOT_TECH3,
                self.rules.ROLE_SLOT_MP,
            }
        )
        can_open_qa_window = bool(is_admin or role_slot == self.rules.ROLE_SLOT_QA)
        can_access_hidden_tabs = access_level in {
            self.rules.ADMIN_ACCESS_ADMIN,
            self.rules.ADMIN_ACCESS_REPORTING,
        }
        can_access_dashboard = bool(can_access_hidden_tabs)
        return RoleSnapshot(
            user_id=normalized,
            role_name=str(role_name or ""),
            role_slot=str(role_slot or self.rules.ROLE_SLOT_NONE),
            access_level=str(access_level or ""),
            is_admin=bool(is_admin),
            agent_tier=int(agent_tier),
            is_tech3=bool(is_tech3),
            is_mp=bool(is_mp),
            can_open_agent_window=bool(can_open_agent_window),
            can_open_qa_window=bool(can_open_qa_window),
            can_access_qa=bool(can_open_qa_window),
            can_access_hidden_tabs=bool(can_access_hidden_tabs),
            can_access_dashboard=bool(can_access_dashboard),
        )

    def is_admin_user(self, user_id: str) -> bool:
        return bool(self.get_role_snapshot(user_id).is_admin)

    def can_open_agent_window(self, user_id: str) -> bool:
        return bool(self.get_role_snapshot(user_id).can_open_agent_window)

    def get_agent_tier(self, user_id: str, default: int = 1) -> int:
        snapshot = self.get_role_snapshot(user_id)
        if snapshot.user_id and int(snapshot.agent_tier) > 0:
            return int(snapshot.agent_tier)
        return int(default)

    def can_access_missing_po_followups(self, user_id: str) -> bool:
        return bool(self.get_role_snapshot(user_id).can_access_hidden_tabs)

    def can_access_hidden_tabs(self, user_id: str) -> bool:
        return bool(self.get_role_snapshot(user_id).can_access_hidden_tabs)

    def can_access_dashboard(self, user_id: str) -> bool:
        return bool(self.get_role_snapshot(user_id).can_access_dashboard)

    def list_admin_users(self) -> list[dict[str, Any]]:
        rows = self.tracker.db.fetchall(
            "SELECT user_id, admin_name, position, location, icon_path, COALESCE(access_level, '') AS access_level "
            "FROM admin_users ORDER BY user_id ASC"
        )
        out: list[dict[str, Any]] = []
        for row in rows:
            user_id = self.rules.normalize_user_id(str(row["user_id"] or ""))
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self.tracker._stored_admin_icon_to_abs_path(stored_icon)
            if abs_icon is None:
                fallback = self.tracker._find_icon_for_admin_user(user_id)
                if fallback is not None:
                    fallback_stored = self.tracker._relative_admin_icon_store_path(fallback)
                    if fallback_stored != stored_icon and self.tracker._can_persist_metadata_repairs():
                        self.tracker.db.execute("UPDATE admin_users SET icon_path=? WHERE user_id=?", (fallback_stored, user_id))
                    abs_icon = fallback
            out.append(
                {
                    "user_id": user_id,
                    "admin_name": str(row["admin_name"] or "").strip(),
                    "position": str(row["position"] or "").strip(),
                    "role_name": str(row["position"] or "").strip(),
                    "location": str(row["location"] or "").strip(),
                    "access_level": self.rules.normalize_admin_access_level(
                        row["access_level"],
                        default=self.rules.ADMIN_ACCESS_NONE,
                    ),
                    "icon_path": str(abs_icon) if abs_icon is not None else "",
                }
            )
        return out

    def list_role_definitions(self) -> list[dict[str, Any]]:
        return self.tracker.list_role_definitions()

    def upsert_role_definition(self, role_name: str, role_slot: str, original_role_name: str = "") -> dict[str, Any]:
        return self.tracker.upsert_role_definition(role_name, role_slot, original_role_name=original_role_name)

    def delete_role_definition(self, role_name: str) -> None:
        self.tracker.delete_role_definition(role_name)

    def list_agents(self, tier_filter: int | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if tier_filter in (1, 2, 3, 4):
            where = "WHERE tier=?"
            params.append(int(tier_filter))
        rows = self.tracker.db.fetchall(
            f"SELECT agent_name, user_id, tier, location, icon_path FROM agents {where} ORDER BY tier ASC, agent_name ASC, user_id ASC",
            tuple(params),
        )
        result: list[dict[str, Any]] = []
        for row in rows:
            user_id = self.rules.normalize_user_id(row["user_id"])
            stored_icon = str(row["icon_path"] or "").strip()
            abs_icon = self.tracker._stored_icon_to_abs_path(stored_icon)
            if abs_icon is None:
                fallback = self.tracker._find_icon_for_user(user_id)
                if fallback is not None:
                    fallback_stored = self.tracker._relative_icon_store_path(fallback)
                    if fallback_stored != stored_icon and self.tracker._can_persist_metadata_repairs():
                        self.tracker.db.execute("UPDATE agents SET icon_path=? WHERE user_id=?", (fallback_stored, user_id))
                    abs_icon = fallback
            result.append(
                {
                    "agent_name": str(row["agent_name"] or ""),
                    "user_id": user_id,
                    "tier": self.rules.normalize_agent_tier(row["tier"]),
                    "location": str(row["location"] or "").strip(),
                    "icon_path": str(abs_icon) if abs_icon is not None else "",
                }
            )
        return result

    def part_owner_choice_items(self, work_order: str = "") -> tuple[list[str], dict[str, str], int]:
        normalized_work_order = self.rules.normalize_work_order(work_order)
        current_owner = ""
        if normalized_work_order:
            existing_row = self.tracker.db.fetchone(
                "SELECT COALESCE(assigned_user_id, '') AS assigned_user_id "
                "FROM parts WHERE is_active=1 AND work_order=? ORDER BY id DESC LIMIT 1",
                (normalized_work_order,),
            )
            if existing_row is not None:
                current_owner = self.rules.normalize_user_id(str(existing_row["assigned_user_id"] or ""))

        items: list[str] = []
        item_lookup: dict[str, str] = {}
        current_index = 0
        for agent_row in self.list_agents():
            agent_user = self.rules.normalize_user_id(str(agent_row.get("user_id", "") or ""))
            if not agent_user:
                continue
            agent_name = str(agent_row.get("agent_name", "") or "").strip()
            display_text = f"{agent_user} - {agent_name}" if agent_name else agent_user
            item_lookup[display_text] = agent_user
            items.append(display_text)
            if current_owner and agent_user == current_owner:
                current_index = len(items) - 1
        return items, item_lookup, current_index

    def agent_display_map(self) -> dict[str, tuple[str, str]]:
        agent_meta: dict[str, tuple[str, str]] = {}
        for agent_row in self.list_agents():
            agent_user = self.rules.normalize_user_id(str(agent_row.get("user_id", "") or ""))
            if not agent_user:
                continue
            agent_meta[agent_user] = (
                str(agent_row.get("agent_name", "") or "").strip(),
                str(agent_row.get("icon_path", "") or "").strip(),
            )
        return agent_meta

    def list_assignable_users(self, work_order: str = "") -> tuple[list[str], dict[str, str], int]:
        return self.part_owner_choice_items(work_order)

    def list_setup_users(self) -> list[dict[str, Any]]:
        return self.tracker.list_setup_users()

    def upsert_setup_user(
        self,
        user_id: str,
        name: str,
        role_name: str,
        location: str,
        access_level: str,
        icon_path: str = "",
    ) -> dict[str, Any]:
        return self.tracker.upsert_setup_user(
            user_id,
            name,
            role_name,
            location,
            access_level,
            icon_path,
        )

    def delete_setup_user(self, user_id: str) -> None:
        self.tracker.delete_setup_user(user_id)

    def upsert_agent(self, user_id: str, agent_name: str, tier: int, icon_path: str = "", location: str = "") -> str:
        return str(self.tracker.upsert_agent(user_id, agent_name, tier, icon_path, location))

    def delete_agent(self, user_id: str) -> None:
        self.tracker.delete_agent(user_id)

    def upsert_admin_user(
        self,
        user_id: str,
        admin_name: str = "",
        position: str = "",
        location: str = "",
        icon_path: str = "",
        access_level: str = "admin",
    ) -> str:
        return str(self.tracker.add_admin_user(user_id, admin_name, position, location, icon_path, access_level=access_level))

    def remove_admin_user(self, user_id: str) -> None:
        self.tracker.remove_admin_user(user_id)
