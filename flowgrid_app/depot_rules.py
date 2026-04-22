from __future__ import annotations

from typing import Any

from .ui_utils import clamp


class DepotRules:
    ADMIN_ACCESS_NONE = ""
    ADMIN_ACCESS_REPORTING = "reporting"
    ADMIN_ACCESS_ADMIN = "admin"
    ADMIN_ACCESS_LEVELS: tuple[str, ...] = (
        ADMIN_ACCESS_REPORTING,
        ADMIN_ACCESS_ADMIN,
    )
    TOUCH_RTV = "RTV"
    TOUCH_COMPLETE = "Complete"
    TOUCH_JUNK = "Junk Out"
    TOUCH_PART_ORDER = "Part Order"
    TOUCH_OTHER = "Other"
    CLOSING_TOUCHES: tuple[str, ...] = (
        TOUCH_COMPLETE,
        TOUCH_JUNK,
        TOUCH_RTV,
    )
    FOLLOW_UP_TOUCHES: tuple[str, ...] = (
        TOUCH_PART_ORDER,
        TOUCH_OTHER,
    )
    AGENT_TIER_LABELS: dict[int, str] = {
        1: "Tech 1",
        2: "Tech 2",
        3: "Tech 3",
        4: "MP",
    }
    TOUCH_CHART_LABELS: dict[str, str] = {
        TOUCH_COMPLETE: "Com.",
        TOUCH_PART_ORDER: "PO",
        TOUCH_JUNK: "JO",
        "Triaged": "Tri",
    }
    CLIENT_FOLLOWUP_WORK_APPROVED = "Work approved"
    CLIENT_FOLLOWUP_LEFT_MESSAGE = "Left message"
    CLIENT_FOLLOWUP_COULDNT_CONTACT = "Couldn't contact"
    CLIENT_FOLLOWUP_ACTIONS: tuple[str, ...] = (
        CLIENT_FOLLOWUP_WORK_APPROVED,
        CLIENT_FOLLOWUP_LEFT_MESSAGE,
        CLIENT_FOLLOWUP_COULDNT_CONTACT,
    )
    CLIENT_FOLLOWUP_NO_CONTACT_ACTIONS: tuple[str, ...] = (
        CLIENT_FOLLOWUP_LEFT_MESSAGE,
        CLIENT_FOLLOWUP_COULDNT_CONTACT,
    )
    CATEGORY_OPTIONS: tuple[str, ...] = (
        "Appliance",
        "Audio",
        "PC",
        "TV",
        "Other",
    )
    CLIENT_FOLLOWUP_STAGE_LABELS: tuple[str, ...] = ("Day 1", "Day 2", "Day 3")
    ROLE_SLOT_NONE = "none"
    ROLE_SLOT_QA = "qa"
    ROLE_SLOT_TECH1 = "tech1"
    ROLE_SLOT_TECH2 = "tech2"
    ROLE_SLOT_TECH3 = "tech3"
    ROLE_SLOT_MP = "mp"
    ROLE_SLOT_LABELS: dict[str, str] = {
        ROLE_SLOT_NONE: "No depot window",
        ROLE_SLOT_QA: "QA",
        ROLE_SLOT_TECH1: "Tech 1",
        ROLE_SLOT_TECH2: "Tech 2",
        ROLE_SLOT_TECH3: "Tech 3",
        ROLE_SLOT_MP: "MP",
    }
    ROLE_SLOT_TO_TIER: dict[str, int] = {
        ROLE_SLOT_TECH1: 1,
        ROLE_SLOT_TECH2: 2,
        ROLE_SLOT_TECH3: 3,
        ROLE_SLOT_MP: 4,
    }
    DEFAULT_ROLE_DEFINITIONS: tuple[tuple[str, str], ...] = (
        ("Tech 1", ROLE_SLOT_TECH1),
        ("Tech 2", ROLE_SLOT_TECH2),
        ("Tech 3", ROLE_SLOT_TECH3),
        ("MP", ROLE_SLOT_MP),
        ("QA", ROLE_SLOT_QA),
        ("Other", ROLE_SLOT_NONE),
    )

    @staticmethod
    def normalize_user_id(value: str) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def normalize_work_order(value: str) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def chart_touch_label(value: str) -> str:
        touch = str(value or "").strip()
        if not touch:
            return ""
        return DepotRules.TOUCH_CHART_LABELS.get(touch, touch)

    @staticmethod
    def normalize_followup_action(value: str) -> str:
        text = str(value or "").strip().lower()
        if not text:
            return ""
        canonical = {
            "work approved": DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED,
            "approved": DepotRules.CLIENT_FOLLOWUP_WORK_APPROVED,
            "left message": DepotRules.CLIENT_FOLLOWUP_LEFT_MESSAGE,
            "message left": DepotRules.CLIENT_FOLLOWUP_LEFT_MESSAGE,
            "couldn't contact": DepotRules.CLIENT_FOLLOWUP_COULDNT_CONTACT,
            "couldnt contact": DepotRules.CLIENT_FOLLOWUP_COULDNT_CONTACT,
            "no contact": DepotRules.CLIENT_FOLLOWUP_COULDNT_CONTACT,
        }
        return canonical.get(text, "")

    @staticmethod
    def followup_stage_label(stage_index: int) -> str:
        idx = int(clamp(int(stage_index), 0, len(DepotRules.CLIENT_FOLLOWUP_STAGE_LABELS) - 1))
        return DepotRules.CLIENT_FOLLOWUP_STAGE_LABELS[idx]

    @staticmethod
    def normalize_agent_tier(value: Any, default: int = 1) -> int:
        raw = value
        if isinstance(raw, str):
            text = raw.strip().upper()
            if text in {"MP", "TECH MP"}:
                return 4
            if text.startswith("TECH "):
                text = text[5:].strip()
            elif text.startswith("TIER "):
                text = text[5:].strip()
            raw = text
        try:
            numeric = int(raw)
        except Exception:
            numeric = int(default)
        return int(clamp(numeric, 1, 4))

    @staticmethod
    def agent_tier_label(value: Any) -> str:
        tier = DepotRules.normalize_agent_tier(value)
        return DepotRules.AGENT_TIER_LABELS.get(tier, f"Tech {tier}")

    @staticmethod
    def normalize_role_slot(value: Any, default: str = ROLE_SLOT_NONE) -> str:
        raw = str(value or "").strip().lower()
        if raw in {"", "none", "other", "off", "no depot window"}:
            return (
                DepotRules.ROLE_SLOT_NONE
                if str(default or "").strip().lower() in {"", DepotRules.ROLE_SLOT_NONE}
                else DepotRules.normalize_role_slot(default, default=DepotRules.ROLE_SLOT_NONE)
            )
        canonical = {
            "qa": DepotRules.ROLE_SLOT_QA,
            "wcs": DepotRules.ROLE_SLOT_QA,
            "tech1": DepotRules.ROLE_SLOT_TECH1,
            "tech 1": DepotRules.ROLE_SLOT_TECH1,
            "tier1": DepotRules.ROLE_SLOT_TECH1,
            "tier 1": DepotRules.ROLE_SLOT_TECH1,
            "1": DepotRules.ROLE_SLOT_TECH1,
            "tech2": DepotRules.ROLE_SLOT_TECH2,
            "tech 2": DepotRules.ROLE_SLOT_TECH2,
            "tier2": DepotRules.ROLE_SLOT_TECH2,
            "tier 2": DepotRules.ROLE_SLOT_TECH2,
            "2": DepotRules.ROLE_SLOT_TECH2,
            "tech3": DepotRules.ROLE_SLOT_TECH3,
            "tech 3": DepotRules.ROLE_SLOT_TECH3,
            "tier3": DepotRules.ROLE_SLOT_TECH3,
            "tier 3": DepotRules.ROLE_SLOT_TECH3,
            "3": DepotRules.ROLE_SLOT_TECH3,
            "mp": DepotRules.ROLE_SLOT_MP,
            "tech mp": DepotRules.ROLE_SLOT_MP,
            "4": DepotRules.ROLE_SLOT_MP,
        }
        if raw in canonical:
            return canonical[raw]
        return (
            DepotRules.ROLE_SLOT_NONE
            if str(default or "").strip().lower() in {"", DepotRules.ROLE_SLOT_NONE}
            else DepotRules.normalize_role_slot(default, default=DepotRules.ROLE_SLOT_NONE)
        )

    @staticmethod
    def role_slot_label(value: Any) -> str:
        slot = DepotRules.normalize_role_slot(value, default=DepotRules.ROLE_SLOT_NONE)
        return DepotRules.ROLE_SLOT_LABELS.get(slot, DepotRules.ROLE_SLOT_LABELS[DepotRules.ROLE_SLOT_NONE])

    @staticmethod
    def role_slot_to_agent_tier(value: Any, default: int = 0) -> int:
        slot = DepotRules.normalize_role_slot(value, default=DepotRules.ROLE_SLOT_NONE)
        return int(DepotRules.ROLE_SLOT_TO_TIER.get(slot, max(0, int(default))))

    @staticmethod
    def role_slot_from_agent_tier(value: Any, default: str = ROLE_SLOT_NONE) -> str:
        tier = int(DepotRules.normalize_agent_tier(value, default=1))
        if tier == 1:
            return DepotRules.ROLE_SLOT_TECH1
        if tier == 2:
            return DepotRules.ROLE_SLOT_TECH2
        if tier == 3:
            return DepotRules.ROLE_SLOT_TECH3
        if tier == 4:
            return DepotRules.ROLE_SLOT_MP
        return DepotRules.normalize_role_slot(default, default=DepotRules.ROLE_SLOT_NONE)

    @staticmethod
    def normalize_admin_access_level(value: Any, default: str = ADMIN_ACCESS_ADMIN) -> str:
        raw = str(value or "").strip().lower()
        if raw in {"reporting", "dashboard", "report"}:
            return DepotRules.ADMIN_ACCESS_REPORTING
        if raw in {"admin", "administrator", "full"}:
            return DepotRules.ADMIN_ACCESS_ADMIN
        if raw in {"", "none", "disabled", "off"}:
            return (
                DepotRules.ADMIN_ACCESS_NONE
                if str(default or "").strip() == DepotRules.ADMIN_ACCESS_NONE
                else DepotRules.normalize_admin_access_level(default, default=DepotRules.ADMIN_ACCESS_ADMIN)
            )
        return (
            DepotRules.ADMIN_ACCESS_NONE
            if str(default or "").strip() == DepotRules.ADMIN_ACCESS_NONE
            else DepotRules.normalize_admin_access_level(default, default=DepotRules.ADMIN_ACCESS_ADMIN)
        )

    @staticmethod
    def admin_access_label(value: Any) -> str:
        normalized = DepotRules.normalize_admin_access_level(value, default=DepotRules.ADMIN_ACCESS_NONE)
        if normalized == DepotRules.ADMIN_ACCESS_ADMIN:
            return "Admin"
        if normalized == DepotRules.ADMIN_ACCESS_REPORTING:
            return "Reporting"
        return "None"


__all__ = ["DepotRules"]
