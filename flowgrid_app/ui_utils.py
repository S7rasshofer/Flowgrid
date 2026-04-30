from __future__ import annotations

from typing import Any


DEFAULT_THEME_PRIMARY = "#C35A00"
DEFAULT_THEME_ACCENT = "#FF9A1F"
DEFAULT_THEME_SURFACE = "#090A0F"


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(fallback)


def normalize_hex(color: str, fallback: str = "#FFFFFF") -> str:
    if not isinstance(color, str):
        return fallback
    value = color.strip().upper()
    if len(value) == 7 and value.startswith("#"):
        try:
            int(value[1:], 16)
            return value
        except ValueError:
            return fallback
    return fallback


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    color = normalize_hex(color)
    return int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)


def rgb_to_hex(r: int, g: int, b: int) -> str:
    return f"#{int(clamp(r, 0, 255)):02X}{int(clamp(g, 0, 255)):02X}{int(clamp(b, 0, 255)):02X}"


def blend(color_a: str, color_b: str, ratio: float) -> str:
    ratio = clamp(ratio, 0.0, 1.0)
    ar, ag, ab = hex_to_rgb(color_a)
    br, bg, bb = hex_to_rgb(color_b)
    return rgb_to_hex(ar + (br - ar) * ratio, ag + (bg - ag) * ratio, ab + (bb - ab) * ratio)


def luminance(color: str) -> float:
    r, g, b = hex_to_rgb(color)

    def channel(v: int) -> float:
        x = v / 255.0
        return x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4

    return 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b)


def contrast_ratio(color_a: str, color_b: str) -> float:
    l1, l2 = luminance(color_a), luminance(color_b)
    hi, lo = (l1, l2) if l1 > l2 else (l2, l1)
    return (hi + 0.05) / (lo + 0.05)


def readable_text(background: str) -> str:
    white_ratio = contrast_ratio("#FFFFFF", background)
    black_ratio = contrast_ratio("#101418", background)
    return "#FFFFFF" if white_ratio >= black_ratio else "#101418"


def shift(color: str, amount: float) -> str:
    target = "#FFFFFF" if amount >= 0 else "#000000"
    return blend(color, target, abs(amount))


def rgba_css(color: str, alpha: float) -> str:
    r, g, b = hex_to_rgb(color)
    a = int(clamp(alpha, 0.0, 1.0) * 255)
    return f"rgba({r}, {g}, {b}, {a})"


def compute_palette(theme: dict[str, str]) -> dict[str, str]:
    primary = normalize_hex(theme.get("primary", DEFAULT_THEME_PRIMARY), DEFAULT_THEME_PRIMARY)
    accent = normalize_hex(theme.get("accent", DEFAULT_THEME_ACCENT), DEFAULT_THEME_ACCENT)
    surface = normalize_hex(theme.get("surface", DEFAULT_THEME_SURFACE), DEFAULT_THEME_SURFACE)

    shell_overlay = shift(primary, -0.60)
    sidebar_overlay = shift(primary, -0.70)
    control_bg = blend(surface, "#1E2A34", 0.22)
    input_bg = blend(surface, "#FFFFFF", 0.08)
    nav_active = blend(accent, primary, 0.35)
    text_color = readable_text(control_bg)
    button_bg = blend(primary, accent, 0.30)

    return {
        "primary": primary,
        "accent": accent,
        "surface": surface,
        "shell_overlay": shell_overlay,
        "sidebar_overlay": sidebar_overlay,
        "label_text": text_color,
        "muted_text": blend(text_color, "#AAB7C2", 0.35),
        "control_bg": control_bg,
        "input_bg": input_bg,
        "button_bg": button_bg,
        "button_text": readable_text(button_bg),
        "nav_active": nav_active,
    }


def safe_layer_defaults(layer: dict[str, Any]) -> dict[str, Any]:
    visible_raw = layer.get("visible", True)
    if isinstance(visible_raw, str):
        visible_text = visible_raw.strip().lower()
        if visible_text in {"0", "false", "no", "off"}:
            visible_value = False
        elif visible_text in {"1", "true", "yes", "on"}:
            visible_value = True
        else:
            visible_value = True
    else:
        visible_value = bool(visible_raw)
    return {
        "image_path": layer.get("image_path", ""),
        "image_x": int(layer.get("image_x", 0)),
        "image_y": int(layer.get("image_y", 0)),
        "image_scale_mode": layer.get("image_scale_mode", "Fit"),
        "image_anchor": layer.get("image_anchor", "Center"),
        "image_scale_percent": int(layer.get("image_scale_percent", 100)),
        "image_opacity": float(clamp(float(layer.get("image_opacity", 1.0)), 0.0, 1.0)),
        "visible": visible_value,
    }


__all__ = [
    "DEFAULT_THEME_ACCENT",
    "DEFAULT_THEME_PRIMARY",
    "DEFAULT_THEME_SURFACE",
    "blend",
    "clamp",
    "compute_palette",
    "contrast_ratio",
    "hex_to_rgb",
    "luminance",
    "normalize_hex",
    "readable_text",
    "rgb_to_hex",
    "rgba_css",
    "safe_int",
    "safe_layer_defaults",
    "shift",
]
