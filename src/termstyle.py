"""Short labeled terminal output. Color is used only when stdout is a terminal."""

from __future__ import annotations

import os
import sys

_CODES = {
    "bold": "1",
    "dim": "2",
    "red": "31",
    "green": "32",
    "yellow": "33",
    "cyan": "36",
}


def _flag(name: str) -> str:
    return (os.getenv(name) or "").strip().lower()


def enabled(stream=None) -> bool:
    if _flag("FORCE_COLOR") in {"1", "true", "yes"}:
        return True
    if _flag("NO_COLOR") not in {"", "0"}:
        return False
    stream = stream or sys.stdout
    return hasattr(stream, "isatty") and stream.isatty()


def paint(text: str, *names: str, stream=None) -> str:
    names = tuple(name for name in names if name)
    if not names or not enabled(stream):
        return text
    code = ";".join(_CODES[name] for name in names)
    return f"\033[{code}m{text}\033[0m"


def heading(text: str) -> None:
    print()
    print(paint(text, "bold"))


def fields(rows: list[tuple[str, str, str]], *, align: str = "left") -> None:
    """Rows of label, value, and an optional color name."""
    label_width = max((len(label) for label, _, _ in rows), default=0)
    value_width = max((len(value) for _, value, _ in rows), default=0) if align == "right" else 0
    for label, value, color in rows:
        shown = value.rjust(value_width) if align == "right" else value
        print(f"  {paint(label.ljust(label_width), 'dim')}  {paint(shown, color)}")


def lines(items: list[str], *, color: str = "", indent: int = 2) -> None:
    pad = " " * indent
    for item in items:
        print(f"{pad}{paint(item, color)}")


def note(text: str) -> None:
    print(paint(f"  {text}", "dim"))
