from __future__ import annotations

from pathlib import Path
from typing import Any


def next_relevant_line(lines: list[str], start_index: int) -> tuple[str | None, int | None]:
    for line in lines[start_index:]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        return stripped, len(line) - len(line.lstrip(" "))
    return None, None


def parse_yaml_scalar(value: str) -> Any:
    normalized = value.strip()
    if normalized in {"null", "Null", "NULL", "~"}:
        return None
    if normalized in {"true", "True", "TRUE"}:
        return True
    if normalized in {"false", "False", "FALSE"}:
        return False
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {"'", '"'}:
        return normalized[1:-1]
    try:
        return int(normalized)
    except ValueError:
        pass
    try:
        return float(normalized)
    except ValueError:
        pass
    return normalized


def parse_simple_yaml_file(path: Path) -> dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]

    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()
        container = stack[-1][1]

        if stripped.startswith("- "):
            if not isinstance(container, list):
                raise ValueError(f"Unexpected list item in {path}: {line}")
            container.append(parse_yaml_scalar(stripped[2:]))
            continue

        if ":" not in stripped:
            continue

        key, _, raw_value = stripped.partition(":")
        key = key.strip()
        value = raw_value.strip()
        if not isinstance(container, dict):
            raise ValueError(f"Unexpected mapping item in {path}: {line}")

        if value == "":
            next_line, next_indent = next_relevant_line(lines, index + 1)
            if next_line is not None and next_indent is not None and next_indent > indent:
                child: dict[str, Any] | list[Any]
                child = [] if next_line.startswith("- ") else {}
            else:
                child = {}
            container[key] = child
            stack.append((indent, child))
            continue

        container[key] = parse_yaml_scalar(value)

    return root
