"""Условие гейта G0: ядро переносится копированием.

Фаза A живёт модулем `capacity/` внутри проекта, фаза B выносит его в пакет. Переход
стоит ровно столько, сколько стоит копирование каталога, — но только пока ядро не
обросло импортами проекта, фреймворка или симуляции. Это и проверяется здесь: словами
такое условие не держится, его проверяет CI.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

CORE = Path(__file__).resolve().parent.parent / "src" / "capacity"

#: Чем ядру позволено пользоваться: своими модулями и стандартной библиотекой.
ALLOWED_ROOTS = {"capacity"} | set(sys.stdlib_module_names)


def core_modules() -> list[Path]:
    return sorted(CORE.rglob("*.py"))


def imported_roots(module: Path) -> set[str]:
    tree = ast.parse(module.read_text(encoding="utf-8"), filename=str(module))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def test_core_exists() -> None:
    assert core_modules(), "ядро не найдено — проверка переносимости бессмысленна"


def test_core_imports_only_stdlib_and_itself() -> None:
    """Ни одного стороннего импорта: ни проекта, ни SQLAlchemy, ни SimPy."""
    offenders = {
        module.relative_to(CORE).as_posix(): sorted(imported_roots(module) - ALLOWED_ROOTS)
        for module in core_modules()
    }
    offenders = {name: roots for name, roots in offenders.items() if roots}

    assert offenders == {}


def test_core_does_not_import_the_simulation() -> None:
    """Симуляция живёт в dev-зависимостях и в ядро попасть не может (G8)."""
    for module in core_modules():
        assert "sim" not in imported_roots(module)


def test_runtime_dependencies_are_empty() -> None:
    """У пакета нет зависимостей времени выполнения — иначе перенос тянет их за собой."""
    pyproject = (CORE.parent.parent / "pyproject.toml").read_text(encoding="utf-8")
    body = pyproject.split("[project]", 1)[1].split("[project.optional", 1)[0]

    assert "dependencies = []" in body
