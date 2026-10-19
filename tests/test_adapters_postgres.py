"""Адаптеры к Postgres: лидерство, состояние, переопределения (п.6, G6, G7).

Живой Postgres здесь не нужен: проверяется текст SQL, который уйдёт на сервер, и
поведение адаптеров на поддельной сессии. Интеграционный прогон против реальной базы —
за проектом, в котором контур запускается.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from sqlalchemy.dialects.postgresql.base import PGDialect

from capacity.ports import OverrideStore, StateStore, TickScope
from capacity.types import Hold
from capacity_adapters.postgres import (
    GOVERNOR_OVERRIDE,
    GOVERNOR_STATE,
    PAUSE_ROW,
    PostgresOverrideStore,
    PostgresStateStore,
    PostgresTickScope,
    advisory_key,
)


@dataclass(slots=True)
class FakeResult:
    rows: list[Any] = field(default_factory=list)
    value: Any = None

    def scalar(self) -> Any:
        return self.value

    def first(self) -> Any:
        return self.rows[0] if self.rows else None

    def __iter__(self) -> Any:
        return iter(self.rows)


@dataclass(slots=True)
class Row:
    """Строка результата: нужны только атрибуты, как у `Row` SQLAlchemy."""

    name: str | None = None
    payload: Mapping[str, Any] | None = None
    consumer_id: str | None = None
    window_limit: int | None = None


@dataclass(slots=True)
class FakeSession:
    """Сессия, записывающая выполненные запросы и отдающая заготовленные ответы."""

    lock_acquired: bool = True
    results: list[FakeResult] = field(default_factory=list)
    executed: list[tuple[Any, Any]] = field(default_factory=list)

    async def execute(self, statement: Any, parameters: Any = None) -> FakeResult:
        self.executed.append((statement, parameters))
        rendered = str(statement)
        if "pg_try_advisory_xact_lock" in rendered:
            return FakeResult(value=self.lock_acquired)
        return self.results.pop(0) if self.results else FakeResult()

    def sql(self, index: int) -> str:
        """Текст запроса ровно в том виде, в каком он уйдёт на сервер Postgres.

        У `text()` метода `compile` нет — такой запрос печатается как есть.
        """
        # `PGDialect.__init__` в SQLAlchemy без аннотаций, строгость тут не наша.
        statement: Any = self.executed[index][0]
        compile_statement = cast(
            "Callable[..., object] | None", getattr(statement, "compile", None)
        )
        if compile_statement is None:
            return str(statement)
        return str(compile_statement(dialect=PGDialect()))  # type: ignore[no-untyped-call]


@dataclass(slots=True)
class FakeSessionScope:
    session: FakeSession
    opened: int = 0
    closed: int = 0

    def __call__(self) -> Any:
        return self._scope()

    @asynccontextmanager
    async def _scope(self) -> AsyncIterator[FakeSession]:
        self.opened += 1
        try:
            yield self.session
        finally:
            self.closed += 1


def scope_for(session: FakeSession, name: str = "domains") -> PostgresTickScope:
    return PostgresTickScope(session_scope=FakeSessionScope(session), scope=name)


# --- лидерство ----------------------------------------------------------------------


async def test_lock_is_transactional_not_session_scoped() -> None:
    """Под PgBouncer сессионный лок теряется, поэтому берётся `xact`-вариант (G6)."""
    session = FakeSession()
    scope = scope_for(session)

    async with scope.lead() as leading:
        assert leading is True

    assert "pg_try_advisory_xact_lock" in session.sql(0)
    assert "pg_try_advisory_lock(" not in session.sql(0)


async def test_lock_key_is_passed_as_a_parameter() -> None:
    session = FakeSession()

    async with scope_for(session, "domains").lead():
        pass

    assert session.executed[0][1] == {"key": advisory_key("domains")}


async def test_losing_the_lock_skips_the_tick() -> None:
    """Лок не взят — такт пропущен, сессии такта нет и писать некуда."""
    session = FakeSession(lock_acquired=False)
    scope = scope_for(session)

    async with scope.lead() as leading:
        assert leading is False
        with pytest.raises(RuntimeError, match="вне транзакции такта"):
            scope.session()


async def test_tick_session_is_available_only_inside_the_scope() -> None:
    session = FakeSession()
    scope = scope_for(session)

    async with scope.lead():
        assert scope.session() is session

    with pytest.raises(RuntimeError, match="вне транзакции такта"):
        scope.session()


async def test_each_tick_opens_its_own_transaction() -> None:
    """Лидерства как состояния нет: лок берётся на каждый такт заново (G6)."""
    session = FakeSession()
    factory = FakeSessionScope(session)
    scope = PostgresTickScope(session_scope=factory, scope="domains")

    for _ in range(3):
        async with scope.lead():
            pass

    assert (factory.opened, factory.closed) == (3, 3)


def test_advisory_key_is_stable_and_fits_bigint() -> None:
    """Ключ не зависит ни от версии сервера, ни от локали — он считается здесь."""
    assert advisory_key("domains") == advisory_key("domains")
    assert advisory_key("domains") != advisory_key("mailing")
    assert 0 <= advisory_key("domains") < 2**63


def test_scope_satisfies_the_port() -> None:
    assert isinstance(scope_for(FakeSession()), TickScope)


# --- состояние политики -------------------------------------------------------------


async def test_state_is_loaded_from_one_scoped_query() -> None:
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    Row(name="deficits", payload={"c0": 2, "c1": 0}),
                    Row(name="holds", payload={"c0": {"amount": 1, "ticks_left": 3}}),
                ]
            )
        ]
    )
    scope = scope_for(session)
    store = PostgresStateStore(scope)

    async with scope.lead():
        deficits, holds = await store.load()

    assert deficits == {"c0": 2, "c1": 0}
    assert holds == {"c0": Hold(amount=1, ticks_left=3)}


async def test_empty_state_is_not_an_error() -> None:
    """Первый запуск контура: строк ещё нет, политика начинает с чистого состояния."""
    scope = scope_for(FakeSession())
    store = PostgresStateStore(scope)

    async with scope.lead():
        deficits, holds = await store.load()

    assert (deficits, holds) == ({}, {})


async def test_state_is_saved_as_an_upsert() -> None:
    """Повтор такта не должен падать на дубликате ключа."""
    session = FakeSession()
    scope = scope_for(session)
    store = PostgresStateStore(scope)

    async with scope.lead():
        await store.save({"c0": 1}, {"c0": Hold(amount=2, ticks_left=4)})

    deficits_sql = session.sql(1)
    assert "INSERT INTO capacity_governor_state" in deficits_sql
    assert "ON CONFLICT (scope, name) DO UPDATE" in deficits_sql


async def test_deficits_and_holds_are_saved_in_the_same_transaction() -> None:
    """По отдельности они рассогласуются: чужая очередь или чужое удержание."""
    session = FakeSession()
    scope = scope_for(session)
    store = PostgresStateStore(scope)

    async with scope.lead():
        await store.save({"c0": 1}, {"c0": Hold(amount=2, ticks_left=4)})
        inside = len(session.executed)

    assert inside == 3  # лок, дефициты, удержания


def test_state_store_satisfies_the_port() -> None:
    assert isinstance(PostgresStateStore(scope_for(FakeSession())), StateStore)


# --- переопределения оператора ------------------------------------------------------


async def test_pins_and_pause_are_read_from_the_database() -> None:
    """`pin` и `pause` живут в БД: лидер меняется между тактами (G7)."""
    session = FakeSession(
        results=[
            FakeResult(
                rows=[
                    Row(consumer_id="c0", window_limit=3),
                    Row(consumer_id=PAUSE_ROW, window_limit=None),
                ]
            )
        ]
    )
    scope = scope_for(session)
    store = PostgresOverrideStore(scope)

    async with scope.lead():
        pins = await store.pinned()
        paused = await store.paused()

    assert pins == {"c0": 3}
    assert paused is True


async def test_overrides_are_read_once_per_tick() -> None:
    """Контур спрашивает пины и паузу подряд — это один и тот же набор строк."""
    session = FakeSession(results=[FakeResult(rows=[Row(consumer_id="c0", window_limit=1)])])
    scope = scope_for(session)
    store = PostgresOverrideStore(scope)

    async with scope.lead():
        await store.pinned()
        await store.paused()
        queries = len(session.executed)

    assert queries == 2  # лок и один SELECT


async def test_no_overrides_means_no_pause() -> None:
    scope = scope_for(FakeSession())
    store = PostgresOverrideStore(scope)

    async with scope.lead():
        assert await store.pinned() == {}
        assert await store.paused() is False


async def test_override_query_is_scoped() -> None:
    """Несколько контуров в одной базе не видят переопределений друг друга."""
    session = FakeSession()
    scope = scope_for(session, "mailing")
    store = PostgresOverrideStore(scope)

    async with scope.lead():
        await store.pinned()

    assert "WHERE capacity_governor_override.scope = " in session.sql(1)


def test_override_store_satisfies_the_port() -> None:
    assert isinstance(PostgresOverrideStore(scope_for(FakeSession())), OverrideStore)


# --- схема --------------------------------------------------------------------------


def test_schema_has_the_two_tables_the_loop_needs() -> None:
    assert GOVERNOR_STATE.name == "capacity_governor_state"
    assert GOVERNOR_OVERRIDE.name == "capacity_governor_override"
    assert [c.name for c in GOVERNOR_STATE.primary_key] == ["scope", "name"]
    assert [c.name for c in GOVERNOR_OVERRIDE.primary_key] == ["scope", "consumer_id"]
