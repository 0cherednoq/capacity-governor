"""Интеграция с живым Postgres: лидерство, состояние, переопределения.

Эти тесты — единственное место, где проверяется поведение настоящей базы: что
`pg_try_advisory_xact_lock` действительно не пускает второго, что upsert переживает
повтор такта, что данные двух контуров не смешиваются. Всё остальное о Postgres
проверено по тексту SQL в `test_adapters_postgres.py`.

Без базы тесты пропускаются: `CAPACITY_TEST_DSN` задаёт подключение, по умолчанию
берётся локальный контейнер. Поднять его:

    docker run -d --name capacity-governor-test-pg \\
        -e POSTGRES_PASSWORD=capacity -e POSTGRES_USER=capacity \\
        -e POSTGRES_DB=capacity -p 55444:5432 postgres:16-alpine
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from capacity.types import Hold
from capacity_adapters.postgres import (
    GOVERNOR_OVERRIDE,
    METADATA,
    PAUSE_ROW,
    PostgresOverrideStore,
    PostgresStateStore,
    PostgresTickScope,
)

DSN = os.environ.get(
    "CAPACITY_TEST_DSN",
    "postgresql+asyncpg://capacity:capacity@127.0.0.1:55444/capacity",
)


async def _engine() -> AsyncEngine | None:
    engine = create_async_engine(DSN, poolclass=None)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(METADATA.create_all)
    except Exception:  # база не поднята — тесты этого модуля пропускаются
        await engine.dispose()
        return None
    return engine


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    live = await _engine()
    if live is None:
        pytest.skip(f"нет Postgres по {DSN}")
    async with live.begin() as connection:
        await connection.execute(GOVERNOR_OVERRIDE.delete())
    try:
        yield live
    finally:
        await live.dispose()


def scope_for(engine: AsyncEngine, name: str) -> PostgresTickScope:
    maker = async_sessionmaker(engine, expire_on_commit=False)
    return PostgresTickScope(session_scope=maker.begin, scope=name)


def overrides_for(
    engine: AsyncEngine, name: str
) -> tuple[PostgresOverrideStore, PostgresTickScope]:
    """Хранилище переопределений и такт, в котором контур его читает.

    Чтение идёт в транзакции такта, поэтому store и scope должны быть одной парой:
    store берёт сессию у своего `scope_provider`, а не у любого другого контура.
    """
    maker = async_sessionmaker(engine, expire_on_commit=False)
    scope = PostgresTickScope(session_scope=maker.begin, scope=name)
    store = PostgresOverrideStore(scope, session_scope=maker.begin)
    return store, scope


# --- лидерство ----------------------------------------------------------------------


async def test_leader_is_taken_and_released_with_the_transaction(
    engine: AsyncEngine,
) -> None:
    """Лок снимается коммитом: следующий такт берёт его снова, без сброса состояния."""
    scope = scope_for(engine, "leader-cycle")

    for _ in range(3):
        async with scope.lead() as leading:
            assert leading is True


async def test_second_tick_cannot_lead_while_the_first_holds_the_lock(
    engine: AsyncEngine,
) -> None:
    """Главное свойство контура: два процесса не пересчитают ёмкость одновременно."""
    first = scope_for(engine, "leader-race")
    second = scope_for(engine, "leader-race")

    async with first.lead() as leading_first:
        assert leading_first is True
        async with second.lead() as leading_second:
            assert leading_second is False

    # Первый закоммитился — лок свободен.
    async with second.lead() as leading_after:
        assert leading_after is True


async def test_different_scopes_do_not_block_each_other(engine: AsyncEngine) -> None:
    """Два контура (домены и рассылка) в одной базе работают независимо."""
    domains = scope_for(engine, "domains")
    mailing = scope_for(engine, "mailing")

    async with domains.lead() as leading_domains, mailing.lead() as leading_mailing:
        assert (leading_domains, leading_mailing) == (True, True)


# --- состояние политики -------------------------------------------------------------


async def test_state_round_trip_through_the_database(engine: AsyncEngine) -> None:
    """Дефициты и удержания переживают такт: лидер может смениться между ними."""
    scope = scope_for(engine, "state-roundtrip")
    store = PostgresStateStore(scope)

    async with scope.lead():
        await store.save({"c0": 2, "c1": 0}, {"c0": Hold(amount=1, ticks_left=4)})

    async with scope.lead():
        deficits, holds = await store.load()

    assert deficits == {"c0": 2, "c1": 0}
    assert holds == {"c0": Hold(amount=1, ticks_left=4)}


async def test_repeated_tick_overwrites_instead_of_failing(engine: AsyncEngine) -> None:
    """Повтор такта не должен падать на дубликате ключа — отсюда upsert."""
    scope = scope_for(engine, "state-upsert")
    store = PostgresStateStore(scope)

    async with scope.lead():
        await store.save({"c0": 1}, {})
    async with scope.lead():
        await store.save({"c0": 5}, {})
    async with scope.lead():
        deficits, _ = await store.load()

    assert deficits == {"c0": 5}


async def test_state_of_two_loops_does_not_mix(engine: AsyncEngine) -> None:
    domains = scope_for(engine, "domains")
    mailing = scope_for(engine, "mailing")
    domains_state = PostgresStateStore(domains)
    mailing_state = PostgresStateStore(mailing)

    async with domains.lead():
        await domains_state.save({"batch": 3}, {})
    async with mailing.lead():
        await mailing_state.save({"campaign": 9}, {})
    async with domains.lead():
        deficits, _ = await domains_state.load()

    assert deficits == {"batch": 3}


async def test_rolled_back_tick_leaves_no_state(engine: AsyncEngine) -> None:
    """Сбой посреди такта не оставляет половины: окна и состояние в одной транзакции."""
    scope = scope_for(engine, "state-rollback")
    store = PostgresStateStore(scope)

    with pytest.raises(RuntimeError, match="сбой такта"):
        async with scope.lead():
            await store.save({"c0": 1}, {})
            raise RuntimeError("сбой такта")

    async with scope.lead():
        deficits, _ = await store.load()

    assert deficits == {}


# --- переопределения оператора ------------------------------------------------------


async def test_operator_pin_and_pause_are_visible_to_the_next_tick(
    engine: AsyncEngine,
) -> None:
    """Оператор правит таблицу — контур видит это на следующем такте, без перезапуска."""
    scope = scope_for(engine, "overrides")
    overrides = PostgresOverrideStore(scope)

    async with engine.begin() as connection:
        await connection.execute(
            GOVERNOR_OVERRIDE.insert(),
            [
                {"scope": "overrides", "consumer_id": "c0", "window_limit": 3},
                {"scope": "overrides", "consumer_id": PAUSE_ROW, "window_limit": None},
            ],
        )

    async with scope.lead():
        pins = await overrides.pinned()
        paused = await overrides.paused()

    assert pins == {"c0": 3}
    assert paused is True


async def test_operator_commands_write_without_waiting_for_the_tick(
    engine: AsyncEngine,
) -> None:
    """Оператор не ждёт, пока контур отпустит лок: запись идёт своей транзакцией (G7)."""
    store, scope = overrides_for(engine, "commands")

    async with scope.lead() as leading:
        assert leading is True  # контур держит лок прямо сейчас
        await store.pin("c0", 4)
        await store.set_paused(True)

    async with scope.lead():
        assert await store.pinned() == {"c0": 4}
        assert await store.paused() is True


async def test_pin_is_idempotent_and_updatable(engine: AsyncEngine) -> None:
    store, scope = overrides_for(engine, "commands")

    await store.pin("c0", 2)
    await store.pin("c0", 7)

    async with scope.lead():
        assert await store.pinned() == {"c0": 7}


async def test_unpin_and_resume_remove_the_rows(engine: AsyncEngine) -> None:
    store, scope = overrides_for(engine, "commands")

    await store.pin("c0", 2)
    await store.set_paused(True)
    await store.unpin("c0")
    await store.set_paused(False)

    async with scope.lead():
        assert await store.pinned() == {}
        assert await store.paused() is False


async def test_pause_row_is_not_a_consumer(engine: AsyncEngine) -> None:
    """Служебную строку паузы нельзя занять потребителем — иначе пауза станет пином."""
    store, _ = overrides_for(engine, "commands")

    with pytest.raises(ValueError, match="служебная строка"):
        await store.pin(PAUSE_ROW, 1)


async def test_unpausing_is_deleting_the_row(engine: AsyncEngine) -> None:
    scope = scope_for(engine, "overrides")
    overrides = PostgresOverrideStore(scope)

    async with engine.begin() as connection:
        await connection.execute(
            GOVERNOR_OVERRIDE.insert(),
            [{"scope": "overrides", "consumer_id": PAUSE_ROW, "window_limit": None}],
        )
    async with engine.begin() as connection:
        await connection.execute(
            GOVERNOR_OVERRIDE.delete().where(
                GOVERNOR_OVERRIDE.c.consumer_id == PAUSE_ROW
            )
        )

    async with scope.lead():
        assert await overrides.paused() is False
