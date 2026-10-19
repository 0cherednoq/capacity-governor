"""Адаптеры к Postgres: лидерство, состояние политики, переопределения (п.6, G6, G7).

Лидерство берётся **транзакционным** `pg_try_advisory_xact_lock` на каждый такт.
Сессионный `pg_try_advisory_lock` держится за соединением, и под PgBouncer в режиме
transaction pooling соединение между запросами уходит в пул: лок либо теряется, либо
удерживается не тем, кто думает. Транзакционный снимается коммитом и безопасен под
любым пулером. Лидерства как состояния здесь нет, поэтому нет ни расщепления лидера,
ни stale-лидера, ни healthcheck'а соединения.

Схема: одна таблица на весь контур. Состояние политики (дефициты, удержания,
применённые окна) и переопределения оператора (`pin`, `pause`) живут в БД, а не в
памяти процесса, потому что лидер меняется между тактами (G6, G7).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Protocol

try:
    from sqlalchemy import (
        Column,
        Integer,
        MetaData,
        String,
        Table,
        text,
    )
    from sqlalchemy.dialects.postgresql import JSONB, insert
except ImportError as error:  # extras не поставлены
    raise ImportError(
        "адаптеру Postgres нужна SQLAlchemy: pip install 'capacity-governor[postgres]'"
    ) from error

from capacity.types import Hold

METADATA = MetaData()

#: Состояние контура: по строке на ключ. `scope` разделяет независимые контуры в одной
#: базе (разные пулы ресурсов), `name` — что именно лежит в строке.
GOVERNOR_STATE = Table(
    "capacity_governor_state",
    METADATA,
    Column("scope", String(64), primary_key=True),
    Column("name", String(32), primary_key=True),
    Column("payload", JSONB, nullable=False),
)

#: Переопределения оператора. `limit` NULL в строке `__paused__` — признак паузы.
GOVERNOR_OVERRIDE = Table(
    "capacity_governor_override",
    METADATA,
    Column("scope", String(64), primary_key=True),
    Column("consumer_id", String(128), primary_key=True),
    Column("window_limit", Integer, nullable=True),
)

PAUSE_ROW = "__paused__"


class Session(Protocol):
    """То, что нужно от `AsyncSession`: выполнить запрос и вернуть результат."""

    async def execute(self, statement: Any, parameters: Any = None) -> Any: ...


class SessionScope(Protocol):
    """Фабрика сессии с транзакцией: `async with factory() as session`."""

    def __call__(self) -> Any: ...


def advisory_key(scope: str) -> int:
    """Ключ advisory-лока: стабильный 63-битный хеш имени контура.

    Хеш считается здесь, а не через `hashtext()` Postgres: тот не документирован как
    стабильный между версиями сервера. Свой расчёт не зависит ни от версии, ни от
    локали, а значит два процесса на разных серверах возьмут один и тот же лок.
    """
    digest = 0
    for byte in scope.encode("utf-8"):
        digest = (digest * 131 + byte) % (2**63 - 1)
    return digest


@dataclass(slots=True)
class PostgresTickScope:
    """`TickScope`: транзакция такта вместе с лидерством.

    Лок берётся внутри транзакции и снимается её коммитом. Не взяли — такт пропущен,
    следующий через `every`; это не ошибка.
    """

    session_scope: SessionScope
    scope: str = "default"
    current: Any = field(default=None, init=False)

    @asynccontextmanager
    async def lead(self) -> AsyncIterator[bool]:
        async with self.session_scope() as session:
            got = await session.execute(
                text("SELECT pg_try_advisory_xact_lock(:key) AS acquired"),
                {"key": advisory_key(self.scope)},
            )
            acquired = bool(got.scalar())
            if not acquired:
                yield False
                return
            self.current = session
            try:
                yield True
            finally:
                self.current = None

    def session(self) -> Any:
        """Сессия текущего такта — её адаптеры передают в Tallyho (`session=`)."""
        if self.current is None:
            raise RuntimeError("вне транзакции такта: сессии нет")
        return self.current


@dataclass(slots=True)
class PostgresStateStore:
    """`StateStore`: дефициты и удержания одной строкой на каждый вид.

    Пишутся в транзакции такта, то есть тем же коммитом, что и окна: по отдельности
    они рассогласуются, и потребитель получит либо чужое удержание, либо чужую очередь.
    """

    scope_provider: PostgresTickScope

    @property
    def scope(self) -> str:
        """Имя контура берётся у транзакции такта, а не задаётся отдельно.

        Повторять строку в трёх местах — значит однажды опечататься в одном из них и
        молча развести состояние по двум ключам: контур будет читать не свою очередь.
        """
        return self.scope_provider.scope

    async def load(self) -> tuple[Mapping[str, int], Mapping[str, Hold]]:
        session = self.scope_provider.session()
        rows = await session.execute(
            GOVERNOR_STATE.select().where(GOVERNOR_STATE.c.scope == self.scope)
        )
        payloads = {row.name: row.payload for row in rows}
        deficits = {str(k): int(v) for k, v in (payloads.get("deficits") or {}).items()}
        holds = {
            str(k): Hold(amount=int(v["amount"]), ticks_left=int(v["ticks_left"]))
            for k, v in (payloads.get("holds") or {}).items()
        }
        return deficits, holds

    async def save(self, deficits: Mapping[str, int], holds: Mapping[str, Hold]) -> None:
        await self._upsert("deficits", dict(deficits))
        await self._upsert(
            "holds",
            {
                cid: {"amount": hold.amount, "ticks_left": hold.ticks_left}
                for cid, hold in holds.items()
            },
        )

    async def _upsert(self, name: str, payload: Mapping[str, Any]) -> None:
        session = self.scope_provider.session()
        statement = insert(GOVERNOR_STATE).values(
            scope=self.scope, name=name, payload=json.loads(json.dumps(payload))
        )
        await session.execute(
            statement.on_conflict_do_update(
                index_elements=[GOVERNOR_STATE.c.scope, GOVERNOR_STATE.c.name],
                set_={"payload": statement.excluded.payload},
            )
        )


@dataclass(slots=True)
class PostgresOverrideStore:
    """`OverrideStore`: `pin` и `pause` оператора (G7).

    Состояние в БД, не в памяти процесса: лидер меняется между тактами, а выключатель
    обязан действовать независимо от того, кто сейчас ведёт контур.

    Чтение идёт в транзакции такта, запись — в своей: оператор нажимает выключатель
    тогда, когда ему нужно, а не когда контур освободит лок. Поэтому для записи нужен
    собственный доступ к сессиям, `session_scope`.
    """

    scope_provider: PostgresTickScope
    session_scope: SessionScope | None = None
    _cache: tuple[int, list[Any]] | None = field(default=None, init=False)

    @property
    def scope(self) -> str:
        """Имя контура берётся у транзакции такта — по той же причине, что и в состоянии."""
        return self.scope_provider.scope

    async def pin(self, consumer_id: str, limit: int) -> None:
        if consumer_id == PAUSE_ROW:
            raise ValueError(f"{PAUSE_ROW} — служебная строка паузы, не потребитель")
        await self._write_override(consumer_id, limit)

    async def unpin(self, consumer_id: str) -> None:
        await self._delete_override(consumer_id)

    async def set_paused(self, paused: bool) -> None:
        if paused:
            await self._write_override(PAUSE_ROW, None)
        else:
            await self._delete_override(PAUSE_ROW)

    async def _write_override(self, consumer_id: str, limit: int | None) -> None:
        statement = insert(GOVERNOR_OVERRIDE).values(
            scope=self.scope, consumer_id=consumer_id, window_limit=limit
        )
        await self._execute_outside_tick(
            statement.on_conflict_do_update(
                index_elements=[
                    GOVERNOR_OVERRIDE.c.scope,
                    GOVERNOR_OVERRIDE.c.consumer_id,
                ],
                set_={"window_limit": statement.excluded.window_limit},
            )
        )

    async def _delete_override(self, consumer_id: str) -> None:
        await self._execute_outside_tick(
            GOVERNOR_OVERRIDE.delete().where(
                (GOVERNOR_OVERRIDE.c.scope == self.scope)
                & (GOVERNOR_OVERRIDE.c.consumer_id == consumer_id)
            )
        )

    async def _execute_outside_tick(self, statement: Any) -> None:
        """Записать переопределение в своей транзакции, не ожидая такта."""
        scope = self.session_scope or self.scope_provider.session_scope
        async with scope() as session:
            await session.execute(statement)
        self._cache = None

    async def pinned(self) -> Mapping[str, int]:
        rows = await self._rows()
        return {
            row.consumer_id: int(row.window_limit)
            for row in rows
            if row.consumer_id != PAUSE_ROW and row.window_limit is not None
        }

    async def paused(self) -> bool:
        rows = await self._rows()
        return any(row.consumer_id == PAUSE_ROW for row in rows)

    async def _rows(self) -> list[Any]:
        """Прочитать переопределения один раз на такт.

        Контур спрашивает `pinned()` и `paused()` подряд в одной транзакции, и это
        один и тот же набор строк: второй запрос был бы лишним обращением к БД на
        каждом такте.
        """
        session = self.scope_provider.session()
        if self._cache is not None and self._cache[0] == id(session):
            return self._cache[1]
        result = await session.execute(
            GOVERNOR_OVERRIDE.select().where(GOVERNOR_OVERRIDE.c.scope == self.scope)
        )
        rows = list(result)
        self._cache = (id(session), rows)
        return rows
