"""Интеграция с живым Identity Hub: ёмкость пула считается настоящим `count()`.

Закрывает то место плана, где значился отсутствующий `eligible_count` (I1): адаптер
считает годные аккаунты агрегатом по фильтру, и здесь проверено, что фильтр действительно
отделяет годных от забаненных, незалогиненных и остывающих — на настоящих записях
Identity Hub, а не на подделке.

Пропускается без базы и без установленного Identity Hub.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from capacity_adapters.identity_hub import IdentityHubCapacity

DSN = os.environ.get(
    "CAPACITY_TEST_DSN",
    "postgresql+asyncpg://capacity:capacity@127.0.0.1:55444/capacity",
)
SCHEMA = "capacity_it_hub"

identity_hub = pytest.importorskip("identity_hub", reason="Identity Hub не установлен")
pytest.importorskip("pydantic", reason="Identity Hub не установлен")

from pydantic import BaseModel, SecretStr  # noqa: E402 - после importorskip


class Credentials(BaseModel):
    login: str


class Session(BaseModel):
    token: SecretStr


CHECKER = identity_hub.Service(
    name="checker",
    title="Domain checker",
    credentials=Credentials,
    session=Session,
)


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    live = create_async_engine(DSN)
    try:
        async with live.connect() as connection:
            await connection.close()
    except Exception:
        await live.dispose()
        pytest.skip(f"нет Postgres по {DSN}")
    try:
        yield live
    finally:
        await live.dispose()


@pytest.fixture
async def space(engine: AsyncEngine) -> AsyncIterator[object]:
    # Ключ шифрования — ровно 32 байта; тестовый, в базу уходят только тестовые записи.
    secrets = identity_hub.StaticSecretsProvider(keys={"k1": b"0" * 32}, active="k1")
    hub = identity_hub.IdentityHub(
        sessionmaker=async_sessionmaker(engine, expire_on_commit=False),
        secrets=secrets,
        services=[CHECKER],
        db_schema=SCHEMA,
        migrate="auto",
    )
    await hub.start()
    # Своё пространство на тест: иначе аккаунты предыдущего попадают в счёт следующего.
    handle = await hub.spaces.ensure(f"capacity-{uuid4().hex[:12]}", "Capacity")
    try:
        yield handle
    finally:
        await hub.aclose()


async def add_identity(
    space: object,
    ref: str,
    *,
    lifecycle: object,
    auth: object,
    banned: bool = False,
) -> None:
    """Создать аккаунт. `standing` при создании не задаётся — только командой `ban`.

    `external_ref` уникален в пределах сервиса, а не пространства, поэтому к нему
    добавляется суффикс: иначе второй тест упал бы на записях первого.
    """
    ref = f"{ref}-{uuid4().hex[:8]}"
    identity = await space.identities.create(  # type: ignore[attr-defined]
        identity_hub.IdentityCreate(
            service=CHECKER,
            external_ref=ref,
            credentials={"login": ref},
            session={"token": "secret"},
            lifecycle=lifecycle,
            auth=auth,
        ),
        actor="capacity-governor-test",
    )
    if banned:
        await space.identities.ban(  # type: ignore[attr-defined]
            identity.id, reason="тестовый бан", actor="capacity-governor-test"
        )


async def test_capacity_counts_only_usable_accounts(space: object) -> None:
    """Годные считаются, забаненные и незалогиненные — нет."""
    ready = identity_hub.Lifecycle.READY
    good = identity_hub.Standing.GOOD
    authenticated = identity_hub.AuthState.AUTHENTICATED

    for index in range(3):
        await add_identity(space, f"ok-{index}", lifecycle=ready, auth=authenticated)
    await add_identity(space, "banned", lifecycle=ready, auth=authenticated, banned=True)
    await add_identity(
        space,
        "no-session",
        lifecycle=ready,
        auth=identity_hub.AuthState.NO_SESSION,
    )
    await add_identity(
        space,
        "provisioning",
        lifecycle=identity_hub.Lifecycle.PROVISIONING,
        auth=authenticated,
    )

    source = IdentityHubCapacity(
        space.identities,  # type: ignore[attr-defined]
        eligible_filter=identity_hub.IdentityFilter(
            service=CHECKER,
            lifecycle=[str(ready)],
            standing=[str(good)],
            auth=[str(authenticated)],
        ),
    )
    capacity = await source.capacity()

    assert capacity.eligible == 3
    assert capacity.allocatable == 3


async def test_banning_the_pool_drops_capacity_to_zero(space: object) -> None:
    """Сценарий §13 документа в миниатюре: пул забанен — ёмкости нет, окна закроются."""
    filters = identity_hub.IdentityFilter(
        service=CHECKER,
        lifecycle=[str(identity_hub.Lifecycle.READY)],
        standing=[str(identity_hub.Standing.GOOD)],
    )
    source = IdentityHubCapacity(space.identities, eligible_filter=filters)  # type: ignore[attr-defined]

    for index in range(2):
        await add_identity(
            space,
            f"doomed-{index}",
            lifecycle=identity_hub.Lifecycle.READY,
            auth=identity_hub.AuthState.AUTHENTICATED,
            banned=True,
        )

    assert (await source.capacity()).allocatable == 0


async def test_remaining_credits_bind_the_tail(space: object) -> None:
    """Кредитов меньше, чем аккаунтов: окон ровно столько, сколько работы оплачено."""
    for index in range(4):
        await add_identity(
            space,
            f"credit-{index}",
            lifecycle=identity_hub.Lifecycle.READY,
            auth=identity_hub.AuthState.AUTHENTICATED,
        )

    async def work_units() -> int:
        return 2

    source = IdentityHubCapacity(
        space.identities,  # type: ignore[attr-defined]
        eligible_filter=identity_hub.IdentityFilter(
            service=CHECKER,
            lifecycle=[str(identity_hub.Lifecycle.READY)],
            standing=[str(identity_hub.Standing.GOOD)],
        ),
        work_units=work_units,
        credits_per_resource=100,
    )

    capacity = await source.capacity()

    assert capacity.eligible == 4
    assert capacity.allocatable == 2
