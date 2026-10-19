# capacity-governor

Политика распределения ёмкости дефицитных ресурсов между потребителями и политика
пополнения ресурса. Готово всё: политики, слой стабилизации, контур, адаптеры,
симуляция и оба сценария гейта, прогнанные на живых Postgres, Tallyho и Identity Hub.
Состояние по шагам — в
[`docs/implementation-progress.md`](docs/implementation-progress.md).

Библиотека отвечает на два вопроса:

* **кому сколько ресурса сейчас** — `MaxMinFair` (общий пул: веса, гарантированный
  минимум, потолок, удержание окна, round robin между тактами), `Sequential`
  (выделенный пул: последовательный расход), `StaticCeiling` (статичный потолок без
  контура);
* **когда пополнять** — `ReorderPoint`: политика запасов (s, S) плюс правило нехватки
  под спрос плюс бюджет заказов.

Чего библиотека не делает: не хранит ресурсы (это Identity Hub), не ведёт учёт работы
(это Tallyho), не знает про домены, письма и брокер.

## Документы

| Документ | Что внутри |
|---|---|
| [`docs/resource-management-plan.md`](docs/resource-management-plan.md) | план реализации: порты, алгоритмы политик, контур, мёртвая зона, симуляция, гейт на вынос в отдельный пакет |
| [`docs/implementation-progress.md`](docs/implementation-progress.md) | журнал прогресса: шаги, приёмка, отступления от плана, результаты прогонов |
| [`docs/releasing.md`](docs/releasing.md) | выпуск: trusted publishing, окружения GitHub, порядок тега |
| [`CHANGELOG.md`](CHANGELOG.md) | что вошло в версии |

Связанные документы в других репозиториях:

| Документ | Что внутри |
|---|---|
| `talyho/docs/resource-management.md` | исходный проектный документ по всей задаче |
| `talyho/docs/resource-management-review.md` | разбор претензий документа со ссылками `файл:строка` |
| `talyho/docs/resource-management-tallyho.md` | план Tallyho: `item.defer`, динамическое окно, `demand()` |
| `AccountLibrarry/docs/resource-management-plan.md` | план Identity Hub: `eligible_count`, события ёмкости |
| `talyho/docs/resource-management-research.md` | как это сделано в Oban, Sidekiq, Temporal, Kueue, YuniKorn, HPA |

## Важно перед началом работы

Отдельный публикуемый пакет — **преждевременно**. План предусматривает гейт:

* **фаза A** — чистые политики и контур живут модулем `capacity/` внутри проекта
  domain-checker, но пишутся в переносимом виде (ядро без импортов проекта);
* **фаза B** — вынос в этот пакет с extras `[tallyho]`, `[identity-hub]`, когда
  политики отработали на двух сценариях: проверка доменов (`MaxMinFair`) и массовая
  рассылка (`Sequential`).

Обоснование — в плане, раздел G0.

С чего начинать: пункты 1–4 и 8 порядка работ (типы, `MaxMinFair` на deficit round
robin, `ReorderPoint`, мёртвая зона, симуляция SimPy). Они не зависят ни от Tallyho,
ни от Identity Hub — это чистые функции и прогоны симуляции. **Сделано.**

## Что в коде

```
src/capacity/            ядро фазы A: ни одного импорта проекта, только stdlib
  types.py               ConsumerState, Allocation, EligibleCapacity, SupplySnapshot
  ports.py               Protocol'ы: источники ёмкости и запаса, окна, дефициты, пины
  policies/              MaxMinFair (DRR), Sequential, StaticCeiling, ReorderPoint
  stabilize.py           мёртвая зона и выдержка при уменьшении окна
  governor.py            plan_tick: чистая часть такта (pin, pause, заказ, метрики)
  loop.py                Governor: порядок такта, лидерство, kick с дебаунсом, цикл
capacity_adapters/       адаптеры: Postgres (лидер, состояние, pin/pause),
                         Tallyho (окна, спрос, журнал заказов), Identity Hub (ёмкость)
sim/                     модель пула на SimPy и шесть прогонов G8
tests/                   таблицы приёмки плана, контур и адаптеры, прогоны симуляции
```

Ядро (`capacity/`) не импортирует ничего, кроме стандартной библиотеки, — это условие
переноса в отдельный пакет, и его держит `tests/test_portability.py`. Адаптеры к
Tallyho и Identity Hub написаны против публичных сигнатур этих сервисов и не
импортируют их: единственная настоящая зависимость — SQLAlchemy, в extras `[postgres]`.

## Как подключить в проекте

Контур собирается из политики, слоя стабилизации и адаптеров. Всё, кроме фильтра
годности и перевода `consumer_id` в батч, — готовый код.

```python
scope = PostgresTickScope(session_scope=sessionmaker, scope="domains")
state = PostgresStateStore(scope, scope="domains")
capacity = IdentityHubCapacity(
    hub.identities,
    eligible_filter=IdentityFilter(lifecycle="ready", standing="good"),
    work_units=lambda: remaining_credits(hub),   # модель кредитов — ваша
    credits_per_resource=100,
)
governor = Governor(
    scope=scope,
    capacity_source=capacity,
    registry=TallyhoConsumers(th, batches_of_the_run),
    windows=TallyhoWindows(th, resolve_batch, journal=state, session_provider=scope.session),
    state=state,
    policy=MaxMinFair(min_hold_ticks=5),        # ceil(время единицы работы / такт) + 1
    clock=lambda: datetime.now(UTC),
    stabilizer=Stabilizer(tolerance=0.1, stabilize_down=timedelta(seconds=300)),
    overrides=PostgresOverrideStore(scope, scope="domains"),
    supply=PoolSupply(capacity, demand_units, ledger.counts, credits_per_resource=100),
    reorder=ReorderPoint(target=50, reorder_ratio=0.2, max_per_window=20),
    replenisher=TallyhoReplenisher(submit_registration_batch),
)
await governor.run(every=timedelta(seconds=5), stop=shutdown)
```

Оператор вмешивается командами контура: `await governor.pin("batch-7", 2)` держит
потребителю окно, `unpin` возвращает его в раздачу, `pause` оставляет все окна как
есть, не переставая считать раздачу, `resume` возвращает управление. Команды пишутся в
БД своей транзакцией и действуют со следующего такта, независимо от того, кто сейчас
лидер.

Две таблицы (`capacity_governor_state`, `capacity_governor_override`) описаны в
`capacity_adapters.postgres.METADATA` — миграция за проектом. Рабочий пример сборки
целиком лежит в `tests/test_wiring.py`.

## Как запускать

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev,postgres]" asyncpg
pytest                  # 225 тестов: политики, контур, адаптеры, симуляция, интеграция
python -m sim            # отчёт по шести прогонам G8
ruff check . && mypy src tests sim
```

Интеграционные прогоны требуют Postgres и пропускаются без него:

```bash
docker run -d --name capacity-governor-test-pg     -e POSTGRES_PASSWORD=capacity -e POSTGRES_USER=capacity     -e POSTGRES_DB=capacity -p 55444:5432 postgres:16-alpine
pytest tests/test_integration_postgres.py -q
```

Прогоны сценариев гейта (`test_integration_tallyho.py`, `test_integration_sequential.py`,
`test_integration_identity_hub.py`) дополнительно требуют установленных `tallyho` и
`identity-hub`; без них они тоже пропускаются. `CAPACITY_TEST_DSN` задаёт подключение.
