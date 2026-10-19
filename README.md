# Capacity Governor

[![CI](https://github.com/0cherednoq/capacity-governor/actions/workflows/ci.yml/badge.svg)](https://github.com/0cherednoq/capacity-governor/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/capacity-governor.svg?cacheSeconds=300)](https://pypi.org/project/capacity-governor/)
[![Python](https://img.shields.io/pypi/pyversions/capacity-governor.svg?cacheSeconds=300)](https://pypi.org/project/capacity-governor/)
[![License](https://img.shields.io/pypi/l/capacity-governor.svg?cacheSeconds=300)](LICENSE)

Capacity Governor — асинхронная Python-библиотека, которая решает, **кому сколько
дефицитного ресурса дать прямо сейчас** и **когда этот ресурс пополнять**. Основной
читатель — разработчик Python 3.11+, у которого несколько потребителей делят пул
ограниченных ресурсов и мешают друг другу.

Библиотека не хранит ресурсы и не выполняет работу. Она считает **окно** каждого
потребителя — сколько работы ему разрешено держать в полёте — и отдаёт это число
вашему коду, который его применяет.

## Когда она нужна

Возьмите её, если узнаёте свою ситуацию:

- один пул делят несколько заданий, и крупное забивает его целиком, а мелкие ждут;
- ресурс кончается в середине работы, и очередь копит ошибки вместо того, чтобы ждать;
- вы развели потребителей по жёстким лимитам в конфиге, и теперь половина лимитов
  простаивает, пока другая упирается в потолок;
- пополнение (регистрация аккаунтов, закупка ключей) запускается то поздно, то дважды;
- приоритет сделан через `sleep` и ручные правки конфига во время инцидента.

Ресурс при этом может быть каким угодно: аккаунты платформ, прокси, слоты браузеров и
GPU, ключи внешних API с квотой, номера для SMS, предоплаченные кредиты. Библиотеке
важно не что это, а как оно расходуется.

Она **не** нужна, если ресурс один на одного потребителя, если лимит статичен и никого
не стесняет, или если очередь и так простаивает — делить там нечего.

## Установка

```bash
uv add capacity-governor
```

Ядро не имеет зависимостей вообще. Адаптер к PostgreSQL ставится отдельно:

```bash
uv add "capacity-governor[postgres]"
```

## Как это выглядит

Двенадцать прокси на три задания скрапинга, одно из которых вдвое важнее:

```python
from capacity import ConsumerState, MaxMinFair

policy = MaxMinFair()
allocation = policy.allocate(
    capacity=12,
    consumers=[
        ConsumerState("urgent-scrape", demand=500, weight=2),
        ConsumerState("daily-scrape", demand=500),
        ConsumerState("backfill", demand=500),
    ],
    deficits={},
)

allocation.grants  # {'urgent-scrape': 6, 'daily-scrape': 3, 'backfill': 3}
```

Политика — чистая функция: те же аргументы дают тот же ответ, базы и времени она не
знает. Её можно вызвать прямо в своём планировщике, а можно обернуть в контур, который
сам пересчитывает окна по расписанию:

```python
scope = PostgresTickScope(session_scope=sessionmaker, scope="scrapers")
governor = Governor(
    scope=scope,
    capacity_source=proxy_pool,     # сколько прокси живо
    registry=jobs,                  # кто и сколько просит
    windows=job_windows,            # куда применить окна
    state=PostgresStateStore(scope),
    policy=MaxMinFair(min_hold_ticks=5),
    clock=clock,
    stabilizer=Stabilizer(),
)
await governor.run(every=timedelta(seconds=5), stop=shutdown)
```

Контур берёт лидерство на каждый такт, пишет только изменившиеся окна и переживает
перезапуск: очередь и удержания лежат в базе, а не в памяти процесса.

## Сценарии

Четыре разбора под разные виды ресурса — с кодом, который запускается без базы:

| Сценарий | Про что | Код |
|---|---|---|
| [Прокси между скраперами](docs/use-cases.md#прокси-между-скраперами) | веса, освобождение доли, падение ёмкости | [`examples/proxy_pool.py`](examples/proxy_pool.py) |
| [Слоты GPU между командами](docs/use-cases.md#слоты-gpu-между-командами) | гарантированный минимум и потолок | [`examples/gpu_slots.py`](examples/gpu_slots.py) |
| [Отправители рассылки](docs/use-cases.md#отправители-рассылки) | последовательный расход, переключение | [`examples/mailing_senders.py`](examples/mailing_senders.py) |
| [Квота внешнего API](docs/use-cases.md#квота-внешнего-api) | когда докупать и сколько | [`examples/api_quota.py`](examples/api_quota.py) |

```bash
python -m examples.proxy_pool
```

## Что внутри

**Политики распределения** отвечают на вопрос «кому сколько сейчас»:

- `MaxMinFair` — общий пул между несколькими потребителями: веса, гарантированный
  минимум, потолок, честная ротация, когда ресурса меньше, чем просят;
- `Sequential` — выделенный пул с последовательным расходом, где чередование ресурсов
  вредно;
- `StaticCeiling` — статичный потолок для ресурсов, которым пересчёт не нужен.

**Политика пополнения** отвечает на вопрос «когда заводить ещё»:

- `ReorderPoint` — планка «упало ниже — дозаказать до цели», правило «не хватает под
  уже поставленную работу», бюджет заказов за окно и учёт того, что уже едет.

**Контур** применяет политику сам: раз в несколько секунд читает ёмкость и спрос, гасит
дрожание окон, пишет только изменения, заказывает пополнение и отдаёт метрики такта.
Оператор вмешивается командами `pin`, `unpin`, `pause`, `resume`.

Подробнее — [`docs/architecture.md`](docs/architecture.md).

## Интеграции

Порты — обычные `Protocol`, поэтому подключить можно что угодно. Готовые адаптеры:

| Адаптер | Что даёт | Extras |
|---|---|---|
| `capacity_adapters.memory` | всё в памяти: примеры, тесты, прототипы | — |
| `capacity_adapters.postgres` | лидерство на advisory-локе, состояние, `pin`/`pause` | `[postgres]` |
| `capacity_adapters.tallyho` | спрос и окна одним запросом, заказ пополнения | `[tallyho]` |
| `capacity_adapters.identity_hub` | ёмкость пула аккаунтов через Identity Hub | `[identity-hub]` |

Адаптеры к Tallyho и Identity Hub написаны против их публичных сигнатур и сами эти
пакеты не импортируют — их можно читать и тестировать, не устанавливая ни один из них.

## Разработка

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev,postgres]" asyncpg
pytest -q                # 272 теста: политики, контур, адаптеры, симуляция, интеграция
python -m sim            # прогоны симуляции: как ведут себя параметры под нагрузкой
ruff check . && mypy src tests sim
```

Интеграционные прогоны требуют PostgreSQL и без него пропускаются:

```bash
docker run -d --name capacity-governor-test-pg \
    -e POSTGRES_PASSWORD=capacity -e POSTGRES_USER=capacity \
    -e POSTGRES_DB=capacity -p 55444:5432 postgres:16-alpine
```

`CAPACITY_TEST_DSN` задаёт подключение. Порядок выпуска — в
[`docs/releasing.md`](docs/releasing.md), изменения — в [`CHANGELOG.md`](CHANGELOG.md).

## Лицензия

MIT — см. [LICENSE](LICENSE).
