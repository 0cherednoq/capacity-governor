# capacity-governor

Политика распределения ёмкости дефицитных ресурсов между потребителями и политика
пополнения ресурса. Чистые политики, слой стабилизации и симуляция готовы; контур и
адаптеры ждут Tallyho и Identity Hub — состояние по шагам в
[`docs/implementation-progress.md`](docs/implementation-progress.md).

Библиотека отвечает на два вопроса:

* **кому сколько ресурса сейчас** — `MaxMinFair` (общий пул: веса, гарантированный
  минимум, потолок, burst, round robin между тактами), `Sequential` (выделенный пул:
  последовательный расход);
* **когда пополнять** — `ReorderPoint`: политика запасов (s, S) плюс правило нехватки
  под спрос плюс бюджет заказов.

Чего библиотека не делает: не хранит ресурсы (это Identity Hub), не ведёт учёт работы
(это Tallyho), не знает про домены, письма и брокер.

## Документы

| Документ | Что внутри |
|---|---|
| [`docs/resource-management-plan.md`](docs/resource-management-plan.md) | план реализации: порты, алгоритмы политик, контур, мёртвая зона, симуляция, гейт на вынос в отдельный пакет |
| [`docs/implementation-progress.md`](docs/implementation-progress.md) | журнал прогресса: шаги, приёмка, отступления от плана, результаты прогонов |

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
  policies/              MaxMinFair (DRR), Sequential, ReorderPoint
  stabilize.py           мёртвая зона и выдержка при уменьшении окна
  governor.py            plan_tick: чистая часть такта (pin, pause, заказ, метрики)
sim/                     модель пула на SimPy и шесть прогонов G8
tests/                   таблицы приёмки плана и приёмка прогонов
```

## Как запускать

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
pytest                  # 97 тестов: таблицы приёмки плана и прогоны симуляции
python -m sim            # отчёт по шести прогонам G8
ruff check . && mypy src tests sim
```
