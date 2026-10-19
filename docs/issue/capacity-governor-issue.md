# Перевести адаптеры на новую версию Tallyho и Identity Hub, убрать переподписку окон

## Контекст

`capacity-governor 0.1.0` уже на PyPI и в extras объявляет `tallyho>=0.0.1`. Но адаптер
Tallyho написан против непубликованного рабочего дерева Tallyho. С опубликованными
пакетами (`tallyho 0.0.1`, `identity-hub 1.0.0a5`) результат такой:

| Набор | Результат |
|---|---|
| `tests/test_integration_tallyho.py` | 5 из 5 падают: `ConfigurationError: max_in_flight должен быть целым >= 1, получено 0`; `set_max_in_flight` в 0.0.1 нет |
| `tests/test_integration_sequential.py` | 5 из 5 падают по той же причине |
| `tests/test_integration_identity_hub.py` | 3 из 3 проходят, если поставить `identity-hub[postgres,alembic]` |
| `tests/test_integration_postgres.py` | проходят |

В следующей версии Tallyho (ветка управления ресурсами, задачи RM*) уже есть всё, что
нужно адаптеру:

| Что | Где в Tallyho | Статус |
|---|---|---|
| Окно на ходу, ноль разрешён, в том числе при создании батча | `BatchHandle.set_max_in_flight` / `clear_max_in_flight` (RM0.1, RM1.1) | готово |
| `resume` не прорывает окно | RM1.2 | готово |
| `item.defer(reason, delay \| until)` без траты попытки | RM2.2 | готово |
| Потолки `max_defers` / `max_deferred_for` | RM2.3 | в работе |
| `Progress.deferred`, `deferred_reasons` | RM2.4 | в работе |
| `th.demand(kinds, attributes=, limit=) -> list[BatchDemand]` одним запросом; в `BatchDemand` есть `max_in_flight` | RM4.1 | готово |
| Ленивый импорт `hook_modules` | RM3.1 | готово |
| `deadline_mode="active"` | RM6.1 | в работе |

В Identity Hub 1.0.0a5 есть `Pool.eligible_count(for_operation=, among=, require_quota=)`.
Он строит тот же запрос кандидатов, что и `pool.lease()`.

Ишью собирает всё, что нужно поменять в governor, чтобы он работал поверх этих версий и
решал исходную задачу: **не выпускать в работу больше, чем есть ресурса, и не жечь
попытки, когда ресурса нет.**

---

## 1. Адаптер Tallyho поверх новой версии

### 1.1. Спрос через `th.demand()`, а не через `view()` на каждый батч

`TallyhoConsumers` сейчас делает `view()` на каждый батч каждый такт. Заменить на один
вызов:

```python
@dataclass(slots=True)
class TallyhoDemandRegistry:
    """ConsumerRegistry поверх th.demand(): один SQL на такт при любом числе батчей."""

    client: TallyhoDemandClient
    kinds: Sequence[str]
    attributes: Mapping[str, object] | None = None
    weight_of: Callable[[BatchDemand], int] = lambda _: 1
    guaranteed_of: Callable[[BatchDemand], int] = lambda _: 0
    ceiling_of: Callable[[BatchDemand], int | None] = lambda _: None

    async def consumers(self) -> Sequence[ConsumerState]:
        rows = await self.client.demand(self.kinds, attributes=self.attributes)
        if rows and rows[0].truncated:
            raise DemandTruncated(len(rows))   # молча раздавать по неполному списку нельзя
        return [
            ConsumerState(
                consumer_id=str(row.id),
                demand=ready_demand(row),
                weight=self.weight_of(row),
                guaranteed=self.guaranteed_of(row),
                ceiling=self.ceiling_of(row),
            )
            for row in rows
            if not row.paused                  # пауза — решение человека, окно ей не нужно
        ]
```

Отдельно нужно решить, что считать спросом. `BatchDemand.pending` включает отложенные
items. Отложенный item не уйдёт в брокер раньше своего срока, поэтому окно под него
простаивает. Предложение:

```python
def ready_demand(row: BatchDemand) -> int:
    """Сколько работы потребитель может запустить прямо сейчас."""
    return max(0, row.pending - row.deferred)
```

Отложенные при этом остаются видны: в метриках (`deferred`) и в правиле нехватки
`ReorderPoint`. Пополнять нужно под всю оставшуюся работу, а не только под готовую.

### 1.2. Журнал окон больше не нужен

Журнал (`WindowJournal`, строка `windows` в `PostgresStateStore`) появился потому, что
прочитать окно у Tallyho было нельзя. Теперь `BatchDemand.max_in_flight` отдаёт его тем же
запросом, что и спрос. Значит:

* `TallyhoWindows.current_windows()` берёт окна из последнего `demand()` такта. Отдельного
  чтения нет: registry и sink делят снимок такта;
* журнал удаляется вместе с его проблемами: растёт бесконечно (завершённые батчи не
  удаляются), расходится с Tallyho после пересоздания схемы, не видит окна, выставленные
  проектом (например, окно кампании из её статуса);
* `None` (окна нет) и `0` (окно закрыто) различаются. Батч с `max_in_flight is None`
  контуру не принадлежит: он его не трогает и раздаёт ему ноль.

### 1.3. Потребители создаются с закрытым окном

Новая версия Tallyho позволяет создать батч сразу с `max_in_flight=0`. Это нужно описать
в README как обязательный порядок: проект создаёт батч закрытым, а открывает его контур
(или `kick()` сразу после коммита). Иначе батч до первого такта работает без ограничения.

### 1.4. Журнал заказов пополнения через `demand()`

`TallyhoOrderLedger` сейчас делает `list_batches()` и `view()` на каждый батч регистраций.
Через `th.demand(kinds=[registration_kind])` получается одно чтение. `ordered_in_window`
по-прежнему нужен по батчам, созданным с начала бюджетного окна. Для этого достаточно
`list_batches(created_after=…)` без `view()`: размер заказа уже кодируется слотами
`slot:{i}`, а остаток в работе отдаёт `demand()`.

### 1.5. Зависимость

После релиза Tallyho поднять extra до `tallyho>=<версия с RM1.1, RM2.2, RM4.1>`. Описывать
API Tallyho своими `Protocol` — правильно, но объявленная совместимость обязана совпадать
с опубликованной версией.

---

## 2. Адаптер Identity Hub поверх `eligible_count`

`IdentityHubCapacity` считает ёмкость через `identities.count(where=IdentityFilter)`.
Этот фильтр знает только поля Identity Hub (`lifecycle`, `standing`, `auth`,
`availability`, теги, точное совпадение меток). Подзапрос к таблице проекта, сравнение по
диапазону, квоты и резерв пула он выразить не может. На сценарии проверки доменов это
даёт:

```
30 аккаунтов в хорошем состоянии, кредиты есть у трёх (по 100 кредитов)
EligibleCapacity(eligible=30, work_units=300).allocatable == 30   # арендовать можно 3
```

Предложение — новый адаптер, который берёт пул и тот же `among`, что идёт в аренду:

```python
@dataclass(slots=True)
class IdentityHubPoolCapacity:
    """CapacitySource: ровно те identity, которые выдаст pool.lease(for_operation=…, among=…)."""

    pool: IdentityHubPool                      # Protocol с eligible_count(...)
    among: Any                                 # тот же Select, что в pool.lease(among=...)
    for_operation: str | None = None
    work_units: Callable[[], Awaitable[int]] | None = None

    async def capacity(self) -> EligibleCapacity:
        hub = await self.pool.eligible_count(for_operation=self.for_operation, among=self.among)
        units = await self.work_units() if self.work_units else None
        return EligibleCapacity(
            eligible=hub.eligible,
            free=hub.free,
            leased=hub.leased,
            reserved=hub.reserved,
            work_units=units,
        )
```

Что ещё нужно:

* `EligibleCapacity.allocatable` должен вычитать `reserved`. Сейчас резерв пула Identity
  Hub уходит в раздачу:
  ```python
  @property
  def allocatable(self) -> int:
      base = self.eligible - self.reserved
      return base if self.work_units is None else min(base, self.work_units)
  ```
* В `eligible_count` есть параметр `limit`: результат, равный ему, означает «насыщено»,
  а не точное число. Адаптер должен передавать `limit` не меньше суммарного потолка
  потребителей.
* В README стоит показать приём «одна функция строит `among` и для аренды, и для ёмкости».
  Тогда разойтись им негде.
* `IdentityHubCapacity` поверх `count(where=…)` можно оставить, но переименовать
  (например, `IdentityFilterCapacity`) и описать, когда он годится: когда годность целиком
  выражается полями Identity Hub.

Имена: в Identity Hub тоже есть тип `EligibleCapacity`. В адаптере импортировать его под
другим именем, чтобы два типа не путались.

---

## 3. Стабилизатор не должен раздавать окон больше, чем есть ресурса

Уменьшение окна откладывается на `stabilize_down` (300 с), кроме перехода в 0. Проверка на
`plan_tick` с `MaxMinFair(min_hold_ticks=5)` и `Stabilizer()` по умолчанию:

```
A один, потом приходит B, ёмкость 30:
  t=50…345 с   windows={'A': 30, 'B': 15}   sum=45 > 30
Ёмкость падает 30 → 10 (кредиты кончаются):
  t=50…300 с   window={'A': 30}   при 10 аккаунтах
```

Почти пять минут в брокер уходит в полтора–три раза больше работы, чем есть аккаунтов.
У HPA, откуда взята асимметрия, уменьшение дорого: под нужно остановить. Здесь уменьшение
окна — один UPDATE. А лишнее окно — это задачи без ресурса: с `tallyho 0.0.1` каждая
сжигает попытку, с новой версией превращается в `defer` и копит `defers` до
`max_defers`.

### 3.1. Инвариант

**Сумма окон, которыми управляет контур, никогда не превышает `allocatable`.**
Откладывать уменьшение разрешено, только пока инвариант соблюдается:

```python
def apply(self, now, recommended, current, capacity: int) -> StabilizeResult:
    ...                                         # текущий расчёт to_write и held_ids
    projected = {**current, **to_write}
    over = sum(projected[cid] for cid in recommended) - capacity
    for cid in sorted(held_ids, key=lambda c: projected[c] - recommended[c], reverse=True):
        if over <= 0:
            break
        cut = min(projected[cid] - recommended[cid], over)
        to_write[cid] = projected[cid] - cut
        over -= cut
```

Пины учитываются в сумме: их окна контур не пишет, но ёмкость они расходуют.

### 3.2. Умолчания

* `stabilize_down` по умолчанию — порядка длительности единицы работы (как правило
  `min_hold_for`), а не 300 с;
* в docstring и README прямо сказать, что стабилизатор экономит записи ценой переподписки,
  и инвариант 3.1 эту цену ограничивает.

### 3.3. История стабилизатора и смена лидера

Лидер берётся на каждый такт, а история рекомендаций живёт в памяти процесса. Процесс,
который был лидером больше `stabilize_down` назад, при возвращении применит уменьшение
сразу: `observing_since` у него старый, а история уже обрезана. Утверждение в docstring
«новый лидер консервативен по построению» верно только для свежего процесса. Варианты:
сбрасывать `observing_since`, если между тактами этого процесса прошло больше
`stabilize_down`, или хранить историю в `StateStore` рядом с дефицитами. При инварианте
3.1 это уже не опасно, но docstring нужно поправить.

---

## 4. Будить отложенную работу, когда ресурс вернулся

Задача, не получившая аккаунт, делает `item.defer(reason="no_accounts", delay=…)`. Когда
ресурс появляется, контур открывает окно сразу. Но отложенные items уйдут в брокер только
по истечении своего срока, и окно до этого простаивает. В новой версии Tallyho пауза и
`resume` срок отложения намеренно не трогают, а API «выпустить отложенные сейчас» нет.

Варианты:

* **В Tallyho** (отдельное ишью туда): `BatchHandle.release_deferred(reasons=…, session=…)`
  — сбросить срок у отложенных с указанными причинами. Контур зовёт его в такте, где окно
  потребителя выросло с 0. Это правильное решение.
* **В governor до этого**: в README рекомендовать короткий `delay` для ресурсных причин
  (порядка такта контура) и полагаться на окно 0 как на основную защиту. Отложение
  остаётся страховкой на редкие промахи между тактами.

---

## 5. Симуляция: отложение не бесплатно

Сейчас модель считает `defer` бесплатным. Прогон 3 (наплыв ×10) даёт 528 отложений, прогон
6 — 75. Предложение:

* метрика **переподписки**: сумма `max(0, Σ окон − allocatable)` по тактам (единица·такт);
* два режима стоимости `defer`: «ретрай брокера» (поведение `tallyho 0.0.1`) и «отложение
  без попытки» (новая версия, с учётом `max_defers`);
* прогон «приход второго потребителя» и «ёмкость падает втрое» из пункта 3 — до и после
  инварианта.

---

## 6. Мелочи

* README: в примере `PostgresStateStore(scope, scope="scrapers")` переменная `scope` не
  объявлена.
* Строку `scope` нужно повторять в `PostgresTickScope`, `PostgresStateStore` и
  `PostgresOverrideStore`. Опечатка молча разведёт состояние по двум ключам. Пусть хранилища
  берут `scope` из переданного `PostgresTickScope`.
* Если журнал окон всё-таки остаётся (пункт 1.2), удалять из него потребителей, которых
  нет в такте.

---

## 7. CI

* Интеграционные прогоны против **опубликованных** версий Tallyho и Identity Hub
  (матрица: последний релиз + main соседнего репозитория). Тогда пункт «Контекст» поймался
  бы на первом коммите.
* В интеграции Identity Hub ставить с `[postgres,alembic]`, иначе миграции падают с
  `ModuleNotFoundError: alembic`.

---

## Критерии приёмки

1. Интеграционные наборы Tallyho и Sequential зелёные против опубликованной версии
   Tallyho с RM1.1, RM2.2, RM4.1.
2. Спрос и окна читаются одним `th.demand()` за такт; журнала окон нет.
3. Сценарий «30 аккаунтов в хорошем состоянии, кредиты у трёх» даёт `allocatable == 3`
   через `IdentityHubPoolCapacity` с тем же `among`, что у аренды.
4. В симуляции на каждом такте `Σ окон ≤ allocatable`. Переподписка равна 0 во всех
   прогонах, включая «приход второго потребителя» и «ёмкость падает втрое».
5. Сквозной сценарий из приёмки Tallyho (RM7.1: 20 единиц работы, ёмкость 5 + 3 + 0,
   пополнение) с governor в контуре: **0 сгоревших ретраев**, 20 платных вызовов, не больше
   одного параллельного запроса с ресурса, 20 из 20 после пополнения.
