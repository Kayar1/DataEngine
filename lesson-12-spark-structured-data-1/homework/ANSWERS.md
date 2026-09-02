# ANSWERS.md

# Bonus: порівняння `unionByName` та `explode`

## 1. Опис підходів

У роботі порівняно два варіанти побудови підсумкової таблиці `summary`.

### Варіант 1 — `unionByName`

У поточній реалізації для кожного dimension окремо виконується `groupBy`,
після чого результати об'єднуються через `unionByName`.

Схематично:

```text
events
 ├── groupBy(event_type)  ──┐
 ├── groupBy(repo_owner)  ──┤
 ├── groupBy(actor_login) ──┼── unionByName
 └── groupBy(hour)        ──┘
```

Таким чином, для чотирьох dimensions створюються чотири незалежні гілки
агрегації.

Перевага цього варіанта — простота та зрозуміла реалізація.

Недолік — кожен dimension обробляється окремою aggregation-гілкою.

---

### Варіант 2 — `explode`

У бонусному варіанті для кожної події створюється масив із чотирьох пар:

```text
(dimension, dimension_value)
```

Після цього масив розкривається через `explode`, і всі dimensions потрапляють
в одну спільну агрегацію.

Схематично:

```text
events
   │
   ▼
array of (dimension, dimension_value)
   │
   ▼
explode
   │
   ▼
один groupBy
   │
   ▼
summary
```

Реалізація:

```python
def build_summary_explode(events: DataFrame, dimensions: list[str]) -> DataFrame:
    dimension_values = F.array(
        *[
            F.struct(
                F.lit(dimension).alias("dimension"),
                F.col(dimension).cast("string").alias("dimension_value"),
            )
            for dimension in dimensions
        ]
    )

    return (
        events
        .select(
            "repo_name",
            F.explode(dimension_values).alias("d"),
        )
        .select(
            F.col("d.dimension").alias("dimension"),
            F.col("d.dimension_value").alias("dimension_value"),
            "repo_name",
        )
        .groupBy("dimension", "dimension_value")
        .agg(
            F.count("*").alias("events"),
            F.countDistinct("repo_name").alias("distinct_repos"),
        )
    )
```

---

## 2. Перевірка результатів

Обидва варіанти повернули однакову кількість рядків:

```text
unionByName: 25 102
explode:     25 102
```

Контрольні суми `events` для кожного dimension:

```text
event_type   → 29 750
hour         → 29 750
actor_login  → 29 750
repo_owner   → 29 750
```

Отже, зміна способу побудови summary не змінила результат агрегації.

---

## 3. Порівняння фізичних планів

### `unionByName`

У фізичному плані видно чотири окремі aggregation-гілки:

* `event_type`
* `repo_owner`
* `actor_login`
* `hour`

Для кожної гілки є два `Exchange`:

```text
event_type   → 2 Exchange
repo_owner   → 2 Exchange
actor_login  → 2 Exchange
hour         → 2 Exchange
```

Разом:

```text
8 Exchange
```

Перший `Exchange` пов'язаний з агрегацією за `(dimension_value, repo_name)`,
а другий — з фінальною агрегацією, яка також виконує
`countDistinct(repo_name)`.

---

### `explode`

У фізичному плані після `Generate` (`explode`) є одна спільна aggregation-гілка:

```text
Generate
   ↓
HashAggregate
   ↓
Exchange
   ↓
HashAggregate
   ↓
Exchange
   ↓
HashAggregate
```

Разом:

```text
2 Exchange
```

Таким чином:

```text
unionByName → 8 Exchange
explode     → 2 Exchange
```

Кількість `Exchange` зменшилася у 4 рази.

---

## ## 4. Час виконання

Benchmark виконано на тому самому наборі даних після кешування `events`.

Перед вимірюванням обидва DataFrame були виконані один раз для прогріву
Spark/JVM.

Результати:

| Варіант       | Рядків результату | Exchange |     Час |
| ------------- | ----------------: | -------: | ------: |
| `unionByName` |            25 102 |        8 | 0.699 s |
| `explode`     |            25 102 |        2 | 0.463 s |

На цьому запуску варіант `explode` виконався приблизно на **33.8% швидше**:

```text
(0.699 - 0.463) / 0.699 × 100 ≈ 33.8%
```

При цьому `explode` має у 4 рази менше `Exchange`:

```text
unionByName → 8 Exchange
explode     → 2 Exchange
```

Це узгоджується з фізичними планами: `unionByName` створює окрему
aggregation-гілку для кожного з чотирьох dimensions, тоді як `explode`
перетворює dimensions у єдиний потік даних і використовує одну спільну
aggregation-гілку.

> Час виконання наведено для конкретного локального запуску та не є
> універсальним показником. Він залежить від CPU, пам'яті, версії Spark,
> JVM та інших умов виконання.


---

## ## 5. Висновок

Обидва варіанти повертають однаковий результат — **25 102 рядки**, тому
зміна способу побудови `summary` не змінює логіку розрахунку.

`unionByName` є простішим для читання та реалізації: кожен dimension
агрегується незалежно, після чого результати об'єднуються.

Варіант з `explode` перетворює всі dimensions у єдину структуру та дозволяє
виконати одну спільну aggregation-гілку.

За результатами фізичного плану:

```text
unionByName → 8 Exchange
explode     → 2 Exchange
```

На виконаному benchmark:

```text
unionByName → 0.699 s
explode     → 0.463 s
```

Тобто на цьому наборі даних `explode` показав приблизно **33.8% менший час
виконання**.

Отже, для цього сценарію `explode` має більш компактний фізичний план і
показав кращий результат benchmark. Водночас збільшення кількості рядків
перед агрегацією через `explode` означає, що його перевага не гарантована
для будь-якого набору даних — фактичну продуктивність потрібно оцінювати
за конкретним фізичним планом і benchmark.
