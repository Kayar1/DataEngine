# NOTES.md

## 1. Рішення й компроміси

### Airflow та стан обробки

DAG `rides_medallion` запускається кожні 5 хвилин, але фактичне вікно обробки не визначається через `data_interval_start` / `data_interval_end`. Джерело — файли Bronze/Landing, а стан того, що вже оброблено, зберігається в самих даних та incremental-моделях dbt. Тому повторний запуск без нових файлів не змінює результат, а retry окремої задачі є безпечним.

### `--indirect-selection cautious`

Для dbt-задач `bronze_contract`, `silver` і `gold` використовується `--indirect-selection cautious`, оскільки деякі тести мають залежності між шарами.

Зокрема, тест `assert_bronze_silver_reconcile` перевіряє одночасно Bronze і Silver. Якщо запускати `dbt test --select source:bronze` без `cautious`, цей тест може виконатися ще до побудови Silver і завершити `bronze_contract` з помилкою `relation "silver.events" does not exist`.

`cautious` залишає тільки тести, для яких вибрані ресурси є достатніми.

### Надійність повторного запуску

DAG побудований послідовно:

`bronze_spark → bronze_contract → silver → gold → reconcile`

Кожен етап має окрему відповідальність. Incremental-моделі використовують стан даних, а не дату запуску DAG. Це дозволяє безпечно повторювати запуск після помилки окремої задачі.

## 2. Журнал використання ШІ

### Випадок 1:

ШІ допоміг проаналізувати помилку Airflow `relation "silver.events" does not exist`. Було встановлено, що `bronze_contract` запускав тест `assert_bronze_silver_reconcile` до побудови Silver. Після цього команда була змінена на використання `--indirect-selection cautious`.

### Випадок 2:

ШІ допоміг перевірити порядок аргументів dbt у `BashOperator`. Було виявлено, що `--project-dir` та `--profiles-dir` стояли перед командою `test/build`, через що dbt завершувався з помилкою `No such option '--project-dir'`. Команди DAG були виправлені на форму `dbt test --project-dir ...` та `dbt build --project-dir ...`.

### Випадок 3:

ШІ допоміг локалізувати помилку Airflow за станами задач. Перевірка `states-for-dag-run` показала, що `bronze_spark` завершився успішно, а `bronze_contract` — failed. Після виправлень окремий запуск `bronze_contract` завершився успішно, а повний `verify.sh orchestrate` завершився результатом `7 passed` і `PASS ✅ orchestrate: усе зелене`.

## 3. Ваші dbt-тести

### `assert_fact_ride_measures_nonnegative.sql`

* **Файл:** `dbt_rides/tests/assert_fact_ride_measures_nonnegative.sql`
* **Що перевіряє:** перевіряє, що wait/trip time, distance, fare, surge, tolls, tip, total і payment не мають від'ємних значень у `fact_ride`.
* **Як переконались, що він ловить помилку:** тест успішно проходить на коректних даних. Умова тесту повертає рядки з від'ємними значеннями, тому така поломка призведе до failed singular test.

### `assert_agg_watermark_matches_fact.sql`

* **Файл:** `dbt_rides/tests/assert_agg_watermark_matches_fact.sql`
* **Що перевіряє:** для кожної пари `requested_hour + pickup_zone_key` перевіряє, що `_ingested_at` в `agg_zone_hourly` дорівнює максимальному `_ingested_at` відповідних записів `fact_ride`.
* **Як переконались, що він ловить помилку:** тест успішно проходить на коректних даних. Він повертає групи, де watermark агрегату відрізняється від максимального watermark у фактах, тому застаріле значення призведе до failed singular test.

## 4. Що зробили б далі

* Додали б окремий моніторинг тривалості задач DAG та кількості retry.
* Додали б alerting для failed DAG runs і порушення reconcile-перевірок.
* Для production-середовища винесли б конфігурацію підключення до PostgreSQL та інші секрети в централізоване сховище секретів.
* Додали б retention/архівацію старих Bronze-файлів та контроль дискового простору.
* Розширили б observability: метрики кількості оброблених подій, нових rides, помилок contract/reconcile та затримки між появою файлу і завершенням Gold.
