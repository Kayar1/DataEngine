"""Sink: Kafka -> landing-зона (NDJSON). ЕТАП 1 — реалізуйте три місця з TODO.

Єдине місце проєкту, де потік перетворюється на файли. Далі працюють Spark і dbt,
тому контракт landing-зони — це контракт усього пайплайну:

    data/landing/dt=YYYY-MM-DD/hour=HH/part-p{partition}-o{first}-o{last}.ndjson

`dt=` і `hour=` — час ЗАПИСУ (processing time), а не час події. Подія, що
сталася о 09:12, але приїхала о 10:30, лежить у `hour=10`. Тому шари нижче
партиціонують за `occurred_at`, а не за шляхом файлу.

Семантика доставки — at-least-once:
  1. пишемо батч у тимчасовий файл і атомарно перейменовуємо (`os.replace`);
  2. тільки після цього комітимо офсети.
Якщо процес упаде між (1) і (2), батч перечитається. Ім'я файлу — це діапазон офсетів
[first..last] однієї партиції, а лог Kafka незмінний, тому те саме ім'я = той самий
вміст. Якби в імені був лише перший офсет, перезаписаний файл міг би вийти ДОВШИМ за
той, що Bronze уже завантажив, — і хвіст тихо загубився б (Bronze ідемпотентний за
іменем файлу). Дублікати між файлами все одно можливі — прибирати їх має Silver.
"""

from __future__ import annotations

import os
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException

from common import config

BATCH_MAX_MESSAGES = int(os.environ.get("BATCH_MAX_MESSAGES", "500"))
BATCH_MAX_SECONDS = float(os.environ.get("BATCH_MAX_SECONDS", "10"))
# 0 — крутитися вічно (docker compose). >0 — вийти, коли топік «висох» на стільки секунд
# (потрібно тестам: consumer має завершитися сам, а не висіти).
IDLE_EXIT_SECONDS = float(os.environ.get("IDLE_EXIT_SECONDS", "0"))
# Скільки брокер чекає на heartbeat, перш ніж вважати consumer мертвим і віддати його партиції
# іншому. Після `kill -9` нові повідомлення не йдуть рівно стільки, тож значення — компроміс
# між швидким відновленням і хибними ребалансами.
SESSION_TIMEOUT_MS = int(os.environ.get("SESSION_TIMEOUT_MS", "10000"))

_running = True


def _stop(signum: int, frame: FrameType | None) -> None:
    global _running
    _running = False


def landing_path(
    base: Path, ingested_at: datetime, partition: int, first_offset: int, last_offset: int
) -> Path:
    return (
        base
        / f"dt={ingested_at.astimezone(UTC):%Y-%m-%d}"
        / f"hour={ingested_at.astimezone(UTC):%H}"
        / f"part-p{partition}-o{first_offset:012d}-o{last_offset:012d}.ndjson"
    )


def write_batch(
    base: Path,
    ingested_at: datetime,
    records: list[tuple[int, int, bytes]],
) -> list[Path]:
    """Записує батч (partition, offset, value) у landing."""
    by_partition: dict[int, list[tuple[int, bytes]]] = {}

    for partition, offset, value in records:
        by_partition.setdefault(partition, []).append((offset, value))

    written: list[Path] = []

    try:
        for partition, partition_records in sorted(by_partition.items()):
            partition_records.sort(key=lambda item: item[0])

            first_offset = partition_records[0][0]
            last_offset = partition_records[-1][0]

            path = landing_path(
                base,
                ingested_at,
                partition,
                first_offset,
                last_offset,
            )

            path.parent.mkdir(parents=True, exist_ok=True)

            tmp_path = path.with_name(path.name + ".tmp")

            try:
                with tmp_path.open("wb") as f:
                    for _, value in partition_records:
                        if not isinstance(value, bytes):
                            raise TypeError("Kafka value must be bytes")
                        f.write(value.rstrip(b"\r\n"))
                        f.write(b"\n")

                    f.flush()
                    os.fsync(f.fileno())

                if path.exists():
                    tmp_path.unlink()
                else:
                    os.replace(tmp_path, path)

                written.append(path)

            except Exception:
                if tmp_path.exists():
                    tmp_path.unlink()
                raise

    except Exception:
        for path in written:
            if path.exists():
                path.unlink()
        raise

    return written


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": config.KAFKA_GROUP_ID,
            # Офсети комітимо руками — тільки після успішного запису файлу.
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": SESSION_TIMEOUT_MS,
            # Топіка може ще не бути (Kafka щойно стартувала) — перевіряємо метадані частіше.
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )
    consumer.subscribe([config.KAFKA_TOPIC])
    print(f"consumer: {config.KAFKA_TOPIC} -> {config.LANDING_DIR} (group={config.KAFKA_GROUP_ID})")

    batch: list[tuple[int, int, bytes]] = []
    batch_started = time.monotonic()
    last_message_at = time.monotonic()
    total = 0
    waiting_for_topic = False

    def flush() -> None:
        nonlocal batch, batch_started, total
        if not batch:
            batch_started = time.monotonic()
            return
        files = write_batch(config.LANDING_DIR, datetime.now(UTC), batch)
        consumer.commit(asynchronous=False)
        total += len(batch)
        print(f"  {len(batch)} подій -> {', '.join(p.name for p in files)} (всього {total})")
        batch = []
        batch_started = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - batch_started >= BATCH_MAX_SECONDS:
                    flush()
                # Поки нічого не прочитано, додатково чекаємо на ребаланс групи: після падіння
                # попереднього consumer-а партиції звільняються лише через session timeout.
                idle_limit = IDLE_EXIT_SECONDS + (0 if total else SESSION_TIMEOUT_MS / 1000)
                if IDLE_EXIT_SECONDS and time.monotonic() - last_message_at >= idle_limit:
                    print(f"топік порожній {idle_limit:.0f} с — виходжу")
                    break
                continue
            error = msg.error()
            if error is not None:
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Топік ще не створено або Kafka перезапущено: це очікувано, а не збій.
                    if not waiting_for_topic:
                        print(f"топік {config.KAFKA_TOPIC} ще не існує — чекаю")
                        waiting_for_topic = True
                    continue
                raise KafkaException(error)
            waiting_for_topic = False

            partition, offset, value = msg.partition(), msg.offset(), msg.value()
            if partition is None or offset is None or value is None:
                continue  # службове повідомлення / tombstone: писати в landing нічого
            last_message_at = time.monotonic()
            batch.append((partition, offset, value))
            if (
                len(batch) >= BATCH_MAX_MESSAGES
                or time.monotonic() - batch_started >= BATCH_MAX_SECONDS
            ):
                flush()
    finally:
        flush()
        consumer.close()
        print(f"consumer зупинено, записано {total} подій")


if __name__ == "__main__":
    main()
