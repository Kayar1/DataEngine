# syntax=docker/dockerfile:1
#
# Завдання 1 — Dockerfile для ingestor. Специфікація: ../SPEC.md → «Dockerfile».
#
# Що має бути (див. SPEC.md для деталей і балів):
#   * multi-stage build: окремий builder-стейдж ставить залежності, runtime-стейдж
#     копіює тільки готовий venv + код (без pip/uv-кешу у фінальному образі);
#   * залежності з app/requirements.txt (psycopg);
#   * НЕ запускати від root — створіть і використайте non-root користувача;
#   * точка монтування кешу (/cache) має належати цьому користувачу;
#   * ENTRYPOINT запускає app/gh_ingest.py.
#
# Перевірте збірку: docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

# TODO: реалізуйте multi-stage Dockerfile тут.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/gh_ingest.py .

RUN useradd --create-home --uid 1000 appuser \
    && mkdir -p /cache \
    && chown -R appuser:appuser /cache /app

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"


ENTRYPOINT ["python", "gh_ingest.py"]
