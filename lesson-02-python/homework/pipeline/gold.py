"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

import polars as pl

from . import config

import os

def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:

    GOLD_REPO_ACTIVITY_DIR = os.path.dirname(config.GOLD_REPO_ACTIVITY)
    if not os.path.exists(GOLD_REPO_ACTIVITY_DIR):
        os.makedirs(GOLD_REPO_ACTIVITY_DIR)

    df = (silver
        .group_by("repo_name")
        .agg(
            [
                pl.len().cast(pl.Int64).alias("event_count"),
                pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"),
            ]
        )
        .sort("event_count", descending=True)
    )
    
    print(f"Gold 1 table {df}")

    df.write_parquet(config.GOLD_REPO_ACTIVITY, compression="zstd",)


def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    
    GOLD_ACTIVITY_PER_MINUTE_DIR = os.path.dirname(config.GOLD_ACTIVITY_PER_MINUTE)
    if not os.path.exists(GOLD_ACTIVITY_PER_MINUTE_DIR):
        os.makedirs(GOLD_ACTIVITY_PER_MINUTE_DIR)

    df = (silver
        .with_columns(
            pl.col("created_at")
            .dt.truncate("1m")
            .alias("minute")
        )
        .group_by("minute")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count")
        )
        # 3. Сортируем хронологически от более ранних минут к более поздним
        .sort("minute", descending=False)
    )
    
    print(f"Gold 2 table {df}")

    df.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE, compression="zstd",)


def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    
    GOLD_PUSH_COMMITS_DIR = os.path.dirname(config.GOLD_PUSH_COMMITS)
    if not os.path.exists(GOLD_PUSH_COMMITS_DIR):
        os.makedirs(GOLD_PUSH_COMMITS_DIR)

    df = (silver
        .filter(pl.col("event_type") == "PushEvent")
        .group_by("repo_name")
        .agg(
            [
                pl.len().cast(pl.Int64).alias("push_events"),
                pl.col("commit_count").sum().cast(pl.Int64).alias("total_commits"),
            ]
        )
        .sort("total_commits", descending=True)
    )
        
    print(f"Gold 3 table {df}")

    df.write_parquet(config.GOLD_PUSH_COMMITS, compression="zstd",)