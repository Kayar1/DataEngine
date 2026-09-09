-- Тест: churn у fact_pull_request завжди = additions + deletions.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select
    pr_id,
    additions,
    deletions,
    churn
from {{ ref('fact_pull_request') }}
where coalesce(churn, 0) != coalesce(additions, 0) + coalesce(deletions, 0)