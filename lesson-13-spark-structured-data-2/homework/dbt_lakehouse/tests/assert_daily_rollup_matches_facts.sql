-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select
    1 as _test_failure
where (
    select coalesce(sum(commits), 0)
    from {{ ref('fact_repo_activity_daily') }}
) != (
    select count(*)
    from {{ ref('fact_commit') }}
)