WITH watched_repos AS (
    SELECT DISTINCT repo_name
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'WatchEvent'
)

SELECT
    w.repo_name
FROM watched_repos AS w
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ ref('stg_events') }} AS s
    WHERE s.repo_name = w.repo_name
      AND s.event_type = 'PushEvent'
)
ORDER BY repo_name
