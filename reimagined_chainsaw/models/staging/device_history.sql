WITH DateRanges AS (
  SELECT
    ss."logId" AS device_id,
    ss."time" AS start_time,
    es."time" AS end_time,
    date_part('minute', es."time" - ss."time") AS usage_minutes,
    extract(dow from ss."time") AS dow
  FROM
    {{ ref("StartSession") }} ss
  LEFT JOIN
    {{ ref("EndSession") }} es ON (ss."sessionId" = es."sessionId")
  JOIN
    {{ ref("SetLogType") }} slt ON (ss."logId" = slt."logId")
  WHERE
    "logType" = 'Device'
),
DateRangesWithWeek AS (
  SELECT
    *,
    (EXTRACT(EPOCH FROM start_time) / (3600 * 24))::int / 7 AS week_number
  FROM
    DateRanges
  WHERE
    dow NOT IN (0, 6)  -- Exclude Saturday (0) and Sunday (6)
)
SELECT
  device_id,
  start_time::date AS date,
  SUM(usage_minutes) AS total_usage_minutes,
  AVG(usage_minutes) OVER (
    PARTITION BY device_id
    ORDER BY start_time
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS avg_usage_minutes_7d
FROM
  DateRangesWithWeek
GROUP BY device_id, start_time, usage_minutes
ORDER BY
  device_id, start_time

