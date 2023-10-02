/* Return the most recent school name ass*/
SELECT
    sn."logId",
    sn."name" AS school_name
FROM {{ ref("SetName") }} sn
JOIN (
    SELECT
        "logId",
        MAX("time") AS max_time
    FROM {{ ref("SetName") }}
    GROUP BY "logId"
) b ON sn."logId" = b."logId" AND sn."time" = b.max_time
