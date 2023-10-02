with session_stats as (
    select 
        ss."sessionId"  as session_id, 
        ss."time" as start_time,
        es."time" as end_time,
        sn."school_name"
    from {{ ref("StartSession") }}  ss 
    left join {{ ref("EndSession") }} es on (ss."sessionId" = es."sessionId")
    join {{ ref("SetLogType") }} slt on (ss."logId" = slt."logId")
    join{{ ref("AssignToSchool") }} ats on (slt."logId" = ats."logId")
    join {{ ref("school_name") }} sn on (ats."schoolId" = sn."logId")
    where "logType" = 'Device'
    order by ss.time asc
)
select 
    session_id::varchar
    ,start_time::timestamp
    ,end_time::timestamp
    ,school_name::varchar
from session_stats
