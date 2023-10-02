with devices as (
    select ss."logId" as device_id, ss."time" as first_session_started 
    from {{ ref("StartSession") }} ss 
    join {{ ref("SetLogType") }}  slt on (ss."logId" = slt."logId")
    where "logType" = 'Device'
    order by ss.time asc
)
select 
    device_id::varchar as device_id
    ,first_session_started::timestamp as first_session_started
from devices