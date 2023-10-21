with school_sessions as 
 (
 	select 
 	ats."schoolId",
 	sn.school_name,
 	extract(epoch from (es.time - ss.time)/60) as session_length,
 	ss.time as start_time,
 	es.time as end_time
 from {{ ref("StartSession") }} ss 
 left join  {{ ref("EndSession") }} es on (ss."sessionId" = es."sessionId")
 join  {{ ref("SetLogType") }}  slt on (ss."logId" = slt."logId")
 join  {{ ref("AssignToSchool") }}  ats on (slt."logId" = ats."logId")
 join {{ ref("school_name") }}  sn on (ats."schoolId" = sn."logId")
 where slt."logType" = 'Device'
 order by ss.time asc
 )
 select
 	"schoolId",
 	"school_name",
 	count(*) as device_count,
 	PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY session_length) as median_session_length,
 	min(session_length) as mean_session_length
 from school_sessions
 group by "schoolId", "school_name"
