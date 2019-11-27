--------------------------------------------------------------------------------------------------------------------------
-- 1. LOS RU Data Check (from POSTGIS to Hive)
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_BF_LOS_RU
 where schedule_id = 8460965
;


select * from BUILDING_3DS_HEADER
 where tbd_key = 'B2867238935235937'
;

--------------------------------------------------------------------------------------------------------------------------
-- 2. LOS Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_LOS drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;

insert into RESULT_NR_BF_LOS partition (schedule_id)
select tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, 
       case when sum(case when value = 1 then 1 else 0 end) > 0 then 1 else 0 end as los,
       max(schedule_id) as schedule_id
  from RESULT_NR_BF_LOS_RU
 where schedule_id=8460965  
 group by tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz
;

/*
with AREA as
(
select a.scenario_id, b.schedule_id,
       a.buildinganalysis3d_resolution as resolution
  from SCENARIO a, SCHEDULE b
 where b.schedule_id = 8460965
   and a.scenario_id = b.scenario_id
),
RSLT as
(
select c.scenario_id, a.schedule_id, c.resolution,
       a.ru_id, a.tbd_key, a.rx_tm_xpos, a.rx_tm_ypos,
       int(a.rx_tm_xpos - b.ext_sx) div c.resolution as x_point,
       int(a.rx_tm_ypos - b.ext_sy) div c.resolution as y_point,
       a.rx_floorz,
       a.value
  from RESULT_NR_BF_LOS_RU a, BUILDING_3DS_HEADER b, AREA c
 where a.schedule_id = 8460965
   and a.tbd_key = b.tbd_key
   and a.schedule_id = c.schedule_id
)
insert into RESULT_NR_BF_LOS partition (schedule_id)
select tbd_key, rx_tm_xpos, rx_tm_ypos, x_point, y_point, rx_floorz, 
       case when sum(case when value = 1 then 1 else 0 end) > 0 then 1 else 0 end as los,
       max(schedule_id) as schedule_id
  from RSLT
 group by tbd_key, rx_tm_xpos, rx_tm_ypos, x_point, y_point, rx_floorz
;
*/

-- Check Result Data
select * from RESULT_NR_BF_LOS where schedule_id=8460965;
-- 2705934

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
