--------------------------------------------------------------------------------------------------------------------------
-- 1. LOS RU Data Check (from POSTGIS to Hive)
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_2D_LOS_RU
 where schedule_id = 8463189k
;

select schedule_id, ru_id, rx_tm_xpos, rx_tm_ypos, value
  from RESULT_NR_2D_LOS_RU
 where schedule_id = 8463189
;

select schedule_id, count(*) from RESULT_NR_2D_LOS_RU
group by schedule_id
;
8460062	10744360 -- ChangWon BINtoBIN (from PostGIS)
8460964	69150    -- Legacy Changwon 1RU UMA
8460966	40138    -- Legacy Changwon 1RU UMI
8463189	7712507  -- Legacy Changwon TRAFFIC Radial
8460970	77218266 -- Legacy GangnamGu 1350RU Radial
8460853	7515983  -- Legacy Changwon BINtoBIN
8460855	7529899  -- Legacy Changwon Radial
;

--------------------------------------------------------------------------------------------------------------------------
-- 2. LOS Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
--alter table RESULT_NR_2D_LOS drop partition(schedule_id=8463189);
alter table RESULT_NR_2D_LOS drop partition(schedule_id=8460062);

set hive.exec.dynamic.partition.mode=nonstrict;

with AREA as
(
select a.scenario_id, b.schedule_id,
       a.tm_startx div a.resolution * a.resolution as tm_startx,
       a.tm_starty div a.resolution * a.resolution as tm_starty,
       a.tm_endx div a.resolution * a.resolution as tm_endx,
       a.tm_endy div a.resolution * a.resolution as tm_endy,
       a.resolution
  from SCENARIO a, SCHEDULE b
 where b.schedule_id = 8460062
   and a.scenario_id = b.scenario_id
)
insert into RESULT_NR_2D_LOS partition (schedule_id)
select max(AREA.scenario_id) as scenario_id,
       RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution as rx_tm_xpos,
       RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution as rx_tm_ypos,
       (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution as x_point,
       (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution as y_point,
       case when sum(case when RSLT.value = 1 then 1 else 0 end) > 0 then 1 else 0 end as los,
--       case when sum(case when upper(RSLT.is_bld) = 'T' THEN 1 else 0 end) > 0 then 1 else 0 end as los, -- PLB Check Only
       max(AREA.schedule_id) as schedule_id
  from AREA, RESULT_NR_2D_LOS_RU RSLT
 where RSLT.schedule_id = AREA.schedule_id
-- and RSLT.ru_id = 1012253245
   and AREA.tm_startx <= RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution and RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution < AREA.tm_endx
   and AREA.tm_starty <= RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution and RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution < AREA.tm_endy
  group by RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution, RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution,
           (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution, (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution
;

-- Check Result Data
select * from RESULT_NR_2D_LOS
 where schedule_id = 8460062
;

select los, count(*) from RESULT_NR_2D_LOS
where schedule_id = 8460062
group by los
;
-- 0: 28822 1: 62978 (참고: BIN방식 Legacy)


--------------------------------------------------------------------------------------------------------------------------
-- 3. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_LOS where schedule_id = 8460062;" | sed 's/[[:space:]]\+/,/g' > los_test.csv

--hive -e "select scenario_id, rx_tm_xpos, rx_tm_ypos, rx_x_point, rx_y_point, value, schedule_id from RESULT_NR_2D_LOS_RU_temp_8460855;" | sed 's/[[:space:]]\+/,/g' > los_test.csv;

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
