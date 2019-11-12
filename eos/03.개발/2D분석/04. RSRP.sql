--------------------------------------------------------------------------------------------------------------------------
-- RSRP
--
-- NOTE. ENB_ID, CELL_ID가 동일한 RU들은 BIN별로 RSRPPilot을 합산한값이 최종 RSRP값.
--
--
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- 1. RSRPPilot RU Data Check
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_2D_RSRPPILOT_RU where schedule_id=8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 2. RSRP Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSRP_RU drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;

WITH OVERLAB AS
(
select enb_id, cell_id, rx_tm_xpos, rx_tm_ypos,
       case when sum(power(10., rsrppilot / 10.)) = 0. then -9999
            else 10. * log10 (sum(power(10., rsrppilot / 10.)))
        end as rsrppilot
  from RESULT_NR_2D_RSRPPILOT_RU
 where schedule_id = 8463189
 group by enb_id, cell_id, rx_tm_xpos, rx_tm_ypos
 having count(*) > 1
)
insert into RESULT_NR_2D_RSRP_RU partition (schedule_id)
select a.scenario_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos,
       a.los, a.pathloss, a.antenna_gain, a.pathlossprime, a.rsrppilot,
       case when b.rsrppilot is not null then b.rsrppilot else a.rsrppilot end rsrp,
       a.schedule_id
  from (select * from RESULT_NR_2D_RSRPPILOT_RU where schedule_id = 8463189) a left outer join OVERLAB b
    on (a.enb_id = b.enb_id and a.cell_id = b.cell_id and a.rx_tm_xpos = b.rx_tm_xpos and a.rx_tm_ypos = b.rx_tm_ypos)
;

-- Check Result Data
select * from RESULT_NR_2D_RSRP_RU where schedule_id = 8463189;

select ru_id,count(*) from RESULT_NR_2D_RSRP_RU
 where schedule_id = 8463189
 group by ru_id
; 

--------------------------------------------------------------------------------------------------------------------------
-- 3. RSRP Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSRP drop partition(schedule_id=8463189);

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
 where b.schedule_id = 8463189
   and a.scenario_id = b.scenario_id
)
insert into RESULT_NR_2D_RSRP partition (schedule_id)
select max(AREA.scenario_id) as scenario_id,
       RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution as rx_tm_xpos,
       RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution as rx_tm_ypos,
       (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution as x_point,
       (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution as y_point,       
       max(rsrp) as rsrp,
       max(AREA.schedule_id) as schedule_id
  from AREA, RESULT_NR_2D_RSRP_RU RSLT
 where RSLT.schedule_id = AREA.schedule_id
   and AREA.tm_startx <= RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution and RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution < AREA.tm_endx
   and AREA.tm_starty <= RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution and RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution < AREA.tm_endy
  group by RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution, RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution,
           (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution, (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution
;

-- Check Result Data
select * from RESULT_NR_2D_RSRP where schedule_id = 8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 4. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_RSRP where schedule_id = 8463189;;" | sed 's/[[:space:]]\+/,/g' > rsrp_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
