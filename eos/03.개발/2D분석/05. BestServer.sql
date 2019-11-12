--------------------------------------------------------------------------------------------------------------------------
-- BestServer
--
-- NOTE. RSRP Pilot으로부터 BestServer 산출(시나리오 결과만 있음)
--
--
--------------------------------------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------------------------------------
-- 1. RSRPPilot RU Data Check
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_2D_RSRPPILOT_RU where schedule_id=8463189;

--------------------------------------------------------------------------------------------------------------------------
-- 2. BestServer Analyze by Scenario Area
--    NOTE. It does not need to Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_BESTSERVER drop partition(schedule_id=8463189);

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
),
RSLT as
(
SELECT a.scenario_id, a.schedule_id, a.rx_tm_xpos, a.rx_tm_ypos, b.ru_seq
  FROM
	(
	select scenario_id, schedule_id, rx_tm_xpos, rx_tm_ypos, ru_id
	  from
		(
		select scenario_id, schedule_id, rx_tm_xpos, rx_tm_ypos,
		       ru_id, rsrppilot,
		       row_number() over(partition by rx_tm_xpos, rx_tm_ypos order by rsrppilot desc) rsrppilot_ord
		  from RESULT_NR_2D_RSRPPILOT_RU
		 where schedule_id = 8463189
		) a
	 where a.rsrppilot_ord = 1
	) a,
	(
	SELECT a.schedule_id, b.ru_id, b.ru_seq
	  from SCHEDULE a, SCENARIO_NR_RU b
	 WHERE a.schedule_id = 8463189
	   AND a.scenario_id = b.scenario_id
	) b
where a.schedule_id = b.schedule_id
  and a.ru_id = b.ru_id
)
insert into RESULT_NR_2D_BESTSERVER partition (schedule_id)
select max(AREA.scenario_id) as scenario_id,
       RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution as rx_tm_xpos,
       RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution as rx_tm_ypos,
       (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution as x_point,
       (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution as y_point,       
       max(RSLT.ru_seq) as ru_seq,
       max(AREA.schedule_id) as schedule_id
  from AREA, RSLT
 where RSLT.schedule_id = AREA.schedule_id
   and AREA.tm_startx <= RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution and RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution < AREA.tm_endx
   and AREA.tm_starty <= RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution and RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution < AREA.tm_endy
  group by RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution, RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution,
           (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution, (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution
;

-- Check Result Data
select * from RESULT_NR_2D_BESTSERVER
 where schedule_id = 8463189
;

--------------------------------------------------------------------------------------------------------------------------
-- 3. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_BESTSERVER where schedule_id = 8463189;" | sed 's/[[:space:]]\+/,/g' > bestserver_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------

