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
select * from RESULT_NR_BF_RSRPPILOT_RU where schedule_id=8460965;

--------------------------------------------------------------------------------------------------------------------------
-- 2. BestServer Analyze by Scenario Area
--    NOTE. It does not need to Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_BESTSERVER drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;

with RSLT as
(
SELECT a.schedule_id, a.ru_id, a.tbd_key, a.rx_tm_xpos, a.rx_tm_ypos, a.rx_floorz, b.ru_seq
  FROM
    (
    select schedule_id, ru_id, tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz
      from
        (
        select schedule_id, ru_id, tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz,
               rsrppilot,
               row_number() over(partition by rx_tm_xpos, rx_tm_ypos, rx_floorz order by rsrppilot desc) rsrppilot_ord
          from RESULT_NR_BF_RSRPPILOT_RU
         where schedule_id = 8460965
        ) a
     where a.rsrppilot_ord = 1
    ) a,
    (
    SELECT a.schedule_id, b.ru_id, b.ru_seq
      from SCHEDULE a, SCENARIO_NR_RU b
     WHERE a.schedule_id = 8460965
       AND a.scenario_id = b.scenario_id
    ) b
where a.schedule_id = b.schedule_id
  and a.ru_id = b.ru_id
)
insert into RESULT_NR_BF_BESTSERVER partition (schedule_id)
select tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, 
       max(RSLT.ru_seq) as ru_seq,
       max(schedule_id) as schedule_id
  from RSLT
 group by tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz
;

-- Check Result Data
select * from RESULT_NR_BF_BESTSERVER where schedule_id = 8460965;

--------------------------------------------------------------------------------------------------------------------------
-- 3. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_BF_BESTSERVER where schedule_id = 8463189;" | sed 's/[[:space:]]\+/,/g' > bestserver_bf_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------

