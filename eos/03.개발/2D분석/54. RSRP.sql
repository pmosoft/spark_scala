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
select * from RESULT_NR_BF_RSRPPILOT_RU where schedule_id=8460965;


--------------------------------------------------------------------------------------------------------------------------
-- 2. RSRP Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_RSRP_RU drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;

WITH OVERLAB AS
(
select enb_id, cell_id, tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, rz,
       case when sum(power(10., rsrppilot / 10.)) = 0. then -9999
            else 10. * log10 (sum(power(10., rsrppilot / 10.)))
        end as rsrppilot
  from RESULT_NR_BF_RSRPPILOT_RU
 where schedule_id = 8460965
 group by enb_id, cell_id, tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, rz
 having count(*) > 1
)
insert into RESULT_NR_BF_RSRP_RU partition (schedule_id)
select a.ru_id, a.enb_id, a.cell_id, a.tbd_key, a.rx_tm_xpos, a.rx_tm_ypos, a.rx_floorz, a.rz,
       a.los, a.pathloss, a.antenna_gain, a.pathlossprime, a.rsrppilot,
       case when b.rsrppilot is not null then b.rsrppilot else a.rsrppilot end rsrp,
       a.schedule_id
  from (select * from RESULT_NR_BF_RSRPPILOT_RU where schedule_id = 8460965) a left outer join OVERLAB b
    on (a.enb_id = b.enb_id and a.cell_id = b.cell_id and a.tbd_key = b.tbd_key and a.rx_tm_xpos = b.rx_tm_xpos and a.rx_tm_ypos = b.rx_tm_ypos and a.rx_floorz = b.rx_floorz and a.rz = b.rz)
;

-- Check Result Data
select * from RESULT_NR_BF_RSRP_RU where schedule_id = 8460965;

--------------------------------------------------------------------------------------------------------------------------
-- 3. RSRP Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_RSRP drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;

insert into RESULT_NR_BF_RSRP partition (schedule_id)
select tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, 
       max(rsrp) as rsrp,
       max(schedule_id) as schedule_id
  from RESULT_NR_BF_RSRP_RU
 where schedule_id=8460965
 group by tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz
;

-- Check Result Data
select * from RESULT_NR_BF_RSRP where schedule_id = 8460965;


--------------------------------------------------------------------------------------------------------------------------
-- 4. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_BF_RSRP where schedule_id = 8460965;;" | sed 's/[[:space:]]\+/,/g' > rsrp_bf_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
