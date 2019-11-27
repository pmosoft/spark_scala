--------------------------------------------------------------------------------------------------------------------------
-- RSSI
--
-- NOTE. RSRP Pilot으로부터 RSSI 산출
--
--
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- 1. RSRPPilot RU Data Check
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_BF_RSRPPILOT_RU where schedule_id=8460965;

--------------------------------------------------------------------------------------------------------------------------
-- 2. RSSI Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_RSSI_RU drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.auto.convert.join = false;

with NR_PARAMETER as -- 파라미터 정보
(
select a.scenario_id, b.schedule_id, c.ant_category,
       c.number_of_cc, c.number_of_sc_per_rb, c.rb_per_cc, c.bandwidth_per_cc, c.subcarrierspacing,
       d.noisefigure
  from SCENARIO a, SCHEDULE b, NRSYSTEM c, MOBILE_PARAMETER d
 where b.schedule_id = 8460965  
   and a.scenario_id = b.scenario_id
   and a.scenario_id = c.scenario_id
   and a.scenario_id = d.scenario_id
)
insert into RESULT_NR_BF_RSSI_RU partition (schedule_id)
select a.ru_id, a.enb_id, a.cell_id, a.tbd_key, a.rx_tm_xpos, a.rx_tm_ypos, a.rx_floorz, a.rz,
       a.los, a.pathloss, a.antenna_gain, a.pathlossprime, a.rsrppilot,
       if (RSSINoNoise = 0. , -9999, 10. * log10(RSSINoNoise)) as RSSINoNoise,  
       if ((RSSINoNoise + MobileNoiseFloor) = 0. , -9999, 10. * log10((RSSINoNoise + MobileNoiseFloor))) as RSSI,  
       a.schedule_id
  from
    (
    select a.ru_id, a.enb_id, a.cell_id, a.tbd_key, a.rx_tm_xpos, a.rx_tm_ypos, a.rx_floorz, a.rz,
           a.los, a.pathloss, a.antenna_gain, a.pathlossprime, a.rsrppilot,
           power(10 ,
           case when upper(b.ant_category) = 'COMMON' then
                        a.rsrppilot
                else    a.rsrppilot + 10. * log10(b.number_of_cc * b.number_of_sc_per_rb * b.rb_per_cc)
            end / 10.) as RSSINoNoise, -- dRssidBm
           power(10, 
           case when upper(b.ant_category) = 'COMMON' then
                        -174. + b.noisefigure + 10. * log10 ((b.subcarrierspacing / 1000.) * 1000000.)
                else    -174. + b.noisefigure + 10. * log10 ((b.bandwidth_per_cc * b.number_of_cc) * 1000000.)
            end / 10.)  as MobileNoiseFloor, -- m_dNowMilliWatt
           a.schedule_id
      from RESULT_NR_BF_RSRPPILOT_RU a, NR_PARAMETER b
     where a.schedule_id = 8460965
       and a.schedule_id = b.schedule_id
    ) a
;

-- Check Result Data
select * from RESULT_NR_BF_RSSI_RU where schedule_id = 8460965;


--------------------------------------------------------------------------------------------------------------------------
-- 3. RSSI Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_RSSI drop partition(schedule_id=8460965);

set hive.exec.dynamic.partition.mode=nonstrict;


insert into RESULT_NR_BF_RSSI partition (schedule_id)
select tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, 
       if (sum((power(10, (RSSINoNoise)/10.0))) = 0. , -9999, 10. * log10(sum((power(10, (RSSINoNoise)/10.0))))) as RSSI,
       max(schedule_id) as schedule_id
  from RESULT_NR_BF_RSSI_RU
 where schedule_id=8460965
 group by tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz
;

-- Check Result Data
select * from RESULT_NR_BF_RSSI where schedule_id = 8460965;


--------------------------------------------------------------------------------------------------------------------------
-- 4. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_BF_RSSI where schedule_id = 8460965;" | sed 's/[[:space:]]\+/,/g' > rssi_bf_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
