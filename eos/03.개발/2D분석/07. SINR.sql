--------------------------------------------------------------------------------------------------------------------------
-- SINR
--
-- NOTE. 시나리오 RSSI, RU RSSI, RU LOS, RU RSRP로 부터 SINR 산출
--
--
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- 1. LOS ~ RSSI RU Data Check
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_2D_RSSI_RU where schedule_id = 8463189;

select * from SCENARIO_NR_RU;

select *, power(10, a.RSSINoNoise / 10.),
       10. * log10(power(10, a.RSSINoNoise / 10.))
  from RESULT_NR_2D_RSSI_RU a limit 10;

--------------------------------------------------------------------------------------------------------------------------
-- 2. SINR Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_SINR_RU drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;

with NR_PARAMETER as -- 파라미터 정보
(
select a.scenario_id, b.schedule_id, c.ant_category,
       c.number_of_cc, c.number_of_sc_per_rb, c.rb_per_cc, c.bandwidth_per_cc, c.subcarrierspacing,
       c.dlintercell, c.dlintracell, c.dlcovlimitrsrp_yn, c.dlcoveragelimitrsrplos, c.dlcoveragelimitrsrp,
       c.diversitygainratio,
       d.noisefigure
  from SCENARIO a, SCHEDULE b, NRSYSTEM c, MOBILE_PARAMETER d
 where b.schedule_id = 8463189  
   and a.scenario_id = b.scenario_id
   and a.scenario_id = c.scenario_id
   and a.scenario_id = d.scenario_id
),
RU as -- RU_SEQ 정보
(
select a.scenario_id, b.schedule_id, a.ru_id, a.ru_seq
  from SCENARIO_NR_RU a, SCHEDULE b
 where b.schedule_id = 8463189
   and a.scenario_id = b.scenario_id
),
ruRSSI as -- RU별 RSSI 정보
(
select a.scenario_id, a.schedule_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos, a.los, a.pathloss,
       a.antenna_gain, a.pathlossprime, a.rsrppilot, a.rssinonoise, a.rssi, c.ru_seq,
       b.ant_category, b.dlcovlimitrsrp_yn,
   	   power(10, 
       case when upper(b.ant_category) = 'COMMON' then
                    -174. + b.noisefigure + 10. * log10 ((b.subcarrierspacing / 1000.) * 1000000.)
            else    -174. + b.noisefigure + 10. * log10 ((b.bandwidth_per_cc * b.number_of_cc) * 1000000.)
        end / 10.) as m_dNowMilliWatt
  from RESULT_NR_2D_RSSI_RU a, NR_PARAMETER b, RU c
 where a.schedule_id = 8463189
   and a.schedule_id = b.schedule_id
   and a.schedule_id = c.schedule_id
   and a.ru_id = c.ru_id
),
ruSumRSSI as -- ENB_ID, CELL_ID, BIN 별 RSSI sum
(
SELECT a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos,
       sum(power(10, a.RSSINoNoise / 10.) * b.diversitygainratio) as RSSINoNoiseSUMMilliWatt
  from RESULT_NR_2D_RSSI_RU a, NR_PARAMETER b
 where a.schedule_id = 8463189
   and a.schedule_id = b.schedule_id
 group by a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos
),
ScenRSSI as -- 시나리오 RSSI 정보
(
select scenario_id, schedule_id, rx_tm_xpos, rx_tm_ypos, x_point, y_point, rssi
  from RESULT_NR_2D_RSSI
 where schedule_id = 8463189
),
ruRSSIResult as -- ruRSSI + ruSumRSSI 결과(RU별 결과)
(
select a.scenario_id, a.schedule_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos, a.los, a.pathloss,
       a.antenna_gain, a.pathlossprime, a.rsrppilot, a.rssinonoise, a.rssi, a.ru_seq,
       a.ant_category, a.dlcovlimitrsrp_yn,
       a.m_dNowMilliWatt,
       b.RSSINoNoiseSUMMilliWatt
  from ruRSSI a left outer join ruSumRSSI b
   on (a.enb_id = b.enb_id and a.cell_id = b.cell_id and a.rx_tm_xpos = b.rx_tm_xpos and a.rx_tm_ypos = b.rx_tm_ypos)
),
SINRtemp as
(
SELECT a.scenario_id, a.schedule_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos, a.los, a.pathloss,
       a.antenna_gain, a.pathlossprime, a.rsrppilot, a.rssinonoise, a.rssi, a.ru_seq,
       a.ant_category, a.dlcovlimitrsrp_yn,
       a.m_dNowMilliWatt,
       a.RSSINoNoiseSUMMilliWatt,
       a.rssi -
       if
       (
       ((b.dlintercell * (power(10, c.rssi / 10.) - a.m_dNowMilliWatt) - (b.dlintercell - b.dlintracell) * a.RSSINoNoiseSUMMilliWatt) + a.m_dNowMilliWatt) == 0,
       -9999,
       10. * log10(((b.dlintercell * (power(10, c.rssi / 10.) - a.m_dNowMilliWatt) - (b.dlintercell - b.dlintracell) * a.RSSINoNoiseSUMMilliWatt) + a.m_dNowMilliWatt))
       ) as fSINRdB,
       b.dlcoveragelimitrsrplos, b.dlcoveragelimitrsrp,
       c.rssi as scenrssi
  from ruRSSIResult a, NR_PARAMETER b, ScenRSSI c
 where a.schedule_id = 8463189
   and a.schedule_id = b.schedule_id
   and a.schedule_id = c.schedule_id
   and a.rx_tm_xpos = c.rx_tm_xpos
   and a.rx_tm_ypos = c.rx_tm_ypos
)
insert into RESULT_NR_2D_SINR_RU partition (schedule_id)
select a.scenario_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos, a.los, a.pathloss,
       a.antenna_gain, a.pathlossprime, a.rsrppilot, a.rssinonoise, a.rssi, a.ru_seq,
       a.scenrssi, b.rsrp,
       round(if (a.fSINRdB > 35., 35., a.fSINRdB),2) as SINR,
--       round(case when a.dlcovlimitrsrp_yn = 1 THEN
  --               case when b.rsrp <= if (a.los = 1, a.dlcoveragelimitrsrplos, a.dlcoveragelimitrsrp) then 340282346638528859811704183484516925440.
    --                  else if (a.fSINRdB > 35., 35., a.fSINRdB)
      --            end
        --    else if (a.fSINRdB > 35., 35., a.fSINRdB)
       -- end,2) as SINR,
--       a.ant_category, a.dlcovlimitrsrp_yn,
--       a.m_dNowMilliWatt,
--       a.RSSINoNoiseSUMMilliWatt
       a.schedule_id
  from SINRtemp a, RESULT_NR_2D_RSRP_RU b
 where a.schedule_id = b.schedule_id
   and a.ru_id = b.ru_id
   and a.rx_tm_xpos = b.rx_tm_xpos
   and a.rx_tm_ypos = b.rx_tm_ypos
;

-- Check Result Data
select * from RESULT_NR_2D_SINR_RU where schedule_id = 8463189;

--------------------------------------------------------------------------------------------------------------------------
-- 3. SINR Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_SINR drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.execution.engine=tez;

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
SINRtemp as
(
SELECT a.scenario_id, a.schedule_id, a.rx_tm_xpos, a.rx_tm_ypos,
       case when b.ru_seq is null then null else a.sinr end as sinr
  from (select * from RESULT_NR_2D_SINR_RU where schedule_id = 8463189) a
       left outer join 
       (select * from RESULT_NR_2D_BESTSERVER where schedule_id = 8463189) b
    on (a.schedule_id = b.schedule_id and a.rx_tm_xpos = b.rx_tm_xpos and a.rx_tm_ypos = b.rx_tm_ypos and a.ru_seq = b.ru_seq)
)
insert into RESULT_NR_2D_SINR partition (schedule_id)
select max(AREA.scenario_id) as scenario_id,
       RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution as rx_tm_xpos,
       RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution as rx_tm_ypos,
       (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution as x_point,
       (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution as y_point,
       max(sinr) as SINR,
       max(AREA.schedule_id) as schedule_id
  from AREA, SINRtemp RSLT
 where RSLT.schedule_id = AREA.schedule_id
   and AREA.tm_startx <= RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution and RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution < AREA.tm_endx
   and AREA.tm_starty <= RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution and RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution < AREA.tm_endy
  group by RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution, RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution,
           (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution, (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution
;

-- Check Result Data
select * from RESULT_NR_2D_SINR where schedule_id = 8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 4. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_SINR where schedule_id = 8463189;" | sed 's/[[:space:]]\+/,/g' > sinr_test.csv
