--------------------------------------------------------------------------------------------------------------------------
-- PATHLOSS Prime
--
-- NOTE. Pathloss Prime := Pathloss -
--                         (
--                           Antenna Gain
--                         + AllLoss [MobileGain - MobileFeederLoss - MobileCarLoss - MobileBuildingLoss - MobileBodyLoss]
--                         - RU FadeMargin
--                         - RU FeederLoss
--                         - RU BeamMismatchMargin
--                         )
--                         + RU 5GBeamFormingLoss [LOS여부에 따라 값이 다름]
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- RSRP Pilot
--
-- NOTE. RSRP Pilot := m_dREPerRB - PathlossPrime
--       m_dREPerRB := m_dEIRPPerRB - 10. * log10(NRSYSTEM.number_of_sc_per_rb)
--       m_dEIRPPerRB := m_dTxEIRP - (10. * log10(m_dTotalRB)) - SCENARIO_NR_RU.correction_value
--       m_dTotalRB := NRSYSTEM.number_of_cc * NRSYSTEM.rb_per_cc
--       m_dTxEIRP := m_dTxPowerdBm + m_dPowerCombineGain + m_dAntennaGain
--       m_dTxPowerdBm := NRSECTORPARAMETER.txpwrdbm
--       m_dPowerCombineGain := NRSECTORPARAMETER.powercombininggain
--       m_dAntennaGain := NRSECTORPARAMETER.antennagain
--
-- NRSYSTEM, SCENARIO_NR_RU, NRSECTORPARAMETER 테이블 정보는 scenario_id 별로 데이터가 존재.
--
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- AntennaGain Data Check (실제는 필요없지만 현재 로직이 구현되어 있지 않아서 Legacy에서 데이터를 추출)
--------------------------------------------------------------------------------------------------------------------------
-- 안테나 Gain from Legacy (창원 152 RU Radial UI TEST)
select * from RESULT_NR_2D_PILOT_RU_temp_8460968;

select ru_id,count(*) from RESULT_NR_2D_PILOT_RU_temp_8460968
group by ru_id
;

-- 안테나 Gain from Legacy (창원 152 RU BINtoBIN)
select * from RESULT_NR_2D_PILOT_RU_temp_8460853;
-- 7515983

select ru_id,count(*) from RESULT_NR_2D_PILOT_RU_temp_8460853
group by ru_id
;

-- 안테나 Gain from Legacy (창원 154 RU Radial TRAFFIC)
select * from RESULT_NR_2D_PILOT_RU_temp_8463189;
-- 7712507

select ru_id,count(*) from RESULT_NR_2D_PILOT_RU_temp_8463189
group by ru_id
;

--------------------------------------------------------------------------------------------------------------------------
-- 1. PathlossPrime and RSRPPilot Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSRPPILOT_RU drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;

WITH PARAM AS
(
SELECT b.scenario_id, a.schedule_id, b.ru_id, b.enb_id, b.sector_ord as cell_id,
       nvl(d.mobilegain, 0) - nvl(d.feederloss,0) - nvl(d.carloss,0) - nvl(d.buildingloss,0) - nvl(d.bodyloss,0) as all_loss,
       b.fade_margin as ru_fade_margin, b.feeder_loss as ru_feeder_loss,
       c.beammismatchmargin, c.losbeamformingloss, c.nlosbeamformingloss,
       e.number_of_sc_per_rb,
       (c.txpwrdbm + c.powercombininggain + c.antennagain) - (10. * log10(e.number_of_cc * e.rb_per_cc)) - b.correction_value as EIRPPerRB
  FROM SCHEDULE a, SCENARIO_NR_RU b, NRSECTORPARAMETER c, MOBILE_PARAMETER d, NRSYSTEM e
 WHERE a.schedule_id = 8463189
   AND a.scenario_id = b.scenario_id
   and b.scenario_id = c.scenario_id
   and b.ru_id = c.ru_id
   and a.scenario_id = d.scenario_id
   and a.scenario_id = e.scenario_id
),
ANTGAIN AS
(
select schedule_id, ru_id,
       rx_tm_xpos, rx_tm_ypos,
       0 as antgain,
       null as los,
       null as pathloss,
       null as plprime,
       null as rsrppilot
  from RESULT_NR_2D_PATHLOSS_RU
 where schedule_id = 8463189 
),
PLPRIME_temp AS
(
SELECT PATHLOSS.scenario_id, PATHLOSS.ru_id, PARAM.enb_id, PARAM.cell_id,
       PATHLOSS.rx_tm_xpos, PATHLOSS.rx_tm_ypos, PATHLOSS.los, PATHLOSS.pathloss,
--       0 as antenna_gain,
       PATHLOSS.pathloss -
       (
       0 --@@@ antenna gain
       + PARAM.all_loss
       - PARAM.ru_fade_margin
       - PARAM.ru_feeder_loss
       - PARAM.beammismatchmargin
       )
       + IF (PATHLOSS.los = 1, PARAM.losbeamformingloss, PARAM.nlosbeamformingloss) as pathlossprime,
       PARAM.EIRPPerRB,
       PARAM.number_of_sc_per_rb,
       PATHLOSS.schedule_id
  FROM RESULT_NR_2D_PATHLOSS_RU PATHLOSS, PARAM
 WHERE PATHLOSS.schedule_id = 8463189
   and PATHLOSS.schedule_id = PARAM.schedule_id
   AND PATHLOSS.ru_id = PARAM.ru_id
)
insert into RESULT_NR_2D_RSRPPILOT_RU partition (schedule_id)
select PLPRIME_temp.scenario_id, PLPRIME_temp.ru_id, PLPRIME_temp.enb_id, PLPRIME_temp.cell_id,
       PLPRIME_temp.rx_tm_xpos, PLPRIME_temp.rx_tm_ypos,
       PLPRIME_temp.los, PLPRIME_temp.pathloss,
       ANTGAIN.antgain, 
       PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0) as pathlossprime,
       PLPRIME_temp.EIRPPerRB - 10. * log10(PLPRIME_temp.number_of_sc_per_rb) - (PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0)) as RSRPPilot,
       ANTGAIN.antgain, ANTGAIN.los, ANTGAIN.pathloss, ANTGAIN.plprime, ANTGAIN.rsrppilot,
       PLPRIME_temp.schedule_id
  from PLPRIME_temp left outer join ANTGAIN
    on (PLPRIME_temp.rx_tm_xpos = ANTGAIN.rx_tm_xpos and PLPRIME_temp.rx_tm_ypos = ANTGAIN.rx_tm_ypos and PLPRIME_temp.ru_id = ANTGAIN.ru_id)
;

-- Check Result Data
select * from RESULT_NR_2D_RSRPPILOT_RU
 where schedule_id=8463189
;

---------------------------------------------------------E-N-D---------------------------------------------------------------------------






/*
WITH PARAM AS
(
SELECT b.scenario_id, a.schedule_id, b.ru_id, b.enb_id, b.sector_ord as cell_id,
       nvl(d.mobilegain, 0) - nvl(d.feederloss,0) - nvl(d.carloss,0) - nvl(d.buildingloss,0) - nvl(d.bodyloss,0) as all_loss,
       b.fade_margin as ru_fade_margin, b.feeder_loss as ru_feeder_loss,
       c.beammismatchmargin, c.losbeamformingloss, c.nlosbeamformingloss,
       e.number_of_sc_per_rb,
       (c.txpwrdbm + c.powercombininggain + c.antennagain) - (10. * log10(e.number_of_cc * e.rb_per_cc)) - b.correction_value as EIRPPerRB
  FROM SCHEDULE a, SCENARIO_NR_RU b, NRSECTORPARAMETER c, MOBILE_PARAMETER d, NRSYSTEM e
 WHERE a.schedule_id = 8463189
   AND a.scenario_id = b.scenario_id
   and b.scenario_id = c.scenario_id
   and b.ru_id = c.ru_id
   and a.scenario_id = d.scenario_id
   and a.scenario_id = e.scenario_id
),
ANTGAIN AS
(
select a.scenario_id, a.schedule_id, a.ru_id,
       a.rx_tm_xpos div b.resolution * b.resolution as rx_tm_xpos,
       a.rx_tm_ypos div b.resolution * b.resolution as rx_tm_ypos,
       a.antgain,
       a.los, a.pathloss, a.plprime, a.rsrppilot -- 비교 검증용
  from
    (
    --@@@
    select --5104573 as scenario_id, 8460062 as schedule_id,
           scenario_id, schedule_id,
           ru_id, rx_tm_xpos, rx_tm_ypos,
           antgain, los, pathloss, plprime, rsrppilot
--    from RESULT_NR_2D_PILOT_RU_temp_8460964 -- 1RU
--    from RESULT_NR_2D_PILOT_RU_temp_8460968 --152RU(Radial)
--    from RESULT_NR_2D_PILOT_RU_temp_8460853 --152RU(BINtoBIN)
      from RESULT_NR_2D_PILOT_RU_temp_8463189 --154RU(Radial TRAFFIC)
    ) a left outer join
    (  
    select a.scenario_id, b.schedule_id, a.resolution
      from SCENARIO a, SCHEDULE b
     where b.schedule_id = 8463189
       and a.scenario_id = b.scenario_id
     limit 1
    ) b 
),
PLPRIME_temp AS
(
SELECT PATHLOSS.scenario_id, PATHLOSS.ru_id, PARAM.enb_id, PARAM.cell_id,
       PATHLOSS.rx_tm_xpos, PATHLOSS.rx_tm_ypos, PATHLOSS.los, PATHLOSS.pathloss,
--       0 as antenna_gain,
       PATHLOSS.pathloss -
       (
       0 --@@@ antenna gain
       + PARAM.all_loss
       - PARAM.ru_fade_margin
       - PARAM.ru_feeder_loss
       - PARAM.beammismatchmargin
       )
       + IF (PATHLOSS.los = 1, PARAM.losbeamformingloss, PARAM.nlosbeamformingloss) as pathlossprime,
       PARAM.EIRPPerRB,
       PARAM.number_of_sc_per_rb,
       PATHLOSS.schedule_id
  FROM RESULT_NR_2D_PATHLOSS_RU PATHLOSS, PARAM
 WHERE PATHLOSS.schedule_id = 8463189
   and PATHLOSS.schedule_id = PARAM.schedule_id
   AND PATHLOSS.ru_id = PARAM.ru_id
)
insert into RESULT_NR_2D_RSRPPILOT_RU partition (schedule_id)
select PLPRIME_temp.scenario_id, PLPRIME_temp.ru_id, PLPRIME_temp.enb_id, PLPRIME_temp.cell_id,
       PLPRIME_temp.rx_tm_xpos, PLPRIME_temp.rx_tm_ypos,
       PLPRIME_temp.los, PLPRIME_temp.pathloss,
       ANTGAIN.antgain, 
       PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0) as pathlossprime,
       PLPRIME_temp.EIRPPerRB - 10. * log10(PLPRIME_temp.number_of_sc_per_rb) - (PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0)) as RSRPPilot,
       ANTGAIN.antgain, ANTGAIN.los, ANTGAIN.pathloss, ANTGAIN.plprime, ANTGAIN.rsrppilot,
       PLPRIME_temp.schedule_id
  from PLPRIME_temp left outer join ANTGAIN
--  from PLPRIME_temp join ANTGAIN
    on (PLPRIME_temp.rx_tm_xpos = ANTGAIN.rx_tm_xpos and PLPRIME_temp.rx_tm_ypos = ANTGAIN.rx_tm_ypos and PLPRIME_temp.ru_id = ANTGAIN.ru_id)
;

-- Check Result Data
select * from RESULT_NR_2D_RSRPPILOT_RU
 where schedule_id=8463189
;

select * from RESULT_NR_2D_RSRPPILOT_RU 
 where schedule_id=8463189
   and antenna_gain is not null
;

select a.*, a.pathlossprime - a.temp_plprime as diff_val
 from RESULT_NR_2D_RSRPPILOT_RU a
 where a.schedule_id=8463189;
;

select min(a.pathlossprime - a.temp_plprime), max(a.pathlossprime - a.temp_plprime), avg(a.pathlossprime - a.temp_plprime)
 from RESULT_NR_2D_RSRPPILOT_RU a
;

select a.*, abs(a.pathlossprime - a.temp_plprime) as diff_val
 from RESULT_NR_2D_RSRPPILOT_RU a
-- order by 2 desc
where abs(a.pathlossprime - a.temp_plprime) > 30
;

select los, temp_los, count(*) from RESULT_NR_2D_RSRPPILOT_RU
group by los, temp_los
;
*/





----------------------------------------------------------------------------------------------------------------------------------- OLD
--------------------------------------------------------------------------------------------------------------------------
-- PathlossPrime Analyze for RU Unit
--------------------------------------------------------------------------------------------------------------------------
/*
alter table RESULT_NR_2D_PATHLOSSPRIME_RU drop partition(schedule_id=8460062);

WITH PARAM AS
(
SELECT b.scenario_id, a.schedule_id, b.ru_id,
       nvl(d.mobilegain, 0) - nvl(d.feederloss,0) - nvl(d.carloss,0) - nvl(d.buildingloss,0) - nvl(d.bodyloss,0) as all_loss,
       b.fade_margin as ru_fade_margin, b.feeder_loss as ru_feeder_loss,
       c.beammismatchmargin, c.losbeamformingloss, c.nlosbeamformingloss
  FROM SCHEDULE a, SCENARIO_NR_RU b, NRSECTORPARAMETER c, MOBILE_PARAMETER d
 WHERE a.schedule_id = 8460062
   AND a.scenario_id = b.scenario_id
   and b.scenario_id = c.scenario_id
   and b.ru_id = c.ru_id
   and a.scenario_id = d.scenario_id
),
ANTGAIN AS
(
select a.scenario_id, a.schedule_id, a.ru_id,
       a.rx_tm_xpos div b.resolution * b.resolution as rx_tm_xpos,
       a.rx_tm_ypos div b.resolution * b.resolution as rx_tm_ypos,
       a.antgain,
       a.los, a.pathloss, a.plprime, a.rsrppilot -- 비교 검증용
  from
	(
	--@@@
	select 5104573 as scenario_id, 8460062 as schedule_id,
	       --scenario_id, schedule_id,
	       ru_id, rx_tm_xpos, rx_tm_ypos,
	       antgain, los, pathloss, plprime, rsrppilot
--	  from RESULT_NR_2D_PILOT_RU_temp_8460964 -- 1RU
--	  from RESULT_NR_2D_PILOT_RU_temp_460855	 -- 152RU(Radial) @@@
	  from RESULT_NR_2D_PILOT_RU_temp_8460968 --152RU(Radial)
	) a left outer join
	(  
	select a.scenario_id, b.schedule_id, a.resolution
	  from SCENARIO a, SCHEDULE b
	 where b.schedule_id = 8460062
	   and a.scenario_id = b.scenario_id
	 limit 1
	) b 
),
PLPRIME_temp AS
(
SELECT PATHLOSS.scenario_id, PATHLOSS.ru_id,
       PATHLOSS.rx_tm_xpos, PATHLOSS.rx_tm_ypos, PATHLOSS.los, PATHLOSS.pathloss,
--       0 as antenna_gain,
       PATHLOSS.pathloss -
       (
       0 --@@@ antenna gain
       + PARAM.all_loss
       - PARAM.ru_fade_margin
       - PARAM.ru_feeder_loss
       - PARAM.beammismatchmargin
       )
       + IF (PATHLOSS.los = 1, PARAM.losbeamformingloss, PARAM.nlosbeamformingloss) as pathlossprime,
       PATHLOSS.schedule_id
  FROM RESULT_NR_2D_PATHLOSS_RU PATHLOSS, PARAM
 WHERE PATHLOSS.schedule_id = 8460062
   and PATHLOSS.schedule_id = PARAM.schedule_id
   AND PATHLOSS.ru_id = PARAM.ru_id
)
insert into RESULT_NR_2D_PATHLOSSPRIME_RU partition (schedule_id)
select PLPRIME_temp.scenario_id, PLPRIME_temp.ru_id,
       PLPRIME_temp.rx_tm_xpos, PLPRIME_temp.rx_tm_ypos,
       PLPRIME_temp.los, PLPRIME_temp.pathloss,
       ANTGAIN.antgain, 
       PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0) as pathlossprime,
--		case when ANTGAIN.antgain is not null then ANTGAIN.plprime else PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0) end as pathlossprime,
       ANTGAIN.antgain, ANTGAIN.los, ANTGAIN.pathloss, ANTGAIN.plprime, ANTGAIN.rsrppilot,
       PLPRIME_temp.schedule_id
  from PLPRIME_temp left outer join ANTGAIN
    on (PLPRIME_temp.rx_tm_xpos = ANTGAIN.rx_tm_xpos and PLPRIME_temp.rx_tm_ypos = ANTGAIN.rx_tm_ypos and PLPRIME_temp.ru_id = ANTGAIN.ru_id)
;
*/

/*
drop table RESULT_NR_2D_PATHLOSSPRIME_RU;
CREATE TABLE RESULT_NR_2D_PATHLOSSPRIME_RU
(
	SCENARIO_ID int,
	RU_ID string,
	RX_TM_XPOS int,
	RX_TM_YPOS int,
	LOS int,
	PATHLOSS float,
	ANTENNA_GAIN float,
	PATHLOSSPRIME float,
	temp_antgain float,
	temp_los int,
	temp_pathloss float,
	temp_plprime float,
	temp_rsrplilot float
)
partitioned BY (SCHEDULE_ID int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS PARQUET
;

select * from RESULT_NR_2D_PILOT_RU_temp_8460964
;

select * from RESULT_NR_2D_PATHLOSS_RU
;
-- 

select * from RESULT_NR_2D_PATHLOSSPRIME_RU a
;

select a.*, a.pathlossprime - a.temp_plprime as diff_val
 from RESULT_NR_2D_PATHLOSSPRIME_RU a
;
-- 

select min(a.pathlossprime - a.temp_plprime), max(a.pathlossprime - a.temp_plprime), avg(a.pathlossprime - a.temp_plprime)
 from RESULT_NR_2D_PATHLOSSPRIME_RU a
;
-- -40.17 37.89

select a.*, a.pathlossprime - a.temp_plprime as diff_val
 from RESULT_NR_2D_PATHLOSSPRIME_RU a
where a.pathlossprime - a.temp_plprime < -30
;

select los, temp_los, count(*) from RESULT_NR_2D_PATHLOSSPRIME_RU
group by los, temp_los
;
*/

/*
--------------------------------------------------------------------------------------------------------------------------
-- RSRPPliot Analyze for RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSRPPILOT_RU drop partition(schedule_id=8460062);

--set hive.exec.dynamic.partition.mode=nonstrict;

WITH PARAM AS
(
SELECT b.scenario_id, a.schedule_id, b.ru_id, b.enb_id, b.sector_ord as cell_id,
--       b.correction_value,
       d.number_of_sc_per_rb,
--       d.number_of_cc, d.rb_per_cc,
--       c.powercombininggain, c.txpwrdbm, c.antennagain,
--       c.txpwrdbm + c.powercombininggain + c.antennagain as TxEIRP,
       (c.txpwrdbm + c.powercombininggain + c.antennagain) - (10. * log10(d.number_of_cc * d.rb_per_cc)) - b.correction_value as EIRPPerRB
  FROM SCHEDULE a, SCENARIO_NR_RU b, NRSECTORPARAMETER c, NRSYSTEM d
 WHERE a.schedule_id = 8460062
   AND a.scenario_id = b.scenario_id
   and b.scenario_id = c.scenario_id
   and b.ru_id = c.ru_id
   and a.scenario_id = d.scenario_id
)
insert into RESULT_NR_2D_RSRPPILOT_RU partition (schedule_id)
select PLPRIME.scenario_id, PLPRIME.ru_id, PARAM.enb_id, PARAM.cell_id,
       PLPRIME.rx_tm_xpos, PLPRIME.rx_tm_ypos, PLPRIME.los, PLPRIME.pathloss, PLPRIME.antenna_gain, PLPRIME.pathlossprime,
       PARAM.EIRPPerRB - 10. * log10(PARAM.number_of_sc_per_rb) - PLPRIME.pathlossprime as RSRPPilot,
       PLPRIME.schedule_id
  FROM RESULT_NR_2D_PATHLOSSPRIME_RU as PLPRIME, PARAM
 WHERE PLPRIME.schedule_id = 8460062
   and PLPRIME.schedule_id = PARAM.schedule_id
   AND PLPRIME.ru_id = PARAM.ru_id  
;

select * from RESULT_NR_2D_RSRPPILOT_RU
;
*/

