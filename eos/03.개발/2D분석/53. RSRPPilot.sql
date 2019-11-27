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

-------------------------------------------------------------------------------------------------------------------------
-- 1. PathlossPrime and RSRPPilot Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_BF_RSRPPILOT_RU drop partition(schedule_id=8460965);

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
 WHERE a.schedule_id = 8460965
   AND a.scenario_id = b.scenario_id
   and b.scenario_id = c.scenario_id
   and b.ru_id = c.ru_id
   and a.scenario_id = d.scenario_id
   and a.scenario_id = e.scenario_id
),
ANTGAIN AS
(
select schedule_id,
       ru_id, tbd_key, rx_tm_xpos, rx_tm_ypos, rx_floorz, rz,
       0 as antgain
  from RESULT_NR_BF_PATHLOSS_RU
 where schedule_id = 8460965
),
PLPRIME_temp AS
(
SELECT PATHLOSS.ru_id, PARAM.enb_id, PARAM.cell_id, PATHLOSS.tbd_key,
       PATHLOSS.rx_tm_xpos, PATHLOSS.rx_tm_ypos, PATHLOSS.rx_floorz, PATHLOSS.rz,
       PATHLOSS.los, PATHLOSS.pathloss,
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
  FROM RESULT_NR_BF_PATHLOSS_RU PATHLOSS, PARAM
 WHERE PATHLOSS.schedule_id = 8460965
   and PATHLOSS.schedule_id = PARAM.schedule_id
   AND PATHLOSS.ru_id = PARAM.ru_id
)
insert into RESULT_NR_BF_RSRPPILOT_RU partition (schedule_id)
select PLPRIME_temp.ru_id, PLPRIME_temp.enb_id, PLPRIME_temp.cell_id, PLPRIME_temp.tbd_key,
       PLPRIME_temp.rx_tm_xpos, PLPRIME_temp.rx_tm_ypos, PLPRIME_temp.rx_floorz, PLPRIME_temp.rz,
       PLPRIME_temp.los, PLPRIME_temp.pathloss,
       ANTGAIN.antgain, 
       PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0) as pathlossprime,
       PLPRIME_temp.EIRPPerRB - 10. * log10(PLPRIME_temp.number_of_sc_per_rb) - (PLPRIME_temp.pathloss - nvl(ANTGAIN.antgain,0)) as RSRPPilot,
       PLPRIME_temp.schedule_id
  from PLPRIME_temp left outer join ANTGAIN
    on (PLPRIME_temp.rx_tm_xpos = ANTGAIN.rx_tm_xpos and PLPRIME_temp.rx_tm_ypos = ANTGAIN.rx_tm_ypos and PLPRIME_temp.rx_floorz = ANTGAIN.rx_floorz and PLPRIME_temp.ru_id = ANTGAIN.ru_id and PLPRIME_temp.tbd_key = ANTGAIN.tbd_key)
;

-- Check Result Data
select * from RESULT_NR_BF_PATHLOSS_RU
 where schedule_id=8460965
;

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
