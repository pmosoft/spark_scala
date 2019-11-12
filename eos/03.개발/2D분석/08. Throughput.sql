--------------------------------------------------------------------------------------------------------------------------
-- Throughput
--
-- NOTE. SINR로부터 Throughput 산출(시나리오 결과만 있음)
--
--
--------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------
-- 1. SINR Data Check
--------------------------------------------------------------------------------------------------------------------------
select * from RESULT_NR_2D_SINR where schedule_id = 8463189;


select * from NRUETRAFFIC
 where scenario_id = 5112167
;


--------------------------------------------------------------------------------------------------------------------------
-- 2. Throughput Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_THROUGHPUT drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.execution.engine=tez;

with NR_PARAMETER as -- 파라미터 정보
(
select a.scenario_id, b.schedule_id, 
       c.number_of_cc as nCC, c.dldataratio as dDLRatio, c.rb_per_cc as dMaxRBs, c.dloh as dDLOH,
       d.downlinkfreq,
       case when d.downlinkfreq < 10000. then
                  0.001 / (14. * 2.)
            else  0.001 / (14. * 8.)
       end as dAvr_Symbol_Duration,
       c.dlsinroffset as dSINROffset,
       e.rx_layer,
       case when d.downlinkfreq < 10000. and e.rx_layer = 2 then 256
            when d.downlinkfreq < 10000. and e.rx_layer = 4 then 256
            when d.downlinkfreq >= 10000. then 256
            else -1
       end as ModulationTableType,
       case when d.downlinkfreq < 10000. and e.rx_layer = 2 then 1
            when d.downlinkfreq < 10000. and e.rx_layer = 4 then 1
            when d.downlinkfreq >= 10000. then 1
            else -1
       end as DLULType,
       case when d.downlinkfreq < 10000. and e.rx_layer = 2 then 2
            when d.downlinkfreq < 10000. and e.rx_layer = 4 then 4
            when d.downlinkfreq >= 10000. then 2
            else -1
       end as UELayer         
  from SCENARIO a, SCHEDULE b, NRSYSTEM c, FABASE d, MOBILE_PARAMETER e
 where b.schedule_id = 8463189  
   and a.scenario_id = b.scenario_id
   and a.scenario_id = c.scenario_id
   and a.system_id = d.systemtype
   and a.fa_seq = d.fa_seq
   and a.scenario_id = e.scenario_id
),
NR_DLTRAFFIC as --@@@
(
select a.scenario_id, a.schedule_id,
       a.nCC, a.dDLRatio, a.dMaxRBs, a.dDLOH,
       a.downlinkfreq, a.dAvr_Symbol_Duration,
       a.dSINROffset, a.rx_layer,
       b.modulation1, b.modulation2, b.modulation3, b.modulation4, b.modulation5, 
       b.modulation6, b.modulation7, b.modulation8, b.modulation9, b.modulation10,
       b.modulation11, b.modulation12, b.modulation13, b.modulation14, b.modulation15,
       b.layer1, b.layer2, b.layer3, b.layer4, b.layer5,
       b.layer6, b.layer7, b.layer8, b.layer9, b.layer10,
       b.layer11, b.layer12, b.layer13, b.layer14, b.layer15,
       b.coderate1, b.coderate2, b.coderate3, b.coderate4, b.coderate5,
       b.coderate6, b.coderate7, b.coderate8, b.coderate9, b.coderate10,
       b.coderate11, b.coderate12, b.coderate13, b.coderate14, b.coderate15,
       b.snr1, b.snr2, b.snr3, b.snr4, b.snr5,
       b.snr6, b.snr7, b.snr8, b.snr9, b.snr10,
       b.snr11, b.snr12, b.snr13, b.snr14, b.snr15
  from NR_PARAMETER a, NRUETRAFFIC b
 where a.scenario_id = b.scenario_id
   and b.rx_modulation = a.ModulationTableType
   and b.dlul_type = a.DLULType
   and b.rx_layer = a.UELayer
),
THROUGHPUTtemp as
(
select a.scenario_id, a.schedule_id, a.rx_tm_xpos, a.rx_tm_ypos, a.x_point, a.y_point,
       a.sinr as dSNR,
       b.dSINROffset,
       b.modulation1, b.modulation2, b.modulation3, b.modulation4, b.modulation5, 
       b.modulation6, b.modulation7, b.modulation8, b.modulation9, b.modulation10,
       b.modulation11, b.modulation12, b.modulation13, b.modulation14, b.modulation15,
       b.layer1, b.layer2, b.layer3, b.layer4, b.layer5,
       b.layer6, b.layer7, b.layer8, b.layer9, b.layer10,
       b.layer11, b.layer12, b.layer13, b.layer14, b.layer15,
       b.coderate1, b.coderate2, b.coderate3, b.coderate4, b.coderate5,
       b.coderate6, b.coderate7, b.coderate8, b.coderate9, b.coderate10,
       b.coderate11, b.coderate12, b.coderate13, b.coderate14, b.coderate15,
       b.snr1, b.snr2, b.snr3, b.snr4, b.snr5,
       b.snr6, b.snr7, b.snr8, b.snr9, b.snr10,
       b.snr11, b.snr12, b.snr13, b.snr14, b.snr15,
       b.nCC * b.layer15 * b.modulation15 * b.dDLRatio * b.coderate15 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp15,
       b.nCC * b.layer14 * b.modulation14 * b.dDLRatio * b.coderate14 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp14,
       b.nCC * b.layer13 * b.modulation13 * b.dDLRatio * b.coderate13 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp13,
       b.nCC * b.layer12 * b.modulation12 * b.dDLRatio * b.coderate12 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp12,
       b.nCC * b.layer11 * b.modulation11 * b.dDLRatio * b.coderate11 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp11,
       b.nCC * b.layer10 * b.modulation10 * b.dDLRatio * b.coderate10 * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp10,
       b.nCC * b.layer9  * b.modulation9  * b.dDLRatio * b.coderate9  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp9,
       b.nCC * b.layer8  * b.modulation8  * b.dDLRatio * b.coderate8  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp8,
       b.nCC * b.layer7  * b.modulation7  * b.dDLRatio * b.coderate7  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp7,
       b.nCC * b.layer6  * b.modulation6  * b.dDLRatio * b.coderate6  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp6,
       b.nCC * b.layer5  * b.modulation5  * b.dDLRatio * b.coderate5  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp5,
       b.nCC * b.layer4  * b.modulation4  * b.dDLRatio * b.coderate4  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp4,
       b.nCC * b.layer3  * b.modulation3  * b.dDLRatio * b.coderate3  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp3,
       b.nCC * b.layer1  * b.modulation2  * b.dDLRatio * b.coderate2  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp2,
       b.nCC * b.layer1  * b.modulation1  * b.dDLRatio * b.coderate1  * b.dMaxRBs * (12.0 / b.dAvr_Symbol_Duration) * (1.0 - b.dDLOH) * 0.000001 as thp1,
		case when b.snr14 + b.dSINROffset < 0. or b.snr15 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr14 + b.dSINROffset + (abs(if(b.snr14 + b.dSINROffset < a.sinr, b.snr14 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr14 + b.dSINROffset
		 end as dInfValue14,
		case when b.snr14 + b.dSINROffset < 0. or b.snr15 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr15 + b.dSINROffset + (abs(if(b.snr14 + b.dSINROffset < a.sinr, b.snr14 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr15 + b.dSINROffset
		 end as dSupValue14,
		case when b.snr14 + b.dSINROffset < 0. or b.snr15 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr14 + b.dSINROffset < a.sinr, b.snr14 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue14,
		case when b.snr13 + b.dSINROffset < 0. or b.snr14 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr13 + b.dSINROffset + (abs(if(b.snr13 + b.dSINROffset < a.sinr, b.snr13 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr13 + b.dSINROffset
		 end as dInfValue13,
		case when b.snr13 + b.dSINROffset < 0. or b.snr14 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr14 + b.dSINROffset + (abs(if(b.snr13 + b.dSINROffset < a.sinr, b.snr13 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr14 + b.dSINROffset
		 end as dSupValue13,
		case when b.snr13 + b.dSINROffset < 0. or b.snr14 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr13 + b.dSINROffset < a.sinr, b.snr13 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue13,
		case when b.snr12 + b.dSINROffset < 0. or b.snr13 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr12 + b.dSINROffset + (abs(if(b.snr12 + b.dSINROffset < a.sinr, b.snr12 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr12 + b.dSINROffset
		 end as dInfValue12,
		case when b.snr12 + b.dSINROffset < 0. or b.snr13 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr13 + b.dSINROffset + (abs(if(b.snr12 + b.dSINROffset < a.sinr, b.snr12 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr13 + b.dSINROffset
		 end as dSupValue12,
		case when b.snr12 + b.dSINROffset < 0. or b.snr13 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr12 + b.dSINROffset < a.sinr, b.snr12 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue12,
		case when b.snr11 + b.dSINROffset < 0. or b.snr12 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr11 + b.dSINROffset + (abs(if(b.snr11 + b.dSINROffset < a.sinr, b.snr11 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr11 + b.dSINROffset
		 end as dInfValue11,
		case when b.snr11 + b.dSINROffset < 0. or b.snr12 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr12 + b.dSINROffset + (abs(if(b.snr11 + b.dSINROffset < a.sinr, b.snr11 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr12 + b.dSINROffset
		 end as dSupValue11,
		case when b.snr11 + b.dSINROffset < 0. or b.snr12 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr11 + b.dSINROffset < a.sinr, b.snr11 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue11,
		case when b.snr10 + b.dSINROffset < 0. or b.snr11 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr10 + b.dSINROffset + (abs(if(b.snr10 + b.dSINROffset < a.sinr, b.snr10 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr10 + b.dSINROffset
		 end as dInfValue10,
		case when b.snr10 + b.dSINROffset < 0. or b.snr11 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr11 + b.dSINROffset + (abs(if(b.snr10 + b.dSINROffset < a.sinr, b.snr10 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr11 + b.dSINROffset
		 end as dSupValue10,
		case when b.snr10 + b.dSINROffset < 0. or b.snr11 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr10 + b.dSINROffset < a.sinr, b.snr10 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue10,
		case when b.snr9 + b.dSINROffset < 0. or b.snr10 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr9 + b.dSINROffset + (abs(if(b.snr9 + b.dSINROffset < a.sinr, b.snr9 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr9 + b.dSINROffset
		 end as dInfValue9,
		case when b.snr9 + b.dSINROffset < 0. or b.snr10 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr10 + b.dSINROffset + (abs(if(b.snr9 + b.dSINROffset < a.sinr, b.snr9 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr10 + b.dSINROffset
		 end as dSupValue9,
		case when b.snr9 + b.dSINROffset < 0. or b.snr10 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr9 + b.dSINROffset < a.sinr, b.snr9 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue9,
		case when b.snr8 + b.dSINROffset < 0. or b.snr9 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr8 + b.dSINROffset + (abs(if(b.snr8 + b.dSINROffset < a.sinr, b.snr8 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr8 + b.dSINROffset
		 end as dInfValue8,
		case when b.snr8 + b.dSINROffset < 0. or b.snr9 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr9 + b.dSINROffset + (abs(if(b.snr8 + b.dSINROffset < a.sinr, b.snr8 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr9 + b.dSINROffset
		 end as dSupValue8,
		case when b.snr8 + b.dSINROffset < 0. or b.snr9 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr8 + b.dSINROffset < a.sinr, b.snr8 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue8,
		case when b.snr7 + b.dSINROffset < 0. or b.snr8 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr7 + b.dSINROffset + (abs(if(b.snr7 + b.dSINROffset < a.sinr, b.snr7 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr7 + b.dSINROffset
		 end as dInfValue7,
		case when b.snr7 + b.dSINROffset < 0. or b.snr8 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr8 + b.dSINROffset + (abs(if(b.snr7 + b.dSINROffset < a.sinr, b.snr7 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr8 + b.dSINROffset
		 end as dSupValue7,
		case when b.snr7 + b.dSINROffset < 0. or b.snr8 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr7 + b.dSINROffset < a.sinr, b.snr7 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue7,
		case when b.snr6 + b.dSINROffset < 0. or b.snr7 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr6 + b.dSINROffset + (abs(if(b.snr6 + b.dSINROffset < a.sinr, b.snr6 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr6 + b.dSINROffset
		 end as dInfValue6,
		case when b.snr6 + b.dSINROffset < 0. or b.snr7 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr7 + b.dSINROffset + (abs(if(b.snr6 + b.dSINROffset < a.sinr, b.snr6 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr7 + b.dSINROffset
		 end as dSupValue6,
		case when b.snr6 + b.dSINROffset < 0. or b.snr7 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr6 + b.dSINROffset < a.sinr, b.snr6 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue6,
		case when b.snr5 + b.dSINROffset < 0. or b.snr6 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr5 + b.dSINROffset + (abs(if(b.snr5 + b.dSINROffset < a.sinr, b.snr5 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr5 + b.dSINROffset
		 end as dInfValue5,
		case when b.snr5 + b.dSINROffset < 0. or b.snr6 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr6 + b.dSINROffset + (abs(if(b.snr5 + b.dSINROffset < a.sinr, b.snr5 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr6 + b.dSINROffset
		 end as dSupValue5,
		case when b.snr5 + b.dSINROffset < 0. or b.snr6 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr5 + b.dSINROffset < a.sinr, b.snr5 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue5,
		case when b.snr4 + b.dSINROffset < 0. or b.snr5 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr4 + b.dSINROffset + (abs(if(b.snr4 + b.dSINROffset < a.sinr, b.snr4 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr4 + b.dSINROffset
		 end as dInfValue4,
		case when b.snr4 + b.dSINROffset < 0. or b.snr5 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr5 + b.dSINROffset + (abs(if(b.snr4 + b.dSINROffset < a.sinr, b.snr4 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr5 + b.dSINROffset
		 end as dSupValue4,
		case when b.snr4 + b.dSINROffset < 0. or b.snr5 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr4 + b.dSINROffset < a.sinr, b.snr4 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue4,
		case when b.snr3 + b.dSINROffset < 0. or b.snr4 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr3 + b.dSINROffset + (abs(if(b.snr3 + b.dSINROffset < a.sinr, b.snr3 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr3 + b.dSINROffset
		 end as dInfValue3,
		case when b.snr3 + b.dSINROffset < 0. or b.snr4 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr4 + b.dSINROffset + (abs(if(b.snr3 + b.dSINROffset < a.sinr, b.snr3 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr4 + b.dSINROffset
		 end as dSupValue3,
		case when b.snr3 + b.dSINROffset < 0. or b.snr4 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr3 + b.dSINROffset < a.sinr, b.snr3 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue3,
		case when b.snr2 + b.dSINROffset < 0. or b.snr3 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr2 + b.dSINROffset + (abs(if(b.snr2 + b.dSINROffset < a.sinr, b.snr2 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr2 + b.dSINROffset
		 end as dInfValue2,
		case when b.snr2 + b.dSINROffset < 0. or b.snr3 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr3 + b.dSINROffset + (abs(if(b.snr2 + b.dSINROffset < a.sinr, b.snr2 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr3 + b.dSINROffset
		 end as dSupValue2,
		case when b.snr2 + b.dSINROffset < 0. or b.snr3 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr2 + b.dSINROffset < a.sinr, b.snr2 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue2,
		case when b.snr1 + b.dSINROffset < 0. or b.snr2 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr1 + b.dSINROffset + (abs(if(b.snr1 + b.dSINROffset < a.sinr, b.snr1 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr1 + b.dSINROffset
		 end as dInfValue1,
		case when b.snr1 + b.dSINROffset < 0. or b.snr2 + b.dSINROffset < 0. or a.sinr < 0. then
		           b.snr2 + b.dSINROffset + (abs(if(b.snr1 + b.dSINROffset < a.sinr, b.snr1 + b.dSINROffset, a.sinr)) + 1.)
		     else  b.snr2 + b.dSINROffset
		 end as dSupValue1,
		case when b.snr1 + b.dSINROffset < 0. or b.snr2 + b.dSINROffset < 0. or a.sinr < 0. then
		           a.sinr  + (abs(if(b.snr1 + b.dSINROffset < a.sinr, b.snr1 + b.dSINROffset, a.sinr)) + 1.)
		     else  a.sinr
		 end as dValue1
  from RESULT_NR_2D_SINR a, NR_DLTRAFFIC b
 where a.schedule_id = 8463189
   and a.schedule_id = b.schedule_id
)
insert into RESULT_NR_2D_THROUGHPUT partition (schedule_id)
select a.scenario_id, a.rx_tm_xpos, a.rx_tm_ypos, a.x_point, a.y_point,
       case when a.dSNR >= a.snr15 + a.dSINROffset then
                   a.thp15
            when a.dSNR >= a.snr14 + a.dSINROffset then
                   a.thp14 + (a.thp15 - a.thp14) * log10(dValue14 / dInfValue14) / log10(dSupValue14 / dInfValue14)
            when a.dSNR >= a.snr13 + a.dSINROffset then
                   a.thp13 + (a.thp14 - a.thp13) * log10(dValue13 / dInfValue13) / log10(dSupValue13 / dInfValue13)
            when a.dSNR >= a.snr12 + a.dSINROffset then
                    a.thp12 + (a.thp13 - a.thp12) * log10(dValue12 / dInfValue12) / log10(dSupValue12 / dInfValue12)
            when a.dSNR >= a.snr11 + a.dSINROffset then
                    a.thp11 + (a.thp12 - a.thp11) * log10(dValue11 / dInfValue11) / log10(dSupValue11 / dInfValue11)
            when a.dSNR >= a.snr10 + a.dSINROffset then
                    a.thp10 + (a.thp11 - a.thp10) * log10(dValue10 / dInfValue10) / log10(dSupValue10 / dInfValue10)
            when a.dSNR >= a.snr9 + a.dSINROffset then
                    a.thp9 + (a.thp10 - a.thp9) * log10(dValue9 / dInfValue9) / log10(dSupValue9 / dInfValue9)
            when a.dSNR >= a.snr8 + a.dSINROffset then
                    a.thp8 + (a.thp9 - a.thp8) * log10(dValue8 / dInfValue8) / log10(dSupValue8 / dInfValue8)
            when a.dSNR >= a.snr7 + a.dSINROffset then
                    a.thp7 + (a.thp8 - a.thp7) * log10(dValue7 / dInfValue7) / log10(dSupValue7 / dInfValue7)
            when a.dSNR >= a.snr6 + a.dSINROffset then
                    a.thp6 + (a.thp7 - a.thp6) * log10(dValue6 / dInfValue6) / log10(dSupValue6 / dInfValue6)
            when a.dSNR >= a.snr5 + a.dSINROffset then
                    a.thp5 + (a.thp6 - a.thp5) * log10(dValue5 / dInfValue5) / log10(dSupValue5 / dInfValue5)
            when a.dSNR >= a.snr4 + a.dSINROffset then
                    a.thp4 + (a.thp5 - a.thp4) * log10(dValue4 / dInfValue4) / log10(dSupValue4 / dInfValue4)
            when a.dSNR >= a.snr3 + a.dSINROffset then
                    a.thp3 + (a.thp4 - a.thp3) * log10(dValue3 / dInfValue3) / log10(dSupValue3 / dInfValue3)
            when a.dSNR >= a.snr2 + a.dSINROffset then
                    a.thp2 + (a.thp3 - a.thp2) * log10(dValue2 / dInfValue2) / log10(dSupValue2 / dInfValue2)
            when a.dSNR >= a.snr1 + a.dSINROffset then
                    a.thp1 + (a.thp2 - a.thp1) * log10(dValue1 / dInfValue1) / log10(dSupValue1 / dInfValue1)
            else 0.
        end as throughput,
       a.schedule_id
  from THROUGHPUTtemp a
;

-- Check Result Data
select * from RESULT_NR_2D_THROUGHPUT where schedule_id = 8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 3. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_THROUGHPUT where schedule_id = 8463189;" | sed 's/[[:space:]]\+/,/g' > throughput_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
