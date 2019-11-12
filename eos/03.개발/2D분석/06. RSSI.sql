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
select * from RESULT_NR_2D_RSRPPILOT_RU where schedule_id=8463189;

--------------------------------------------------------------------------------------------------------------------------
-- 2. RSSI Analyze by RU Unit
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSSI_RU drop partition(schedule_id=8463189);

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join = false;

with NR_PARAMETER as -- 파라미터 정보
(
select a.scenario_id, b.schedule_id, c.ant_category,
       c.number_of_cc, c.number_of_sc_per_rb, c.rb_per_cc, c.bandwidth_per_cc, c.subcarrierspacing,
       d.noisefigure
  from SCENARIO a, SCHEDULE b, NRSYSTEM c, MOBILE_PARAMETER d
 where b.schedule_id = 8463189  
   and a.scenario_id = b.scenario_id
   and a.scenario_id = c.scenario_id
   and a.scenario_id = d.scenario_id
)
insert into RESULT_NR_2D_RSSI_RU partition (schedule_id)
select a.scenario_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos,
       a.los, a.pathloss, a.antenna_gain, a.pathlossprime, a.rsrppilot,
       if (RSSINoNoise = 0. , -9999, 10. * log10(RSSINoNoise)) as RSSINoNoise,  
       if ((RSSINoNoise + MobileNoiseFloor) = 0. , -9999, 10. * log10((RSSINoNoise + MobileNoiseFloor))) as RSSI,  
       a.schedule_id
  from
	(
	select a.scenario_id, a.ru_id, a.enb_id, a.cell_id, a.rx_tm_xpos, a.rx_tm_ypos,
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
	  from RESULT_NR_2D_RSRPPILOT_RU a, NR_PARAMETER b
	 where a.schedule_id = 8463189
	   and a.schedule_id = b.schedule_id
	) a
;

-- Check Result Data
select * from RESULT_NR_2D_RSSI_RU where schedule_id = 8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 3. RSSI Analyze by Scenario Area
--------------------------------------------------------------------------------------------------------------------------
alter table RESULT_NR_2D_RSSI drop partition(schedule_id=8463189);

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
insert into RESULT_NR_2D_RSSI partition (schedule_id)
select max(AREA.scenario_id) as scenario_id,
       RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution as rx_tm_xpos,
       RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution as rx_tm_ypos,
       (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution as x_point,
       (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution as y_point,
       if (sum((power(10, (RSSINoNoise)/10.0))) = 0. , -9999, 10. * log10(sum((power(10, (RSSINoNoise)/10.0))))) as RSSI,
       max(AREA.schedule_id) as schedule_id
  from AREA, RESULT_NR_2D_RSSI_RU RSLT
 where RSLT.schedule_id = AREA.schedule_id
   and AREA.tm_startx <= RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution and RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution < AREA.tm_endx
   and AREA.tm_starty <= RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution and RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution < AREA.tm_endy
  group by RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution, RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution,
           (RSLT.rx_tm_xpos div AREA.resolution * AREA.resolution - AREA.tm_startx) / AREA.resolution, (RSLT.rx_tm_ypos div AREA.resolution * AREA.resolution - AREA.tm_starty) / AREA.resolution
;

-- Check Result Data
select * from RESULT_NR_2D_RSSI where schedule_id = 8463189;


--------------------------------------------------------------------------------------------------------------------------
-- 4. Export Result Data (for Test)
--------------------------------------------------------------------------------------------------------------------------
hive -e "select * from RESULT_NR_2D_RSSI where schedule_id = 8463189;" | sed 's/[[:space:]]\+/,/g' > rssi_test.csv

---------------------------------------------------------E-N-D---------------------------------------------------------------------------
