-- 시나리오 : SCENARIO(ORACLE & HIVE) <= Oracle, Hive에 모두 테이블이 존재함( Oracle -> Hive로 데이터 연동)
select * from SCENARIO
 where scenario_id = :scenario_id
;

-- 스케줄 : SCHEDULE(ORACLE & HIVE)
SELECT * FROM SCHEDULE
 WHERE schedule_id = :schedule_id
;

-- 시설정보 : DU,RU,SITE(ORACLE & HIVE)
SELECT * FROM DU
 where scenario_id = :scenario_id
;

SELECT * FROM DU
 where scenario_id = :scenario_id
;

SELECT * FROM RU
 where scenario_id = :scenario_id
;

SELECT * FROM SITE
 where scenario_id = :scenario_id
;

-- 시나리오에 속한 RU List : SCENARIO_NR_RU 테이블(HIVE) <= Hive에만 테이블이 존재함 (Oracle에서 아래 sql문을 실행하여 Hive 테이블로 데이터 연동)
select T_DU.SCENARIO_ID
      ,T_DU.ENB_ID
      ,T_RU.PCI
      ,T_RU.PCI_PORT
      ,T_RU.RU_ID
      ,T_RU.MAKER
      ,T_RU.SECTOR_ORD
      ,nvl(T_RU.REPEATERATTENUATION, 0) as REPEATERATTENUATION
      ,nvl(T_RU.REPEATERPWRRATIO, 0) as REPEATERPWRRATIO
      ,T_RU.RU_SEQ
      ,T_SITE.RADIUS
      ,T_SITE.FEEDER_LOSS
      ,T_SITE.NOISEFLOOR
      ,T_SITE.CORRECTION_VALUE
      ,T_SITE.FADE_MARGIN
      ,T_SITE.TM_XPOSITION as XPOSITION
      ,T_SITE.TM_YPOSITION as YPOSITION
      ,T_SITE.HEIGHT
      ,T_SITE.SITE_ADDR
      ,T_SITE.TYPE
      ,T_SITE.STATUS
      ,T_SITE.SISUL_CD
      ,T_SITE.MSC
      ,T_SITE.BSC
      ,T_SITE.NETWORKID
      ,T_SITE.USABLETRAFFICCH
      ,T_SITE.SYSTEMID
      ,nvl(T_SITE.RU_TYPE, -1) as RU_TYPE
      ,T_SCENARIO.FA_MODEL_ID
      ,T_SCENARIO.NETWORK_TYPE
      ,T_SCENARIO.RESOLUTION
      ,nvl(T_SCENARIO.FA_SEQ, 0) as FA_SEQ
      ,CASE WHEN T_SITE.TM_XPOSITION - T_SITE.RADIUS < T_SCENARIO.TM_STARTX THEN  T_SCENARIO.TM_STARTX ELSE T_SITE.TM_XPOSITION - T_SITE.RADIUS END AS SITE_STARTX
      ,CASE WHEN T_SITE.TM_YPOSITION - T_SITE.RADIUS < T_SCENARIO.TM_STARTY THEN  T_SCENARIO.TM_STARTY ELSE T_SITE.TM_YPOSITION - T_SITE.RADIUS END AS SITE_STARTY
      ,CASE WHEN T_SITE.TM_XPOSITION + T_SITE.RADIUS > T_SCENARIO.TM_ENDX THEN  T_SCENARIO.TM_ENDX ELSE T_SITE.TM_XPOSITION + T_SITE.RADIUS END AS SITE_ENDX
      ,CASE WHEN T_SITE.TM_YPOSITION + T_SITE.RADIUS > T_SCENARIO.TM_ENDY THEN  T_SCENARIO.TM_ENDY ELSE T_SITE.TM_YPOSITION + T_SITE.RADIUS END AS SITE_ENDY
      ,FLOOR ((
       TRUNC(CASE WHEN T_SITE.TM_XPOSITION + T_SITE.RADIUS > T_SCENARIO.TM_ENDX THEN  T_SCENARIO.TM_ENDX ELSE T_SITE.TM_XPOSITION + T_SITE.RADIUS END)
       - TRUNC(CASE WHEN T_SITE.TM_XPOSITION - T_SITE.RADIUS < T_SCENARIO.TM_STARTX THEN  T_SCENARIO.TM_STARTX ELSE T_SITE.TM_XPOSITION - T_SITE.RADIUS END)
       ) / T_SCENARIO.RESOLUTION ) AS X_BIN_CNT
      ,FLOOR ((
       TRUNC(CASE WHEN T_SITE.TM_YPOSITION + T_SITE.RADIUS > T_SCENARIO.TM_ENDY THEN  T_SCENARIO.TM_ENDY ELSE T_SITE.TM_YPOSITION + T_SITE.RADIUS END )
       - TRUNC(CASE WHEN T_SITE.TM_YPOSITION - T_SITE.RADIUS < T_SCENARIO.TM_STARTY THEN  T_SCENARIO.TM_STARTY ELSE T_SITE.TM_YPOSITION - T_SITE.RADIUS END )
       ) / T_SCENARIO.RESOLUTION ) AS Y_BIN_CNT
  from SCENARIO T_SCENARIO
      ,DU       T_DU
      ,SITE     T_SITE
      ,(select SCENARIO_ID
              ,ENB_ID
              ,PCI
              ,PCI_PORT
              ,RU_ID
              ,SECTOR_ORD
              ,max(MAKER) as MAKER
              ,max(REPEATERATTENUATION) as REPEATERATTENUATION
              ,max(REPEATERPWRRATIO) as REPEATERPWRRATIO
              ,max(RU_SEQ) as RU_SEQ
         from RU
        where SCENARIO_ID = :scenario_id
        group by SCENARIO_ID, ENB_ID, PCI
                ,PCI_PORT, RU_ID, SECTOR_ORD
       ) T_RU
 where T_DU.SCENARIO_ID       = T_SCENARIO.SCENARIO_ID
   and T_DU.SCENARIO_ID       = T_RU.SCENARIO_ID
   and T_DU.ENB_ID            = T_RU.ENB_ID
   and T_RU.SCENARIO_ID       = T_SITE.SCENARIO_ID
   and T_RU.ENB_ID            = T_SITE.ENB_ID
   and T_RU.PCI               = T_SITE.PCI
   and T_RU.PCI_PORT          = T_SITE.PCI_PORT
   and T_RU.RU_ID             = T_SITE.RU_ID
   and T_SITE.TYPE            in ('RU', 'RU_N')
   and T_SITE.STATUS          = 1
   and T_SCENARIO.SCENARIO_ID =  :scenario_id
 order by T_RU.ENB_ID, T_RU.PCI, T_RU.PCI_PORT, T_RU.RU_ID
;

-- 시나리오에 속한 RU의 안테나 : SCENARIO_NR_ANTENNA(HIVE)
select ANTENA.SCENARIO_ID
      ,ANTENA.ANTENA_SEQ as ANTENA_SEQ
      ,ANTENA.RU_ID as RU_ID
      ,ANTENA.ANTENA_NM as ANTENA_NM
      ,nvl(ANTENA.ORIENTATION, 0) as ORIENTATION
      ,nvl(ANTENA.TILTING, 0) as TILTING
      ,ANTENA.ANTENA_ORD as ANTENA_ORD
      , ANTENA.LIMIT_TILTING as LIMIT_TILTING
      , TRU.RU_SEQ as RU_SEQ
  from RU_ANTENA ANTENA, ANTENABASE BASE , RU TRU
 where ANTENA.SCENARIO_ID = :scenario_id
   AND TRU.SCENARIO_ID = :scenario_id
   AND ANTENA.ANTENA_SEQ = BASE.ANTENA_SEQ 
   AND TRU.RU_ID = ANTENA.RU_ID
;

-- 수신점 파라미터 정보 : MOBILE_PARAMETER(ORACLE & HIVE)
SELECT * FROM MOBILE_PARAMETER
 WHERE scenario_id = :scenario_id
;

-- FABASE (ORACLE & HIVE)
배치로 연동 필요


-- 안테나BASE 정보 <= 검토중...
WITH T1 AS
(
SELECT fa_seq
  FROM SCENARIO
  WHERE scenario_id = :scenario_id
) 
SELECT
    A.antena_seq
    ,A.antena_nm
    ,A.maker
    ,A.beamwidth
    ,A.fronttobackratio
    ,A.maxgain
    ,A.class
    ,A.tiltingtype
    ,A.horizontalpattern
    ,A.verticalpattern
    ,A.type
    ,A.fa_seq
    ,A.img_path
    ,A.his_antena_seq
    ,A.def_tilt
    ,A.antena_standard_nm
    ,A.beamheight
    ,B.maker
    ,NVL(B.limit_tilting, -1) AS limit_tilting
FROM (select A.* from ANTENABASE A, T1
       where A.fa_seq = T1.fa_seq
     ) A
 LEFT OUTER JOIN NRPARAMETER_ANT_MAKER B
   ON (A.fa_seq = B.fa_seq AND A.antena_standard_nm = B.antenna_nm)
;

-- 안테나정보 : ANTENABASE(ORACLE & HIVE)
배치로 연동 필요

SELECT * FROM ANTENABASE;

-- 안테나정보 : ANTENABASE_PATTERN(HIVE)
배치로 연동 필요

IN Oracle,

TRUNCATE TABLE ANTENABASE_INFO;

begin
  FOR ANTENABASE_INFO
  IN (
    SELECT antena_seq
      FROM ANTENABASE
    order by antena_seq
  )
  LOOP
    insert into ANTENABASE_PATTERN
    with T1 as
    (
    SELECT /*+ parallel (A 8) */
           antena_seq,
           REGEXP_REPLACE(trim(horizontalpattern), '( ){2,}', ' ') horizontalpattern,
           REGEXP_REPLACE(trim(verticalpattern), '( ){2,}', ' ') verticalpattern
     FROM ANTENABASE A 
    where antena_seq = ANTENABASE_INFO.antena_seq
    ),    
    H as
    (
     select /*+ parallel(T1 8) */ antena_seq, level-1 as degree, Regexp_Substr(horizontalpattern, '[^ ]+', 1, Level) as val from T1
    Connect By Regexp_Substr(horizontalpattern, '[^ ]+', 1, Level) Is Not Null
    ),
    V as
    (
     select /*+ parallel(T1 8) */  antena_seq, level-1 as degree, Regexp_Substr(verticalpattern, '[^ ]+', 1, Level) as val from T1
    Connect By Regexp_Substr(verticalpattern, '[^ ]+', 1, Level) Is Not Null
    )
    select /*+ parallel(H 8) (V 8) */ H.antena_seq, H.degree, to_number(H.val) as horizontal, to_number(V.val) as vertical
      from H, V
     where H.antena_seq = V.antena_seq
       and H.degree = V.degree
    ;
  END LOOP;
end;
/

-- 안테나 MAKER 정보: NRPARAMETER_ANT_MAKER(ORACLE & HIVE)
배치로 연동 필요

SELECT * FROM NRPARAMETER_ANT_MAKER;


