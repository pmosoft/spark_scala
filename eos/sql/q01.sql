SELECT COUNT(*) FROM SCENARIO
;

/

SELECT *
FROM (
SELECT SCENARIO_NM, COUNT(*) CNT
FROM SCENARIO
GROUP BY  SCENARIO_NM
ORDER BY  SCENARIO_NM
) A
ORDER BY CNT DESC
;



/

SELECT RESULT_TIME
FROM SCHEDULE
WHERE RESULT_TIME > 9580.4
GROUP BY RESULT_TIME
ORDER BY RESULT_TIME DESC



/

SELECT * FROM SCENARIO
WHERE SCENARIO_ID IN (
    SELECT SCENARIO_ID
    FROM SCHEDULE
    WHERE RESULT_TIME > 9580.4
)
ORDER BY  SCENARIO_NM
;

SELECT * FROM SCENARIO
WHERE SCENARIO_ID IN (
    SELECT SCENARIO_ID
    FROM SCHEDULE
    WHERE RESULT_TIME > 9580.4
)
ORDER BY  SCENARIO_NM

/

SELECT * FROM SCENARIO
ORDER BY REG_DT


/
SELECT 
       SCENARIO_ID                       --  
     , SCENARIO_NM                       -- 시나리오 이름
     , SIDO                              --  
     , SIGUGUN                           --  
     , DONG                              --  
     ,TRUNC((TM_ENDX-TM_STARTX) * (TM_ENDY-TM_STARTY)) AS 면적
     , FA_MODEL_ID                       --  
     , FA_SEQ                            -- 주파수 일련번호
     , RESOLUTION                        -- RESOLUTION
     , REG_DT                            -- 등록일
     , FLOORLOSS                         --  
     , SCENARIO_SUB_ID                   -- 부모ID
     , SCENARIO_SOLUTION_NUM             -- 솔루션 분석 유형 4가지
     , LOSS_TYPE                         -- LOSS_TYPE
     , BUILDINGANALYSIS3D_YN             -- 3D분석여부
     , BUILDINGANALYSIS3D_RESOLUTION     -- 3D분석Resolution
     , AREA_ID
FROM SCENARIO
WHERE REG_DT > SYSDATE -30
ORDER BY REG_DT DESC


/

CREATE TABLE PSH001 (
TAB_NM VARCHAR(100)
,CNT NUMBER
)

/


SELECT * FROM PSH001
WHERE CNT > 0