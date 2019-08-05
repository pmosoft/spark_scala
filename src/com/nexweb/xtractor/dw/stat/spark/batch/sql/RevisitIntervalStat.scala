package com.nexweb.xtractor.dw.stat.spark.batch.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
import com.nexweb.xtractor.dw.stat.spark.common.OJDBC
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch

/*
 * 설    명 : 일별 1일이상 재방문 추이 통계
 * 입    력 :

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION
TB_VISIT_INTERVAL

 * 출    력 : TB_REVISIT_INTERVAL_STAT
 * 수정내역 :
 * 2019-01-22 | 피승현 | 최초작성
 */
object RevisitIntervalStat {

  var spark: SparkSession = null
  var objNm  = "TB_REVISIT_INTERVAL_STAT"

  var statisDate = ""
  var statisType = ""

  //var objNm  = "TB_REVISIT_INTERVAL_STAT";var statisDate = "20190214"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
    println(objNm + ".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def executeMonthly() = {
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_VISIT_INTERVAL"       ,statisDate,statisType,"",true)
  }

/*

CREATE OR REPLACE FUNCTION              "FN_REVISIT_INTERVAL_CD" (REVISIT_INTERVAL  IN  NUMBER)
/*
??? ?? ??
1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11~15, 16~20, 21~30, 31~60, 61~90, 91~
*/
      RETURN  VARCHAR2 IS
      RTN VARCHAR2(2);

    BEGIN

    IF REVISIT_INTERVAL <= 9 THEN
        RETURN  '0'||REVISIT_INTERVAL;
    ELSIF REVISIT_INTERVAL = 10 THEN
        RETURN '10' ;
    ELSIF REVISIT_INTERVAL >10 AND REVISIT_INTERVAL <=15 THEN
        RETURN '11' ;
    ELSIF REVISIT_INTERVAL >15 AND REVISIT_INTERVAL <=20 THEN
        RETURN '12' ;
    ELSIF REVISIT_INTERVAL >20 AND REVISIT_INTERVAL <=30 THEN
        RETURN '13' ;
    ELSIF REVISIT_INTERVAL >30 AND REVISIT_INTERVAL <=60 THEN
        RETURN '14' ;
    ELSIF REVISIT_INTERVAL >60 AND REVISIT_INTERVAL <=90 THEN
        RETURN '15' ;
    ELSE
        RETURN '99' ;
    END IF;

   END;


                MERGE  INTO TB_VISIT_INTERVAL TA
                USING(
                SELECT V_ID, V_HOST VHOST, TO_DATE(TO_CHAR(MIN(V_DATE), 'YYYYMMDD'), 'YYYYMMDD') V_DATE
                FROM    TB_WL_REFERER_SESSION
                GROUP    BY V_ID, V_HOST
                ) TB
                ON
                (
                TA.V_ID = TB.V_ID AND TA.VHOST = TB.VHOST
                )
                WHEN MATCHED THEN
                UPDATE SET
                TA.PREV_V_DATE = TA.V_DATE,
                TA.V_DATE = TB.V_DATE
                WHERE TA.V_DATE < TB.V_DATE
                WHEN NOT MATCHED THEN
                INSERT ( TA.V_ID, TA.V_DATE, TA.PREV_V_DATE, TA.VHOST)
                VALUES( TB.V_ID, TB.V_DATE, TB.V_DATE, TB.VHOST)


          INSERT INTO TB_REVISIT_INTERVAL_STAT(STATIS_DATE,REVISIT_INTERVAL,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH INTERVAL_CD AS
          (
          SELECT  V_ID, PREV_V_DATE, VHOST
          FROM    TB_VISIT_INTERVAL
          WHERE   PREV_V_DATE <= TO_DATE(?,'YYYYMMDD')
          )
          SELECT  ? STATIS_DATE,
                 NVL(TA.REVISIT_INTERVAL_CD, 'TOTAL') REVISIT_INTERVAL_CD,
                 NVL(TA.VHOST, 'TOTAL') VHOST,
                 NVL(TA.ATTR_ID, 'ALL') AS ATTR_ID,
                 NVL(VISIT_CNT, 0) AS VISIT_CNT,
                 NVL(TOT_VISITOR_CNT, 0) AS TOT_VISITOR_CNT,
                 NVL(NEW_VISITOR_CNT, 0) AS NEW_VISITOR_CNT,
                 NVL(RE_VISITOR_CNT, 0) AS RE_VISITOR_CNT,
                 NVL(LOGIN_CNT, 0) AS LOGIN_CNT,
                 NVL(LOGIN_VISITOR_CNT, 0) AS LOGIN_VISITOR_CNT,
                 NVL(DUR_TIME, 0) AS DUR_TIME,
                 NVL(PAGE_CNT, 0) AS PAGE_CNT,
                 NVL(PAGE_VIEW, 0) AS PAGE_VIEW,
                 NVL(BOUNCE_CNT, 0) AS BOUNCE_CNT
          FROM    (
            --방문수, 방문자수, 신규 방문자수, 재방문자
            SELECT NVL(REVISIT_INTERVAL_CD, 'TOTAL') AS REVISIT_INTERVAL_CD,
              NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
              SUM(VISIT_CNT) VISIT_CNT,
              COUNT(UNIQUE V_ID) TOT_VISITOR_CNT,
              COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
              COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM
            (
              SELECT  V_ID, VHOST,
                VISIT_CNT,
                FN_REVISIT_INTERVAL_CD(TRUNC(REVISIT_INTERVAL)) REVISIT_INTERVAL_CD
              FROM   (
                --1일 이상 재방문
                SELECT  TA.V_ID, TA.VHOST,
                       VISIT_CNT,
                       F_VISIT_TIME - PREV_V_DATE REVISIT_INTERVAL
                FROM    (
                      SELECT  V_ID, VHOST,
                       COUNT(1) VISIT_CNT,
                       MAX(V_DATE) F_VISIT_TIME
                      FROM    TB_WL_REFERER_SEGMENT
                      GROUP   BY V_ID, VHOST
                      ) TA, INTERVAL_CD TB
                WHERE   TA.V_ID = TB.V_ID
                AND TA.VHOST = TB.VHOST
                )
            )
            GROUP BY ROLLUP (VHOST, REVISIT_INTERVAL_CD)
            ) TA,
            (
            --로그인수, 로그인 방문자수
            SELECT NVL(REVISIT_INTERVAL_CD, 'TOTAL') AS REVISIT_INTERVAL_CD,
              NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
              SUM(LOGIN_CNT) LOGIN_CNT,
              COUNT(UNIQUE L_ID) LOGIN_VISITOR_CNT
            FROM
            (
              SELECT  V_ID, L_ID, VHOST,
                LOGIN_CNT,
                FN_REVISIT_INTERVAL_CD(TRUNC(REVISIT_INTERVAL)) REVISIT_INTERVAL_CD
              FROM   (
                --1일 이상 재방문
                SELECT  TA.V_ID, L_ID, TA.VHOST,
                       LOGIN_CNT,
                       F_VISIT_TIME - PREV_V_DATE REVISIT_INTERVAL
                FROM    (
                  SELECT V_ID, L_ID, VHOST,
                    SUM(LOGIN_CNT) LOGIN_CNT,
                    MAX(TO_DATE(STATIS_TIME, 'YYYYMMDDHH24MISS'))F_VISIT_TIME
                    FROM TB_MEMBER_CLASS_SEG_STAT
                  GROUP BY  V_ID, L_ID, VHOST
                  ) TA, INTERVAL_CD TB
                WHERE   TA.V_ID = TB.V_ID
                AND TA.VHOST = TB.VHOST
                )
            )
            GROUP BY ROLLUP (VHOST, REVISIT_INTERVAL_CD)
            ) TB,
            (
            --  이탈율, 페이지뷰, 페이지수, 체류시간
            SELECT NVL(REVISIT_INTERVAL_CD, 'TOTAL') AS REVISIT_INTERVAL_CD,
              NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
              COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(PAGE_CNT) AS PAGE_CNT,
              SUM(DUR_TIME) AS DUR_TIME
            FROM
            (
              SELECT  V_ID, SESSION_ID, VHOST,
                PAGE_CNT,
                PAGE_VIEW,
                DUR_TIME,
                FN_REVISIT_INTERVAL_CD(TRUNC(REVISIT_INTERVAL)) REVISIT_INTERVAL_CD
              FROM   (
                SELECT  TA.V_ID, TA.VHOST, SESSION_ID,
                  F_VISIT_TIME - PREV_V_DATE REVISIT_INTERVAL,
                  PAGE_CNT,
                  PAGE_VIEW,
                  DUR_TIME
                  FROM    (
                    SELECT VHOST, V_ID, SESSION_ID,
                    MAX(TO_DATE(STATIS_DATE, 'YYYYMMDDHH24MISS'))F_VISIT_TIME,
                    SUM(PAGE_VIEW) AS PAGE_VIEW,
                    SUM(PAGE_CNT) AS PAGE_CNT,
                    SUM(DUR_TIME) DUR_TIME
                    FROM TB_SEGMENT_SESSION_LOG
                    GROUP BY VHOST, V_ID, SESSION_ID
                    ) TA, INTERVAL_CD TB
                  WHERE TA.VHOST = TB.VHOST
                  AND   TA.V_ID = TB.V_ID
                )
            )
            GROUP BY ROLLUP (VHOST, REVISIT_INTERVAL_CD)
            ) TC
            WHERE TA.VHOST = TB.VHOST(+) AND TA.REVISIT_INTERVAL_CD = TB.REVISIT_INTERVAL_CD(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
            AND   TA.VHOST = TC.VHOST(+) AND TA.REVISIT_INTERVAL_CD = TC.REVISIT_INTERVAL_CD(+) AND TA.ATTR_ID = TC.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                             AS STATIS_DATE
         , CASE WHEN TA.REVISIT_INTERVAL_CD IS NULL THEN 'TOTAL' ELSE TA.REVISIT_INTERVAL_CD END       AS REVISIT_INTERVAL
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                                 AS GVHOST
         , 'ALL'                                                                                       AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)            AS VISIT_CNT
         , SUM(CASE WHEN TA.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TA.TOT_VISITOR_CNT   END)            AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TA.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TA.NEW_VISITOR_CNT   END)            AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TA.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TA.RE_VISITOR_CNT    END)            AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)            AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)            AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)            AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END)            AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)            AS PAGE_VIEW
         , SUM(CASE WHEN TC.BOUNCE_CNT        IS NULL THEN 0 ELSE TC.BOUNCE_CNT        END)            AS BOUNCE_CNT
    FROM
           (
           -- 방문수, 방문자수, 신규 방문자수, 재방문자
           SELECT GVHOST                                                                                           AS GVHOST
                , REVISIT_INTERVAL_CD                                                                              AS REVISIT_INTERVAL_CD
                , COUNT(*)                                                                                         AS VISIT_CNT2
                , COUNT(DISTINCT SESSION_ID)                                                                       AS VISIT_CNT
                , COUNT(DISTINCT V_ID)                                                                             AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = ${statisDate} THEN V_ID ELSE NULL END))    AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != ${statisDate} THEN V_ID ELSE NULL END))    AS RE_VISITOR_CNT
           FROM   (
                  SELECT TA.GVHOST
                       , TA.SESSION_ID
                       , TB.REVISIT_INTERVAL_CD
                       , TA.V_ID
                       , TA.VISIT_CNT
                  FROM   TB_REFERER_SESSION TA, TB_VISIT_INTERVAL TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.V_ID = TB.V_ID
                  )
           GROUP BY GVHOST, REVISIT_INTERVAL_CD
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인수, 로그인 방문자수
           SELECT
                  TA.GVHOST                                                     AS GVHOST
                , TB.REVISIT_INTERVAL_CD                                        AS REVISIT_INTERVAL_CD
                , SUM(TA.LOGIN_CNT)                                             AS LOGIN_CNT
                , COUNT(DISTINCT TA.T_ID)                                       AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT GVHOST                                                AS GVHOST
                       , V_ID                                                  AS V_ID
                       , T_ID                                                  AS T_ID
                       , COUNT(*)                                              AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, V_ID, T_ID
                  ) TA, TB_VISIT_INTERVAL TB
           WHERE  TA.GVHOST = TB.GVHOST
           AND    TA.V_ID   = TB.V_ID
           GROUP BY TA.GVHOST, TB.REVISIT_INTERVAL_CD
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.REVISIT_INTERVAL_CD = TB.REVISIT_INTERVAL_CD
           LEFT OUTER JOIN
           (
           -- 이탈율, 페이지뷰, 페이지수, 체류시간
           SELECT
                  GVHOST                                                          AS GVHOST
                , REVISIT_INTERVAL_CD                                             AS REVISIT_INTERVAL_CD
                , SUM(PAGE_VIEW)                                                  AS PAGE_VIEW
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , SUM(DUR_TIME)                                                   AS DUR_TIME
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END)    AS BOUNCE_CNT
           FROM   (
                  SELECT TA.GVHOST
                       , TA.SESSION_ID
                       , TB.REVISIT_INTERVAL_CD
                       , TA.PAGE_CNT
                       , TA.PAGE_VIEW
                       , TA.DUR_TIME
                  FROM   TB_ACCESS_SESSION TA, TB_VISIT_INTERVAL TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.V_ID = TB.V_ID
                  )
           GROUP BY GVHOST, REVISIT_INTERVAL_CD
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.REVISIT_INTERVAL_CD = TC.REVISIT_INTERVAL_CD
    GROUP BY ROLLUP(TA.GVHOST, TA.REVISIT_INTERVAL_CD)
    """
    spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
        SELECT * FROM TB_VISIT_INTERVAL
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    s"""
                  SELECT TA.GVHOST
                       , TA.SESSION_ID
                       , TB.REVISIT_INTERVAL_CD
                       , TA.V_ID
                       , TA.VISIT_CNT
                  FROM   TB_REFERER_SESSION TA, TB_VISIT_INTERVAL TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.V_ID = TB.V_ID
    """
    spark.sql(qry).take(100).foreach(println);

    */

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm); sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm, true, statisDate)
  }

  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
