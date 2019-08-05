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
 * 설    명 : 일/월별 재방문자 추이 통계
 * 입    력 :

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_REVISIT_STAT
 * 수정내역 :
 * 2019-01-21 | 피승현 | 최초작성
 */
object RevisitStat {

  var spark: SparkSession = null
  var objNm  = "TB_REVISIT_STAT"

  var statisDate = ""
  var statisType = ""
  var revistInterval = ""
  var revistCd = ""

  //var statisDate = "20181219"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
    println(objNm + ".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    revistInterval = "BASE_DAY"
    revistCd = "'20'||SUBSTR(V_ID, 2, 6) = '"+statisDate+"'"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    revistInterval = "BASE_MONTH"
    revistCd = "'20'||SUBSTR(V_ID, 2, 4) = '"+statisDate+"'"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_REVISIT_STAT
          (STATIS_DATE,REVISIT_INTERVAL,USER_TYPE,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          SELECT   TA.STATIS_DATE,
                   NVL(TA.REVISIT_INTERVAL, 'BASE_DAY') REVISIT_INTERVAL,
                   NVL(TA.USER_TYPE, 'ALL') USER_TYPE,
                   NVL(TA.VHOST, 'TOTAL') VHOST,
                   NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
                   NVL(VISIT_CNT, 0) VISIT_CNT,
                   NVL(TOT_VISITOR_CNT, 0) TOT_VISITOR_CNT,
                   NVL(NEW_VISITOR_CNT, 0) NEW_VISITOR_CNT,
                   NVL(RE_VISITOR_CNT, 0) RE_VISITOR_CNT,
                   NVL(LOGIN_CNT, 0) LOGIN_CNT,
                   NVL(LOGIN_VISITOR_CNT, 0) LOGIN_VISITOR_CNT,
                   NVL(DUR_TIME, 0) DUR_TIME,
                   NVL(PAGE_CNT, 0) PAGE_CNT,
                   NVL(PAGE_VIEW, 0) PAGE_VIEW,
                   NVL(BOUNCE_CNT, 0) BOUNCE_CNT
              FROM    (
                  -- 방문수, 방문자수, 신규 방문자, 재방문자
                  SELECT  ? STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                           COUNT(1) VISIT_CNT,
                           COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
                           COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                           COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
                  FROM    TB_WL_REFERER_SEGMENT
                  GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
                  ) TA,
                  (
                  --  로그인수, 로그인 방문자수
                  SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                           SUM(LOGIN_CNT) LOGIN_CNT,
                           COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
                  FROM    TB_MEMBER_CLASS_SEG_STAT
                  GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
                  ) TB,
                  (
                  -- 체류시간, 페이지뷰
                  SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                           SUM(PAGE_VIEW) PAGE_VIEW,
                           SUM(DUR_TIME) DUR_TIME
                  FROM    TB_SEGMENT_SESSION_LOG
                  GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
                  ) TC,
                  (
                  -- 이탈수
                  SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                           COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
                           SUM(PAGE_CNT) PAGE_CNT
                  FROM    (
                          SELECT  SESSION_ID, TA.VHOST, REVISIT_DAY,
                               COUNT(1) PAGE_VIEW,
                               COUNT(DISTINCT TA.URL) PAGE_CNT
                          FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB
                          WHERE  TA.URL = TB.URL
                          AND    TA.VHOST = TB.VHOST
                          AND      TB.SUB_TYPE ='Y'
                          GROUP   BY SESSION_ID, TA.VHOST, REVISIT_DAY
                       )
                  GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
              ) TD
              WHERE     1=1
              AND       TA.STATIS_DATE = TB.STATIS_DATE(+) AND TA.VHOST = TB.VHOST(+) AND TA.REVISIT_INTERVAL = TB.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TB.USER_TYPE(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
              AND       TA.STATIS_DATE = TC.STATIS_DATE(+) AND TA.VHOST = TC.VHOST(+) AND TA.REVISIT_INTERVAL = TC.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TC.USER_TYPE(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
              AND       TA.STATIS_DATE = TD.STATIS_DATE(+) AND TA.VHOST = TD.VHOST(+) AND TA.REVISIT_INTERVAL = TD.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TD.USER_TYPE(+) AND TA.ATTR_ID = TD.ATTR_ID(+)
          UNION
          SELECT   TA.STATIS_DATE,
                   NVL(TA.REVISIT_INTERVAL, 'BASE_MONTH') REVISIT_INTERVAL,
                   NVL(TA.USER_TYPE, 'ALL') USER_TYPE,
                   NVL(TA.VHOST, 'TOTAL') VHOST,
                   NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
                   NVL(VISIT_CNT, 0) VISIT_CNT,
                   NVL(TOT_VISITOR_CNT, 0) TOT_VISITOR_CNT,
                   NVL(NEW_VISITOR_CNT, 0) NEW_VISITOR_CNT,
                   NVL(RE_VISITOR_CNT, 0) RE_VISITOR_CNT,
                   NVL(LOGIN_CNT, 0) LOGIN_CNT,
                   NVL(LOGIN_VISITOR_CNT, 0) LOGIN_VISITOR_CNT,
                   NVL(DUR_TIME, 0) DUR_TIME,
                   NVL(PAGE_CNT, 0) PAGE_CNT,
                   NVL(PAGE_VIEW, 0) PAGE_VIEW,
                   NVL(BOUNCE_CNT, 0) BOUNCE_CNT
              FROM    (
                  -- 방문수, 방문자수, 신규 방문자, 재방문자
                  SELECT ? STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                          COUNT(1) VISIT_CNT,
                          COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
                          COUNT(UNIQUE (CASE  WHEN ( RVID >= TO_CHAR(SYSDATE -30, 'YYYYMMDD')) OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                          COUNT(UNIQUE (CASE  WHEN RVID < TO_CHAR(SYSDATE -30, 'YYYYMMDD') AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
                  FROM
                  (
                  SELECT  V_ID, VHOST, REVISIT_MON,
                          '20'||(CASE WHEN '20'||SUBSTR(V_ID, 2, 2) > TO_CHAR(SYSDATE, 'YYYY') THEN TO_CHAR(SYSDATE, 'YY') ELSE SUBSTR(V_ID, 2, 2) END)||
                          (CASE WHEN SUBSTR(V_ID, 4, 2) > '12' THEN '12' ELSE SUBSTR(V_ID, 4, 2) END)||
                          (CASE WHEN SUBSTR(V_ID, 6, 2) > '30' THEN '30' ELSE SUBSTR(V_ID, 6, 2) END) AS RVID
                  FROM    TB_WL_REFERER_SEGMENT
                  )
                  GROUP   BY ROLLUP(VHOST, REVISIT_MON)
                  ) TA,
                            (
                            SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                                     SUM(LOGIN_CNT) LOGIN_CNT,
                                     COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
                            FROM    TB_MEMBER_CLASS_SEG_STAT
                            GROUP   BY ROLLUP(VHOST, REVISIT_MON)
                            ) TB,
                  (
                  --  로그인수, 로그인 방문자수
                            SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                                     SUM(PAGE_VIEW) PAGE_VIEW,
                                     SUM(DUR_TIME) DUR_TIME
                            FROM    TB_SEGMENT_SESSION_LOG
                            GROUP   BY ROLLUP(VHOST, REVISIT_MON)
                  ) TC,
                  (
                  -- 이탈수
                  SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
                           COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
                           SUM(PAGE_CNT) PAGE_CNT
                  FROM    (
                          SELECT  SESSION_ID, TA.VHOST, REVISIT_MON,
                               COUNT(1) PAGE_VIEW,
                               COUNT(DISTINCT TA.URL) PAGE_CNT
                          FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB
                          WHERE  TA.URL = TB.URL
                          AND    TA.VHOST = TB.VHOST
                          AND      TB.SUB_TYPE ='Y'
                          GROUP   BY SESSION_ID, TA.VHOST, REVISIT_MON
                       )
                  GROUP   BY ROLLUP(VHOST, REVISIT_MON)
              ) TD
              WHERE     1=1
              AND       TA.STATIS_DATE = TB.STATIS_DATE(+) AND TA.VHOST = TB.VHOST(+) AND TA.REVISIT_INTERVAL = TB.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TB.USER_TYPE(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
              AND       TA.STATIS_DATE = TC.STATIS_DATE(+) AND TA.VHOST = TC.VHOST(+) AND TA.REVISIT_INTERVAL = TC.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TC.USER_TYPE(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
              AND       TA.STATIS_DATE = TD.STATIS_DATE(+) AND TA.VHOST = TD.VHOST(+) AND TA.REVISIT_INTERVAL = TD.REVISIT_INTERVAL(+) AND TA.USER_TYPE = TD.USER_TYPE(+) AND TA.ATTR_ID = TD.ATTR_ID(+)

 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                             AS STATIS_DATE
         , '${revistInterval}'                                                                         AS REVISIT_INTERVAL
         , CASE WHEN TA.REVISIT_CD IS NULL THEN 'TOTAL' ELSE TA.REVISIT_CD END                         AS USER_TYPE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                                 AS GVHOST
         , 'ALL'                                                                                       AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)            AS VISIT_CNT
         , SUM(CASE WHEN TA.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TA.TOT_VISITOR_CNT   END)            AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TA.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TA.NEW_VISITOR_CNT   END)            AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TA.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TA.RE_VISITOR_CNT    END)            AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)            AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)            AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)            AS DUR_TIME
         , SUM(CASE WHEN TD.PAGE_CNT          IS NULL THEN 0 ELSE TD.PAGE_CNT          END)            AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)            AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)            AS BOUNCE_CNT
    FROM
           (
           --유입경로별 방문수
           SELECT GVHOST                                                                               AS GVHOST
                , CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END              AS REVISIT_CD
                , COUNT(*)                                                                             AS VISIT_CNT
                , COUNT(DISTINCT V_ID)                                                                 AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN ${revistCd} THEN V_ID ELSE NULL END) )                     AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN ${revistCd} THEN V_ID ELSE NULL END) )                     AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST, CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END
           ) TA
           LEFT OUTER JOIN
           (
           --유입경로별 로그인수
           SELECT
                  TA.GVHOST                                                     AS GVHOST
                , CASE WHEN '${statisType}' = 'D' THEN TA.REVISIT_DAY ELSE TA.REVISIT_MONTH END AS REVISIT_CD
                , SUM(TB.LOGIN_CNT)                                             AS LOGIN_CNT
                , COUNT(DISTINCT TB.T_ID)                                       AS LOGIN_VISITOR_CNT
           FROM   TB_REFERER_SESSION TA,
                  (
                  SELECT GVHOST                                                AS GVHOST
                       , SESSION_ID                                            AS SESSION_ID
                       , T_ID                                                  AS T_ID
                       , COUNT(*)                                              AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TB
           WHERE  TA.GVHOST     = TB.GVHOST
           AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, REVISIT_CD
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.REVISIT_CD = TB.REVISIT_CD
           LEFT OUTER JOIN
           (
           -- 유입경로별 체류시간, 페이지뷰
           SELECT
                  GVHOST                                                          AS GVHOST
                , CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END AS REVISIT_CD
                , SUM(PAGE_VIEW)                                                  AS PAGE_VIEW
                , SUM(DUR_TIME)                                                   AS DUR_TIME
           FROM   TB_ACCESS_SESSION
           GROUP BY ROLLUP(GVHOST, REVISIT_CD)
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.REVISIT_CD = TC.REVISIT_CD
           LEFT OUTER JOIN
           (
           --이탈수
           SELECT
                  GVHOST                                                          AS GVHOST
                , REVISIT_CD                                                      AS REVISIT_CD
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)             AS BOUNCE_CNT
           FROM   (
                  SELECT GVHOST                                                   AS GVHOST
                       , CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END AS REVISIT_CD
                       , SESSION_ID                                               AS SESSION_ID
                       , SUM(PAGE_VIEW)                                           AS PAGE_VIEW
                       , SUM(PAGE_CNT)                                            AS PAGE_CNT
                   FROM TB_ACCESS_SESSION
                   GROUP BY GVHOST, REVISIT_CD, SESSION_ID
            )
            GROUP BY ROLLUP(GVHOST, REVISIT_CD)
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.REVISIT_CD = TD.REVISIT_CD
    GROUP BY ROLLUP(TA.GVHOST, TA.REVISIT_CD)
    """
    spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    s"""
           SELECT
                  GVHOST                                                          AS GVHOST
                , REVISIT_CD                                                      AS REVISIT_CD
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END) AS BOUNCE_CNT
           FROM   (
                  SELECT GVHOST                                                   AS GVHOST
                       , CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END AS REVISIT_CD
                       , SESSION_ID                                               AS SESSION_ID
                       , SUM(PAGE_VIEW)                                           AS PAGE_VIEW
                       , SUM(PAGE_CNT)                                            AS PAGE_CNT
                  FROM TB_ACCESS_SESSION
                  GROUP BY GVHOST, REVISIT_CD, SESSION_ID
                  )
            GROUP BY ROLLUP(GVHOST, REVISIT_CD)
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND REVISIT_INTERVAL='"+revistInterval+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
