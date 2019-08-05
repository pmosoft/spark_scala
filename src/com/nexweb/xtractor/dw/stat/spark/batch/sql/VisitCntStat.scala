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
 * 설    명 : 일/월별 방문자 추이 통계
 * 입    력 :

TB_WL_REFERER
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_VISIT_CNT_STAT
 * 수정내역 :
 * 2019-01-18 | 피승현 | 최초작성
 */
object VisitCntStat {

  var spark: SparkSession = null
  var objNm = "TB_VISIT_CNT_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_VISIT_CNT_STAT"; var statisDate = "20190312"; var statisType = "D"
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
    //------------------------------------------------------
    println(objNm + ".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def loadTables() = {
    //LoadTable.lodRefererTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)

  }

/*

INSERT  INTO TB_VISIT_CNT_STAT
(STATIS_DATE,VISIT_CNT_CD,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
WITH VISIT_CD AS
(
SELECT  V_ID, VHOST,
  FN_VISIT_COUNT_CD(COUNT(1)) VISIT_COUNT_CD
FROM    TB_WL_REFERER_SEGMENT
GROUP   BY V_ID, VHOST
)
SELECT   ? AS STATIS_DATE,
   NVL(TA.VISIT_COUNT_CD, '00') AS VISIT_COUNT_CD,
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
   FROM
   (
  --방문수, 방문자수, 신규 방문자수
  SELECT  NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
       NVL(VISIT_COUNT_CD, '00') AS VISIT_COUNT_CD,
       SUM(VISIT_CNT) AS VISIT_CNT,
       COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
       COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
       COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
       FROM
       (
    SELECT  V_ID, VHOST,
      FN_VISIT_COUNT_CD(COUNT(1)) VISIT_COUNT_CD, COUNT(1) AS VISIT_CNT
    FROM    TB_WL_REFERER_SEGMENT
    GROUP   BY V_ID, VHOST
       )
       GROUP BY ROLLUP(VHOST, VISIT_COUNT_CD)
  ) TA,
  (
  --로그인수, 로그인 방문자수
  SELECT   NVL(TA.VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
     NVL(VISIT_COUNT_CD, '00') AS VISIT_COUNT_CD,
       SUM(LOGIN_CNT) LOGIN_CNT,
       COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
  FROM    TB_MEMBER_CLASS_SEG_STAT TA, VISIT_CD TB
  WHERE TA.V_ID = TB.V_ID
  AND   TA.VHOST = TB.VHOST
  GROUP BY ROLLUP (TA.VHOST, VISIT_COUNT_CD)
  ) TB,
  (
  --체류시간, 페이지뷰
  SELECT VHOST, ATTR_ID, VISIT_COUNT_CD,
  SUM(PAGE_VIEW) PAGE_VIEW,
  SUM(DUR_TIME) DUR_TIME
    FROM
    (
    SELECT  NVL(TA.VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
      NVL(VISIT_COUNT_CD, '00') AS VISIT_COUNT_CD,
      SUM(PAGE_VIEW) PAGE_VIEW,
      SUM(DUR_TIME) DUR_TIME
    FROM    TB_SEGMENT_SESSION_LOG TA, VISIT_CD TB
    WHERE TA.V_ID = TB.V_ID
    AND   TA.VHOST = TB.VHOST
    GROUP   BY ROLLUP(TA.VHOST, VISIT_COUNT_CD)
    )
  GROUP BY VHOST, ATTR_ID, VISIT_COUNT_CD
  ) TC,
  (
  -- 이탈수
  SELECT  NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
    NVL(VISIT_COUNT_CD, '00') AS VISIT_COUNT_CD,
       COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
       SUM(PAGE_CNT) PAGE_CNT
  FROM    (
      SELECT  SESSION_ID, TA.VHOST, NVL(VISIT_COUNT_CD, '01') AS VISIT_COUNT_CD,
     COUNT(1) PAGE_VIEW,
     COUNT(DISTINCT TA.URL) PAGE_CNT
      FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, VISIT_CD TC
      WHERE  TA.URL = TB.URL
      AND    TA.VHOST = TB.VHOST
      AND      TB.SUB_TYPE ='Y'
      AND    TA.V_ID = TC.V_ID
      AND    TA.VHOST = TC.VHOST
      GROUP   BY SESSION_ID, TA.VHOST, VISIT_COUNT_CD
       )
  GROUP   BY ROLLUP(VHOST, VISIT_COUNT_CD)
  ) TD
  WHERE     TA.VHOST = TB.VHOST(+) AND TA.VISIT_COUNT_CD = TB.VISIT_COUNT_CD(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
  AND       TA.VHOST = TC.VHOST(+) AND TA.VISIT_COUNT_CD = TC.VISIT_COUNT_CD(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
  AND       TA.VHOST = TD.VHOST(+) AND TA.VISIT_COUNT_CD = TD.VISIT_COUNT_CD(+) AND TA.ATTR_ID = TD.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                             AS STATIS_DATE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                                 AS GVHOST
         , CASE WHEN TA.VISIT_CNT_CD IS NULL THEN 'TOTAL'
                WHEN TA.VISIT_CNT_CD = '1' THEN '01'
                WHEN TA.VISIT_CNT_CD = '2' THEN '02'
                WHEN TA.VISIT_CNT_CD = '3' THEN '03'
                WHEN TA.VISIT_CNT_CD = '4' THEN '04'
                WHEN TA.VISIT_CNT_CD = '5' THEN '05'
                WHEN TA.VISIT_CNT_CD = '6' THEN '06'
                ELSE TA.VISIT_CNT_CD
           END                                                                                         AS VISIT_CNT_CD
         , 'ALL'                                                                                       AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)            AS VISIT_CNT
         , SUM(CASE WHEN TE.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TE.TOT_VISITOR_CNT   END)            AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TE.NEW_VISITOR_CNT   END)            AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TE.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TE.RE_VISITOR_CNT    END)            AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)            AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)            AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)            AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END)            AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)            AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)            AS BOUNCE_CNT
    FROM
           (
           --방문수, 방문자수, 신규방문자, 재방문자
           SELECT GVHOST                                                                        AS GVHOST
                , VISIT_CNT_CD                                                                  AS VISIT_CNT_CD
                , SUM(VISIT_CNT)                                                                AS VISIT_CNT
           FROM   (
                  SELECT
                         GVHOST                                                                 AS GVHOST
                       , V_ID                                                                   AS V_ID
                       , CASE WHEN COUNT(*) >= 6 THEN 6 ELSE COUNT(*) END                       AS VISIT_CNT_CD
                       , COUNT(*)                                                               AS VISIT_CNT
                  FROM   TB_REFERER_SESSION
                  GROUP BY GVHOST, V_ID
                  )
           GROUP BY GVHOST, VISIT_CNT_CD
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인수, 로그인 방문자수
           SELECT
                 GVHOST                                                               AS GVHOST
               , VISIT_CNT_CD                                                         AS VISIT_CNT_CD
               , SUM(LOGIN_CNT)                                                       AS LOGIN_CNT
               , SUM(LOGIN_VISITOR_CNT)                                               AS LOGIN_VISITOR_CNT
           FROM  (
                 SELECT
                       GVHOST                                                           AS GVHOST
                     , V_ID                                                             AS V_ID
                     , CASE WHEN COUNT(*) >= 6 THEN 6 ELSE COUNT(*) END                 AS VISIT_CNT_CD
                     , COUNT(*)                                                         AS LOGIN_CNT
                     , COUNT(DISTINCT T_ID)                                             AS LOGIN_VISITOR_CNT
                 FROM  TB_MEMBER_CLASS_SESSION
                 GROUP BY GVHOST, V_ID
                 )
           GROUP BY GVHOST, VISIT_CNT_CD
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.VISIT_CNT_CD = TB.VISIT_CNT_CD
           LEFT OUTER JOIN
           (
           --체류시간, 페이지뷰, 페이지수
           SELECT
                  GVHOST                                                                AS GVHOST
                , VISIT_CNT_CD                                                          AS VISIT_CNT_CD
                , SUM(PAGE_CNT)                                                         AS PAGE_CNT
                , SUM(PAGE_VIEW)                                                        AS PAGE_VIEW
                , SUM(DUR_TIME)                                                         AS DUR_TIME
           FROM   (
                  SELECT
                         GVHOST                                                         AS GVHOST
                       , V_ID                                                           AS V_ID
                       , CASE WHEN COUNT(*) >= 6 THEN 6 ELSE COUNT(*) END               AS VISIT_CNT_CD
                       , SUM(PAGE_CNT)                                                  AS PAGE_CNT
                       , SUM(PAGE_VIEW)                                                 AS PAGE_VIEW
                       , SUM(DUR_TIME)                                                  AS DUR_TIME
                  FROM   TB_ACCESS_SESSION2
                  GROUP BY GVHOST, V_ID
                  )
           GROUP BY GVHOST, VISIT_CNT_CD
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.VISIT_CNT_CD = TC.VISIT_CNT_CD
           LEFT OUTER JOIN
           (
           --이탈수
           SELECT
                  GVHOST                                                                    AS GVHOST
                , VISIT_CNT_CD                                                              AS VISIT_CNT_CD
                , SUM(BOUNCE_CNT)                                                           AS BOUNCE_CNT
           FROM   (
                  SELECT
                         GVHOST                                                             AS GVHOST
                       , V_ID                                                               AS V_ID
                       , CASE WHEN COUNT(*) >= 6 THEN 6 ELSE COUNT(*) END                   AS VISIT_CNT_CD
                       , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)                AS BOUNCE_CNT
                  FROM   TB_ACCESS_SESSION
                  GROUP BY GVHOST, V_ID
                  )
           GROUP BY GVHOST, VISIT_CNT_CD
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.VISIT_CNT_CD = TD.VISIT_CNT_CD
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT GVHOST                                                                        AS GVHOST
                , VISIT_CNT_CD                                                                  AS VISIT_CNT_CD
                , COUNT(DISTINCT V_ID)                                                          AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   (
                  SELECT
                         GVHOST                                                                 AS GVHOST
                       , V_ID                                                                   AS V_ID
                       , CASE WHEN VISIT_CNT >= 6 THEN 6 ELSE VISIT_CNT END                       AS VISIT_CNT_CD
                  FROM   TB_REFERER_SESSION3
                  )
           GROUP BY GVHOST, VISIT_CNT_CD
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.VISIT_CNT_CD = TE.VISIT_CNT_CD
    GROUP BY TA.GVHOST, TA.VISIT_CNT_CD
    """
    //spark.sql(qry).take(100).foreach(println);
    /*
    qry =
    """
           SELECT
                  GVHOST                                                                AS GVHOST
                , VISIT_CNT_CD                                                          AS VISIT_CNT_CD
                , SUM(PAGE_VIEW)                                                        AS PAGE_VIEW
                , SUM(DUR_TIME)                                                         AS DUR_TIME
           FROM   (
                  SELECT
                         GVHOST                                                         AS GVHOST
                       , V_ID                                                                   AS V_ID
                       , CASE WHEN COUNT(*) >= 6 THEN 6 ELSE COUNT(*) END               AS VISIT_CNT_CD
                       , SUM(PAGE_VIEW)                                                 AS PAGE_VIEW
                       , SUM(DUR_TIME)                                                  AS DUR_TIME
                  FROM   TB_ACCESS_SESSION
                  GROUP BY GVHOST, V_ID
                  )
           GROUP BY GVHOST, VISIT_CNT_CD
    """
    val sqlDf = spark.sql(qry).take(100).foreach(println);

    spark.sql("SELECT * FROM TB_VISIT_CNT_STAT").take(100).foreach(println);
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
