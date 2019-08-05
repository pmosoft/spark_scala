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
 * 수정내역 :
 * 2019-01-28 | 피승현 | 최초작성
 */
object PageStat {

  var spark: SparkSession = null
  var objNm = "TB_PAGE_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_PAGE_STAT"; var statisDate = "20190313"; var statisType = "D"
  //var objNm = "TB_PAGE_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M";

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
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2" ,statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_PAGE_STAT
          (STATIS_DATE,VHOST,URL,ATTR_ID,PAGE_VIEW,PAGE_CNT,TOT_VISITOR_CNT,FIRST_CNT,LAST_CNT,SCROLL_CNT,DUR_TIME,BOUNCE_CNT,ART_YN,CREATE_DATE,ART_SUBJECT,CATEGORY,SUB_CATEGORY)
                    SELECT  ? STATIS_DATE,
            TA.VHOST,
            TA.URL,
            TA.ATTR_ID,
            NVL(PAGE_VIEW, 0) AS PAGE_VIEW,
            NVL(PAGE_CNT, 0) AS PAGE_CNT,
            NVL(TOT_VISITOR_CNT, 0) AS TOT_VISITOR_CNT,
            NVL(FIRST_CNT, 0) AS FIRST_CNT,
            NVL(LAST_CNT, 0) AS LAST_CNT,
            0 AS SCROLL_CNT,
            NVL(ROUND(DUR_TIME,2), 0) AS DUR_TIME,
            NVL(BOUNCE_CNT, 0) AS BOUNCE_CNT,
            '' ART_YN,
            '' CREATE_DATE,
            '' ART_SUBJECT,
            '' CATEGORY,
            '' SUB_CATEGORY
          FROM    (
              --페이지뷰/방문자수
              SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL')AS URL,
              'ALL' AS ATTR_ID,
                  COUNT(1) PAGE_VIEW,
                COUNT(DISTINCT SESSION_ID) PAGE_CNT,
                  COUNT(DISTINCT V_ID) TOT_VISITOR_CNT
              FROM    (
              SELECT VHOST,
                CASE WHEN URL LIKE '/20dbnews/%' THEN '20dbnews' ELSE URL END AS URL,
                ART_YN, SESSION_ID, V_ID,
                                  ART_SUBJECT, CATEGORY, SUB_CATEGORY, CREATE_DATE
                FROM TB_WL_URL_ACCESS_SEGMENT
                 ) TA, TB_PAGE_DUMMY TB
              WHERE   TA.URL = TB.URL(+)
              GROUP   BY  ROLLUP((VHOST, DECODE(TB.URL, NULL, TA.URL, DUMMY_URL)))
              ) TA,
              (
              --체류시간
              SELECT  NVL(VHOST, 'TOTAL') AS VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL') AS URL, 'ALL' AS ATTR_ID,
                ROUND(SUM(CASE WHEN DUR_TIME > 1800 THEN 1800 ELSE DUR_TIME END)) AS DUR_TIME
              FROM    (
                    SELECT  VHOST, CASE WHEN URL LIKE '/20dbnews/%' THEN '20dbnews' ELSE URL END AS URL,
                     NVL((LEAD(C_TIME) OVER( PARTITION BY VHOST, SESSION_ID ORDER BY C_TIME )-C_TIME)*86400, 0) DUR_TIME
                    FROM    TB_WL_URL_ACCESS_SEGMENT
                    ) TA, TB_PAGE_DUMMY TB
              WHERE   TA.URL = TB.URL(+)
              GROUP   BY ROLLUP(VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL'))
              )TB,
              (
              --이탈수
                SELECT  NVL(VHOST, 'TOTAL') AS VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL') AS URL, 'ALL' AS ATTR_ID,
                 COUNT(1) BOUNCE_CNT
                FROM    (
                SELECT  SESSION_ID, VHOST,
                       MIN(CASE WHEN URL LIKE '/20dbnews/%' THEN '20dbnews' ELSE URL END) AS URL,
                       COUNT(1) PAGE_VIEW
                FROM    TB_WL_URL_ACCESS_SEGMENT
                GROUP   BY SESSION_ID, VHOST
                ) TA, TB_PAGE_DUMMY TB
                WHERE   TA.URL = TB.URL(+)
                AND     PAGE_VIEW = 1
                GROUP   BY ROLLUP (VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL'))
              ) TC,
              (
              --접속종료수
              SELECT  NVL(VHOST, 'TOTAL') AS VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL') AS URL, 'ALL' AS ATTR_ID,
                COUNT(1) LAST_CNT
              FROM    (
                    SELECT  SESSION_ID, VHOST,
                     CASE WHEN URL LIKE '/20dbnews/%' THEN '20dbnews' ELSE URL END AS URL,
                     ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME DESC, URL DESC) RNK
                    FROM    TB_WL_URL_ACCESS_SEGMENT
                    ) TA, TB_PAGE_DUMMY TB
              WHERE   TA.URL = TB.URL(+)
              AND     RNK=1
              GROUP   BY ROLLUP(VHOST, DECODE(TB.URL, NULL, TA.URL, DUMMY_URL))
              ) TD,
              (
              --진입수
              SELECT  NVL(VHOST, 'TOTAL') AS VHOST, NVL(DECODE(TB.URL, NULL, TA.URL, DUMMY_URL), 'TOTAL') AS URL, 'ALL' AS ATTR_ID,
                     COUNT(1) FIRST_CNT
              FROM    (
                    SELECT  SESSION_ID, VHOST,
                     CASE WHEN URL LIKE '/20dbnews/%' THEN '20dbnews' ELSE URL END AS URL,
                     ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME, URL ) RNK
                    FROM    TB_WL_URL_ACCESS_SEGMENT
                    ) TA, TB_PAGE_DUMMY TB
              WHERE   TA.URL = TB.URL(+)
              AND     RNK=1
              GROUP   BY ROLLUP( VHOST, DECODE(TB.URL, NULL, TA.URL, DUMMY_URL) )
              ) TE
          WHERE   TA.URL = TB.URL(+) AND     TA.VHOST = TB.VHOST(+) AND   TA.ATTR_ID = TB.ATTR_ID(+)
          AND     TA.URL = TC.URL(+) AND     TA.VHOST = TC.VHOST(+) AND   TA.ATTR_ID = TC.ATTR_ID(+)
          AND     TA.URL = TD.URL(+) AND     TA.VHOST = TD.VHOST(+) AND   TA.ATTR_ID = TD.ATTR_ID(+)
          AND     TA.URL = TE.URL(+) AND     TA.VHOST = TE.VHOST(+) AND   TA.ATTR_ID = TE.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                  AS STATIS_DATE
         , TA.GVHOST                       AS GVHOST
         , TA.URL                             AS URL
         , 'ALL'                                                                            AS ATTR_ID
         , SUM(CASE WHEN TA.PAGE_VIEW         IS NULL THEN 0 ELSE TA.PAGE_VIEW         END) AS PAGE_VIEW
         , SUM(CASE WHEN TA.PAGE_CNT          IS NULL THEN 0 ELSE TA.PAGE_CNT          END) AS PAGE_CNT
         , SUM(CASE WHEN TA.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TA.TOT_VISITOR_CNT   END) AS VISITOR_CNT
         , SUM(CASE WHEN TA.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TA.LOGIN_VISITOR_CNT END) AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TD.FIRST_CNT         IS NULL THEN 0 ELSE TD.FIRST_CNT         END) AS FIRST_CNT
         , SUM(CASE WHEN TE.LAST_CNT          IS NULL THEN 0 ELSE TE.LAST_CNT          END) AS LAST_CNT
         , SUM(CASE WHEN TA.DUR_TIME          IS NULL THEN 0 ELSE TA.DUR_TIME          END) AS DUR_TIME
         , SUM(CASE WHEN TC.BOUNCE_CNT        IS NULL THEN 0 ELSE TC.BOUNCE_CNT        END) AS BOUNCE_CNT
    FROM
           (
           -- 페이지뷰, 페이지수, 방문자수, 로그인방문자수
           SELECT GVHOST                     AS GVHOST
                , URL                        AS URL
                , SUM(PAGE_VIEW)             AS PAGE_VIEW
                , SUM(PAGE_CNT)              AS PAGE_CNT
                , SUM(DUR_TIME)              AS DUR_TIME
                , COUNT(DISTINCT V_ID)       AS TOT_VISITOR_CNT
                , COUNT(DISTINCT T_ID)       AS LOGIN_VISITOR_CNT
           FROM   TB_ACCESS_SESSION2
           WHERE  GVHOST IS NOT NULL
           GROUP BY GVHOST, URL
           ) TA
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST          AS GVHOST
                , URL             AS URL
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           WHERE  GVHOST IS NOT NULL
           GROUP BY GVHOST, URL
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.URL = TC.URL
           LEFT OUTER JOIN
           (
           --진입수
           SELECT GVHOST    AS GVHOST
                , URL       AS URL
                , COUNT(*)  AS FIRST_CNT
           FROM   (
                  SELECT GVHOST
                       , URL
                       , SESSION_ID
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME ASC ) RNK
                  FROM   TB_ACCESS_SESSION2
                  )
           WHERE  RNK=1
           GROUP BY GVHOST, URL
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.URL = TD.URL
           LEFT OUTER JOIN
           (
           --접속종료수
           SELECT GVHOST    AS GVHOST
                , URL       AS URL
                , COUNT(*)  AS LAST_CNT
           FROM   (
                  SELECT GVHOST
                       , SESSION_ID
                       , URL
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME DESC) AS RNK
                  FROM   TB_ACCESS_SESSION2
                  )
           WHERE  RNK=1
           GROUP BY GVHOST, URL
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.URL = TE.URL
    GROUP BY TA.GVHOST, TA.URL
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    """
           -- 이탈수
           SELECT
                  GVHOST          AS GVHOST
                , URL             AS URL
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, URL
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate+ "'")
    OJDBC.insertTable(spark, objNm)
  }

}
