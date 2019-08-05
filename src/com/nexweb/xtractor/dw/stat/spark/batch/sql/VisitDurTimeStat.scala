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
 * 설    명 : 시간별 방문자 추이 통계
 * 입    력 :

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_VISIT_DUR_TIME_STAT
 * 수정내역 :
 * 2019-01-21 | 피승현 | 최초작성
 */
object VisitDurTimeStat {

  var spark: SparkSession = null
  var objNm  = "TB_VISIT_DUR_TIME_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_DUR_TIME_STAT"; var statisDate = "20190312"; var statisType = "D"
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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_VISIT_DUR_TIME_STAT(STATIS_DATE,DUR_TIME_CD,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH DUR_CD AS
          (
          SELECT  SESSION_ID, PAGE_VIEW, DUR_TIME, FN_DUR_TIME_CD(DUR_TIME) DUR_TIME_CD, VHOST
          FROM    TB_ACCESS_SESSION
          )
          SELECT ? AS STATIS_DATE,
             NVL(TA.DUR_TIME_CD, '00') AS DUR_TIME_CD,
             NVL(TA.VHOST, 'TOTAL') AS VHOST,
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
          FROM
          (
          -- 방문자수, 방문횟수, 신규방문자, 재방문자
          SELECT  NVL(DUR_TIME_CD, '00') AS DUR_TIME_CD, NVL(TA.VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
            SUM(VISIT_CNT) VISIT_CNT,
            COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
            COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
            COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM
            (
            SELECT  SESSION_ID, VHOST,
               COUNT(1) VISIT_CNT,
               MIN(V_ID) V_ID
            FROM    TB_WL_REFERER_SEGMENT
            GROUP   BY SESSION_ID, VHOST
            ) TA, DUR_CD TB
            WHERE   TA.SESSION_ID = TB.SESSION_ID
            AND     TA.VHOST = TB.VHOST
            GROUP BY ROLLUP(TA.VHOST, DUR_TIME_CD)
          ) TA,
          (
          -- 로그인 수, 로그인 방문자수
          SELECT  NVL(DUR_TIME_CD, '00') AS DUR_TIME_CD, NVL(TA.VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
            SUM(LOGIN_CNT) LOGIN_CNT,
            COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
            FROM
            (
            SELECT  VHOST, L_ID, SESSION_ID, LOGIN_CNT
            FROM    TB_MEMBER_CLASS_SEG_STAT
            ) TA, DUR_CD TB
            WHERE   TA.SESSION_ID = TB.SESSION_ID
            AND TA.VHOST = TB.VHOST
            GROUP   BY ROLLUP(TA.VHOST, DUR_TIME_CD)
          ) TB,
          (
          -- 이탈율, 페이지수
          SELECT  NVL(DUR_TIME_CD, '00') AS DUR_TIME_CD, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
            COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
            SUM(PAGE_CNT) PAGE_CNT
            FROM    (
              SELECT VHOST, FN_DUR_TIME_CD(DUR_TIME) DUR_TIME_CD, SESSION_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(PAGE_CNT) AS PAGE_CNT
              FROM TB_ACCESS_SESSION
              GROUP BY VHOST, FN_DUR_TIME_CD(DUR_TIME), SESSION_ID
            )
            GROUP   BY ROLLUP(VHOST, DUR_TIME_CD)
          ) TC,
          (
          SELECT  NVL(VHOST, 'ALL') AS VHOST, NVL(FN_DUR_TIME_CD(DUR_TIME), '00') AS DUR_TIME_CD, 'ALL' AS ATTR_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW, SUM(DUR_TIME) AS DUR_TIME
            FROM    TB_ACCESS_SESSION
            GROUP BY ROLLUP(VHOST, FN_DUR_TIME_CD(DUR_TIME))
          ) TD
          WHERE TA.VHOST = TB.VHOST(+) AND TA.DUR_TIME_CD = TB.DUR_TIME_CD(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
          AND   TA.VHOST = TC.VHOST(+) AND TA.DUR_TIME_CD = TC.DUR_TIME_CD(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
          AND   TA.VHOST = TD.VHOST(+) AND TA.DUR_TIME_CD = TD.DUR_TIME_CD(+) AND TA.ATTR_ID = TD.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.DUR_TIME_CD                                                                                         AS DUR_TIME_CD
         , TA.GVHOST                                                                                              AS GVHOST
         , 'ALL'                                                                                                  AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)                       AS VISIT_CNT
         , SUM(CASE WHEN TE.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TE.TOT_VISITOR_CNT   END)                       AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TE.NEW_VISITOR_CNT   END)                       AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TE.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TE.RE_VISITOR_CNT    END)                       AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)                       AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)                       AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)                       AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END)                       AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)                       AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)                       AS BOUNCE_CNT
    FROM
           (
           -- 방문수
           SELECT GVHOST                                                                                          AS GVHOST
                , DUR_TIME_CD                                                                                     AS DUR_TIME_CD
                , COUNT(*)                                                                                        AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST, DUR_TIME_CD
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인수, 로그인 방문자수
           SELECT
                  TB.GVHOST                                                        AS GVHOST
                , NVL(TA.DUR_TIME_CD, 99)                                          AS DUR_TIME_CD
                , SUM(TB.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TB.T_ID)                                          AS LOGIN_VISITOR_CNT
           FROM   TB_REFERER_SESSION TA
           RIGHT OUTER JOIN
                  (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , T_ID                                                     AS T_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TB
           ON  TA.GVHOST     = TB.GVHOST
           AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TB.GVHOST, NVL(TA.DUR_TIME_CD, 99)
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.DUR_TIME_CD = TB.DUR_TIME_CD
           LEFT OUTER JOIN
           (
           -- 체류시간, 페이지뷰, 페이지수
           SELECT
                  GVHOST                                                          AS GVHOST
                , DUR_TIME_CD                                                     AS DUR_TIME_CD
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , SUM(PAGE_VIEW)                                                  AS PAGE_VIEW
                , SUM(DUR_TIME)                                                   AS DUR_TIME
           FROM   TB_ACCESS_SESSION2
           GROUP BY GVHOST, DUR_TIME_CD
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.DUR_TIME_CD = TC.DUR_TIME_CD
           LEFT OUTER JOIN
           (
           --이탈수
           SELECT
                  GVHOST                                                          AS GVHOST
                , DUR_TIME_CD                                                     AS DUR_TIME_CD
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END)    AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, DUR_TIME_CD
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.DUR_TIME_CD = TD.DUR_TIME_CD
           LEFT OUTER JOIN
           (
           SELECT
                  GVHOST                           AS GVHOST
                , DUR_TIME_CD                      AS DUR_TIME_CD
                , COUNT(DISTINCT V_ID)             AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = STATIS_DATE THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != STATIS_DATE THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST, DUR_TIME_CD
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.DUR_TIME_CD = TE.DUR_TIME_CD
    GROUP BY TA.GVHOST, TA.DUR_TIME_CD
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    """
           SELECT GVHOST                                                                                          AS GVHOST
                , VHOST
                , V_ID
                , SESSION_ID
                , URL
           FROM   TB_WL_URL_ACCESS
           WHERE  GVHOST = 'TMOW'
    """
    spark.sql(qry).take(100).foreach(println);

    spark.sql("SELECT * FROM TB_VISIT_DUR_TIME_STAT").take(100).foreach(println);

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
