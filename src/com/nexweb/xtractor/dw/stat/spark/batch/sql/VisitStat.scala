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

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSIONq

 * 출    력 : TB_VISIT_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 * 2019-01-05 | 피승현 | poc 로직으로 변경
 */
object VisitStat {

  var spark: SparkSession = null
  var objNm = "TB_VISIT_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_VISIT_STAT";var statisDate = "20190312"; var statisType = "D"
  //var statisDate = "20181219"; var statisType = "D"
  //var objNm = "TB_VISIT_STAT";var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
    //spark.sql("DROP TABLE TB_ACCESS_SESSION")
  }

/*

INSERT INTO TB_VISIT_STAT
(STATIS_TYPE,STATIS_DATE,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
SELECT  'D' STATIS_TYPE,
         TA.STATIS_DATE,
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
    --방문수, 방문자수, 신규 방문자수
        SELECT  /*+ PARALLEL(ta,4)*/ ? STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                 COUNT(1) VISIT_CNT,
                 COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
                 COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                 COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
        FROM    TB_WL_REFERER_SEGMENT ta
        GROUP   BY ROLLUP(VHOST)
) TA,
(
        --로그인수, 로그인 방문자수
        SELECT  /*+ PARALLEL(ta,4)*/ ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                 SUM(LOGIN_CNT) LOGIN_CNT,
                 COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
        FROM    TB_MEMBER_CLASS_SEG_STAT ta
        GROUP   BY ROLLUP(VHOST)
) TB,
(
        --체류시간, 페이지뷰
        SELECT  /*+ PARALLEL(ta,4)*/ ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                 SUM(PAGE_VIEW) PAGE_VIEW,
                 SUM(DUR_TIME) DUR_TIME
        FROM    TB_SEGMENT_SESSION_LOG ta
        GROUP   BY ROLLUP(VHOST)
) TC,
(
        -- 이탈수
        SELECT  /*+ PARALLEL(ta,12)*/ ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                 COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
                 SUM(PAGE_CNT) PAGE_CNT
        FROM    (
                SELECT  /*+ PARALLEL(ta,12)*/ SESSION_ID, TA.VHOST,
                     COUNT(1) PAGE_VIEW,
                     COUNT(DISTINCT TA.URL) PAGE_CNT
                FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB
                WHERE  TA.URL = TB.URL
                AND    TA.VHOST = TB.VHOST
                AND      TB.SUB_TYPE ='Y'
                GROUP   BY SESSION_ID, TA.VHOST
             ) ta
        GROUP   BY ROLLUP(VHOST)
) TD
    WHERE

    -- 20180320 세그아이디 제거

    --AND       TA.STATIS_DATE = TB.STATIS_DATE(+) AND TA.VHOST = TB.VHOST(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
    --AND       TA.STATIS_DATE = TC.STATIS_DATE(+) AND TA.VHOST = TC.VHOST(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
    --AND       TA.STATIS_DATE = TD.STATIS_DATE(+) AND TA.VHOST = TD.VHOST(+) AND TA.ATTR_ID = TD.ATTR_ID(+)

            TA.VHOST = TB.VHOST(+)
    AND        TA.VHOST = TC.VHOST(+)
    AND        TA.VHOST = TD.VHOST(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisType}'                                                                  AS STATIS_TYPE
         , '${statisDate}'                                                                  AS STATIS_DATE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                      AS GVHOST
         , 'ALL'                                                                            AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END) AS VISIT_CNT
         , SUM(CASE WHEN TE.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TE.TOT_VISITOR_CNT   END) AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TE.NEW_VISITOR_CNT   END) AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TE.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TE.RE_VISITOR_CNT    END) AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END) AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END) AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END) AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END) AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END) AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END) AS BOUNCE_CNT
    FROM
           (
           -- 방문수
           SELECT
                  GVHOST               AS GVHOST
                , COUNT(*)             AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT GVHOST                          AS GVHOST
                , COUNT(DISTINCT T_ID)            AS LOGIN_VISITOR_CNT
                , COUNT(*)                        AS LOGIN_CNT
           FROM   TB_MEMBER_CLASS_SESSION
           GROUP BY GVHOST
           ) TB
           ON  TA.GVHOST = TB.GVHOST
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
           SELECT
                  GVHOST          AS GVHOST
                , SUM(PAGE_VIEW)  AS PAGE_VIEW
                , SUM(PAGE_CNT)   AS PAGE_CNT
                , SUM(DUR_TIME)   AS DUR_TIME
           FROM   TB_ACCESS_SESSION2
           GROUP BY GVHOST
           ) TC
           ON  TA.GVHOST = TC.GVHOST
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST          AS GVHOST
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST
           ) TD
           ON  TA.GVHOST = TD.GVHOST
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT(CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  LIKE '${statisDate}%' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT(CASE WHEN '20'||SUBSTR(V_ID, 2, 6) NOT LIKE '${statisDate}%' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST
           ) TE
           ON  TA.GVHOST = TE.GVHOST
    GROUP BY ROLLUP(TA.GVHOST)
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    """
           SELECT
                  GVHOST               AS GVHOST
                , COUNT(*)             AS VISIT_CNT
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST
    """
    spark.sql(qry).take(100).foreach(println);


    qry =
    """
           SELECT
                  GVHOST               AS GVHOST
                , COUNT(*)             AS VISIT_CNT
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST
    """
    spark.sql(qry).take(100).foreach(println);

           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN DEVICE_ID IS NULL OR DEVICE_ID = '' THEN 'NA' ELSE DEVICE_ID END) DEVICE_ID
                , COUNT(*)             AS VISIT_CNT
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST, DEVICE_ID





    qry =
    """
           SELECT
                  GVHOST               AS GVHOST
                , V_ID
                , '20'||SUBSTR(V_ID, 2, 6)
           FROM   TB_REFERER_SESSION
           WHERE  GVHOST = 'TMOW'
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    """
           SELECT
                  SESSION_ID, DUR_TIME
           FROM   TB_ACCESS_SESSION
           WHERE  GVHOST = 'MW'
           AND    SESSION_ID = 'A190130023104244737'
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    """
                  SELECT GVHOST                 AS GVHOST
                       , SESSION_ID             AS SESSION_ID
                       , MIN(C_TIME)            AS START_TIME
                       , MAX(C_TIME)            AS END_TIME
                  FROM   TB_WL_URL_ACCESS
                  WHERE  GVHOST = 'MW'
                  AND    SESSION_ID = 'A190130023104244737'
                  GROUP  BY GVHOST, SESSION_ID
                  ORDER BY GVHOST, SESSION_ID
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "' AND STATIS_TYPE='" + statisType + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
