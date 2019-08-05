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

TB_WL_REFERER
TB_WL_URL_ACCESS
TB_MEMBER_CLASS

 * 출    력 : TB_VISIT_TIME_STAT
 * 수정내역 :
 * 2019-01-17 | 피승현 | 최초작성
 */
object VisitTimeStat {

  var spark: SparkSession = null
  var objNm = "TB_VISIT_TIME_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_VISIT_TIME_STAT";var statisDate = "20190311"; var statisType = "D"
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
    //LoadTable.lodRefererTable(spark, statisDate, statisType)
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
    //LoadTable.lodMemberTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION2"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

INSERT INTO TB_VISIT_TIME_STAT
(STATIS_TIME,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
SELECT   NVL(TA.STATIS_TIME, ?||'24') STATIS_TIME,
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
        SELECT  /*+ PARALLEL(ta,4)*/ NVL(TO_CHAR(V_DATE, 'YYYYMMDDHH24'), ?||'24') AS STATIS_TIME, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
             COUNT(1) VISIT_CNT,
             COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
        FROM    TB_WL_REFERER_SEGMENT ta
        GROUP   BY ROLLUP(VHOST, TO_CHAR(V_DATE, 'YYYYMMDDHH24'))
) TA,
(
        --로그인수, 로그인 방문자수
        SELECT  /*+ PARALLEL(ta,4)*/ NVL(STATIS_TIME, ?||'24') AS STATIS_TIME, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
             SUM(LOGIN_CNT) LOGIN_CNT,
             COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
        FROM    TB_MEMBER_CLASS_SEG_STAT ta
        GROUP   BY ROLLUP(VHOST, STATIS_TIME)
) TB,
(
        --체류시간, 페이지뷰
        SELECT  /*+ PARALLEL(ta,4)*/ NVL(STATIS_TIME, ?||'24') AS STATIS_TIME, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
             SUM(PAGE_VIEW) PAGE_VIEW,
             SUM(DUR_TIME) DUR_TIME
        FROM    TB_SEGMENT_SESSION_TIME_LOG ta
        GROUP   BY ROLLUP(VHOST, STATIS_TIME)
) TC,
(
        -- 이탈수
  SELECT  /*+ PARALLEL(ta,12)*/ NVL(TO_CHAR(C_TIME, 'YYYYMMDDHH24'), ?||'24') STATIS_TIME, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
       COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
       SUM(PAGE_CNT) PAGE_CNT
  FROM    (
      SELECT /*+ PARALLEL(ta,12)*/ SESSION_ID, TA.VHOST,
     MIN(C_TIME) C_TIME,
     COUNT(1) PAGE_VIEW,
     COUNT(DISTINCT TA.URL) PAGE_CNT
      FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB
      WHERE  TA.URL = TB.URL
      AND    TA.VHOST = TB.VHOST
      AND      TB.SUB_TYPE ='Y'
      GROUP   BY SESSION_ID, TA.VHOST
       ) ta
  GROUP   BY ROLLUP(VHOST, TO_CHAR(C_TIME, 'YYYYMMDDHH24'))
) TD
    WHERE
    --AND       TA.STATIS_TIME = TB.STATIS_TIME(+) AND TA.VHOST = TB.VHOST(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
    --AND       TA.STATIS_TIME = TC.STATIS_TIME(+) AND TA.VHOST = TC.VHOST(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
    --AND       TA.STATIS_TIME = TD.STATIS_TIME(+) AND TA.VHOST = TD.VHOST(+) AND TA.ATTR_ID = TD.ATTR_ID(+)

            TA.STATIS_TIME = TB.STATIS_TIME(+) AND TA.VHOST = TB.VHOST(+)
    AND  TA.STATIS_TIME = TC.STATIS_TIME(+) AND TA.VHOST = TC.VHOST(+)
    AND  TA.STATIS_TIME = TD.STATIS_TIME(+) AND TA.VHOST = TD.VHOST(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    """
    SELECT
           CASE WHEN TA.STATIS_TIME IS NULL THEN 'TOTAL' ELSE TA.STATIS_TIME END            AS STATIS_TIME
         , CASE WHEN TA.GVHOST      IS NULL THEN 'TOTAL' ELSE TA.GVHOST      END            AS GVHOST
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
           --방문수
           SELECT
                  GVHOST                           AS GVHOST
                , STATIS_TIME                      AS STATIS_TIME
                , COUNT(DISTINCT SESSION_ID)       AS VISIT_CNT
           FROM   TB_REFERER_SESSION2
           GROUP BY GVHOST,STATIS_TIME
           ) TA
           LEFT OUTER JOIN
           (
           --로그인방문자수, 로그인수
           SELECT GVHOST                           AS GVHOST
                , DATE_FORMAT(C_TIME,'yyyyMMddHH') AS STATIS_TIME
                , COUNT(DISTINCT T_ID)            AS LOGIN_VISITOR_CNT
                , COUNT(*)                        AS LOGIN_CNT
           FROM   TB_MEMBER_CLASS_SESSION
           GROUP BY GVHOST, STATIS_TIME
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.STATIS_TIME = TB.STATIS_TIME
           LEFT OUTER JOIN
           (
           --체류시간, 페이지뷰, 페이지수
           SELECT
                  GVHOST                                       AS GVHOST
                , DATE_FORMAT(START_TIME,'yyyyMMddHH')         AS STATIS_TIME
                , SUM(PAGE_VIEW)                               AS PAGE_VIEW
                , SUM(PAGE_CNT)                                AS PAGE_CNT
                , SUM(DUR_TIME)                                AS DUR_TIME
           FROM   TB_ACCESS_SESSION2
           GROUP  BY GVHOST, STATIS_TIME
           ) TC
           ON  TA.GVHOST = TC.GVHOST  AND TA.STATIS_TIME = TC.STATIS_TIME
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST                                                          AS GVHOST
                , STATIS_TIME                                                     AS STATIS_TIME
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END)    AS BOUNCE_CNT
           FROM   (
                  SELECT GVHOST                              AS GVHOST
                       , DATE_FORMAT(START_TIME,'yyyyMMddHH')    AS STATIS_TIME
                       , SESSION_ID                          AS SESSION_ID
                       , SUM(PAGE_VIEW)                      AS PAGE_VIEW
                  FROM   TB_ACCESS_SESSION
                  GROUP BY GVHOST, STATIS_TIME, SESSION_ID
                  )
           GROUP BY GVHOST, STATIS_TIME
           ) TD
           ON  TA.GVHOST = TD.GVHOST  AND TA.STATIS_TIME = TD.STATIS_TIME
           LEFT OUTER JOIN
           (
           --방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST                           AS GVHOST
                , STATIS_TIME                      AS STATIS_TIME
                , COUNT(DISTINCT V_ID)             AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = STATIS_DATE THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != STATIS_DATE THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST,STATIS_TIME
           ) TE
           ON  TA.GVHOST = TE.GVHOST  AND TA.STATIS_TIME = TE.STATIS_TIME
    GROUP BY ROLLUP(TA.STATIS_TIME,TA.GVHOST)
    """
    //spark.sql(qry).take(100).foreach(println);
    /*
    qry =
    """
           SELECT
                  GVHOST                           AS GVHOST
                , DATE_FORMAT(C_TIME,'yyyyMMddHH') AS STATIS_TIME
                , COUNT(DISTINCT SESSION_ID)       AS VISIT_CNT
                , COUNT(DISTINCT V_ID)             AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = STATIS_DATE THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != STATIS_DATE THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_WL_REFERER
           WHERE  GVHOST = 'TMAP'
           GROUP BY GVHOST,STATIS_TIME
    """
    spark.sql(qry).take(100).foreach(println);

    spark.sql("SELECT * FROM TB_VISIT_TIME_STAT").take(100).foreach(println);

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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_TIME LIKE '" + statisDate + "%' OR STATIS_TIME = 'TOTAL'")
    OJDBC.insertTable(spark, objNm)
  }

}
