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
TB_WL_URL_ACCESS
TB_MEMBER_CLASS

 * 출    력 : TB_URL_PRD_STAT
 * 수정내역 :
 * 2019-01-28 | 피승현 | 최초작성
 */
object UrlPrdStat {

  var spark: SparkSession = null
  var objNm = "TB_URL_PRD_STAT"

  var statisDate = ""
  var statisType = ""
  // var objNm = "TB_URL_PRD_STAT"; var statisDate = "20190312"; var statisType = "D"
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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
  }

/*

                    INSERT INTO TB_URL_PRD_STAT(STATIS_DATE, VHOST, ATTR_ID, PAGE_VIEW, PAGE_CNT, VISIT_CNT, TOT_VISITOR_CNT, FIRST_CNT, LAST_CNT, SCROLL_CNT, DUR_TIME, BOUNCE_CNT)
                    SELECT  ? STATIS_DATE,
                            TA.VHOST,
                            TA.ATTR_ID,
                            NVL(PAGE_VIEW, 0) AS PAGE_VIEW,
                            NVL(PAGE_CNT, 0) AS PAGE_CNT,
                            NVL(VISIT_CNT, 0) AS VISIT_CNT,
                            NVL(TOT_VISITOR_CNT, 0) AS TOT_VISITOR_CNT,
                            NVL(FIRST_CNT, 0) AS FIRST_CNT,
                            NVL(LAST_CNT, 0) AS LAST_CNT,
                            0 AS SCROLL_CNT,
                            NVL(ROUND(DUR_TIME,2), 0) AS DUR_TIME,
                            NVL(BOUNCE_CNT, 0) AS BOUNCE_CNT
                        FROM
                            (
                            --방문수/방문자수
                            SELECT NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
                                   COUNT(1) VISIT_CNT,
                                   COUNT(DISTINCT V_ID) TOT_VISITOR_CNT
                                FROM    TB_WL_REFERER_SEGMENT TA
                                GROUP   BY  ROLLUP(VHOST)
                            ) TA,
                            (
                            --체류시간/페이지뷰
                            SELECT  NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                                    SUM(PAGE_VIEW) PAGE_VIEW,
                                    SUM(DUR_TIME) DUR_TIME
                                FROM    TB_SEGMENT_SESSION_LOG
                                GROUP   BY ROLLUP(VHOST)
                            )TB,
                            (
                            --이탈수/페이지수
                            SELECT  NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
                                    COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
                                    SUM(PAGE_CNT) PAGE_CNT
                                FROM
                                    (
                                    SELECT  SESSION_ID, TA.VHOST,
                                            COUNT(1) PAGE_VIEW,
                                            COUNT(DISTINCT TA.URL) PAGE_CNT
                                        FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB
                                        WHERE  TA.URL = TB.URL
                                        AND    TA.VHOST = TB.VHOST
                                        AND      TB.SUB_TYPE ='Y'
                                        GROUP   BY SESSION_ID, TA.VHOST
                                    )
                                GROUP   BY ROLLUP(VHOST)
                            ) TC,
                            (
                            --접속종료수
                            SELECT  NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
                                    COUNT(1) LAST_CNT
                                FROM
                                    (
                                    SELECT  SESSION_ID, VHOST,
                                            ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME DESC, URL DESC) RNK
                                        FROM    TB_WL_URL_ACCESS_SEGMENT TA
                                    )
                                WHERE   RNK=1
                                GROUP   BY ROLLUP(VHOST)
                            ) TD,
                            (
                            --진입수
                            SELECT  NVL(VHOST, 'TOTAL') AS VHOST, 'ALL' AS ATTR_ID,
                                    COUNT(1) FIRST_CNT
                                FROM
                                    (
                                    SELECT  SESSION_ID, VHOST,
                                            ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME, URL ) RNK
                                        FROM    TB_WL_URL_ACCESS_SEGMENT TA
                                    )
                                WHERE   RNK=1
                                GROUP   BY ROLLUP(VHOST)
                            ) TE
                        WHERE   TA.VHOST = TB.VHOST(+) AND   TA.ATTR_ID = TB.ATTR_ID(+)
                        AND     TA.VHOST = TC.VHOST(+) AND   TA.ATTR_ID = TC.ATTR_ID(+)
                        AND     TA.VHOST = TD.VHOST(+) AND   TA.ATTR_ID = TD.ATTR_ID(+)
                        AND     TA.VHOST = TE.VHOST(+) AND   TA.ATTR_ID = TE.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                  AS STATIS_DATE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                      AS GVHOST
         , 'ALL'                                                                            AS ATTR_ID
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END) AS PAGE_VIEW
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END) AS PAGE_CNT
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END) AS VISIT_CNT
         , SUM(CASE WHEN TG.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TG.TOT_VISITOR_CNT   END) AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.FIRST_CNT         IS NULL THEN 0 ELSE TE.FIRST_CNT         END) AS FIRST_CNT
         , SUM(CASE WHEN TF.LAST_CNT          IS NULL THEN 0 ELSE TF.LAST_CNT          END) AS LAST_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END) AS DUR_TIME
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
                  GVHOST                                                 AS GVHOST
                , SUM(PAGE_VIEW)                                         AS PAGE_VIEW
                , SUM(PAGE_CNT)                                          AS PAGE_CNT
                , SUM(DUR_TIME)                                          AS DUR_TIME
           FROM   TB_ACCESS_SESSION2
           GROUP BY GVHOST
           ) TC
           ON  TA.GVHOST = TC.GVHOST
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST                                                 AS GVHOST
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)    AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST
           ) TD
           ON  TA.GVHOST = TD.GVHOST
           LEFT OUTER JOIN
           (
           --진입수
           SELECT GVHOST    AS GVHOST
                , COUNT(*)  AS FIRST_CNT
           FROM   (
                  SELECT GVHOST
                       , SESSION_ID
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME ASC ) RNK
                  FROM   TB_ACCESS_SESSION2
                  )
           WHERE  RNK=1
           GROUP BY GVHOST
           ) TE
           ON  TA.GVHOST = TE.GVHOST
           LEFT OUTER JOIN
           (
           --접속종료수
           SELECT GVHOST    AS GVHOST
                , COUNT(*)  AS LAST_CNT
           FROM   (
                  SELECT GVHOST
                       , SESSION_ID
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME DESC) AS RNK
                  FROM   TB_ACCESS_SESSION2
                  )
           WHERE  RNK=1
           GROUP BY GVHOST
           ) TF
           ON  TA.GVHOST = TF.GVHOST
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST
           ) TG
           ON  TA.GVHOST = TG.GVHOST         
    GROUP BY TA.GVHOST
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    """
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
