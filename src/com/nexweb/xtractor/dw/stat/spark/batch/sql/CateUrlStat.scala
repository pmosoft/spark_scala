package com.nexweb.xtractor.dw.stat.spark.batch.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
import com.nexweb.xtractor.dw.stat.spark.common.OJDBC
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch

/*
 * 설    명 : 일/월별 카테고리별 페이지 분석 통계
 * 입    력 :

 * 출    력 : TB_CATE_URL_STAT
 * 수정내역 :
 * 2019-01-31 | 피승현 | 최초작성
 */
object CateUrlStat {

  var spark : SparkSession = null
  var objNm  = "TB_CATE_URL_STAT"

  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""
  //var objNm  = "TB_CATE_URL_STAT"; var statisDate = "20190311"; var statisType = "D"
  //var objNm  = "TB_CATE_URL_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"CATE_URL_MAP_FRONT"     ,statisDate2,statisType2,"",true)
    LoadTable.lodAllColTable(spark,"TB_PAGE_STAT"           ,statisDate,statisType,"",true)
  }

/*

                INSERT INTO TB_URL_STAT
                (STATIS_DATE, VHOST, URL, PAGE_VIEW, PAGE_CNT, TOT_VISITOR_CNT, DUR_TIME, BOUNCE_CNT, FIRST_CNT, LAST_CNT)
              SELECT  ? STATIS_DATE, TA.VHOST, TA.URL, PAGE_VIEW, PAGE_CNT, TOT_VISITOR_CNT, DUR_TIME, BOUNCE_CNT, FIRST_CNT, LAST_CNT
                FROM    (
                            --페이지뷰/순페이지뷰/방문자수
                            SELECT  TA.URL, VHOST,
                                         COUNT(1) PAGE_VIEW,
                                         COUNT(DISTINCT SESSION_ID) PAGE_CNT,
                                         COUNT(DISTINCT V_ID) TOT_VISITOR_CNT
                            FROM    TB_WL_URL_ACCESS_SESSION TA
                            GROUP   BY TA.URL, VHOST
                            ) TA,
                            (
                            --체류시간
                            SELECT  URL, VHOST,
                             AVG(CASE WHEN DUR_TIME > 1800 THEN 1800 ELSE DUR_TIME END) AS DUR_TIME
                            FROM    (
                                        SELECT  TA.URL, VHOST,
                                                     NVL((LEAD(C_TIME) OVER( PARTITION BY VHOST, SESSION_ID ORDER BY C_TIME )-C_TIME)*86400, 0) DUR_TIME
                                        FROM    TB_WL_URL_ACCESS_SESSION TA
                                        )
                            GROUP   BY URL, VHOST
                            )TB,
                            (
                            --이탈수
                            SELECT  URL, VHOST,
                                         COUNT(1) BOUNCE_CNT
                            FROM    (
                                        SELECT  SESSION_ID, VHOST,
                                                     MIN(TA.URL) URL,
                                                     COUNT(1) PAGE_VIEW
                                        FROM    TB_WL_URL_ACCESS_SESSION TA
                                        GROUP   BY SESSION_ID, VHOST
                                        )
                            WHERE PAGE_VIEW = 1
                            GROUP   BY URL, VHOST
                            ) TC,
                            (
                            --접속종료수
                            SELECT  URL, VHOST,
                                         COUNT(1) LAST_CNT
                            FROM    (
                                        SELECT  SESSION_ID, VHOST,
                                                     TA.URL,
                                                     ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME DESC, URL DESC) RNK
                                        FROM    TB_WL_URL_ACCESS_SESSION TA
                                        )
                            WHERE   RNK=1
                            GROUP   BY URL , VHOST
                            ) TD,
                            (
                            --진입수
                            SELECT  URL, VHOST,
                                         COUNT(1) FIRST_CNT
                            FROM    (
                                        SELECT  SESSION_ID, VHOST,
                                                     TA.URL,
                                                     ROW_NUMBER() OVER(PARTITION BY SESSION_ID, VHOST ORDER BY C_TIME, URL ) RNK
                                        FROM    TB_WL_URL_ACCESS_SESSION TA
                                        )
                            WHERE   RNK=1
                            GROUP   BY URL, VHOST
                            ) TE
                WHERE   TA.URL = TB.URL(+)
                AND     TA.VHOST = TB.VHOST(+)
                AND     TA.URL = TC.URL(+)
                AND     TA.VHOST = TC.VHOST(+)
                AND     TA.URL = TD.URL(+)
                AND     TA.VHOST = TD.VHOST(+)
                AND     TA.URL = TE.URL(+)
                AND     TA.VHOST = TE.VHOST(+)

        INSERT INTO TB_CATE_URL_STAT
        (STATIS_TYPE, STATIS_DATE, CATE_ID, VHOST, PVIEW, VISIT_CNT, VISITOR_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, BOUNCE_CNT, LAST_CNT, REG_DATE)
        WITH CATE_URL_MAP AS (
                    SELECT ANC_CATE_ID, CATE_ID, URL, VHOST
                    FROM (
                        SELECT SUBSTR(TA.CATE_PATH, 1, DECODE(INSTR(TA.CATE_PATH, '>', 2), 0, LENGTH(TA.CATE_PATH) + 1, INSTR(TA.CATE_PATH, '>', 2)) - 1) AS ANC_CATE_ID,
                            TA.CATE_ID, TB.URL, VHOST
                        FROM (
                            SELECT TA.CATE_ID, LEVEL AS DEPTH_LEV, SUBSTR(SYS_CONNECT_BY_PATH(TA.CATE_ID, '>'), 2) AS CATE_PATH
                            FROM TB_CATE_MAP TA
                            WHERE TA.USE_YN = 'Y'
                            START WITH TA.CATE_ID IN (
                                SELECT CATE_ID
                                FROM TB_CATE_MAP
                            )
                            CONNECT BY PRIOR TA.CATE_ID = TA.PRNT_CATE_ID
                            ORDER SIBLINGS BY SEQ
                        ) TA
                        INNER JOIN TB_CATE_URL_MAP TB
                        ON TA.CATE_ID = TB.CATE_ID
                    ) TA
                ),
                CATE_MAP AS (
                    SELECT ANC_CATE_ID, CATE_ID, VHOST
                    FROM CATE_URL_MAP
                    GROUP BY ANC_CATE_ID, CATE_ID, VHOST
                )
                SELECT ? STATIS_TYPE, ? STATIS_DATE, CATE_ID, VHOST,
                    NVL(SUM(PVIEW), 0) AS PVIEW,
                    NVL(SUM(VISIT_CNT), 0) AS VISIT_CNT,
                    NVL(SUM(VISITOR_CNT), 0) AS VISITOR_CNT,
                    NVL(SUM(LOGIN_VISITOR_CNT), 0) AS LOGIN_VISITOR_CNT,
                    NVL(SUM(DUR_TIME), 0) AS DUR_TIME,
                    NVL(SUM(PAGE_CNT), 0) AS PAGE_CNT,
                    NVL(SUM(BOUNCE_CNT), 0) AS BOUNCE_CNT,
                    NVL(SUM(LAST_CNT), 0) AS LAST_CNT,
                    SYSDATE
                FROM (
                    --페이지뷰
                    SELECT /*+ LEADING(TA TB) USE_NL(TA, TB) */
                        ANC_CATE_ID AS CATE_ID, TA.VHOST,
                        SUM(TB.PAGE_VIEW) AS PVIEW,
                        0 AS VISIT_CNT,
                        0 AS VISITOR_CNT,
                        0 AS LOGIN_VISITOR_CNT,
                        AVG(TB.DUR_TIME) AS DUR_TIME,
                        SUM(TB.PAGE_CNT) AS PAGE_CNT,
                        SUM(TB.BOUNCE_CNT)  AS BOUNCE_CNT,
                        SUM(TB.LAST_CNT)  AS LAST_CNT
                    FROM CATE_URL_MAP TA
                    INNER JOIN TB_URL_STAT TB
                    ON TA.URL = TB.URL
                    AND TA.VHOST = TB.VHOST
                    WHERE TB.STATIS_DATE >= ?
                    AND TB.STATIS_DATE <= ?
                    GROUP BY ANC_CATE_ID, TA.VHOST
                    UNION ALL
                    --진입수
                    SELECT /*+ LEADING(TA TB) USE_HASH(TA TB) FULL(TB) PARALLEL(TB 4) PQ_DISTRIBUTE(TB BROADCAST NONE) */
                        ANC_CATE_ID AS CATE_ID, TA.VHOST,
                        0 AS PVIEW,
                        SUM(VISIT_CNT) AS VISIT_CNT,
                        COUNT(DISTINCT V_ID) AS VISITOR_CNT,
                        COUNT(DISTINCT U_ID) AS LOGIN_VISITOR_CNT,
                        0 AS DUR_TIME,
                        0 AS PAGE_CNT,
                        0 AS BOUNCE_CNT,
                        0 AS LAST_CNT
                    FROM CATE_URL_MAP TA
                    INNER JOIN TB_CATE_LOG TB
                    ON TA.CATE_ID = TB.CATE_ID
                    AND TA.URL = TB.URL
                    AND TA.VHOST = TB.VHOST
                    WHERE VISIT_DATE >= ?
                    AND VISIT_DATE <= ?
                    GROUP BY ANC_CATE_ID, TA.VHOST
                ) TA
                GROUP BY CATE_ID, VHOST

        INSERT INTO TB_CATE_URL_STAT (
          STATIS_TYPE, STATIS_DATE, VHOST, URL, PVIEW, VISIT_CNT, VISITOR_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, BOUNCE_CNT, LAST_CNT
        )
                SELECT ? AS STATIS_TYPE, ? AS STATIS_DATE, 'tmembership.tworld.co.kr' VHOST, URL,
                    NVL(SUM(PVIEW), 0) AS PVIEW,
                    NVL(SUM(VISIT_CNT), 0) AS VISIT_CNT,
                    NVL(SUM(VISITOR_CNT), 0) AS VISITOR_CNT,
                    NVL(SUM(LOGIN_VISITOR_CNT), 0) AS LOGIN_VISITOR_CNT,
                    NVL(SUM(DUR_TIME), 0) AS DUR_TIME,
                    NVL(SUM(PAGE_CNT), 0) AS PAGE_CNT,
                    NVL(SUM(BOUNCE_CNT), 0) AS BOUNCE_CNT,
                    NVL(SUM(LAST_CNT), 0) AS LAST_CNT
                FROM (
                    SELECT URL,
                        SUM(PAGE_VIEW) AS PVIEW,
                        0 AS VISIT_CNT,
                        0 AS VISITOR_CNT,
                        0 AS LOGIN_VISITOR_CNT,
                        AVG(DUR_TIME) AS DUR_TIME,
                        SUM(PAGE_CNT) AS PAGE_CNT,
                        SUM(BOUNCE_CNT) AS BOUNCE_CNT,
                        SUM(LAST_CNT) AS LAST_CNT
                    FROM TB_URL_STAT
                    WHERE STATIS_DATE >= ?
                    AND STATIS_DATE <= ?
                    GROUP BY URL
                    UNION ALL
                    SELECT URL,
                        0 AS PVIEW,
                        SUM(VISIT_CNT) AS VISIT_CNT,
                        COUNT(UNIQUE V_ID) AS VISITOR_CNT,
                        COUNT(UNIQUE U_ID) AS LOGIN_VISITOR_CNT,
                        0 AS DUR_TIME,
                        0 AS PAGE_CNT,
                        0 AS BOUNCE_CNT,
                        0 AS LAST_CNT
                    FROM TB_CATE_LOG
                    WHERE VISIT_DATE >= ?
                    AND VISIT_DATE <= ?
                    GROUP BY URL
                ) TA
                GROUP BY URL
 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
          '"""+statisType+"""'           AS STATIS_TYPE
         ,'"""+statisDate+"""'           AS STATIS_DATE
         , TA.GVHOST                     AS GVHOST
         , TA.ANC_CATE_ID                AS CATE_ID
         , TA.URL                        AS URL
         , SUM(PAGE_VIEW)                AS PAGE_VIEW
         , SUM(PAGE_CNT)                 AS PAGE_CNT
         , SUM(VISITOR_CNT)              AS VISITOR_CNT
         , SUM(LOGIN_VISITOR_CNT)        AS LOGIN_VISITOR_CNT
         , SUM(FIRST_CNT)                AS FIRST_CNT
         , SUM(LAST_CNT)                 AS LAST_CNT
         , SUM(DUR_TIME)                 AS DUR_TIME
         , SUM(BOUNCE_CNT)               AS BOUNCE_CNT
    FROM   CATE_URL_MAP_FRONT TA
           LEFT OUTER JOIN
           TB_PAGE_STAT TB
    WHERE  TA.GVHOST = TB.GVHOST
    AND    TA.URL    = TB.URL
    GROUP BY TA.GVHOST, TA.ANC_CATE_ID, TA.URL
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
    SELECT DISTINCT GVHOST
    FROM   TB_PAGE_STAT TB
    """
    spark.sql(qry).take(100).foreach(println);
     */

    /*

    qry =
    s"""
    SELECT DISTINCT GVHOST
    FROM   CATE_URL_MAP_FRONT TB
    """
    spark.sql(qry).take(100).foreach(println);
     */


    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
