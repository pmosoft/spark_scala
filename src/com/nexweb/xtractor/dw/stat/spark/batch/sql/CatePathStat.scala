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
 * 설    명 :
 * 입    력 :

TB_WL_URL_ACCESS
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_CATE_PATH_STAT
 * 수정내역 :
 * 2019-02-09 | 피승현 | 최초작성
 */
object CatePathStat {

  var spark: SparkSession = null
  var objNm = "TB_CATE_PATH_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_CATE_PATH_STAT";var statisDate = "20190311"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_NCATE_URL_MAP_FRONT"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
  }

/*

        INSERT  INTO TB_CATE_PATH_STAT
        ( STATIS_DATE, VHOST, CATE_ID, NEXT_CATE_ID, PAGE_VIEW, DUR_TIME )
        SELECT  ? STATIS_DATE, VHOST, CATE_ID, NEXT_CATE_ID, COUNT(1) PAGE_VIEW, AVG(DUR_TIME) DUR_TIME
        FROM    (
                SELECT    SESSION_ID, VHOST,
                           C_TIME,
                           CASE WHEN DUR_TIME > 1800 THEN 1800 ELSE DUR_TIME END AS DUR_TIME,
                            CATE_ID,
                            NEXT_CATE_ID
            FROM    (
                        SELECT  SESSION_ID, TA.VHOST,
                                 C_TIME,
                                 (NVL(LEAD(C_TIME) OVER(PARTITION BY TA.VHOST, SESSION_ID ORDER BY C_TIME, TA.URL), C_TIME ) - C_TIME) * 86400 DUR_TIME,
                                 CATE_ID,
                                 NVL(LEAD(CATE_ID) OVER(PARTITION BY TA.VHOST, SESSION_ID ORDER BY C_TIME, TA.URL), 'N' ) NEXT_CATE_ID
                        FROM    TB_WL_URL_ACCESS_SESSION TA,
                                (
                                SELECT TO_CHAR(CATE_ID) CATE_ID, URL, VHOST
                                FROM  TB_CATE_URL_MAP
                                ) TB
                        WHERE   TA.URL = TB.URL
                        AND     TA.VHOST = TB.VHOST
                        ORDER   BY TA.VHOST, SESSION_ID, C_TIME
                        )
                )
        GROUP   BY VHOST, CATE_ID, NEXT_CATE_ID

*/

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'    AS STATIS_DATE
         , GVHOST             AS GVHOST
         , CATE_ID            AS CATE_ID
         , NEXT_CATE_ID       AS NEXT_CATE_ID
         , SUM(PAGE_VIEW)     AS PAGE_VIEW
         , SUM(DUR_TIME)      AS DUR_TIME
         , SUM(VISITOR_CNT)   AS VISITOR_CNT
    FROM   (
           SELECT
                  GVHOST
                , CATE_ID
                , CASE WHEN NEXT_CATE_ID IS NULL OR NEXT_CATE_ID = '' THEN 'NA' ELSE NEXT_CATE_ID END AS NEXT_CATE_ID
                , SUM(PAGE_VIEW)       AS PAGE_VIEW
                , SUM(DUR_TIME)        AS DUR_TIME
                , COUNT(DISTINCT V_ID) AS VISITOR_CNT
           FROM   (
                  SELECT TA.GVHOST
                       , TB.CATE_ID
                       , LEAD(TB.CATE_ID) OVER(PARTITION BY TA.GVHOST, TA.SESSION_ID ORDER BY TA.START_TIME ) AS NEXT_CATE_ID
                       , TA.PAGE_VIEW
                       , TA.DUR_TIME
                       , TA.V_ID
                  FROM   TB_ACCESS_SESSION2  TA,
                         TB_NCATE_URL_MAP_FRONT TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.URL    = TB.URL
                  ) TA
           WHERE LENGTH(CATE_ID) > 0
           GROUP BY GVHOST, CATE_ID, NEXT_CATE_ID
           )
    GROUP BY GVHOST, CATE_ID, NEXT_CATE_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    """

           SELECT
                  TA.GVHOST
                , TB.CATE_ID
                , LEAD(TB.CATE_ID) OVER(PARTITION BY TA.GVHOST, TA.SESSION_ID ORDER BY TA.START_TIME, TA.URL) NEXT_CATE_ID
                , TA.PAGE_VIEW
                , TA.DUR_TIME
                , TA.V_ID
           FROM   TB_ACCESS_SESSION  TA,
                  CATE_URL_MAP_FRONT TB
           WHERE  TA.GVHOST = TB.GVHOST
           AND    TA.URL    = TB.URL

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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
