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

 * 출    력 : TB_URL_PATH_STAT
 * 수정내역 :
 * 2019-02-09 | 피승현 | 최초작성
 */
object UrlPathStat {

  var spark: SparkSession = null
  var objNm = "TB_URL_PATH_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_URL_PATH_STAT";var statisDate = "20190311"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
  }

/*

        INSERT  INTO TB_URL_PATH_STAT
        ( STATIS_DATE, VHOST, URL, NEXT_URL, PAGE_VIEW, DUR_TIME, VISITOR_CNT )
        SELECT  ? STATIS_DATE, VHOST, URL, NEXT_URL, COUNT(1) PAGE_VIEW, AVG(DUR_TIME) AS DUR_TIME, COUNT(DISTINCT V_ID) VISITOR_CNT
        FROM    (
                SELECT    SESSION_ID, C_TIME, URL, NEXT_URL, VHOST,
                        CASE WHEN DUR_TIME > 1800 THEN 1800 ELSE DUR_TIME END AS DUR_TIME, V_ID
                FROM    (
                            SELECT  SESSION_ID,
                                    C_TIME,
                                    NVL((LEAD(C_TIME) OVER( PARTITION BY VHOST, SESSION_ID ORDER BY C_TIME, URL )-C_TIME)*86400, 0) DUR_TIME,
                                    TA.URL, VHOST,
                                    NVL(LEAD(TA.URL) OVER(PARTITION BY VHOST, SESSION_ID  ORDER BY C_TIME, URL), 'N' ) NEXT_URL, V_ID
                            FROM    TB_WL_URL_ACCESS_SESSION TA
                            ORDER   BY SESSION_ID, C_TIME, VHOST
                        )
                )
        GROUP   BY URL, NEXT_URL, VHOST

*/

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'   AS STATIS_DATE
         , GVHOST            AS GVHOST
         , URL               AS URL
         , NEXT_URL          AS NEXT_URL
         , SUM(PAGE_VIEW)    AS PAGE_VIEW
         , SUM(DUR_TIME)     AS DUR_TIME
         , SUM(VISITOR_CNT)  AS VISITOR_CNT
    FROM   (
           SELECT
                  GVHOST
                , URL
                , CASE WHEN NEXT_URL IS NULL OR NEXT_URL = '' THEN 'NA' ELSE NEXT_URL END AS NEXT_URL
                , SUM(PAGE_VIEW)       AS PAGE_VIEW
                , SUM(DUR_TIME)        AS DUR_TIME
                , COUNT(DISTINCT V_ID) AS VISITOR_CNT
           FROM   TB_ACCESS_SESSION2
           WHERE GVHOST IS NOT NULL
           GROUP BY GVHOST, URL, NEXT_URL
           )
    GROUP BY GVHOST, URL, NEXT_URL
    """
    spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    """

           SELECT GVHOST, URL, PREV_URL, COUNT(1) PAGE_VIEW
           FROM   (
                  SELECT GVHOST
                       , URL
                       ,(CASE WHEN HOST||DIR_CGI IS NULL THEN 'NA' ELSE HOST||DIR_CGI END) AS PREV_URL
                  FROM  TB_WL_REFERER
                  )
           GROUP  BY GVHOST, URL, PREV_URL

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
