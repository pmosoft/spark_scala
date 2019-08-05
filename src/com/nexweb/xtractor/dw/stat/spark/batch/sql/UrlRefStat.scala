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

 * 출    력 : TB_URL_REF_STAT
 * 수정내역 :
 * 2019-02-09 | 피승현 | 최초작성
 */
object UrlRefStat {

  var spark: SparkSession = null
  var objNm = "TB_URL_REF_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_URL_REF_STAT";var statisDate = "20190311"; var statisType = "D"

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
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION3"       ,statisDate,statisType,"",true)
  }

/*

INSERT INTO TB_URL_REF_STAT
(STATIS_dATE, VHOST , URL, PREV_URL, PAGE_VIEW)
WITH PREV_URL_LIST AS
(
SELECT  V_HOST, URL, COUNT(1) RNUM
FROM    TB_WL_REFERER_SESSION
GROUP   BY V_HOST, URL
)
SELECT  ? AS STATIS_DATE, VHOST, URL, PREV_URL, SUM(PAGE_VIEW) PAGE_VIEW
FROM    (
        SELECT  VHOST, URL, PREV_URL, COUNT(1) PAGE_VIEW
        FROM    (
                SELECT  SESSION_ID, VHOST, V_ID, TA.URL, PREV_URL
                FROM    (
                        SELECT  TA.*, ROW_NUMBER() OVER(PARTITION BY VHOST, URL ORDER BY DECODE(PREV_URL, 'NN', 0, 1), C_TIME, URL) RNUM
                        FROM    (
                                SELECT  TA.*,
                                        NVL(LAG(TA.URL) OVER(PARTITION BY VHOST, V_ID ORDER BY C_TIME, TA.URL), 'NN') PREV_URL
                                FROM    TB_WL_URL_ACCESS_SESSION TA
                                ) TA
                        ) TA, PREV_URL_LIST TB
                WHERE   TA.VHOST = TB.V_HOST
                AND     TA.URL = TB.URL
                AND     TA.RNUM > TB.RNUM
                )
        GROUP   BY VHOST, URL, PREV_URL
        UNION   ALL
        SELECT  VHOST, URL, PREV_URL, COUNT(1) PAGE_VIEW
        FROM    (
                SELECT  V_HOST AS VHOST, URL, NVL(HOST||DIR_CGI, 'NN') PREV_URL
                FROM  TB_WL_REFERER_SESSION
                )
        GROUP   BY VHOST, URL, PREV_URL
        )
GROUP   BY VHOST, URL, PREV_URL

*/

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                  AS STATIS_DATE
         , GVHOST                           AS GVHOST
         , URL                              AS URL
         , PREV_URL                         AS PREV_URL
         , SUM(PAGE_VIEW)                   AS PAGE_VIEW
    FROM   (
           SELECT GVHOST
                , URL
                , PREV_URL
                , COUNT(1) PAGE_VIEW
           FROM   (
                   SELECT GVHOST
                        , SESSION_ID
                        , URL
                        , NVL(BF_URL, PREV_DOMAIN) AS PREV_URL
                   FROM   (
                          SELECT GVHOST
                               , C_TIME
                               , SESSION_ID
                               , URL
                               , LAG(URL) OVER(PARTITION BY GVHOST, V_ID ORDER BY C_TIME ASC ) AS BF_URL
                               , NVL(IF(PREV_DOMAIN==VHOST, 'NONE', PREV_DOMAIN), 'N') AS PREV_DOMAIN
                          FROM   TB_ACCESS_SESSION3
                          WHERE  GVHOST IS NOT NULL
                          )
                  )
           GROUP  BY GVHOST, URL, PREV_URL
           )
    GROUP BY GVHOST, URL, PREV_URL
    """
    //spark.sql(qry).take(100).foreach(println);

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
