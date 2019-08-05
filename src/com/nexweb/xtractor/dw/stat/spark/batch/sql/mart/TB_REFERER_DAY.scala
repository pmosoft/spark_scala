package com.nexweb.xtractor.dw.stat.spark.batch.sql.mart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch

/*
 * 설    명 : [일별] 세션아이디별 접속 정보
 * 입    력 :

TB_WL_REFERER

 * 출    력 : TB_REFERER_DAY
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_REFERER_DAY {

  var spark : SparkSession = null
  var objNm  = "TB_REFERER_DAY"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_REFERER_DAY";var statisDate = "20190512"; var statisType = "D"
  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut()
  }

  def loadTables() = {
    LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodRefererTable(spark, statisDate, statisType)
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT GVHOST
            , VHOST
            , NVL(SESSION_ID, V_ID) AS SESSION_ID
            , URL
            , V_ID
            , U_ID
            , T_ID
            , C_TIME
            , HOST
            , DIR_CGI
            , DOMAIN
            , CATEGORY
            , V_IP
            , TRIM(REPLACE(REPLACE(SUBSTR(KEYWORD, 1, 150),'?', ''),'？', '')) AS KEYWORD
            , NVL(MOBILE_YN, 'ETC') AS MOBILE_YN
            , NVL(OS, 'ETC') AS OS
            , NVL(BROWSER, 'ETC') AS BROWSER
            , NVL(OS_VER, 'ETC') AS OS_VER
            , NVL(BROWSER_VER, 'ETC') AS BROWSER_VER
            , NVL(XLOC, 'ETC') AS XLOC
            , NVL(LANG, 'ETC') AS LANG
            , NVL(SUBSTR(DEVICE_ID,1,50), 'ETC') AS DEVICE_ID
      FROM   TB_WL_REFERER
      WHERE GVHOST IN ('OW', 'MA', 'MW')
      AND   LENGTH(URL) > 0
      UNION
      SELECT GVHOST, VHOST, SESSION_ID, URL, V_ID, U_ID, T_ID, C_TIME, HOST, '' AS DIR_CGI, 
             DOMAIN, CATEGORY, V_IP, TRIM(REPLACE(REPLACE(SUBSTR(KEYWORD, 1, 150),'?', ''),'？', '')) AS KEYWORD, MOBILE_YN, OS, BROWSER, OS_VER, BROWSER_VER, XLOC, LANG, DEVICE_ID
      FROM
          (
          SELECT TA.GVHOST, VHOST, NVL(TA.SESSION_ID, V_ID) AS SESSION_ID, URL, V_ID, U_ID, T_ID, C_TIME, F_REF_HOST AS HOST, '' AS DIR_CGI, 
                 '' AS DOMAIN, '' AS CATEGORY, IP AS V_IP, KEYWORD, 
                 MOBILE_YN, OS, BROWSER, OS_VER, BROWSER_VER, XLOC, LANG, 
                 SUBSTR(DEVICE_ID,1,50) AS DEVICE_ID,
                 ROW_NUMBER() OVER(PARTITION BY TA.GVHOST, TA.SESSION_ID ORDER BY C_TIME  ASC) AS RNUM
          FROM TB_WL_URL_ACCESS TA,
                (
                SELECT DISTINCT GVHOST, SESSION_ID
                FROM TB_WL_URL_ACCESS
                WHERE LENGTH(SESSION_ID) > 0
                AND GVHOST IN ('OW', 'MA', 'MW')
                MINUS
                SELECT DISTINCT GVHOST, SESSION_ID
                FROM TB_WL_REFERER
                WHERE LENGTH(SESSION_ID) > 0
                AND GVHOST IN ('OW', 'MA', 'MW')
                ) TB
          WHERE TA.GVHOST IN ('OW', 'MA', 'MW')
          AND   LENGTH(TA.URL) > 0
          AND   TA.GVHOST = TB.GVHOST
          AND   TA.SESSION_ID = TB.SESSION_ID
      )
      WHERE RNUM = 1
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------

    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
