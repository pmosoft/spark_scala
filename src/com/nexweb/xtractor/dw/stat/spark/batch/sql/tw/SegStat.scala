package com.nexweb.xtractor.dw.stat.spark.batch.sql.tw


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
 * 설    명 : 일/월별 메뉴별 방문자 통계
 * 입    력 :
  - TB_WL_URL_ACCESS
  - TB_SEG_MENU
  - TB_SEG_URL_MAPPING
  - TB_WL_PV_STAT

 * 출    력 : TB_SEG_STAT
 * 수정내역 :
 * 2018-11-27 | 피승현 | 최초작성
 */
object SegStat {

  var spark : SparkSession = null
  var objNm  = "TB_SEG_STAT"
  var obj2Nm = "TB_SEG_URL_TMP"


  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""

  //var objNm  = "TB_SEG_STAT";var obj2Nm = "TB_SEG_URL_TMP";var statisDate = "20190303";var statisType = "D"
  //var objNm  = "TB_SEG_STAT"; var obj2Nm = "TB_SEG_URL_TMP"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D" 

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
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
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def loadTables() = {
    //LoadTable.lodAccessTable(spark,statisDate,statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_SEG_MENU",statisDate2,statisType2,"",true)
    LoadTable.lodAllColTable(spark,"TB_SEG_URL_MAPPING",statisDate2,statisType2,"",true)
    LoadTable.lodAllColTable(spark,"TB_WL_PV_STAT",statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT  DISTINCT TA.GVHOST, TA.MENU_ID, TB.V_ID, 
        '${statisDate}'       AS STATIS_DATE
    FROM
        (
        SELECT TA.GVHOST
             , TA.MENU_ID
             , TB.URL
        FROM   TB_SEG_MENU        TA
             , TB_SEG_URL_MAPPING TB
        WHERE  TA.MENU_ID = TB.MENU_ID
        AND    TA.USE_FLAG = 'Y'
        ) TA,
        (
        SELECT GVHOST, V_ID, URL
        FROM   TB_ACCESS_DAY
        WHERE  V_ID IS NOT NULL
        ) TB
    WHERE TA.GVHOST = TB.GVHOST
    AND   TA.URL    = TB.URL
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val obj2Df = spark.sql(qry)
    obj2Df.cache.createOrReplaceTempView(obj2Nm);obj2Df.count()

    qry =
    s"""
    SELECT
           '${statisType}'     AS STATIS_TYPE
         , '${statisDate}'     AS STATIS_DATE
         , TA.GVHOST           AS GVHOST
         , TA.MENU_ID          AS MENU_ID
         , CASE WHEN 'D' = '${statisType}' THEN '${statisDate}' ELSE CONCAT('${statisType}','01') END AS BATCH_START_DATE
         , CASE WHEN 'D' = '${statisType}' THEN '${statisDate}'
                ELSE DATE_FORMAT(LAST_DAY(CONCAT(SUBSTR('${statisDate}',1,4),'-',SUBSTR('${statisDate}',5,2))),'yyyyMMdd')
           END AS BATCH_END_DATE
         , TA.VISITOR_CNT      AS VISITOR_CNT
         , TB.PVIEW            AS PVIEW
    FROM
        (
        SELECT
              GVHOST      AS GVHOST
            , MENU_ID     AS MENU_ID
            , COUNT(V_ID) AS VISITOR_CNT
        FROM  TB_SEG_URL_TMP
        GROUP BY GVHOST, MENU_ID
        ) TA ,
        (
        SELECT TB.GVHOST
             , TB.MENU_ID
             , SUM(TA.PVIEW) AS PVIEW
        FROM
            (
            SELECT GVHOST
                 , URL
                 , PVIEW
            FROM   TB_WL_PV_STAT
            ) TA,
            (
            SELECT TA.GVHOST
                 , TA.MENU_ID
                 , TB.URL
            FROM   TB_SEG_MENU TA
                 , TB_SEG_URL_MAPPING TB
            WHERE  TA.GVHOST   = TB.GVHOST
            AND    TA.MENU_ID  = TB.MENU_ID
            AND    TA.USE_FLAG = 'Y'
            AND    TA.DAY      = 'Y'
            ) TB
        WHERE TA.GVHOST = TB.GVHOST AND TA.URL = TB.URL
        GROUP BY TB.GVHOST, TB.MENU_ID
        ) TB
    WHERE TA.GVHOST      = TB.GVHOST
    AND   TA.MENU_ID     = TB.MENU_ID
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
    MakeParquet.dfToParquet(obj2Nm,true,statisDate)
  }

  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "' AND STATIS_TYPE='" + statisType + "'")
    OJDBC.insertTable(spark, objNm)
  }
}
