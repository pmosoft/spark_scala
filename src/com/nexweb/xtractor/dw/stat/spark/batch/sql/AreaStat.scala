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
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_AREA_STAT
 * 수정내역 :
 * 2019-02-08 | 피승현 | 최초작성
 */
object AreaStat {

  var spark: SparkSession = null
  var objNm = "TB_AREA_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_AREA_STAT";var statisDate = "20190311"; var statisType = "D"

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
    LoadTable.lodAccessTable(spark, statisDate, statisType)
  }

/*

      INSERT INTO TB_AREA_STAT
      (STATIS_DATE, VHOST, AREA_CD, CLICK_VISITOR, CLICK_CNT)
      SELECT TO_CHAR(C_TIME, 'YYYYMMDD') AS STATIS_DATE, VHOST, AREA_CODE, COUNT(UNIQUE V_ID) AS UVIEW, COUNT(V_ID) AS PVIEW
      FROM TB_WL_URL_ACCESS
      WHERE C_TIME BETWEEN TO_DATE(?,'YYYYMMDD') AND TO_DATE(?,'YYYYMMDD') +0.99999
      AND  AREA_CODE IS NOT NULL
      GROUP BY TO_CHAR(C_TIME, 'YYYYMMDD'), VHOST, AREA_CODE


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'             AS STATIS_DATE
         , GVHOST                      AS GVHOST
         , REPLACE(AREA_CODE,'#','')   AS AREA_CODE
         , COUNT(DISTINCT V_ID)        AS CLICK_VISITOR
         , COUNT(V_ID)                 AS CLICK_CNT
    FROM   TB_WL_URL_ACCESS
    WHERE  LENGTH(AREA_CODE) > 0
    GROUP BY GVHOST, REPLACE(AREA_CODE,'#','')
    """
    spark.sql(qry).take(100).foreach(println);

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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
