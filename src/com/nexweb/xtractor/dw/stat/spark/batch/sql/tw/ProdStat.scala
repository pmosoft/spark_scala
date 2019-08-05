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
 * 설    명 : 일/월별 접속 URL UV 통계
 * 입    력 :
  - TB_PROD_LOG
 * 출    력 : TB_PROD_STAT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */

object ProdStat {

  var spark : SparkSession = null
  var objNm  = "TB_PROD_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_PROD_STAT"; var statisDate = "20190313"; var statisType = "D"
  //var objNm  = "TB_PROD_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_PROD_LOG",statisDate,statisType,"",true)
  }

  def excuteSql() = {

    val qry =
    """
    SELECT
         '"""+statisDate+"""'   AS STATIS_DATE
        ,'"""+statisType+"""'   AS STATIS_TYPE
        , GVHOST                AS GVHOST
        , NVL(P_ID, 'NONE')     AS P_ID
        , 'NONE'                AS CATE_ID
        , COUNT(1)              AS P_COUNT
        , COUNT(DISTINCT V_ID)  AS U_COUNT
    FROM  TB_PROD_LOG
    GROUP BY GVHOST, NVL(P_ID, 'NONE')
    """
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
