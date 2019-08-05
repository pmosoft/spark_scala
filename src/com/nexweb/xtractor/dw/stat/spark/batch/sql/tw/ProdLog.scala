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
 * 설    명 : 일별
 * 입    력 :
  - TB_WL_URL_ACCESS
 * 출    력 : TB_PROD_LOG
 * 수정내역 :
 * 2018-11-28 | 피승현 | 최초작성
 */
object ProdLog {

  var spark : SparkSession = null
  var objNm  = "TB_PROD_LOG"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_PROD_LOG";var statisDate = "20190305"; var statisType = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
         '${statisDate}'   AS STATIS_DATE
        , GVHOST        AS GVHOST
        , VHOST         AS VHOST
        , URL           AS URL
        , SUBSTR(P_ID,1,10)      AS P_ID
        , V_ID          AS V_ID
        , U_ID          AS U_ID
    FROM  TB_ACCESS_DAY
    WHERE P_ID IS NOT NULL
    AND GVHOST IN ('OW', 'MW', 'MA')
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


  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
