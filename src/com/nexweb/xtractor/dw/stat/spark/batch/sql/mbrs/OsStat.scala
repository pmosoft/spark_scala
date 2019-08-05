package com.nexweb.xtractor.dw.stat.spark.batch.sql.mbrs

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
  - TB_MEMBER_CLASS
 * 출    력 : POC_OS_STAT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object OsStat {

  var spark : SparkSession = null
  var objNm  = "POC_OS_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "POC_OS_STAT";var statisDate = "20190508"; var statisType = "D"

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
    LoadTable.lodMemberTable(spark, statisDate, statisType)
    //LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = "" 
    qry =
    s"""
    SELECT '${statisDate}' AS STATIS_DATE, 
	   GVHOST, 
	   NVL(OS, 'ETC') AS OS, 
	   COUNT(DISTINCT LOGIN_ID) AS UNIQUE_VISITOR
	   FROM TB_MEMBER_CLASS
	   WHERE GVHOST IN ('TMAP', 'TMOW', 'TMMW')
	   GROUP BY GVHOST, NVL(OS, 'ETC')
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
