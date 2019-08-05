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
 * 설    명 : 일/월별
 * 입    력 :
  - TB_CS_CODE_PROCESS
  - TB_CS_CODE_INFO
  - TB_CS_LAST_VER_FRONT
 * 출    력 : - TB_CS_CODE_STAT
 * 수정내역 :
 * 2018-12-10 | 피승현 | 최초작성
 */
object MemberNotVID {

  var spark : SparkSession = null
  var objNm  = "TB_MEMBER_NOT_VID_CNT"
  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_MEMBER_NOT_VID_CNT"; var statisDate = "20190306"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

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
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {

    var qry = ""
    qry =
    s"""
    SELECT '${statisType}' AS STATIS_TYPE, 
    	  '${statisDate}' AS STATIS_DATE, 
    	  COUNT(U_ID) AS NOT_VID_CNT
    FROM (
	    SELECT U_ID, SUM(VID_MARK) AS VID_CNT
	   	FROM (  
		   	SELECT U_ID, 
		           CASE WHEN V_ID IS NULL THEN 0 ELSE 1 END AS VID_MARK
		    FROM TB_MEMBER_CLASS_DAY 
	    )
	    GROUP BY U_ID
    )
    WHERE VID_CNT = 0
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "' AND STATIS_TYPE='" + statisType + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
