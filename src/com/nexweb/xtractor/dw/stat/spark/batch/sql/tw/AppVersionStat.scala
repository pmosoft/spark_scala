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
 * 출    력 : TB_APP_VERSION_STAT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object AppVersionStat {

  var spark : SparkSession = null
  var objNm  = "TB_APP_VERSION_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_APP_VERSION_STAT";var statisDate = "20190326"; var statisType = "D"

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
    //LoadTable.lodAccessTable(spark,statisDate,statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = "" 
    qry =
    s"""
    SELECT '${statisDate}' AS STATIS_DATE, 
	   TA.GVHOST, 
	   TA.OS, 
	   TA.APP_VERSION,
	   VISITOR_CNT, 
	   PAGE_VIEW, 
	   NVL(SVC_UV,0) AS SVC_UV, 
	   NVL(SVC_PV,0) AS SVC_PV, 
	   NVL(LOGIN_UV,0) AS LOGIN_UV, 
	   NVL(LOGIN_PV,0) AS LOGIN_PV 
	FROM (
	SELECT 
			GVHOST,
			OS,
			OPT2 AS APP_VERSION,
			COUNT(DISTINCT V_ID) AS VISITOR_CNT,
			COUNT(V_ID) AS PAGE_VIEW
	FROM TB_ACCESS_DAY
	WHERE GVHOST = 'MA'
	AND LENGTH(V_ID) > 0
	GROUP BY GVHOST, OS, OPT2
	) TA 
    LEFT OUTER JOIN 
    ( 
	SELECT 
			GVHOST,
			OS,
			OPT2 AS APP_VERSION,
			COUNT(DISTINCT U_ID) AS SVC_UV,
			COUNT(U_ID) AS SVC_PV
	FROM TB_ACCESS_DAY
	WHERE GVHOST = 'MA'
	AND LENGTH(U_ID) > 0
	GROUP BY GVHOST, OS, OPT2
	) TB 
    ON TA.GVHOST = TB.GVHOST AND TA.OS = TB.OS AND TA.APP_VERSION = TB.APP_VERSION
	LEFT OUTER JOIN 
	(
	SELECT 
			GVHOST,
			OS,
			OPT2 AS APP_VERSION,
			COUNT(DISTINCT T_ID) AS LOGIN_UV,
			COUNT(T_ID) AS LOGIN_PV
	FROM TB_ACCESS_DAY
	WHERE GVHOST = 'MA'
	AND LENGTH(T_ID) > 0
	GROUP BY GVHOST, OS, OPT2
	) TC
	ON TA.GVHOST = TC.GVHOST AND TA.OS = TC.OS AND TA.APP_VERSION = TC.APP_VERSION
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
