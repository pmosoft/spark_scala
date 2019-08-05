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
  - TB_WL_URL_ACCESS
 * 출    력 : POC_XTOS_STAT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object XtosStat {

  var spark : SparkSession = null
  var objNm  = "POC_XTOS_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "POC_XTOS_STAT";var statisDate = "20190521"; var statisType = "D"

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
    LoadTable.lodAccessTable(spark, statisDate, statisType)
  }

  def excuteSql() = {
    
    var qry = ""
    qry =
    s"""
    SELECT '${statisDate}' AS STATIS_DATE, 
	   GVHOST, 
	   OPT3 AS XTOS_ID, 
	   V_ID,
	   T_ID
	   FROM TB_WL_URL_ACCESS
	   WHERE GVHOST IN ('TMAP', 'TMOW', 'TMMW')
	   AND LENGTH(OPT3) > 0
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val obj2Df = spark.sql(qry)
    obj2Df.cache.createOrReplaceTempView("XTOS_LOG");obj2Df.count()
 
    qry =
    s"""
    SELECT '${statisType}' AS STATIS_TYPE, 
    TA.STATIS_DATE, 
    TA.GVHOST, 
    TA.CHNL_ID, 
    TA.XTOS_ID, 
    XTOS_VISITOR,
    XTOS_PAGEVIEW,
    NVL(XTOS_LOGIN_VISITOR,0) AS XTOS_LOGIN_VISITOR, 
    NVL(XTOS_LOGIN_PAGEVIEW,0) AS XTOS_LOGIN_PAGEVIEW
    FROM
    ( 
     SELECT 
     STATIS_DATE, 
	   GVHOST, 
	   SUBSTR(XTOS_ID,1,2) AS CHNL_ID,
	   XTOS_ID, 
	   COUNT(DISTINCT V_ID) AS XTOS_VISITOR,
	   COUNT(V_ID) AS XTOS_PAGEVIEW
	   FROM XTOS_LOG
	   GROUP BY STATIS_DATE, GVHOST, XTOS_ID
	  ) TA
	  LEFT OUTER JOIN 
	  ( 
     SELECT 
     STATIS_DATE, 
	   GVHOST, 
	   SUBSTR(XTOS_ID,1,2) AS CHNL_ID,
	   XTOS_ID, 
	   COUNT(DISTINCT T_ID) AS XTOS_LOGIN_VISITOR,
	   COUNT(T_ID) AS XTOS_LOGIN_PAGEVIEW
	   FROM XTOS_LOG
	   WHERE LENGTH(T_ID) > 0
	   GROUP BY STATIS_DATE, GVHOST, XTOS_ID
	  )  TB
    ON TA.STATIS_DATE = TB.STATIS_DATE
	  AND   TA.GVHOST = TB.GVHOST
	  AND   TA.CHNL_ID = TB.CHNL_ID
	  AND   TA.XTOS_ID = TB.XTOS_ID
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
