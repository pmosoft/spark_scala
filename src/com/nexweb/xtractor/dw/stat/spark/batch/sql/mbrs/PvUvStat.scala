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
  - TB_CATE_STAT
 * 출    력 : POC_PVUV_STAT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object PvUvStat {

  var spark : SparkSession = null
  var objNm  = "POC_PVUV_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "POC_PVUV_STAT";var statisDate = "20190509"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_CATE_STAT" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = "" 
    qry =
    s"""
        SELECT TA.STATIS_DATE, TOT_VISITOR_CNT, TOT_PAGE_VIEW, TMAP_VISITOR_CNT, TMAP_PAGE_VIEW, TMOW_VISITOR_CNT, TMOW_PAGE_VIEW, TMMW_VISITOR_CNT, TMMW_PAGE_VIEW
        FROM(
        SELECT STATIS_DATE, SUM(VISITOR_CNT) AS TOT_VISITOR_CNT, SUM(PAGE_VIEW) AS TOT_PAGE_VIEW
        FROM TB_CATE_STAT
        WHERE STATIS_TYPE = '${statisType}'
        AND STATIS_DATE = '${statisDate}'
        AND CATE_ID IN ('TMAP', 'TMOW', 'TMMW')
        GROUP BY STATIS_DATE
        ) TA,
        (
        SELECT STATIS_DATE, 'ALL' AS CLS,
        SUM(CASE WHEN GVHOST = 'TMAP' THEN VISITOR_CNT ELSE 0 END) AS TMAP_VISITOR_CNT,
        SUM(CASE WHEN GVHOST = 'TMAP' THEN PAGE_VIEW ELSE 0 END) AS TMAP_PAGE_VIEW,
        SUM(CASE WHEN GVHOST = 'TMOW' THEN VISITOR_CNT ELSE 0 END) AS TMOW_VISITOR_CNT,
        SUM(CASE WHEN GVHOST = 'TMOW' THEN PAGE_VIEW ELSE 0 END) AS TMOW_PAGE_VIEW,
        SUM(CASE WHEN GVHOST = 'TMMW' THEN VISITOR_CNT ELSE 0 END) AS TMMW_VISITOR_CNT,
        SUM(CASE WHEN GVHOST = 'TMMW' THEN PAGE_VIEW ELSE 0 END) AS TMMW_PAGE_VIEW
        FROM TB_CATE_STAT
        WHERE STATIS_TYPE = '${statisType}'
        AND STATIS_DATE = '${statisDate}'
        AND CATE_ID IN ('TMAP', 'TMOW', 'TMMW')
        GROUP BY STATIS_DATE
        ) TB
        WHERE TA.STATIS_DATE = TB.STATIS_DATE
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
