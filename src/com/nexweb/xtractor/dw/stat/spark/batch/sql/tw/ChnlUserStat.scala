package com.nexweb.xtractor.dw.stat.spark.batch.sql.tw

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
TB_MEMBER_CLASS_SESSIONq

 * 출    력 : TB_CHNL_USER_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 * 2019-01-05 | 피승현 | poc 로직으로 변경
 */
object ChnlUserStat {

  var spark: SparkSession = null
  var objNm = "TB_CHNL_USER_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_CHNL_USER_STAT";var statisDate = "20190306"; var statisType = "D"
  //var statisDate = "20181219"; var statisType = "D"
  //var objNm = "TB_CHNL_USER_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
  }


  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT V_ID, 
      		MAX(CASE WHEN GVHOST = 'OW' THEN V_ID ELSE NULL END) AS OW_VISITOR,
      		MAX(CASE WHEN GVHOST = 'MW' THEN V_ID ELSE NULL END) AS MW_VISITOR,
      		MAX(CASE WHEN GVHOST = 'MA' THEN V_ID ELSE NULL END) AS MA_VISITOR
      FROM TB_REFERER_DAY
      WHERE GVHOST IN ('OW', 'MW', 'MA')
      GROUP BY V_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("VISITOR_LOG");sqlDf2.count()

    qry =
    s"""
      SELECT LOGIN_ID, 
      		MAX(CASE WHEN GVHOST = 'OW' THEN LOGIN_ID ELSE NULL END) AS OW_USER,
      		MAX(CASE WHEN GVHOST = 'MW' THEN LOGIN_ID ELSE NULL END) AS MW_USER,
      		MAX(CASE WHEN GVHOST = 'MA' THEN LOGIN_ID ELSE NULL END) AS MA_USER
      FROM TB_MEMBER_CLASS_DAY
      WHERE GVHOST IN ('OW', 'MW', 'MA')
      GROUP BY LOGIN_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("MEMBER_LOG");sqlDf3.count()

    qry =
    s"""
    SELECT
           '${statisType}' AS STATIS_TYPE
         , TA.STATIS_DATE
         , TOT_VISIT_CNT,
		       TW_VISIT_CNT, MW_VISIT_CNT, MA_VISIT_CNT,
		       TOT_USER_CNT, OW_USER_CNT, MW_USER_CNT, MA_USER_CNT,
		       OW_MW_DUPL_USER_CNT, OW_MA_DUPL_USER_CNT, MW_MA_DUPL_USER_CNT,
		       OW_DUPL_USER_CNT, MW_DUPL_USER_CNT, MA_DUPL_USER_CNT, TOT_DUPL_USER_CNT
		FROM
		(
		SELECT '${statisDate}' AS STATIS_DATE,
				COUNT(DISTINCT V_ID) AS TOT_VISIT_CNT,
				COUNT(DISTINCT CASE WHEN OW_VISITOR IS NOT NULL THEN V_ID ELSE NULL END) AS TW_VISIT_CNT,
				COUNT(DISTINCT CASE WHEN MW_VISITOR IS NOT NULL THEN V_ID ELSE NULL END) AS MW_VISIT_CNT,
				COUNT(DISTINCT CASE WHEN MA_VISITOR IS NOT NULL THEN V_ID ELSE NULL END) AS MA_VISIT_CNT
		FROM VISITOR_LOG
		) TA,
		(
		SELECT '${statisDate}' AS STATIS_DATE,
				COUNT(DISTINCT LOGIN_ID) AS TOT_USER_CNT,
				COUNT(DISTINCT CASE WHEN OW_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS OW_USER_CNT,
				COUNT(DISTINCT CASE WHEN MW_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS MW_USER_CNT,
				COUNT(DISTINCT CASE WHEN MA_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS MA_USER_CNT,
				COUNT(DISTINCT CASE WHEN OW_USER IS NOT NULL AND MW_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS OW_MW_DUPL_USER_CNT,
				COUNT(DISTINCT CASE WHEN OW_USER IS NOT NULL AND MA_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS OW_MA_DUPL_USER_CNT,
				COUNT(DISTINCT CASE WHEN MW_USER IS NOT NULL AND MA_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS MW_MA_DUPL_USER_CNT,
				0 AS OW_DUPL_USER_CNT,
				0 AS MW_DUPL_USER_CNT,
				0 AS MA_DUPL_USER_CNT,
				COUNT(DISTINCT CASE WHEN OW_USER IS NOT NULL AND MW_USER IS NOT NULL AND MA_USER IS NOT NULL THEN LOGIN_ID ELSE NULL END) AS TOT_DUPL_USER_CNT
		FROM MEMBER_LOG
		) TB
		WHERE TA.STATIS_DATE = TB.STATIS_DATE
    """
    //spark.sql(qry).take(100).foreach(println);

 
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "' AND STATIS_TYPE='" + statisType + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
