package com.nexweb.xtractor.dw.stat.spark.batch.sql.tw

import java.util.Calendar
import java.text.SimpleDateFormat
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
import com.nexweb.xtractor.dw.stat.spark.common.Batch

/*
 * 설    명 : 일/월별 통합 방문자 통계
 * 입    력 :

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_TOTAL_VISIT_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 * 2019-01-05 | 피승현 | poc 로직으로 변경
 */
object TotalVisitStat {

  var spark: SparkSession = null
  var objNm = "TB_TOTAL_VISIT_STAT"

  var statisDate = ""
  var statisType = ""
  var whereCond = ""
  //var objNm = "TB_TOTAL_VISIT_STAT";var statisDate = "20190508"; var statisType = "D"
  //var statisDate = "20181219"; var statisType = "D"
  //var objNm = "TB_TOTAL_VISIT_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"

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
  
//  val getStatDt:Integer = {
//    val dataFormat = new SimpleDateFormat("yyyyMMdd")
//    val cal = Calendar.getInstance()
//    cal.setTime(dataFormat.parse(statisDate))
//    cal.add(Calendar.DAY_OF_YEAR, -1)
//    cal.get(Calendar.DAY_OF_WEEK)
//  }
//  
//  val getStartWeek:String = {
//    
//    if(getStatDt == 1){
//    val dataFormat = new SimpleDateFormat("yyyyMMdd")
//    val cal = Calendar.getInstance()
//    cal.setTime(dataFormat.parse(statisDate))
//    cal.add(Calendar.DAY_OF_YEAR,-8)
//    dataFormat.format(cal.getTime())
//    } else {
//    val dataFormat = new SimpleDateFormat("yyyyMMdd")
//    val cal = Calendar.getInstance()
//    cal.setTime(dataFormat.parse(statisDate))
//    cal.add(Calendar.DAY_OF_YEAR,-(getStatDt - 1))
//    dataFormat.format(cal.getTime())
//    }
//  }
//  
//  val getEndWeek:String = {
//    
//    if(getStatDt == 1){
//    val dataFormat = new SimpleDateFormat("yyyyMMdd")
//    val cal = Calendar.getInstance()
//    cal.setTime(dataFormat.parse(statisDate))
//    dataFormat.format(cal.getTime())
//    } else {
//    val dataFormat = new SimpleDateFormat("yyyyMMdd")
//    val cal = Calendar.getInstance()
//    cal.setTime(dataFormat.parse(statisDate))
//    cal.add(Calendar.DAY_OF_YEAR,-(getStatDt -7))
//    dataFormat.format(cal.getTime())
//    }
//  }
  //1,2,3,4,5
  
  
  

  def executeWeekly() = {
    //------------------------------------------------------
    println(objNm + ".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "W"
    
    //whereCond = "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') >= '"+getStartWeek+"' AND DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') <= '"+getEndWeek+"' "
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }
    
  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3" ,statisDate,statisType,"",true)
    LoadTable.lodMemberTable(spark, statisDate, statisType)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT V_ID, 
      		MAX(CASE WHEN GVHOST IN ('OW','MW','MA') THEN 'Y' ELSE NULL END) AS TW_VISITOR, 
      		MAX(CASE WHEN GVHOST IN ('TDOW','TWMW') THEN 'Y' ELSE NULL END) AS TD_VISITOR, 
      		MAX(CASE WHEN GVHOST IN ('TMOW','TMMW','TMAP') THEN 'Y' ELSE NULL END) AS TM_VISITOR 
      FROM TB_REFERER_SESSION3
      GROUP BY V_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("TOTAL_VID");sqlDf2.count()

    qry =
    s"""
      SELECT U_ID, 
      		MAX(CASE WHEN GVHOST IN ('OW','MW','MA') THEN 'Y' ELSE NULL END) AS TW_SVC_ID, 
      		MAX(CASE WHEN GVHOST IN ('TDOW','TWMW') THEN 'Y' ELSE NULL END) AS TD_SVC_ID, 
      		MAX(CASE WHEN GVHOST IN ('TMOW','TMMW','TMAP') THEN 'Y' ELSE NULL END) AS TM_SVC_ID 
      FROM TB_MEMBER_CLASS
      GROUP BY U_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("TOTAL_UID");sqlDf3.count()

    qry =
    s"""
      SELECT LOGIN_ID, 
      		MAX(CASE WHEN GVHOST IN ('OW','MW','MA') THEN 'Y' ELSE NULL END) AS TW_LOGIN_ID, 
      		MAX(CASE WHEN GVHOST IN ('TDOW','TWMW') THEN 'Y' ELSE NULL END) AS TD_LOGIN_ID, 
      		MAX(CASE WHEN GVHOST IN ('TMOW','TMMW','TMAP') THEN 'Y' ELSE NULL END) AS TM_LOGIN_ID 
      FROM TB_MEMBER_CLASS
      GROUP BY LOGIN_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf4 = spark.sql(qry)
    sqlDf4.cache.createOrReplaceTempView("TOTAL_LOGINID");sqlDf4.count()

    qry =
    s"""
		SELECT '${statisType}' AS STATIS_TYPE
		, '${statisDate}' AS STATIS_DATE
		, TA.SITE_GB
		, VISIT_CNT
		, SVC_CNT
		, LOGIN_CNT
  		FROM
    		(
    		SELECT 
    		    'T1' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_VISITOR = 'Y' AND TD_VISITOR = 'Y'  THEN V_ID ELSE NULL END) AS VISIT_CNT
    		FROM TOTAL_VID
    		) TA,
    		(
    		SELECT 
    		    'T1' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_SVC_ID = 'Y' AND TD_SVC_ID = 'Y'  THEN U_ID ELSE NULL END) AS SVC_CNT
    		FROM TOTAL_UID
    		) TB,
    		(
    		SELECT 
    		    'T1' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_LOGIN_ID = 'Y' AND TD_LOGIN_ID = 'Y'  THEN LOGIN_ID ELSE NULL END) AS LOGIN_CNT
    		FROM TOTAL_LOGINID
    		) TC
  		WHERE TA.SITE_GB = TB.SITE_GB AND TA.SITE_GB = TC.SITE_GB
  	UNION
		SELECT '${statisType}' AS STATIS_TYPE
		, '${statisDate}' AS STATIS_DATE
		, TA.SITE_GB
		, VISIT_CNT
		, SVC_CNT
		, LOGIN_CNT
  		FROM
    		(
    		SELECT 
    		    'T2' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_VISITOR = 'Y' AND TM_VISITOR = 'Y'  THEN V_ID ELSE NULL END) AS VISIT_CNT
    		FROM TOTAL_VID
    		) TA,
    		(
    		SELECT 
    		    'T2' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_SVC_ID = 'Y' AND TM_SVC_ID = 'Y'  THEN U_ID ELSE NULL END) AS SVC_CNT
    		FROM TOTAL_UID
    		) TB,
    		(
    		SELECT 
    		    'T2' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_LOGIN_ID = 'Y' AND TM_LOGIN_ID = 'Y'  THEN LOGIN_ID ELSE NULL END) AS LOGIN_CNT
    		FROM TOTAL_LOGINID
    		) TC
  		WHERE TA.SITE_GB = TB.SITE_GB AND TA.SITE_GB = TC.SITE_GB
  	UNION
		SELECT '${statisType}' AS STATIS_TYPE
		, '${statisDate}' AS STATIS_DATE
		, TA.SITE_GB
		, VISIT_CNT
		, SVC_CNT
		, LOGIN_CNT
  		FROM
    		(
    		SELECT 
    		    'T3' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TD_VISITOR = 'Y' AND TM_VISITOR = 'Y'  THEN V_ID ELSE NULL END) AS VISIT_CNT
    		FROM TOTAL_VID
    		) TA,
    		(
    		SELECT 
    		    'T3' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TD_SVC_ID = 'Y' AND TM_SVC_ID = 'Y'  THEN U_ID ELSE NULL END) AS SVC_CNT
    		FROM TOTAL_UID
    		) TB,
    		(
    		SELECT 
    		    'T3' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TD_LOGIN_ID = 'Y' AND TM_LOGIN_ID = 'Y'  THEN LOGIN_ID ELSE NULL END) AS LOGIN_CNT
    		FROM TOTAL_LOGINID
    		) TC
  		WHERE TA.SITE_GB = TB.SITE_GB AND TA.SITE_GB = TC.SITE_GB
  	UNION
		SELECT '${statisType}' AS STATIS_TYPE
		, '${statisDate}' AS STATIS_DATE
		, TA.SITE_GB
		, VISIT_CNT
		, SVC_CNT
		, LOGIN_CNT
  		FROM
    		(
    		SELECT 
    		    'T4' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_VISITOR = 'Y' AND TD_VISITOR = 'Y' AND TM_VISITOR = 'Y'  THEN V_ID ELSE NULL END) AS VISIT_CNT
    		FROM TOTAL_VID
    		) TA,
    		(
    		SELECT 
    		    'T4' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_SVC_ID = 'Y' AND TD_SVC_ID = 'Y' AND TM_SVC_ID = 'Y'  THEN U_ID ELSE NULL END) AS SVC_CNT
    		FROM TOTAL_UID
    		) TB,
    		(
    		SELECT 
    		    'T4' AS SITE_GB, 
    				COUNT(DISTINCT CASE WHEN TW_LOGIN_ID = 'Y' AND TD_LOGIN_ID = 'Y' AND TM_LOGIN_ID = 'Y'  THEN LOGIN_ID ELSE NULL END) AS LOGIN_CNT
    		FROM TOTAL_LOGINID
    		) TC
  		WHERE TA.SITE_GB = TB.SITE_GB AND TA.SITE_GB = TC.SITE_GB
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
