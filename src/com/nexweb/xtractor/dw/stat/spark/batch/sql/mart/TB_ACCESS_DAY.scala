package com.nexweb.xtractor.dw.stat.spark.batch.sql.mart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch

/*
 * 설    명 : [일별] 세션아이디별 접속 정보
 * 입    력 :

TB_ACCESS_DAY

 * 출    력 : TB_ACCESS_DAY
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_ACCESS_DAY {

  var spark : SparkSession = null
  var objNm  = "TB_ACCESS_DAY"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_ACCESS_DAY";var statisDate = "20190312"; var statisType = "D"
  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut()
  }

  def loadTables() = {
    LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodRefererTable(spark, statisDate, statisType)
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
		SELECT 
			GVHOST,
			VHOST,
			URL,
			V_ID,
			U_ID,
			T_ID,
			C_TIME,
			IP,
			PREV_URL,
			P_ID,
			KEYWORD,
			PREV_DOMAIN,
			NVL(SESSION_ID, V_ID) AS SESSION_ID,
			F_REF_HOST,
      NVL(OS, 'ETC') AS OS,
      NVL(BROWSER, 'ETC') AS BROWSER,
      NVL(OS_VER, 'ETC') AS OS_VER,
      NVL(BROWSER_VER, 'ETC') AS BROWSER_VER,
			NVL(SUBSTR(DEVICE_ID,1,50), 'ETC') AS DEVICE_ID,
			LOGIN_TYPE,
			FRONT_URL,
			USER_URL,
			OPT_PARAM,
			SVC_ID,
			OPT2
		FROM TB_WL_URL_ACCESS
		WHERE GVHOST IN ('OW', 'MA', 'MW')
		AND LENGTH(URL) > 0
		UNION
		SELECT TA.GVHOST
			, VHOST
			, URL
			, V_ID
			, U_ID
			, T_ID
			, C_TIME
			, V_IP AS IP
			, DIR_CGI AS PREV_URL
			, '' AS P_ID
			, KEYWORD
			, DOMAIN AS PREV_DOMAIN
			, NVL(TA.SESSION_ID, V_ID) AS SESSION_ID
			, HOST AS F_REF_HOST
      , NVL(OS, 'ETC') AS OS
      , NVL(BROWSER, 'ETC') AS BROWSER
      , NVL(OS_VER, 'ETC') AS OS_VER
      , NVL(BROWSER_VER, 'ETC') AS BROWSER_VER
			, NVL(SUBSTR(DEVICE_ID,1,50), 'ETC') AS DEVICE_ID
			, LOGIN_TYPE
			, URL AS FRONT_URL
			, '' AS USER_URL
			, '' AS OPT_PARAM
			, '' AS SVC_ID
			, OPT2
		FROM TB_WL_REFERER TA,
		(
		SELECT DISTINCT GVHOST, SESSION_ID
			FROM TB_WL_URL_ACCESS
			WHERE LENGTH(SESSION_ID) > 0
			AND GVHOST IN ('OW', 'MA', 'MW')
		MINUS
		SELECT DISTINCT GVHOST, SESSION_ID
			FROM TB_WL_REFERER
			WHERE LENGTH(SESSION_ID) > 0
			AND GVHOST IN ('OW', 'MA', 'MW')
		) TB
		WHERE TA.GVHOST IN ('OW', 'MA', 'MW')
		AND TA.GVHOST = TB.GVHOST
		AND TA.SESSION_ID = TB.SESSION_ID
		AND LENGTH(URL) > 0
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------

    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
    //spark.sql("DROP TABLE TB_ACCESS_SESSION")
    //sqlDf.cache.createOrReplaceTempView("TB_ACCESS_SESSION");sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
