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

TB_WL_URL_ACCESS

 * 출    력 : TB_ACCESS_SESSION
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_ACCESS_SESSION3 {

  var spark : SparkSession = null
  var objNm  = "TB_ACCESS_SESSION3"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_ACCESS_SESSION3";var statisDate = "20190312"; var statisType = "D"
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
    //spark.sql("DROP TABLE TB_WL_URL_ACCESS")
    //spark.sql("SELECT COUNT(*) FROM TB_WL_URL_ACCESS").take(100).foreach(println);
    //spark.sql("SELECT * FROM TB_WL_URL_ACCESS").take(100).foreach(println);
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
				SELECT GVHOST
				   , VHOST
				   , NVL(SESSION_ID, V_ID) AS SESSION_ID
				   , V_ID
				   , T_ID
				   , URL
				   , PREV_DOMAIN
				   , C_TIME
				FROM   TB_WL_URL_ACCESS
				WHERE LENGTH(GVHOST) > 0
				AND LENGTH(URL) > 0
				UNION
				SELECT TA.GVHOST
				   , TA.VHOST
				   , NVL(TA.SESSION_ID, V_ID) AS SESSION_ID
				   , V_ID
				   , T_ID
				   , URL
				   , DOMAIN AS PREV_DOMAIN
				   , C_TIME
				FROM TB_WL_REFERER TA,
				(
				SELECT DISTINCT GVHOST, SESSION_ID
				FROM TB_WL_URL_ACCESS
				WHERE LENGTH(SESSION_ID) > 0
				MINUS
				SELECT DISTINCT GVHOST, SESSION_ID
				FROM TB_WL_REFERER
				WHERE LENGTH(SESSION_ID) > 0
				) TB
				WHERE TA.GVHOST = TB.GVHOST
				AND   TA.SESSION_ID = TB.SESSION_ID
				AND LENGTH(TA.GVHOST) > 0
				AND LENGTH(TA.URL) > 0
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
