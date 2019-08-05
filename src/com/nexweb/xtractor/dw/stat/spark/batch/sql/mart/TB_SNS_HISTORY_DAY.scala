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
 * 설    명 : [일별] SNS 공유 정보
 * 입    력 :

TB_SNS_HISTORY

 * 출    력 : TB_ACCESS_SESSION
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_SNS_HISTORY_DAY {

  var spark : SparkSession = null
  var objNm  = "TB_SNS_HISTORY_DAY"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_SNS_HISTORY_DAY";var statisDate = "20190508"; var statisType = "D"
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
    LoadTable.lodSnsTable(spark, statisDate, statisType)
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST
         , VHOST
         , V_ID
         , U_ID
         , T_ID
         , PROD_ID
         , SNS_ID
         , OS
         , BROWSER
         , OS_VER
         , BROWSER_VER
         , XLOC
         , LANG
         , DEVICE_ID
    FROM TB_SNS_HISTORY
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
