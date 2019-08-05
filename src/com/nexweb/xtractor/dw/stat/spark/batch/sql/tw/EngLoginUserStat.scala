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
 * 설    명 : [일별/월별] 영문 로그인 UV 통계정보 생성
 * 입    력 :

  TB_MEMBER_CLASS
  - STATIS_DATE
  - GVHOST
  - VHOST
  - T_ID
  - OPT3

 * 출    력 : TB_ENG_LOGIN_USER_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 */
object EngLoginUserStat {

  var spark : SparkSession = null
  var objNm  = "TB_ENG_LOGIN_USER_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_ENG_LOGIN_USER_STAT"; var statisDate = "20190306"; var statisType = "D"
  //var objNm  = "TB_ENG_LOGIN_USER_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; 

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

    // tobe 기준 처리로직으로 변경요
    var qry = ""
    qry =
    s"""
    SELECT
         '"""+statisDate+"""'  AS STATIS_DATE
        ,'"""+statisType+"""'  AS STATIS_TYPE
        , GVHOST               AS GVHOST
        , COUNT(DISTINCT T_ID) AS USER_CNT
    FROM  TB_MEMBER_CLASS_DAY
    WHERE OPT3 = 'eng'
    GROUP BY STATIS_DATE, GVHOST
    """
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
