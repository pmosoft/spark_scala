package com.nexweb.xtractor.dw.stat.spark.batch.sql

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
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_INNER_KW_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 * 2019-01-05 | 피승현 | poc 로직으로 변경
 */
object InnerKwStat {

  var spark: SparkSession = null
  var objNm = "TB_INNER_KW_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_INNER_KW_STAT";var statisDate = "20190516"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

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
    LoadTable.lodAllColTable(spark,"TB_INNER_KW",statisDate,statisType,"",true)
  }

/*

TB_WL_INNER_KW
 GVHOST
VHOST
C_TIME
V_ID
U_ID
S_ID
URL
KEYWORD
TYPE


insert into tb_inner_kw_stat
select  ?,keyword, count(1)
From tb_wl_inner_kw
where c_time between to_date(?,'YYYYMMDD') and to_date(?,'YYYYMMDD') + 0.99999
group by keyword

          INSERT    INTO TB_WL_INNER_KW_DAY
          (
          URL, V_ID, U_ID, KEYWORD, C_TIME, TYPE, SESSION_ID, VHOST
          )
          SELECT    URL, V_ID, U_ID, REPLACE(KEYWORD, 'y=', '') KEYWORD, C_TIME, OPT, SESSION_ID, VHOST
          FROM        TB_WL_URL_ACCESS_SESSION
          WHERE    APPLY_STAT = 'K'
          AND  KEYWORD <> 'y='

 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT '${statisDate}'                                             AS STATIS_DATE
         , GVHOST
         , SUBSTR(KEYWORD,1,100) AS KEYWORD
         , COUNT(*)                                                    AS SEARCH_CNT
    FROM   TB_INNER_KW
    GROUP BY GVHOST, SUBSTR(KEYWORD,1,100)
    """
    spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    """
    """
    spark.sql(qry).take(100).foreach(println);

    */

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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
