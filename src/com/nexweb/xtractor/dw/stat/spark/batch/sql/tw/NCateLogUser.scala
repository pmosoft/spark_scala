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
  - TB_NCATE_MAP_USER
  - TB_NCATE_URL_MAP_USER
  - TB_WL_URL_ACCESS
  - TB_WL_REFERER
 * 출    력 : TB_NCATE_LOG_USER
 * 수정내역 :
 * 2018-12-07 | 피승현 | 최초작성
 */
object NCateLogUser {

  var spark : SparkSession = null
  var objNm  = "TB_NCATE_LOG_USER"

  var statisDate = ""
  var statisType = ""

  //var objNm  = "TB_NCATE_LOG_USER";var statisDate = "20190212";var statisType = "D"

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
    LoadTable.lodRefererTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_NCATE_MAP_USER"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_URL_MAP_USER" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
          '${statisDate}'   AS STATIS_DATE
        , TB.GVHOST         AS GVHOST
        , TB.VHOST          AS VHOST
        , TA.CATE_ID        AS CATE_ID
        , TA.URL            AS URL
        , TB.V_ID           AS V_ID
        , TB.U_ID           AS U_ID
        , COUNT(TC.V_ID)    AS VISIT_CNT
        , ''                AS LOGIN_TYPE
        , TB.SVC_ID         AS SVC_ID
        , TB.T_ID           AS LOGIN_ID
    FROM
        (
        SELECT TC.*
        FROM
              TB_NCATE_MAP_USER TA,
              TB_NCATE_URL_MAP_USER TC
        WHERE TA.VERSION_ID = TC.VERSION_ID
        AND   TA.GVHOST     = TC.GVHOST
        AND   TA.VHOST      = TC.VHOST
        AND   TA.CATE_ID    = TC.CATE_ID
        ) TA
        INNER JOIN TB_WL_URL_ACCESS TB
          ON  TA.GVHOST = TB.GVHOST
          AND TA.VHOST  = TB.VHOST
          AND TA.URL    = TB.USER_URL
        LEFT OUTER JOIN TB_WL_REFERER TC
          ON  TA.GVHOST = TB.GVHOST
          AND TA.VHOST  = TB.VHOST
          AND TB.V_ID   = TC.V_ID
          AND TB.C_TIME = TC.C_TIME
    GROUP BY TB.GVHOST, TB.VHOST, TA.CATE_ID, TA.URL, TB.V_ID, TB.U_ID, TB.SVC_ID, TB.T_ID
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
