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
 * 입    력 :
  - TB_WL_URL_ACCESS
 * 출    력 : TB_WL_PV_STAT_USER
 * 수정내역 :
 * 2018-12-07 | 피승현 | 최초작성
 */
object PvStatUser {

  var spark : SparkSession = null
  var objNm = "TB_WL_PV_STAT_USER"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_WL_PV_STAT_USER";var statisDate = "20190212";var statisType = "D"

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
    LoadTable.lodAccessTable(spark,statisDate,statisType)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
          STATIS_DATE                               AS STATIS_DATE
        , GVHOST                                    AS GVHOST
        , VHOST                                     AS VHOST
        , URL                                       AS URL
        , COUNT(*)                                  AS PVIEW
        , COUNT(DISTINCT V_ID)                      AS VISITOR_CNT
        , 0                                         AS IS_FIRST_CNT
        , 0                                         AS IS_LAST_CNT
        , 0                                         AS IS_RELOAD_CNT
        , AVG(DUR_TIME)                             AS DUR_TIME
        , 0                                         AS PI_DUR_TIME
        , COUNT(DISTINCT CASE WHEN U_ID IS NULL THEN NULL ELSE U_ID END) AS MEMBER_VISITOR_CNT
        , SUM(CASE WHEN U_ID IS NULL THEN 0 ELSE 1 END) AS MEMBER_PVIEW
        , 0                                         AS SIMPLE_VISITOR_CNT
        , 0                                         AS SIMPLE_PVIEW
        , 0                                         AS NCATE_MEMBER_PVIEW
    FROM
        (
        SELECT
              STATIS_DATE                           AS STATIS_DATE
            , GVHOST                                AS GVHOST
            , VHOST                                 AS VHOST
            , URL                                   AS URL
            , V_ID                                  AS V_ID
            , DUR_TIME                              AS DUR_TIME
            , U_ID                                  AS U_ID
        FROM
            (
            SELECT
                  STATIS_DATE
                , GVHOST
                , VHOST
                , USER_URL                          AS URL
                , V_ID
                , DUR_TIME
                , U_ID
                , VHOST
            FROM  TB_WL_URL_ACCESS
            WHERE USER_URL > ' '
            ) TA
        ) TA
    GROUP BY STATIS_DATE, GVHOST, VHOST, URL
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
