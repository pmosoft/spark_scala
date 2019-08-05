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
  - TB_WL_REFERER
  - TB_MEMBER_CLASS
 * 출    력 : TB_WL_VISITOR_STAT_USER
 * 수정내역 :
 * 2018-12-07 | 피승현 | 최초작성
 */
object VisitorStatUser {

  var spark : SparkSession = null
  var objNm  = "TB_WL_VISITOR_STAT_USER"

  var statisDate = ""
  var statisType = ""
  //var statisDate = "20181219"; var statisType = "D"

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
    LoadTable.lodRefererTable(spark, statisDate, statisType)
    LoadTable.lodMemberTable(spark, statisDate, statisType)
  }

  def excuteSql() = {
    val qry =
    """
    SELECT
          TA.STATIS_DATE              AS STATIS_DATE
        , TA.GVHOST                   AS GVHOST
        , TA.VHOST                    AS VHOST
        , TA.T_VISITOR                AS T_VISITOR
        , TB.M_VISITOR                AS M_VISITOR
        , TA.T_VISITOR - TB.M_VISITOR AS NM_VISITOR
        , TC.F_VISITOR                AS F_VISITOR
        , 0                           AS P_VISITOR
        , 0                           AS B_VISITOR
        , 0                           AS O_VISITOR
        , 0                           AS M_PI_VISITOR
        , 0                           AS NM_PI_VISITOR
    FROM
        (
        SELECT
              STATIS_DATE                       AS STATIS_DATE
            , GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(DISTINCT V_ID)              AS T_VISITOR
        FROM  TB_WL_REFERER
        GROUP BY STATIS_DATE, GVHOST, VHOST
        ) TA
        INNER JOIN
        (
        SELECT
              STATIS_DATE                       AS STATIS_DATE
            , GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(DISTINCT LOGIN_ID)          AS M_VISITOR
        FROM  TB_MEMBER_CLASS
        GROUP BY STATIS_DATE, GVHOST, VHOST
        ) TB
        ON TA.STATIS_DATE = TB.STATIS_DATE AND TA.GVHOST = TB.GVHOST AND TA.VHOST = TB.VHOST
        INNER JOIN
        (
        SELECT
              STATIS_DATE                       AS STATIS_DATE
            , GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(V_ID)                       AS F_VISITOR
        FROM
            (
            SELECT DISTINCT
                  STATIS_DATE                       AS STATIS_DATE
                , GVHOST                            AS GVHOST
                , VHOST                             AS VHOST
                , V_ID
                , (CASE WHEN SUBSTR(V_ID, 1, 2) = 'A0'
                                   THEN SUBSTR(V_ID, 4, 6)
                                   ELSE SUBSTR(V_ID, 2, 6)
                              END)  AS FIRST_VISIT
            FROM  TB_WL_REFERER
            ) TA
        WHERE FIRST_VISIT = STATIS_DATE
        GROUP BY STATIS_DATE, GVHOST, VHOST
        ) TC
        ON TA.STATIS_DATE = TC.STATIS_DATE AND TA.GVHOST = TC.GVHOST AND TA.VHOST = TC.VHOST
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
