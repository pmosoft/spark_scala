package com.nexweb.xtractor.dw.stat.spark.batch.sql

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
 * 출    력 : TB_WL_VISITOR_STAT_FRONT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object VisitorStatFront {

  var spark : SparkSession = null
  var objNm  = "TB_WL_VISITOR_STAT_FRONT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_WL_VISITOR_STAT_FRONT"; var statisDate = "20190303"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
         '${statisDate}'                              AS STATIS_DATE
        , TA.GVHOST                                   AS GVHOST
        , TA.VHOST                                    AS VHOST
        , NVL(TA.T_VISITOR, 0)                        AS T_VISITOR
        , NVL(TB.M_VISITOR, 0)                        AS M_VISITOR
        , NVL(TA.T_VISITOR, 0) - NVL(TB.M_VISITOR, 0) AS NM_VISITOR
        , NVL(TC.F_VISITOR, 0)                        AS F_VISITOR
        , 0                                           AS P_VISITOR
        , 0                                           AS B_VISITOR
        , 0                                           AS O_VISITOR
        , 0                                           AS M_PI_VISITOR
        , 0                                           AS NM_PI_VISITOR
    FROM
        (
        SELECT
              GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(DISTINCT V_ID)              AS T_VISITOR
        FROM TB_REFERER_DAY
        GROUP BY GVHOST, VHOST
        ) TA
        LEFT OUTER JOIN
        (
        SELECT
              GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(DISTINCT LOGIN_ID)          AS M_VISITOR
        FROM  TB_MEMBER_CLASS_DAY
        GROUP BY GVHOST, VHOST
        ) TB
        ON TA.GVHOST = TB.GVHOST AND TA.VHOST = TB.VHOST
        LEFT OUTER JOIN
        (
        SELECT
              GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , COUNT(V_ID)                       AS F_VISITOR
        FROM
            (
            SELECT DISTINCT
                  GVHOST                            AS GVHOST
                , VHOST                             AS VHOST
                , V_ID                                 AS V_ID
                ,(CASE WHEN SUBSTR(V_ID, 1, 2) = 'A0'
                       THEN SUBSTR(V_ID, 4, 6)
                       ELSE SUBSTR(V_ID, 2, 6)
                  END)                                 AS FIRST_VISIT
             FROM  TB_REFERER_DAY
             )
        WHERE FIRST_VISIT = '${statisDate}'
        GROUP BY GVHOST, VHOST
        ) TC
        ON TA.GVHOST = TC.GVHOST AND TA.VHOST = TC.VHOST
    """
    //spark.sql(qry).take(100).foreach(println);
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
