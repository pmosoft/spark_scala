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

/*
 * 설    명 : 일별 유입 통계
 * 입    력 :

  TB_WL_REFERER
  - STATIS_DATE
  - GVHOST
  - VHOST
  - V_ID
  - HOST
  - DIR_CGI
  - KEYWORD
  - DOMAIN

 * 출    력 : TB_WL_REFERER_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 */
object RefererStat {

  var spark : SparkSession = null
  var objNm  = "TB_WL_REFERER_STAT"
  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_WL_REFERER_STAT";var statisDate = "20190304"; var statisType = "D"

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
    //LoadTable.lodRefererTable(spark,statisDate,statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {

    var qry = ""
    qry =
    s"""
    SELECT
         '${statisDate}'                                    AS STATIS_DATE
        , GVHOST                                            AS GVHOST
        , VHOST                                             AS VHOST
        , HOST                                              AS HOST
        , DIR_CGI                                           AS DIR_CGI
        , MAX(CASE WHEN RNUM = 1 THEN DOMAIN ELSE '' END)   AS DOMAIN
        , MAX(CASE WHEN RNUM = 1 THEN CATEGORY ELSE '' END) AS CATEGORY
        , COUNT(DISTINCT V_ID)                              AS VISITOR_CNT
        , COUNT(V_ID)                                       AS VISITOR_VCNT
    FROM
        (
        SELECT
              GVHOST                                        AS GVHOST
            , VHOST                                         AS VHOST
            , V_ID                                          AS V_ID
            , HOST                                          AS HOST
            , DIR_CGI                                       AS DIR_CGI
            , DOMAIN                                        AS DOMAIN
            , CATEGORY                                      AS CATEGORY
            , RNUM                                          AS RNUM
            , MAX(RNUM) OVER()                              AS MAX_RNUM
        FROM
           (
            SELECT
                  GVHOST                                    AS GVHOST
                , VHOST                                     AS VHOST
                , V_ID                                      AS V_ID
                , HOST                                      AS HOST
                , DIR_CGI                                   AS DIR_CGI
                , DOMAIN                                    AS DOMAIN
                , CATEGORY                                  AS CATEGORY
                , ROW_NUMBER() OVER(PARTITION BY HOST, DIR_CGI ORDER BY C_TIME) AS RNUM
            FROM  TB_REFERER_DAY
            ) A
        ) A
    GROUP BY GVHOST, VHOST, HOST, DIR_CGI
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
