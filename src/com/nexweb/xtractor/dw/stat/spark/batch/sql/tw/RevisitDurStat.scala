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
 * 설    명 : [일별] 재방문 통계정보 생성
 * 입    력 : TB_WL_REFERER
 * 출    력 : TB_WL_REVISIT_DUR_STAT
 * 수정내역 :
 * 2018-11-21 | 피승현 | 최초작성
 */
object RevisitDurStat {

  var spark : SparkSession = null
  var objNm  = "TB_WL_REVISIT_DUR_STAT"
  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_WL_REVISIT_DUR_STAT";var statisDate = "20190303"; var statisType = "D"

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
  }

  def excuteSql() = {

    var qry = ""
    qry =
    s"""
    SELECT
         '${statisDate}'                                            AS STATIS_DATE
        , GVHOST                                                    AS GVHOST
        , VHOST                                                     AS VHOST
        , V_ID                                                      AS V_ID
        , MIN(C_TIME)                                               AS EARLIST_TIME
        , MAX(CASE WHEN RNUM = 1 THEN CATEGORY ELSE '' END)         AS EARLIST_CATE
        , MAX(C_TIME)                                               AS LATEST_TIME
        , MAX(CASE WHEN RNUM = MAX_RNUM THEN CATEGORY ELSE '' END)  AS LATEST_CATE
        , COUNT(V_ID)                                               AS V_COUNT
        , MAX(U_ID)                                                 AS U_ID
    FROM
        (
        SELECT
              GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , V_ID                              AS V_ID
            , U_ID                              AS U_ID
            , C_TIME                            AS C_TIME
            , CATEGORY                          AS CATEGORY
            , RNUM                              AS RNUM
            , MAX(RNUM) OVER(PARTITION BY V_ID) AS MAX_RNUM
        FROM
            (
            SELECT
                  GVHOST                        AS GVHOST
                , VHOST                         AS VHOST
                , V_ID                          AS V_ID
                , U_ID                          AS U_ID
                , C_TIME                        AS C_TIME
                , CATEGORY                      AS CATEGORY
                , ROW_NUMBER() OVER(PARTITION BY V_ID ORDER BY C_TIME ASC) AS RNUM
            FROM TB_REFERER_DAY
            ) A
        ) A
    WHERE (RNUM = 1 OR RNUM = MAX_RNUM)
    GROUP BY GVHOST, VHOST, V_ID
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
