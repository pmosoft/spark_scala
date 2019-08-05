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
 * 설    명 : [일별] 방문 통계정보 생성
 * 입    력 :

  TB_WL_REFERER
  - STATIS_DATE
  - GVHOST
  - VHOST
  - V_ID

  TB_MEMBER_CLASS
  - STATIS_DATE
  - GVHOST
  - VHOST
  - T_ID

 * 출    력 : TB_WL_VISITOR_STAT
 * 수정내역 :
 * 2018-11-20 | 피승현 | 최초작성
 */
object VisitorStat {

  var spark : SparkSession = null
  var objNm  = "TB_WL_VISITOR_STAT"

  var statisDate = ""
  var statisType = ""
  // var objNm  = "TB_WL_VISITOR_STAT"; var statisDate = "20190308"; var statisType = "D"

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
    //LoadTable.lodRefererTable(spark, statisDate, statisType)
    //LoadTable.lodMemberTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
 }

  def excuteSql() = {

    var qry = ""
    
    qry =
    s"""
    SELECT
         '${statisDate}'              AS STATIS_DATE
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
              GVHOST                  AS GVHOST
            , VHOST                   AS VHOST
            , COUNT(DISTINCT V_ID)    AS T_VISITOR
        FROM  TB_REFERER_DAY
        GROUP BY GVHOST, VHOST
        ) TA
        INNER JOIN
        (
        SELECT
              GVHOST                  AS GVHOST
            , VHOST                   AS VHOST
            , COUNT(DISTINCT T_ID)    AS M_VISITOR
        FROM  TB_MEMBER_CLASS_DAY
        GROUP BY GVHOST, VHOST
        ) TB
        ON  TA.GVHOST  = TB.GVHOST
        AND TA.VHOST   = TB.VHOST
        INNER JOIN
        (
        SELECT
              GVHOST                  AS GVHOST
            , VHOST                   AS VHOST
            , COUNT(V_ID)             AS F_VISITOR
        FROM
            (
            SELECT
                  DISTINCT
                  GVHOST                      AS GVHOST
                , VHOST                       AS VHOST
                , V_ID                        AS V_ID
                , '20'||SUBSTR(V_ID, 2, 6)    AS FIRST_VISIT
            FROM  TB_REFERER_DAY
            )
        WHERE FIRST_VISIT = '${statisDate}'
        GROUP BY GVHOST, VHOST
        ) TC
        ON  TA.GVHOST  = TC.GVHOST
        AND TA.VHOST   = TC.VHOST
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
