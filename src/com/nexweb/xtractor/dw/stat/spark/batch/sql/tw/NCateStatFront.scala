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
 * 설    명 : 일/월별 카테고리별 페이지 분석 통계
 * 입    력 :
  - TB_NCATE_LAST_VER_FRONT
  - TB_NCATE_MAP_FRONT
  - CATE_URL_MAP_FRONT

  - TB_WL_PV_STAT_FRONT
  - TB_NCATE_LOG_FRONT

 * 출    력 : TB_NCATE_STAT_FRONT
 * 수정내역 :
 * 2018-12-06 | 피승현 | 최초작성
 */
object NCateStatFront {

  var spark : SparkSession = null
  var objNm  = "TB_NCATE_STAT_FRONT"
  var obj2Nm = ""

  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""
  //var objNm  = "TB_NCATE_STAT_FRONT";var statisDate = "20190313"; var statisType = "D"
  //var objNm  = "TB_NCATE_STAT_FRONT"; var prevYyyymmDt = "20190430";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"CATE_URL_MAP_FRONT"     ,statisDate2,statisType2,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_LOG_FRONT"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_WL_PV_STAT_FRONT"    ,statisDate,statisType,"",true)
  }

  def excuteSql() = {

//    var qry = " SELECT VERSION_ID FROM TB_NCATE_LAST_VER_FRONT"
//    println(qry)
//    val tarDt = spark.sql(qry).take(1)
//    var VERSION_ID = tarDt(0)(0).toString()

    var qry = ""
    qry =
    s"""
    SELECT
        '${statisType}'               AS STATIS_TYPE
      , '${statisDate}'               AS STATIS_DATE
      , TA.GVHOST                     AS GVHOST
      , '${statisDate2}'              AS VERSION_ID
      , TA.CATE_ID                    AS CATE_ID
      , TA.PVIEW                      AS PVIEW
      , TA.VISIT_CNT                  AS VISIT_CNT
      , TA.VISITOR_CNT                AS VISITOR_CNT
      , TA.LOGIN_VISITOR_CNT          AS LOGIN_VISITOR_CNT
      , TA.DUR_TIME                   AS DUR_TIME
      , TA.MEMBER_LOGIN_VISITOR_CNT   AS MEMBER_LOGIN_VISITOR_CNT
      , TA.MEMBER_PVIEW               AS MEMBER_PVIEW
      , TA.SIMPLE_LOGIN_VISITOR_CNT   AS SIMPLE_LOGIN_VISITOR_CNT
      , TA.SIMPLE_PVIEW               AS SIMPLE_PVIEW
      , TA.TOTAL_LOGIN_VISITOR_CNT    AS TOTAL_LOGIN_VISITOR_CNT
      , TA.DUPL_LOGIN_VISITOR_CNT     AS DUPL_LOGIN_VISITOR_CNT
      , TA.SVC_UPDATE_UV              AS SVC_UPDATE_UV
      , TA.LOGIN_ID_VISITOR_CNT       AS LOGIN_ID_VISITOR_CNT
    FROM
        (
        SELECT
              GVHOST                                    AS GVHOST
            , CATE_ID                                   AS CATE_ID
            , NVL(SUM(PVIEW), 0)                        AS PVIEW
            , NVL(SUM(VISIT_CNT), 0)                    AS VISIT_CNT
            , NVL(SUM(VISITOR_CNT), 0)                  AS VISITOR_CNT
            , NVL(SUM(LOGIN_VISITOR_CNT), 0)            AS LOGIN_VISITOR_CNT
            , NVL(ROUND(SUM(DUR_TIME)), 0)              AS DUR_TIME
            , NVL(SUM(MEMBER_LOGIN_VISITOR_CNT), 0)     AS MEMBER_LOGIN_VISITOR_CNT
            , NVL(SUM(NCATE_MEMBER_PVIEW), 0)           AS MEMBER_PVIEW
            , NVL(SUM(SIMPLE_LOGIN_VISITOR_CNT), 0)     AS SIMPLE_LOGIN_VISITOR_CNT
            , NVL(SUM(SIMPLE_PVIEW), 0)                 AS SIMPLE_PVIEW
            , NVL(SUM(TOTAL_LOGIN_VISITOR_CNT), 0)      AS TOTAL_LOGIN_VISITOR_CNT
            , NVL(SUM(DUPL_LOGIN_VISITOR_CNT), 0)       AS DUPL_LOGIN_VISITOR_CNT
            , NVL(SUM(SVC_UPDATE_UV), 0)                AS SVC_UPDATE_UV
            , NVL(SUM(LOGIN_ID_VISITOR_CNT), 0)         AS LOGIN_ID_VISITOR_CNT
        FROM
            (
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.ANC_CATE_ID                        AS CATE_ID
                , SUM(TB.PVIEW)                         AS PVIEW
                , 0                                     AS VISIT_CNT
                , 0                                     AS VISITOR_CNT
                , 0                                     AS LOGIN_VISITOR_CNT
                , AVG(TB.DUR_TIME)                      AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , SUM(NVL(NCATE_MEMBER_PVIEW,0))        AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , SUM(SIMPLE_PVIEW)                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , 0                                     AS SVC_UPDATE_UV
                , 0                                     AS LOGIN_ID_VISITOR_CNT
            FROM  CATE_URL_MAP_FRONT TA,
                  TB_WL_PV_STAT_FRONT TB
            WHERE TA.GVHOST = TB.GVHOST
            AND   TA.URL    = TB.URL
            GROUP BY TA.GVHOST, TA.ANC_CATE_ID
            UNION ALL
            SELECT
                  TA.GVHOST                                                                AS GVHOST
                , TA.ANC_CATE_ID                                                           AS CATE_ID
                , 0                                                                        AS PVIEW
                , SUM(TB.VISIT_CNT)                                                        AS VISIT_CNT
                , COUNT(DISTINCT TB.V_ID)                                                  AS VISITOR_CNT
                , COUNT(DISTINCT TB.U_ID)                                                  AS LOGIN_VISITOR_CNT
                , 0                                                                        AS DUR_TIME
                , COUNT(DISTINCT U_ID)- COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))    AS MEMBER_LOGIN_VISITOR_CNT
                , 0                                                                        AS NCATE_MEMBER_PVIEW
                , COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))                          AS SIMPLE_LOGIN_VISITOR_CNT
                , 0                                                                        AS SIMPLE_PVIEW
                , COUNT(DISTINCT U_ID)                                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                                                        AS DUPL_LOGIN_VISITOR_CNT
                , COUNT(DISTINCT TB.SVC_ID)                                                AS SVC_UPDATE_UV
                , COUNT(DISTINCT TB.LOGIN_ID)                                              AS LOGIN_ID_VISITOR_CNT
            FROM  CATE_URL_MAP_FRONT TA,
                  TB_NCATE_LOG_FRONT TB
            WHERE TA.GVHOST  = TB.GVHOST
            AND   TA.CATE_ID = TB.CATE_ID
            AND   TA.URL     = TB.URL
            GROUP BY TA.GVHOST, TA.ANC_CATE_ID
            UNION ALL
					SELECT 
					     GVHOST, CATE_ID,
							0 AS PVIEW,
							0 AS VISIT_CNT,
							0 AS VISITOR_CNT,
							0 AS LOGIN_VISITOR_CNT,
							0 AS DUR_TIME,
							0 AS MEMBER_LOGIN_VISITOR_CNT,
							0 AS NCATE_MEMBER_PVIEW,
							0 AS SIMPLE_LOGIN_VISITOR_CNT,
							0 AS SIMPLE_PVIEW,
							0 AS TOTAL_LOGIN_VISITOR_CNT,
							NVL(COUNT(DISTINCT DUPL_LOGIN_VISITOR_CNT), 0) AS DUPL_LOGIN_VISITOR_CNT,
							0 AS SVC_UPDATE_UV,
							0 AS LOGIN_ID_VISITOR_CNT
					FROM (
							SELECT GVHOST, 
								ANC_CATE_ID AS CATE_ID,
								IF(COUNT(DISTINCT LOGIN_TYPE)==2, U_ID, NULL) AS DUPL_LOGIN_VISITOR_CNT
							FROM (
							    SELECT GVHOST, ANC_CATE_ID, CATE_ID
								FROM   CATE_URL_MAP_FRONT
								GROUP BY GVHOST, ANC_CATE_ID, CATE_ID
							) TA
							INNER JOIN (
								SELECT
									CATE_ID, U_ID, LOGIN_TYPE
								FROM TB_NCATE_LOG_FRONT TA
								WHERE LENGTH(U_ID) > 0
								GROUP BY CATE_ID, U_ID, LOGIN_TYPE
							) TB
							ON TA.CATE_ID = TB.CATE_ID
							GROUP BY GVHOST, ANC_CATE_ID, U_ID
					)
					GROUP BY GVHOST, CATE_ID	
            ) TA
        GROUP BY GVHOST, CATE_ID
        ) TA
    """

    /*

    qry =
    s"""
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.VHOST                              AS VHOST
                , TA.ANC_CATE_ID                        AS CATE_ID
                , MAX(VERSION_ID)                       AS VERSION_ID
                , SUM(TB.PVIEW)                         AS PVIEW
                , 0                                     AS VISIT_CNT
                , 0                                     AS VISITOR_CNT
                , 0                                     AS LOGIN_VISITOR_CNT
                , AVG(TB.DUR_TIME)                      AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , SUM(NVL(NCATE_MEMBER_PVIEW,0))        AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , SUM(SIMPLE_PVIEW)                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , 0                                     AS SVC_UPDATE_UV
                , 0                                     AS LOGIN_ID_VISITOR_CNT
            FROM  CATE_URL_MAP_FRONT TA,
                  TB_WL_PV_STAT_FRONT TB
            WHERE TA.GVHOST = TB.GVHOST
            AND   TA.VHOST  = TB.VHOST
            AND   TA.URL    = TB.URL
            GROUP BY TA.GVHOST, TA.VHOST, TA.ANC_CATE_ID
    """

     */

    //spark.sql(qry).take(100).foreach(println);
    //spark.sql("SELECT * FROM CATE_URL_MAP_FRONT").take(100).foreach(println);

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
