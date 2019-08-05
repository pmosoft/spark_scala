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
 * 설    명 : 일/월별 접속 URL UV 통계
 * 입    력 :
  - TB_WL_PV_STAT_FRONT
  - TB_NCATE_LOG_FRONT
 * 출    력 : TB_NCATE_URL_STAT_FRONT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */

object NCateURLStatFront {

  var spark : SparkSession = null
  var objNm  = "TB_NCATE_URL_STAT_FRONT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_NCATE_URL_STAT_FRONT"; var statisDate = "20190313"; var statisType = "D"
  //var objNm  = "TB_NCATE_URL_STAT_FRONT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"

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
    LoadTable.lodAllColTable(spark,"TB_WL_PV_STAT_FRONT"    ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_LOG_FRONT"     ,statisDate,statisType,"",true)
  }

  def excuteSql() = {

    val qry =
    """
    SELECT
         '"""+statisDate+"""'                   AS STATIS_DATE
        , '"""+statisType+"""'                   AS STATIS_TYPE
        , GVHOST                                AS GVHOST
        , VHOST                                 AS VHOST
        , URL                                   AS URL
        , NVL(SUM(PVIEW), 0)                    AS PVIEW
        , NVL(SUM(VISIT_CNT), 0)                AS VISIT_CNT
        , NVL(SUM(VISITOR_CNT), 0)              AS VISITOR_CNT
        , NVL(SUM(LOGIN_VISITOR_CNT), 0)        AS LOGIN_VISITOR_CNT
        , NVL(SUM(DUR_TIME), 0)                 AS DUR_TIME
        , NVL(SUM(MEMBER_LOGIN_VISITOR_CNT), 0) AS MEMBER_LOGIN_VISITOR_CNT
        , NVL(SUM(NCATE_MEMBER_PVIEW), 0)       AS MEMBER_PVIEW
        , NVL(SUM(SIMPLE_LOGIN_VISITOR_CNT), 0) AS SIMPLE_LOGIN_VISITOR_CNT
        , NVL(SUM(SIMPLE_PVIEW), 0)             AS SIMPLE_PVIEW
        , NVL(SUM(TOTAL_LOGIN_VISITOR_CNT), 0)  AS TOTAL_LOGIN_VISITOR_CNT
        , NVL(SUM(DUPL_LOGIN_VISITOR_CNT), 0)   AS DUPL_LOGIN_VISITOR_CNT
        , NVL(SUM(SVC_UPDATE_UV), 0)            AS SVC_UPDATE_UV
        , NVL(SUM(LOGIN_ID_VISITOR_CNT), 0)     AS LOGIN_ID_VISITOR_CNT
    FROM
        (
        SELECT
              GVHOST                            AS GVHOST
            , VHOST                             AS VHOST
            , URL                               AS URL
            , SUM(PVIEW)                        AS PVIEW
            , 0                                 AS VISIT_CNT
            , 0                                 AS VISITOR_CNT
            , 0                                 AS LOGIN_VISITOR_CNT
            , AVG(DUR_TIME)                     AS DUR_TIME
            , 0                                 AS MEMBER_LOGIN_VISITOR_CNT
            , SUM(NVL(NCATE_MEMBER_PVIEW,0))    AS NCATE_MEMBER_PVIEW
            , 0                                 AS SIMPLE_LOGIN_VISITOR_CNT
            , SUM(SIMPLE_PVIEW)                 AS SIMPLE_PVIEW
            , 0                                 AS TOTAL_LOGIN_VISITOR_CNT
            , 0                                 AS DUPL_LOGIN_VISITOR_CNT
            , 0                                 AS SVC_UPDATE_UV
            , 0                                 AS LOGIN_ID_VISITOR_CNT
        FROM  TB_WL_PV_STAT_FRONT
        GROUP BY GVHOST, VHOST, URL
        UNION ALL
        SELECT
              GVHOST                                                                      AS GVHOST
            , VHOST                                                                       AS VHOST
            , URL                                                                         AS URL
            , 0                                                                           AS PVIEW
            , SUM(VISIT_CNT)                                                              AS VISIT_CNT
            , COUNT(DISTINCT V_ID)                                                        AS VISITOR_CNT
            , COUNT(DISTINCT U_ID)                                                        AS LOGIN_VISITOR_CNT
            , 0                                                                           AS DUR_TIME
            , COUNT(DISTINCT U_ID)- COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))       AS MEMBER_LOGIN_VISITOR_CNT
            , 0                                                                           AS MEMBER_PVIEW
            , COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))                             AS SIMPLE_LOGIN_VISITOR_CNT
            , 0                                                                           AS SIMPLE_PVIEW
            , COUNT(DISTINCT U_ID)                                                        AS TOTAL_LOGIN_VISITOR_CNT
            , 0                                                                           AS DUPL_LOGIN_VISITOR_CNT
            , COUNT(DISTINCT SVC_ID)                                                      AS SVC_UPDATE_UV
            , COUNT(DISTINCT LOGIN_ID)                                                    AS LOGIN_ID_VISITOR_CNT
        FROM  TB_NCATE_LOG_FRONT
        GROUP BY GVHOST, VHOST, URL
				UNION ALL
				SELECT 
				       GVHOST
             , VHOST
				     , URL
    				 , 0 AS PVIEW
    				 , 0 AS VISIT_CNT
    				 , 0 AS VISITOR_CNT
    				 , 0 AS LOGIN_VISITOR_CNT
    				 , 0 AS DUR_TIME
    				 , 0 AS MEMBER_LOGIN_VISITOR_CNT
    				 , 0 AS NCATE_MEMBER_PVIEW
    				 , 0 AS SIMPLE_LOGIN_VISITOR_CNT
    				 , 0 AS SIMPLE_PVIEW
    				 , 0 AS TOTAL_LOGIN_VISITOR_CNT
    				 , COUNT(DISTINCT U_ID) AS DUPL_LOGIN_VISITOR_CNT
    				 , 0 AS SVC_UPDATE_UV
    				 , 0 AS LOGIN_ID_VISITOR_CNT
				FROM
				(
					SELECT TC.GVHOST, TC.VHOST, TC.URL, TC.U_ID
					FROM
					(
						(
						SELECT GVHOST, VHOST, URL, U_ID
						FROM TB_NCATE_LOG_FRONT 
						WHERE U_ID IS NOT NULL 
						AND LOGIN_TYPE='A' 
						GROUP BY GVHOST, VHOST, URL, U_ID
					  ) TC
						INNER JOIN
					  (
						SELECT GVHOST, VHOST, URL, U_ID
						FROM TB_NCATE_LOG_FRONT 
						WHERE U_ID IS NOT NULL 
						AND LOGIN_TYPE='Z' 
						GROUP BY GVHOST, VHOST, URL, U_ID
					  ) TD
						ON TC.GVHOST = TD.GVHOST AND TC.VHOST = TD.VHOST AND TC.U_ID=TD.U_ID AND TC.URL=TD.URL
					)
				)GROUP BY GVHOST, VHOST, URL, U_ID 
    ) TA
    WHERE LENGTH(URL) > 0
    GROUP BY GVHOST, VHOST, URL
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
