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
  - TB_NCATE_LAST_VER_USER
  - TB_NCATE_MAP_USER
  - CATE_URL_MAP_USER

  - TB_WL_PV_STAT_USER
  - TB_NCATE_LOG_USER

 * 출    력 : TB_NCATE_STAT_USER
 * 수정내역 :
 * 2018-12-06 | 피승현 | 최초작성
 */
object NCateStatUser {

  var spark : SparkSession = null
  var objNm  = "TB_NCATE_STAT_USER"
  var obj2Nm = ""

  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""
  //var objNm  = "TB_NCATE_STAT_USER";var statisDate = "20181219"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

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
    LoadTable.lodAllColTable(spark,"TB_NCATE_MAP_USER"     ,statisDate2,statisType2,"",true)
    LoadTable.lodAllColTable(spark,"CATE_URL_MAP_USER"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_LOG_USER"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_WL_PV_STAT_USER"    ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
         '${statisType}'              AS VERSION_ID
        ,'${statisDate}'              AS STATIS_DATE
        ,'${statisType}'              AS STATIS_TYPE
      , TA.GVHOST                     AS GVHOST
      , TA.VHOST                      AS VHOST
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
            , VHOST                                     AS VHOST
            , CATE_ID                                   AS CATE_ID
            , NVL(SUM(PVIEW), 0)                        AS PVIEW
            , NVL(SUM(VISIT_CNT), 0)                    AS VISIT_CNT
            , NVL(SUM(VISITOR_CNT), 0)                  AS VISITOR_CNT
            , NVL(SUM(LOGIN_VISITOR_CNT), 0)            AS LOGIN_VISITOR_CNT
            , NVL(SUM(DUR_TIME), 0)                     AS DUR_TIME
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
                , TA.VHOST                              AS VHOST
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
            FROM  CATE_URL_MAP_USER TA,
                  TB_WL_PV_STAT_USER TB
            WHERE TA.GVHOST = TB.GVHOST
            AND   TA.VHOST  = TB.VHOST
            AND   TA.URL    = TB.URL
            GROUP BY TA.GVHOST, TA.VHOST, TA.ANC_CATE_ID
            UNION ALL
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.VHOST                              AS VHOST
                , TA.ANC_CATE_ID                        AS CATE_ID
                , 0                                     AS PVIEW
                , SUM(TB.VISIT_CNT)                     AS VISIT_CNT
                , 0                                     AS VISITOR_CNT
                , 0                                     AS LOGIN_VISITOR_CNT
                , 0                                     AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , 0                                     AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , 0                                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , 0                                     AS SVC_UPDATE_UV
                , 0                                     AS LOGIN_ID_VISITOR_CNT
            FROM  CATE_URL_MAP_USER TA,
                  TB_NCATE_LOG_USER TB
            WHERE TA.GVHOST  = TB.GVHOST
            AND   TA.VHOST   = TB.VHOST
            AND   TA.CATE_ID = TB.CATE_ID
            AND   TA.URL     = TB.URL
            GROUP BY TA.GVHOST, TA.VHOST, TA.ANC_CATE_ID
            UNION ALL
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.VHOST                              AS VHOST
                , TA.CATE_ID                            AS CATE_ID
                , 0                                     AS PVIEW
                , 0                                     AS VISIT_CNT
                , COUNT(DISTINCT TB.V_ID)               AS VISITOR_CNT
                , 0                                     AS LOGIN_VISITOR_CNT
                , 0                                     AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , 0                                     AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , 0                                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , 0                                     AS SVC_UPDATE_UV
                , 0                                     AS LOGIN_ID_VISITOR_CNT
            FROM
                  TB_NCATE_LOG_USER TA,
                  (
                  SELECT
                        GVHOST
                      , VHOST
                      , CATE_ID
                      , V_ID
                  FROM  TB_NCATE_LOG_USER TA
                  WHERE INSTR(V_ID, '.') <= 0
                  GROUP BY GVHOST, VHOST, CATE_ID, V_ID
                  ) TB
            WHERE TA.GVHOST  = TB.GVHOST
            AND   TA.VHOST   = TB.VHOST
            AND   TA.CATE_ID = TB.CATE_ID
            GROUP BY TA.GVHOST, TA.VHOST, TA.CATE_ID
            UNION ALL
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.VHOST                              AS VHOST
                , TA.ANC_CATE_ID                        AS CATE_ID
                , 0                                     AS PVIEW
                , 0                                     AS VISIT_CNT
                , 0                                     AS VISITOR_CNT
                , COUNT(DISTINCT TB.U_ID)               AS LOGIN_VISITOR_CNT
                , 0                                     AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , 0                                     AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , 0                                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , COUNT(DISTINCT TB.SVC_ID)             AS SVC_UPDATE_UV
                , COUNT(DISTINCT TB.LOGIN_ID)           AS LOGIN_ID_VISITOR_CNT
            FROM
                (
                SELECT GVHOST, VHOST, ANC_CATE_ID, CATE_ID
                FROM   CATE_URL_MAP_USER
                GROUP BY GVHOST, VHOST, ANC_CATE_ID, CATE_ID
                ) TA,
                (
                SELECT
                      GVHOST
                    , VHOST
                    , CATE_ID
                    , U_ID
                    , SVC_ID
                    , LOGIN_ID
                FROM  TB_NCATE_LOG_USER TA
                WHERE U_ID IS NOT NULL
                GROUP BY GVHOST, VHOST, CATE_ID, U_ID, SVC_ID, LOGIN_ID
                ) TB
            WHERE TA.GVHOST  = TB.GVHOST
            AND   TA.VHOST   = TB.VHOST
            AND   TA.CATE_ID = TB.CATE_ID
            GROUP BY TA.GVHOST, TA.VHOST, TA.ANC_CATE_ID
            UNION ALL
            SELECT
                  TA.GVHOST                             AS GVHOST
                , TA.VHOST                              AS VHOST
                , TA.CATE_ID                            AS CATE_ID
                , 0                                     AS PVIEW
                , 0                                     AS VISIT_CNT
                , 0                                     AS VISITOR_CNT
                , 0                                     AS LOGIN_VISITOR_CNT
                , 0                                     AS DUR_TIME
                , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
                , 0                                     AS NCATE_MEMBER_PVIEW
                , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
                , 0                                     AS SIMPLE_PVIEW
                , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
                , 0                                     AS DUPL_LOGIN_VISITOR_CNT
                , 0                                     AS SVC_UPDATE_UV
                , 0                                     AS LOGIN_ID_VISITOR_CNT
            FROM
                (
                SELECT
                      TA.GVHOST                         AS GVHOST
                    , TA.VHOST                          AS VHOST
                    , TA.ANC_CATE_ID                    AS CATE_ID
                    , 0                                 AS DUPL_LOGIN_VISITOR_CNT
                FROM
                    (
                    SELECT GVHOST, VHOST, ANC_CATE_ID, CATE_ID
                    FROM   CATE_URL_MAP_USER
                    GROUP BY GVHOST, VHOST, ANC_CATE_ID, CATE_ID
                    ) TA,
                    (
                    SELECT
                          GVHOST
                        , VHOST
                        , CATE_ID
                        , U_ID
                    FROM  TB_NCATE_LOG_USER TA
                    WHERE U_ID IS NOT NULL
                    GROUP BY GVHOST, VHOST, CATE_ID, U_ID
                    ) TB
                WHERE TA.GVHOST = TB.GVHOST
                AND   TA.VHOST  = TB.VHOST
                AND   TA.ANC_CATE_ID = TB.CATE_ID
                GROUP BY TA.GVHOST, TA.VHOST, TA.ANC_CATE_ID, TB.U_ID
                ) TA
            GROUP BY TA.GVHOST, TA.VHOST, TA.CATE_ID
            ) TA
        GROUP BY GVHOST, VHOST, CATE_ID
        ) TA
    WHERE TA.GVHOST = TB.GVHOST
    AND   TA.VHOST  = TB.VHOST
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
