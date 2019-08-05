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
 * 설    명 : 일/월별
 * 입    력 :
  - TB_WL_REFERER
  - TB_MEMBER_CLASS
  - TB_NCATE_STAT_FRONT

 * 출    력 : - TB_VISIT_TW_STAT
 * 수정내역 :
 * 2019-01-29 | 피승현 | 최초작성
 */

object VisitTwStat {

  var spark : SparkSession = null
  var objNm  = "TB_VISIT_TW_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_TW_STAT";var statisDate = "20190304"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

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
    //LoadTable.lodRefererTable(spark, statisDate, statisType)
    //LoadTable.lodMemberTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_STAT_FRONT"    ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
          '${statisDate}'           AS STATIS_DATE
         ,'${statisType}'           AS STATIS_TYPE
        , GVHOST                    AS GVHOST
        , SUM(TOT_VISITOR_CNT)      AS TOT_VISITOR_CNT
        , SUM(NEW_VISITOR_CNT)      AS NEW_VISITOR_CNT
        , SUM(RE_VISITOR_CNT)       AS RE_VISITOR_CNT
        , SUM(FM_VISITOR_CNT)       AS FM_VISITOR_CNT
        , SUM(AM_VISITOR_CNT)       AS AM_VISITOR_CNT
        , SUM(DUP_VISITOR_CNT)      AS DUP_VISITOR_CNT
        , SUM(LOGIN_VISITOR_CNT)    AS LOGIN_VISITOR_CNT
        , SUM(TOT_PAGE_VIEW)        AS TOT_PAGE_VIEW
        , SUM(LOGIN_ID_VISITOR_CNT) AS LOGIN_ID_VISITOR_CNT
        , SUM(SVC_UPDATE_UV)        AS SVC_UPDATE_UV
    FROM
        (
        SELECT
              GVHOST                   AS GVHOST
            , T_VISITOR                AS TOT_VISITOR_CNT
            , F_VISITOR                AS NEW_VISITOR_CNT
            , T_VISITOR - F_VISITOR    AS RE_VISITOR_CNT
            , 0                        AS FM_VISITOR_CNT
            , 0                        AS AM_VISITOR_CNT
            , 0                        AS DUP_VISITOR_CNT
            , 0                        AS LOGIN_VISITOR_CNT
            , 0                        AS TOT_PAGE_VIEW
            , 0                        AS SVC_UPDATE_UV
            , 0                        AS LOGIN_ID_VISITOR_CNT
        FROM
            (
            SELECT
                  TA.GVHOST            AS GVHOST
                , NVL(TA.T_VISITOR, 0) AS T_VISITOR
                , NVL(TB.M_VISITOR, 0) AS M_VISITOR
                , NVL(TC.F_VISITOR, 0) AS F_VISITOR
            FROM
                (
                SELECT
                       GVHOST                                    AS GVHOST
                     , COUNT(DISTINCT V_ID)                      AS T_VISITOR
                FROM   TB_REFERER_DAY
                GROUP BY GVHOST
                ) TA
                LEFT OUTER JOIN
                (
                SELECT
                       GVHOST                                    AS GVHOST
                     , COUNT(DISTINCT LOGIN_ID)                  AS M_VISITOR
                FROM   TB_MEMBER_CLASS_DAY TA
                GROUP BY GVHOST
                ) TB
                ON TA.GVHOST = TB.GVHOST
                LEFT OUTER JOIN
                (
                SELECT
                       GVHOST                                     AS GVHOST
                     , COUNT(V_ID)                                AS F_VISITOR
                FROM
                       (
                       SELECT
                             DISTINCT
                             GVHOST                                 AS GVHOST
                           , V_ID                                   AS V_ID
                           ,(CASE WHEN SUBSTR(V_ID, 1, 2) = 'A0'
                                  THEN SUBSTR(V_ID, 4, 6)
                                  ELSE SUBSTR(V_ID, 2, 6)
                             END)                                   AS FIRST_VISIT
                       FROM  TB_REFERER_DAY
                       ) TA
                WHERE '20'||FIRST_VISIT = '${statisDate}'
                GROUP BY GVHOST
                ) TC
                ON TA.GVHOST = TC.GVHOST
            )
        UNION ALL
        SELECT
               TA.GVHOST         AS GVHOST
             , 0                 AS TOT_VISITOR_CNT
             , 0                 AS NEW_VISITOR_CNT
             , 0                 AS RE_VISITOR_CNT
             , COUNT(DISTINCT CASE WHEN U_ID IS NOT NULL THEN T_ID ELSE NULL END) AS FM_VISITOR_CNT
             , COUNT(DISTINCT CASE WHEN U_ID IS NULL THEN T_ID ELSE NULL END) AS AM_VISITOR_CNT
             , COUNT(DISTINCT CASE WHEN RNUM > 1    THEN T_ID ELSE NULL END) AS DUP_VISITOR_CNT
             , COUNT(DISTINCT T_ID)                 AS LOGIN_VISITOR_CNT
             , 0                 AS TOT_PAGE_VIEW
             , 0                 AS SVC_UPDATE_UV
             , 0                 AS LOGIN_ID_VISITOR_CNT
        FROM
               (
               SELECT
                      GVHOST                AS GVHOST
                    , T_ID
                    , U_ID
                    , TYPE
                    , DENSE_RANK() OVER(PARTITION BY T_ID ORDER BY TYPE ASC) +
                      DENSE_RANK() OVER(PARTITION BY T_ID ORDER BY TYPE DESC) - 1  AS RNUM
               FROM   TB_MEMBER_CLASS_DAY
               ) TA
        GROUP BY GVHOST
        UNION ALL
        SELECT
              GVHOST                    AS GVHOST
            , 0                         AS TOT_VISITOR_CNT
            , 0                         AS NEW_VISITOR_CNT
            , 0                         AS RE_VISITOR_CNT
            , 0                         AS FM_VISITOR_CNT
            , 0                         AS AM_VISITOR_CNT
            , 0                         AS DUP_VISITOR_CNT
            , 0                         AS LOGIN_VISITOR_CNT
            , SUM(PVIEW)                AS TOT_PAGE_VIEW
            , SUM(SVC_UPDATE_UV)        AS SVC_UPDATE_UV
            , SUM(LOGIN_ID_VISITOR_CNT) AS LOGIN_ID_VISITOR_CNT
        FROM  TB_NCATE_STAT_FRONT
        WHERE CATE_ID IN ('MA', 'MW', 'OW') 
        GROUP BY GVHOST
        ) TA
    GROUP BY GVHOST
    """
    spark.sql(qry).take(100).foreach(println);

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
