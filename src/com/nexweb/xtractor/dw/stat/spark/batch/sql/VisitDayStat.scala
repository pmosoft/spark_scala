package com.nexweb.xtractor.dw.stat.spark.batch.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
import com.nexweb.xtractor.dw.stat.spark.common.OJDBC
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch

/*
 * 설    명 : 일별/월별 방문자수 추이 통계
 * 입    력 :

TB_WL_REFERER
TB_WL_URL_ACCESS
TB_MEMBER_CLASS

 * 출    력 : TB_VISIT_DAY_STAT
 * 수정내역 :
 * 2019-01-17 | 피승현 | 최초작성
 */
object VisitDayStat {

  var spark: SparkSession = null
  var objNm = "TB_VISIT_DAY_STAT"

  var statisDate = ""
  var statisType = ""
  //var statisDate = "20181219"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
    println(objNm + ".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
    println(objNm + ".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def loadTables() = {
//    LoadTable.lodAccessTable(spark, statisDate, statisType)
//    LoadTable.lodRefererTable(spark, statisDate, statisType)
//    LoadTable.lodMemberTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION2"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*
(이 소스를 사용하지 않고 VisitTimeStat group by만 time에서 day로 변경하고 월배치만 수행한다.
INSERT  INTO TB_VISIT_DAY_STAT
(STATIS_TYPE, STATIS_DATE, VISIT_DAY_CD, VHOST, TOT_VISITOR_CNT, LOGIN_VISITOR_CNT, NOLOGIN_VISITOR_CNT)
SELECT  ? STATIS_TYPE,
             ? STATIS_DATE,
             VISIT_DAY_CD,
             VHOST,
             TOT_VISITOR_CNT,
             LOGIN_VISITOR_CNT,
             TOT_VISITOR_CNT - LOGIN_VISITOR_CNT NOLOGIN_VISITOR_CNT
FROM    (
            SELECT    NVL(VISIT_DAY_CD,'0')  VISIT_DAY_CD,
                   NVL(TA.VHOST, 'TOTAL') VHOST,
                         COUNT(DISTINCT TA.V_ID) TOT_VISITOR_CNT,
                  COUNT(DISTINCT TB.L_ID) LOGIN_VISITOR_CNT
            FROM    (
                        SELECT  V_ID, VISIT_CNT VISIT_DAY_CD, V_HOST VHOST
                        FROM    (
                                    SELECT  V_ID, SUM(VISIT_CNT) VISIT_CNT, V_HOST
                                    FROM    (
                                                SELECT  V_ID,
                                                             TO_CHAR(V_DATE, 'YYYYMMDD') VISIT_DT, V_HOST,
                                                             1 VISIT_CNT
                                                FROM    TB_WL_REFERER_SESSION_HIS
                                                WHERE    V_DATE BETWEEN TO_DATE(?,'YYYYMMDD') AND TO_DATE(?,'YYYYMMDD') + 0.99999
                                                GROUP   BY V_ID, TO_CHAR(V_DATE, 'YYYYMMDD'), V_HOST
                                                )
                                    GROUP   BY V_ID, V_HOST
                                    )
                        ) TA,
                        (
                        SELECT  V_ID, L_ID,TYPE, VHOST
                        FROM    TB_MEMBER_CLASS
                        WHERE    C_TIME BETWEEN TO_DATE(?,'YYYYMMDD') AND TO_DATE(?,'YYYYMMDD') + 0.99999
                        AND    V_ID > ' '
                        GROUP   BY V_ID, L_ID,TYPE, VHOST
                        ) TB
            WHERE   TA.V_ID = TB.V_ID(+)
             AND     TA.VHOST = TB.VHOST(+)
            GROUP   BY ROLLUP((VISIT_DAY_CD, TA.VHOST))
  )


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisType}'                                       AS STATIS_TYPE
         , '${statisDate}'                                       AS STATIS_DATE
         , GVHOST                                                AS GVHOST
         , SUM(VISIT_DAY_CD)                                     AS VISIT_DAY_CD
         , SUM(TOT_VISITOR_CNT)                                  AS TOT_VISITOR_CNT
         , SUM(LOGIN_VISITOR_CNT)                                AS LOGIN_VISITOR_CNT
         , SUM(TOT_VISITOR_CNT - LOGIN_VISITOR_CNT)              AS NOLOGIN_VISITOR_CNT
    FROM
           (
           SELECT
                  CASE WHEN TA.GVHOST IS NULL       THEN 'TOTAL' ELSE TA.GVHOST       END AS GVHOST
                , CASE WHEN TA.VISIT_DAY_CD IS NULL THEN 0       ELSE TA.VISIT_DAY_CD END AS VISIT_DAY_CD
                , COUNT(TA.V_ID)                                                          AS TOT_VISITOR_CNT
                , COUNT(TB.LOGIN_ID_CNT)                                                  AS LOGIN_VISITOR_CNT
           FROM   (
                   SELECT GVHOST              AS GVHOST
                        , V_ID                AS V_ID
                        , SUM(VISIT_CNT)      AS VISIT_DAY_CD
                   FROM   (
                          SELECT GVHOST      AS GVHOST
                               , V_ID        AS V_ID
                               , STATIS_DATE AS STATIS_DATE
                               , 1           AS VISIT_CNT
                          FROM   TB_REFERER_SESSION2
                          GROUP BY GVHOST, V_ID, STATIS_DATE
                          )
                   GROUP BY GVHOST, V_ID
                  ) TA
                  LEFT JOIN
                  (
                   SELECT GVHOST              AS GVHOST
                        , V_ID                AS V_ID
                        , SUM(LOGIN_ID_CNT)   AS LOGIN_ID_CNT
                   FROM   (
                          SELECT GVHOST
                               , V_ID
                               , T_ID
                               , 1           AS LOGIN_ID_CNT
                          FROM   TB_MEMBER_CLASS_SESSION
                          WHERE  V_ID > ' '
                          GROUP BY GVHOST, V_ID, T_ID
                          )
                   GROUP BY GVHOST, V_ID
                  ) TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.V_ID   = TB.V_ID
           GROUP BY TA.GVHOST, TA.VISIT_DAY_CD
           )
    GROUP BY GVHOST
    """
    //val sqlDf = spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm); sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm, true, statisDate)
  }

  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "' AND STATIS_TYPE='" + statisType + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
