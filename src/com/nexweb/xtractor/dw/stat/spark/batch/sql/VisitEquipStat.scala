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
 * 설    명 : 시간별 방문자 추이 통계
 * 입    력 :

TB_WL_REFERER
TB_WL_URL_ACCESS
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_VISIT_MOBILE_STAT
 * 수정내역 :
 * 2019-01-23 | 피승현 | 최초작성
 */
object VisitEquipStat {

  var spark: SparkSession = null
  var objNm  = "TB_VISIT_EQUIP_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_EQUIP_STAT"; var statisDate = "20190312"; var statisType = "D"
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
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

                     INSERT  INTO TB_VISIT_MOBILE_STAT
                    ( STATIS_DATE, VHOST, MOBILE_YN_TYPE, VISIT_CNT, TOT_VISITOR_CNT, NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT )
                     SELECT ? STATIS_DATE,
                                 NVL(TA.VHOST, 'TOTAL') VHOST,
                                 DECODE(TA.MOBILE_YN, 'Y', 'MOBILE', 'N', 'PC', 'T', 'TABLET', 'A', 'APP', 'ETC', 'ETC', 'TOTAL') MOBILE_YN,
                                 NVL(VISIT_CNT, 0) VISIT_CNT,
                                 NVL(TOT_VISITOR_CNT, 0) TOT_VISITOR_CNT,
                                 NVL(NEW_VISITOR_CNT, 0) NEW_VISITOR_CNT,
                                 NVL(RE_VISITOR_CNT, 0) RE_VISITOR_CNT,
                                 NVL(LOGIN_CNT, 0) LOGIN_CNT,
                                 NVL(LOGIN_VISITOR_CNT, 0) LOGIN_VISITOR_CNT,
                                 NVL(DUR_TIME, 0) DUR_TIME,
                                 NVL(PAGE_CNT, 0) PAGE_CNT,
                                 NVL(PAGE_VIEW, 0) PAGE_VIEW,
                                 NVL(BOUNCE_CNT, 0) BOUNCE_CNT
                    FROM    (
                                --유입경로별 방문수
                                SELECT  NVL(MOBILE_YN, 'TOTAL') AS MOBILE_YN,
                                        NVL(V_HOST, 'TOTAL') AS  VHOST,
                                             COUNT(1) VISIT_CNT,
                                             COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
                                             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                                             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
                                FROM    TB_WL_REFERER_SESSION
                                GROUP   BY ROLLUP(V_HOST, MOBILE_YN)
                                ) TA,
                                (
                                 --유입경로별 로그인수
                                SELECT   NVL(MOBILE_YN, 'TOTAL') AS MOBILE_YN,
                                             NVL(TA.VHOST, 'TOTAL') AS VHOST,
                                             SUM(LOGIN_CNT) LOGIN_CNT,
                                             COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
                                FROM    (
                                            SELECT  U_ID, SESSION_ID,
                                                    MOBILE_YN, V_HOST VHOST
                                            FROM    TB_WL_REFERER_SESSION
                                            ) TA,  TB_MEMBER_CLASS_DAY_STAT TB
                                WHERE   TA.SESSION_ID = TB.SESSION_ID
                                AND        TA.VHOST=  TB.VHOST
                                GROUP   BY ROLLUP(TA.VHOST, MOBILE_YN)
                                ) TB,
                                (
                                --유입경로별 체류시간, 페이지뷰
                                SELECT  NVL(MOBILE_YN, 'TOTAL') AS MOBILE_YN,
                                             NVL(TA.VHOST, 'TOTAL') AS VHOST,
                                             SUM(DUR_TIME) DUR_TIME,
                                             SUM(PAGE_VIEW) PAGE_VIEW
                                FROM    (
                                            SELECT  MOBILE_YN,
                                                    SESSION_ID, V_HOST VHOST
                                            FROM    TB_WL_REFERER_SESSION
                                            ) TA, TB_SESSION_LOG TB
                                WHERE   TA.SESSION_ID = TB.SESSION_ID
                                AND        TA.VHOST = TB.VHOST
                                GROUP   BY ROLLUP(TA.VHOST, MOBILE_YN)
                                ) TC,
                                (
                                --유입경로별 이탈수
                                SELECT  NVL(MOBILE_YN, 'TOTAL') AS MOBILE_YN,
                                             NVL(TA.VHOST, 'TOTAL') AS VHOST,
                                             SUM(PAGE_CNT) AS PAGE_CNT,
                                             COUNT(DECODE(PAGE_VIEW,1,TA.SESSION_ID, NULL)) BOUNCE_CNT
                                    FROM    (
                                            SELECT  SESSION_ID, V_HOST VHOST,
                                                    MOBILE_YN
                                            FROM    TB_WL_REFERER_SESSION
                                            ) TA,
                                            (
                                            SELECT  SESSION_ID, TA.VHOST,
                                                         COUNT(1) PAGE_VIEW,
                                                         COUNT(DISTINCT TA.URL) PAGE_CNT
                                            FROM    TB_WL_URL_ACCESS_SESSION TA, TB_URL_COMMENT TB
                                            WHERE  TA.URL = TB.URL
                                            AND       TA.VHOST = TB.VHOST
                                            AND      TB.SUB_TYPE ='Y'
                                            GROUP   BY SESSION_ID, TA.VHOST
                                            ) TB
                                WHERE   TA.SESSION_ID = TB.SESSION_ID
                                AND       TA.VHOST = TB.VHOST
                                GROUP   BY ROLLUP(TA.VHOST, MOBILE_YN)
                                ) TD
                    WHERE   TA.MOBILE_YN = TB.MOBILE_YN(+) AND TA.VHOST = TB.VHOST(+)
                    AND       TA.MOBILE_YN = TC.MOBILE_YN(+) AND TA.VHOST = TC.VHOST(+)
                    AND       TA.MOBILE_YN = TD.MOBILE_YN(+) AND TA.VHOST = TD.VHOST(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT DISTINCT GVHOST, MOBILE_YN  
             FROM
             (
             SELECT GVHOST
                  , (CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
             FROM TB_REFERER_SESSION
             UNION
             SELECT GVHOST
                  , (CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
             FROM TB_REFERER_SESSION3
             UNION
             SELECT GVHOST
                  , (CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
             FROM TB_ACCESS_SESSION
             UNION
             SELECT GVHOST
                  , (CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
             FROM TB_ACCESS_SESSION2
             )
    """
    spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("ALL_MOBILE_YN"); sqlDf2.count()
    
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.MOBILE_YN                                      AS EQUIP_TYPE
         , TA.GVHOST                                            AS GVHOST
         , SUM(CASE WHEN TF.VISIT_CNT         IS NULL THEN 0 ELSE TF.VISIT_CNT         END)                       AS VISIT_CNT
         , SUM(CASE WHEN TE.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TE.TOT_VISITOR_CNT   END)                       AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TE.NEW_VISITOR_CNT   END)                       AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TE.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TE.RE_VISITOR_CNT    END)                       AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)                       AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)                       AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)                       AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END)                       AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)                       AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)                       AS BOUNCE_CNT
    FROM
           (
           SELECT GVHOST, MOBILE_YN
           FROM ALL_MOBILE_YN
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT
                  TA.GVHOST                                                        AS GVHOST
                ,(CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) MOBILE_YN
                , SUM(TB.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TB.T_ID)                                          AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT DISTINCT
                         GVHOST
                       , MOBILE_YN
                       , SESSION_ID
                  FROM   TB_REFERER_SESSION
                  ) TA,
                  (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , T_ID                                                     AS T_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TB
           WHERE  TA.GVHOST     = TB.GVHOST
           AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, TA.MOBILE_YN
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.MOBILE_YN = TB.MOBILE_YN
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
           SELECT
                  GVHOST
                ,(CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
                , SUM(PAGE_VIEW)                                                              AS PAGE_VIEW
                , SUM(PAGE_CNT)                                                               AS PAGE_CNT
                , SUM(DUR_TIME)                                                               AS DUR_TIME
           FROM   TB_ACCESS_SESSION2
           GROUP BY GVHOST, MOBILE_YN
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.MOBILE_YN = TC.MOBILE_YN
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST
                ,(CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) AS MOBILE_YN
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)                         AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, MOBILE_YN
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.MOBILE_YN = TD.MOBILE_YN
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) MOBILE_YN
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST, MOBILE_YN
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.MOBILE_YN = TE.MOBILE_YN
           LEFT OUTER JOIN
           (
           -- 방문수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN MOBILE_YN IS NULL OR MOBILE_YN = '' THEN 'NA' ELSE MOBILE_YN END) MOBILE_YN
                , COUNT(*)             AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST, MOBILE_YN
           ) TF
           ON  TA.GVHOST = TF.GVHOST AND TA.MOBILE_YN = TF.MOBILE_YN
    GROUP BY TA.GVHOST, TA.MOBILE_YN
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
           SELECT
                             GVHOST
                , MOBILE_YN
                , SUM(PAGE_VIEW)  AS PAGE_VIEW
                , SUM(PAGE_CNT)                                            AS PAGE_CNT
                , SUM(DUR_TIME)   AS DUR_TIME
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)      AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, MOBILE_YN
    """
    spark.sql(qry).take(100).foreach(println);

    */

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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
