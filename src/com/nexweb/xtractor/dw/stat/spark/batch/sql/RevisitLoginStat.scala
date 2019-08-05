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
 * 설    명 : 일/월별 재방문자 추이 통계
 * 입    력 :

TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_REVISIT_LOGIN_STAT
 * 수정내역 :
 * 2019-01-22 | 피승현 | 최초작성
 */
object RevisitLoginStat {

  var spark: SparkSession = null
  var objNm  = "TB_REVISIT_LOGIN_STAT"
  var objNm2 = "LOGIN_DATA"
  var objNm3 = "LOGIN_DATA2"

  var statisDate = ""
  var statisType = ""
  var revistInterval = ""
  var revistCd = ""

  //var statisDate = "20181219"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
    println(objNm + ".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    revistInterval = "BASE_DAY"
    revistCd = "'20'||SUBSTR(V_ID, 2, 6) = '"+statisDate+"'"
    loadTables(); excuteSql(); saveToParqeut(); ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    revistInterval = "BASE_MONTH"
    revistCd = "'20'||SUBSTR(V_ID, 2, 4) = '"+statisDate+"'"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_REVISIT_LOGIN_STAT(STATIS_DATE, REVISIT_INTERVAL, USER_TYPE, VHOST, ATTR_ID, LOGIN_CNT, LOGIN_VISITOR_CNT, NEW_VISITOR_CNT, RE_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH LOGIN_DATA AS
          (
          SELECT  V_ID, VHOST, SESSION_ID,
            FN_REVISIT_DAY(V_ID) AS REVISIT_DAY,
                  FN_REVISIT_MONTH(V_ID) AS REVISIT_MON,
                  'L' AS LOGIN_YN,
                  NVL(FN_MOBILE_YN(V_ID), 'ETC') AS MOBILE_YN,
                  FN_AGE_CD(TO_CHAR(SYSDATE, 'YYYY') - OPT1) AS AGE,
                  NVL(OPT2, 'SETC') AS GENDER
          FROM TB_MEMBER_CLASS_DAY
          ),
          LOGIN_DATA2 AS
          (
          SELECT  V_ID, VHOST, SESSION_ID, REVISIT_DAY, REVISIT_MON, LOGIN_YN, MOBILE_YN, AGE, GENDER
          FROM  (
              SELECT  V_ID, VHOST, SESSION_ID, REVISIT_DAY, REVISIT_MON, LOGIN_YN, MOBILE_YN, AGE, GENDER,
                  ROW_NUMBER() OVER( PARTITION BY SESSION_ID ORDER BY V_ID) RNUM
              FROM  LOGIN_DATA
              )
          WHERE  RNUM = 1
          )
          SELECT   TA.STATIS_DATE,
             NVL(TA.REVISIT_INTERVAL, 'BASE_DAY') REVISIT_INTERVAL,
             NVL(TA.USER_TYPE, 'ALL') USER_TYPE,
             NVL(TA.VHOST, 'TOTAL') VHOST,
             NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
             NVL(LOGIN_CNT, 0) LOGIN_CNT,
             NVL(LOGIN_VISITOR_CNT, 0) LOGIN_VISITOR_CNT,
             NVL(NEW_VISITOR_CNT, 0) NEW_VISITOR_CNT,
             NVL(RE_VISITOR_CNT, 0) RE_VISITOR_CNT,
             NVL(DUR_TIME, 0) DUR_TIME,
             NVL(PAGE_CNT, 0) PAGE_CNT,
             NVL(PAGE_VIEW, 0) PAGE_VIEW,
             NVL(BOUNCE_CNT, 0) BOUNCE_CNT
              FROM
              (
          SELECT  ? STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
             COUNT(1) LOGIN_CNT,
             COUNT(DISTINCT V_ID) LOGIN_VISITOR_CNT,
             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
             COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
          FROM    LOGIN_DATA
          GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
          ) TA,
          (
          SELECT  ?  STATIS_DATE, NVL(TB.VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(TB.REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
             SUM(PAGE_VIEW) PAGE_VIEW,
             SUM(DUR_TIME) DUR_TIME
          FROM    TB_SEGMENT_SESSION_LOG TA, LOGIN_DATA TB
          WHERE   TA.SESSION_ID = TB.SESSION_ID
          GROUP   BY ROLLUP(TB.VHOST, TB.REVISIT_DAY)
          ) TB,
          (
          SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_DAY' AS REVISIT_INTERVAL, NVL(REVISIT_DAY, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
             COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
             SUM(PAGE_CNT) PAGE_CNT
          FROM    (
            SELECT  TC.SESSION_ID, TC.VHOST, TC.REVISIT_DAY,
                 COUNT(1) PAGE_VIEW,
                 COUNT(DISTINCT TA.URL) PAGE_CNT
            FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, LOGIN_DATA2 TC
            WHERE  TA.URL = TB.URL
            AND    TA.VHOST = TB.VHOST
            AND    TB.SUB_TYPE ='Y'
                  AND    TA.SESSION_ID = TC.SESSION_ID
            GROUP   BY TC.SESSION_ID, TC.VHOST, TC.REVISIT_DAY
               )
          GROUP   BY ROLLUP(VHOST, REVISIT_DAY)
          ) TC
          WHERE    TA.STATIS_DATE = TB.STATIS_DATE(+)
          AND TA.VHOST = TB.VHOST(+)
          AND TA.REVISIT_INTERVAL = TB.REVISIT_INTERVAL(+)
          AND TA.USER_TYPE = TB.USER_TYPE(+)
          --AND TA.ATTR_ID = TB.ATTR_ID(+)
          AND       TA.STATIS_DATE = TC.STATIS_DATE(+)
          AND TA.VHOST = TC.VHOST(+)
          AND TA.REVISIT_INTERVAL = TC.REVISIT_INTERVAL(+)
          AND TA.USER_TYPE = TC.USER_TYPE(+)
          --AND TA.ATTR_ID = TC.ATTR_ID(+)

          UNION
          SELECT   TA.STATIS_DATE,
             NVL(TA.REVISIT_INTERVAL, 'BASE_MONTH') REVISIT_INTERVAL,
             NVL(TA.USER_TYPE, 'ALL') USER_TYPE,
             NVL(TA.VHOST, 'TOTAL') VHOST,
             NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
             NVL(LOGIN_CNT, 0) LOGIN_CNT,
             NVL(LOGIN_VISITOR_CNT, 0) LOGIN_VISITOR_CNT,
             NVL(NEW_VISITOR_CNT, 0) NEW_VISITOR_CNT,
             NVL(RE_VISITOR_CNT, 0) RE_VISITOR_CNT,
             NVL(DUR_TIME, 0) DUR_TIME,
             NVL(PAGE_CNT, 0) PAGE_CNT,
             NVL(PAGE_VIEW, 0) PAGE_VIEW,
             NVL(BOUNCE_CNT, 0) BOUNCE_CNT
              FROM
              (
          SELECT ? STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
            COUNT(1) LOGIN_CNT,
            COUNT(DISTINCT V_ID) LOGIN_VISITOR_CNT,
            COUNT(UNIQUE (CASE  WHEN ( RVID >= TO_CHAR(SYSDATE -30, 'YYYYMMDD')) OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
            COUNT(UNIQUE (CASE  WHEN RVID < TO_CHAR(SYSDATE -30, 'YYYYMMDD') AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
          FROM
          (
          SELECT  V_ID, VHOST, REVISIT_MON,
            '20'||(CASE WHEN '20'||SUBSTR(V_ID, 2, 2) > TO_CHAR(SYSDATE, 'YYYY') THEN TO_CHAR(SYSDATE, 'YY') ELSE SUBSTR(V_ID, 2, 2) END)||
            (CASE WHEN SUBSTR(V_ID, 4, 2) > '12' THEN '12' ELSE SUBSTR(V_ID, 4, 2) END)||
            (CASE WHEN SUBSTR(V_ID, 6, 2) > '30' THEN '30' ELSE SUBSTR(V_ID, 6, 2) END) AS RVID
          FROM    LOGIN_DATA
          )
          GROUP   BY ROLLUP(VHOST, REVISIT_MON)
          ) TA,
          (
          SELECT  ?  STATIS_DATE, NVL(TB.VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(TB.REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
             SUM(PAGE_VIEW) PAGE_VIEW,
             SUM(DUR_TIME) DUR_TIME
          FROM    TB_SEGMENT_SESSION_LOG TA, LOGIN_DATA TB
          WHERE   TA.SESSION_ID = TB.SESSION_ID
          GROUP   BY ROLLUP(TB.VHOST, TB.REVISIT_MON)
          ) TB,
          (
          SELECT  ?  STATIS_DATE, NVL(VHOST, 'TOTAL') VHOST, 'BASE_MONTH' AS REVISIT_INTERVAL, NVL(REVISIT_MON, 'ALL') AS USER_TYPE, 'ALL' AS ATTR_ID,
             COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
             SUM(PAGE_CNT) PAGE_CNT
          FROM    (
            SELECT  TC.SESSION_ID, TC.VHOST, TC.REVISIT_MON,
                 COUNT(1) PAGE_VIEW,
                 COUNT(DISTINCT TA.URL) PAGE_CNT
            FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, LOGIN_DATA2 TC
            WHERE  TA.URL = TB.URL
            AND    TA.VHOST = TB.VHOST
            AND    TB.SUB_TYPE ='Y'
                  AND    TA.SESSION_ID = TC.SESSION_ID
            GROUP   BY TC.SESSION_ID, TC.VHOST, TC.REVISIT_MON
               )
          GROUP   BY ROLLUP(VHOST, REVISIT_MON)
          ) TC
          WHERE  TA.STATIS_DATE = TB.STATIS_DATE(+)
          AND TA.VHOST = TB.VHOST(+)
          AND TA.REVISIT_INTERVAL = TB.REVISIT_INTERVAL(+)
          AND TA.USER_TYPE = TB.USER_TYPE(+)
          --AND TA.ATTR_ID = TB.ATTR_ID(+)

          AND TA.STATIS_DATE = TC.STATIS_DATE(+)
          AND TA.VHOST = TC.VHOST(+)
          AND TA.REVISIT_INTERVAL = TC.REVISIT_INTERVAL(+)
          AND TA.USER_TYPE = TC.USER_TYPE(+)
          --AND TA.ATTR_ID = TC.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""

    qry =
    s"""
    SELECT
           TA.GVHOST                                     AS GVHOST
         , TA.V_ID                                       AS V_ID
         , TA.SESSION_ID                                 AS SESSION_ID
         , CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 6) = '${statisDate}'             THEN 'NV' ELSE 'RV' END AS REVISIT_DAY
         , CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 4) = SUBSTR('${statisDate}',1,6) THEN 'NV' ELSE 'RV' END AS REVISIT_MONTH
    FROM   TB_MEMBER_CLASS_SESSION TA
    """
    //spark.sql(qry).take(100).foreach(println);
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("LOGIN_DATA"); sqlDf2.count()

    qry =
    s"""
    SELECT
           GVHOST
         , V_ID
         , SESSION_ID
         , REVISIT_DAY
         , REVISIT_MONTH
    FROM   (
           SELECT
                  GVHOST
                , V_ID
                , SESSION_ID
                , REVISIT_DAY
                , REVISIT_MONTH
                , ROW_NUMBER() OVER( PARTITION BY SESSION_ID ORDER BY V_ID) AS RNUM
           FROM   LOGIN_DATA
           )
    WHERE  RNUM = 1
    """
    //spark.sql(qry).take(100).foreach(println);
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("LOGIN_DATA2"); sqlDf3.count()

    qry =
    s"""
    SELECT
           '${statisDate}'                                                                             AS STATIS_DATE
         , '${revistInterval}'                                                                         AS REVISIT_INTERVAL
         , CASE WHEN TA.REVISIT_CD IS NULL THEN 'TOTAL' ELSE TA.REVISIT_CD END                         AS USER_TYPE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END                                 AS GVHOST
         , 'ALL'                                                                                       AS ATTR_ID
         , SUM(CASE WHEN TA.LOGIN_CNT         IS NULL THEN 0 ELSE TA.LOGIN_CNT         END)            AS LOGIN_CNT
         , SUM(CASE WHEN TA.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TA.LOGIN_VISITOR_CNT END)            AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TA.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TA.NEW_VISITOR_CNT   END)            AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TA.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TA.RE_VISITOR_CNT    END)            AS RE_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)            AS DUR_TIME
         , SUM(CASE WHEN TD.PAGE_CNT          IS NULL THEN 0 ELSE TD.PAGE_CNT          END)            AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)            AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)            AS BOUNCE_CNT
    FROM
           (
           --유입경로별 방문수
           SELECT GVHOST                                                                                           AS GVHOST
                , CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END                          AS REVISIT_CD
                , COUNT(*)                                                                                         AS LOGIN_CNT
                , COUNT(DISTINCT V_ID)                                                                             AS LOGIN_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN ${revistCd} THEN V_ID ELSE NULL END) ) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN ${revistCd} THEN V_ID ELSE NULL END) ) AS RE_VISITOR_CNT
           FROM   LOGIN_DATA
           GROUP BY GVHOST, CASE WHEN '${statisType}' = 'D' THEN REVISIT_DAY ELSE REVISIT_MONTH END
           ) TA
           LEFT OUTER JOIN
           (
           -- 유입경로별 체류시간, 페이지뷰
           SELECT
                  TA.GVHOST                                                          AS GVHOST
                , CASE WHEN '${statisType}' = 'D' THEN TB.REVISIT_DAY ELSE TB.REVISIT_MONTH END AS REVISIT_CD
                , SUM(TA.PAGE_VIEW)                                                  AS PAGE_VIEW
                , SUM(TA.DUR_TIME)                                                   AS DUR_TIME
           FROM   TB_ACCESS_SESSION TA, LOGIN_DATA TB
           WHERE   TA.SESSION_ID = TB.SESSION_ID
           GROUP BY ROLLUP(TA.GVHOST, REVISIT_CD)
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.REVISIT_CD = TC.REVISIT_CD
           LEFT OUTER JOIN
           (
           --이탈수
           SELECT
                  GVHOST                                                          AS GVHOST
                , REVISIT_CD                                                      AS REVISIT_CD
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END) AS BOUNCE_CNT
           FROM   (
                  SELECT TA.GVHOST                                                   AS GVHOST
                       , CASE WHEN '${statisType}' = 'D' THEN TB.REVISIT_DAY ELSE TB.REVISIT_MONTH END AS REVISIT_CD
                       , TA.SESSION_ID                                               AS SESSION_ID
                       , SUM(TA.PAGE_VIEW)                                           AS PAGE_VIEW
                       , SUM(TA.PAGE_CNT)                                            AS PAGE_CNT
                   FROM  TB_ACCESS_SESSION TA, LOGIN_DATA2 TB
                   WHERE   TA.SESSION_ID = TB.SESSION_ID
                   GROUP BY TA.GVHOST, REVISIT_CD, TA.SESSION_ID
            )
            GROUP BY ROLLUP(GVHOST, REVISIT_CD)
            ) TD
            ON  TA.GVHOST = TD.GVHOST AND TA.REVISIT_CD = TD.REVISIT_CD
    GROUP BY ROLLUP(TA.GVHOST, TA.REVISIT_CD)
    """
    spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    s"""
           SELECT
                  GVHOST                                                          AS GVHOST
                , REVISIT_CD                                                      AS REVISIT_CD
                , SUM(PAGE_CNT)                                                   AS PAGE_CNT
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN SESSION_ID ELSE NULL END) AS BOUNCE_CNT
           FROM   (
                  SELECT TA.GVHOST                                                   AS GVHOST
                       , CASE WHEN '${statisType}' = 'D' THEN TB.REVISIT_DAY ELSE TB.REVISIT_MONTH END AS REVISIT_CD
                       , TA.SESSION_ID                                               AS SESSION_ID
                       , SUM(TA.PAGE_VIEW)                                           AS PAGE_VIEW
                       , SUM(TA.PAGE_CNT)                                            AS PAGE_CNT
                   FROM  TB_ACCESS_SESSION TA, LOGIN_DATA2 TB
                   WHERE   TA.SESSION_ID = TB.SESSION_ID
                   GROUP BY TA.GVHOST, REVISIT_CD, TA.SESSION_ID
            )
            GROUP BY ROLLUP(GVHOST, REVISIT_CD)
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND REVISIT_INTERVAL='"+revistInterval+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
