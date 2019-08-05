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

TB_REFERER_SESSION
TB_ACCESS_SESSION
TB_MEMBER_CLASS_SESSION

 * 출    력 : TB_VISIT_PAGEVIEW_STAT
 * 수정내역 :
 * 2019-01-21 | 피승현 | 최초작성
 */
object VisitPageViewStat {

  var spark: SparkSession = null
  var objNm  = "TB_VISIT_PAGEVIEW_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_PAGEVIEW_STAT";var statisDate = "20190313"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_VISIT_PAGEVIEW_STAT(STATIS_DATE,PAGEVIEW_SECT,VHOST,ATTR_ID,VISIT_CNT,TOT_VISITOR_CNT,NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH PAGE_T AS
          (
          SELECT  SESSION_ID, PAGE_VIEW, DUR_TIME, FN_PAGE_CD(PAGE_CNT) PAGE_CD, VHOST
          FROM    TB_SEGMENT_SESSION_LOG
          )
          SELECT ? AS STATIS_DATE,
            NVL(TA.PAGE_CD, '00') AS PAGE_CD,
            NVL(TA.VHOST, 'TOTAL') AS VHOST,
            NVL(TA.ATTR_ID, 'ALL') AS ATTR_ID,
            NVL(VISIT_CNT, 0) AS VISIT_CNT,
            NVL(TOT_VISITOR_CNT, 0) AS TOT_VISITOR_CNT,
            NVL(NEW_VISITOR_CNT, 0) AS NEW_VISITOR_CNT,
            NVL(RE_VISITOR_CNT, 0) AS RE_VISITOR_CNT,
            NVL(LOGIN_CNT, 0) AS LOGIN_CNT,
            NVL(LOGIN_VISITOR_CNT, 0) AS LOGIN_VISITOR_CNT,
            NVL(DUR_TIME, 0) AS DUR_TIME,
            NVL(PAGE_CNT, 0) AS PAGE_CNT,
            NVL(PAGE_VIEW, 0) AS PAGE_VIEW,
            NVL(BOUNCE_CNT, 0) AS BOUNCE_CNT
            FROM
            (
            -- 방문수, 방문자수, 신규 방문자수, 재방문자수, 체류시간, 페이지뷰
            SELECT NVL(PAGE_CD, '00') AS PAGE_CD, NVL(TA.VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
              SUM(VISIT_CNT) VISIT_CNT,
              COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
              COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
              COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM
            (
            SELECT  SESSION_ID, VHOST,
              COUNT(1) VISIT_CNT,
              MIN(V_ID) V_ID
            FROM    TB_WL_REFERER_SEGMENT
            GROUP   BY SESSION_ID, VHOST
            ) TA, PAGE_T TB
            WHERE   TA.SESSION_ID = TB.SESSION_ID
            AND     TA.VHOST = TB.VHOST
            GROUP   BY ROLLUP(TA.VHOST, PAGE_CD)
            ) TA,
            (
            -- 로그인수, 로그인 방문자수
            SELECT  NVL(PAGE_CD, '00') AS PAGE_CD, NVL(TA.VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
              SUM(LOGIN_CNT) LOGIN_CNT,
              COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
            FROM
              (
              SELECT  VHOST, V_ID, L_ID, SESSION_ID, LOGIN_CNT
              FROM    TB_MEMBER_CLASS_SEG_STAT
              ) TA, PAGE_T  TB
            WHERE   TA.SESSION_ID = TB.SESSION_ID
            AND TA.VHOST = TB.VHOST
            GROUP   BY ROLLUP(TA.VHOST, PAGE_CD)
            ) TB,
            (
            -- 이탈수, 페이지수
            SELECT  NVL(PAGE_CD, '00') AS PAGE_CD, NVL(VHOST, 'TOTAL') VHOST, 'ALL' AS ATTR_ID,
            COUNT(DECODE(PAGE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT,
            SUM(PAGE_CNT) PAGE_CNT
            FROM    (
              SELECT VHOST, FN_PAGE_CD(PAGE_CNT) PAGE_CD, SESSION_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(PAGE_CNT) AS PAGE_CNT
              FROM TB_SEGMENT_SESSION_LOG
              GROUP BY VHOST, FN_PAGE_CD(PAGE_CNT), SESSION_ID
              )
            GROUP   BY ROLLUP(VHOST, PAGE_CD)
            ) TC,
            (
            SELECT  NVL(VHOST, 'ALL') AS VHOST, NVL(FN_PAGE_CD(PAGE_CNT), '00') AS PAGE_CD, 'ALL' AS ATTR_ID,
                SUM(PAGE_VIEW) AS PAGE_VIEW, SUM(DUR_TIME) AS DUR_TIME
              FROM    TB_SEGMENT_SESSION_LOG
              GROUP BY ROLLUP(VHOST, FN_PAGE_CD(PAGE_CNT))
            ) TD
            WHERE TA.VHOST = TB.VHOST(+) AND TA.PAGE_CD = TB.PAGE_CD(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
            AND   TA.VHOST = TC.VHOST(+) AND TA.PAGE_CD = TC.PAGE_CD(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
            AND   TA.VHOST = TD.VHOST(+) AND TA.PAGE_CD = TD.PAGE_CD(+) AND TA.ATTR_ID = TD.ATTR_ID(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST,
           SESSION_ID,
           CASE WHEN PAGE_CNT =  1 THEN '01'
      					WHEN PAGE_CNT =  2 THEN '02'
      					WHEN PAGE_CNT =  3 THEN '03'
      					WHEN PAGE_CNT =  4 THEN '04'
      					WHEN PAGE_CNT =  5 THEN '05'
      					WHEN PAGE_CNT =  6 THEN '06'
      					WHEN PAGE_CNT =  7 THEN '07'
      					WHEN PAGE_CNT =  8 THEN '08'
      					WHEN PAGE_CNT =  9 THEN '09'
      					WHEN PAGE_CNT = 10 THEN '10'
      					WHEN PAGE_CNT BETWEEN 11 AND 15 THEN '11'
      					WHEN PAGE_CNT BETWEEN 16 AND 20 THEN '12'
      					WHEN PAGE_CNT BETWEEN 21 AND 25 THEN '13'
      					WHEN PAGE_CNT BETWEEN 26 AND 30 THEN '14'
      					WHEN PAGE_CNT >= 31             THEN '15'
      					ELSE '99'
					END AS PAGE_CD
					FROM
					    (
              SELECT GVHOST
                   , SESSION_ID
                   , COUNT(DISTINCT URL) AS PAGE_CNT
              FROM   TB_ACCESS_SESSION2
              WHERE  LENGTH(GVHOST) > 0
              GROUP BY GVHOST, SESSION_ID
              )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("SESSION_PAGE_CNT"); sqlDf2.count()

    qry =
    s"""
    SELECT GVHOST,
           V_ID,
           CASE WHEN PAGE_CNT =  1 THEN '01'
      					WHEN PAGE_CNT =  2 THEN '02'
      					WHEN PAGE_CNT =  3 THEN '03'
      					WHEN PAGE_CNT =  4 THEN '04'
      					WHEN PAGE_CNT =  5 THEN '05'
      					WHEN PAGE_CNT =  6 THEN '06'
      					WHEN PAGE_CNT =  7 THEN '07'
      					WHEN PAGE_CNT =  8 THEN '08'
      					WHEN PAGE_CNT =  9 THEN '09'
      					WHEN PAGE_CNT = 10 THEN '10'
      					WHEN PAGE_CNT BETWEEN 11 AND 15 THEN '11'
      					WHEN PAGE_CNT BETWEEN 16 AND 20 THEN '12'
      					WHEN PAGE_CNT BETWEEN 21 AND 25 THEN '13'
      					WHEN PAGE_CNT BETWEEN 26 AND 30 THEN '14'
      					WHEN PAGE_CNT >= 31             THEN '15'
      					ELSE '99'
					END AS PAGE_CD
					FROM
					    (
              SELECT GVHOST
                   , V_ID
                   , COUNT(DISTINCT URL) AS PAGE_CNT
              FROM   TB_ACCESS_SESSION2
              WHERE  LENGTH(GVHOST) > 0
              GROUP BY GVHOST, V_ID
              )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("VID_PAGE_CNT"); sqlDf3.count()
   
 
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                             AS STATIS_DATE
         , TA.GVHOST                                  AS GVHOST
         , TA.PAGE_CD                                AS PAGEVIEW_SECT
         , 'ALL'                                                                                       AS ATTR_ID
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)            AS VISIT_CNT
         , SUM(CASE WHEN TE.TOT_VISITOR_CNT   IS NULL THEN 0 ELSE TE.TOT_VISITOR_CNT   END)            AS TOT_VISITOR_CNT
         , SUM(CASE WHEN TE.NEW_VISITOR_CNT   IS NULL THEN 0 ELSE TE.NEW_VISITOR_CNT   END)            AS NEW_VISITOR_CNT
         , SUM(CASE WHEN TE.RE_VISITOR_CNT    IS NULL THEN 0 ELSE TE.RE_VISITOR_CNT    END)            AS RE_VISITOR_CNT
         , SUM(CASE WHEN TB.LOGIN_CNT         IS NULL THEN 0 ELSE TB.LOGIN_CNT         END)            AS LOGIN_CNT
         , SUM(CASE WHEN TB.LOGIN_VISITOR_CNT IS NULL THEN 0 ELSE TB.LOGIN_VISITOR_CNT END)            AS LOGIN_VISITOR_CNT
         , SUM(CASE WHEN TC.DUR_TIME          IS NULL THEN 0 ELSE TC.DUR_TIME          END)            AS DUR_TIME
         , SUM(CASE WHEN TC.PAGE_CNT          IS NULL THEN 0 ELSE TC.PAGE_CNT          END)            AS PAGE_CNT
         , SUM(CASE WHEN TC.PAGE_VIEW         IS NULL THEN 0 ELSE TC.PAGE_VIEW         END)            AS PAGE_VIEW
         , SUM(CASE WHEN TD.BOUNCE_CNT        IS NULL THEN 0 ELSE TD.BOUNCE_CNT        END)            AS BOUNCE_CNT
    FROM
           (
           -- 방문수
           SELECT TA.GVHOST    AS GVHOST
                , TA.PAGE_CD   AS PAGE_CD
                , COUNT(*)  AS VISIT_CNT
               FROM (
               SELECT TA.GVHOST             AS GVHOST
                    , TA.SESSION_ID         AS SESSION_ID
                    , NVL(TB.PAGE_CD, '99') AS PAGE_CD
               FROM   TB_REFERER_SESSION TA
               LEFT OUTER JOIN SESSION_PAGE_CNT TB
               ON TA.GVHOST = TB.GVHOST AND  TA.SESSION_ID = TB.SESSION_ID
               ) TA
           GROUP BY TA.GVHOST, TA.PAGE_CD
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인수, 로그인 방문자수
           SELECT
                 TB.GVHOST                                                    AS GVHOST
               , NVL(TA.PAGE_CD, '99')                                        AS PAGE_CD
               , SUM(TB.LOGIN_CNT)                                            AS LOGIN_CNT
               , COUNT(DISTINCT TB.T_ID)                                      AS LOGIN_VISITOR_CNT
           FROM   SESSION_PAGE_CNT TA
           RIGHT OUTER JOIN
                  (
                  SELECT GVHOST                                               AS GVHOST
                       , SESSION_ID                                           AS SESSION_ID
                       , T_ID                                                 AS T_ID
                       , COUNT(*)                                             AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TB
           ON  TA.GVHOST     = TB.GVHOST
           AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TB.GVHOST, NVL(TA.PAGE_CD, '99')
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.PAGE_CD = TB.PAGE_CD
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
             SELECT
                    GVHOST                                                      AS GVHOST
                  , PAGE_CD                                                     AS PAGE_CD
                  , SUM(PAGE_VIEW)                                              AS PAGE_VIEW
                  , SUM(PAGE_CNT)                                               AS PAGE_CNT
                  , SUM(DUR_TIME)                                               AS DUR_TIME
             FROM   (
          					SELECT GVHOST, 
          							SESSION_ID, 
          					CASE WHEN COUNT(DISTINCT URL) =  1 THEN '01'
          					WHEN COUNT(DISTINCT URL) =  2 THEN '02'
          					WHEN COUNT(DISTINCT URL) =  3 THEN '03'
          					WHEN COUNT(DISTINCT URL) =  4 THEN '04'
          					WHEN COUNT(DISTINCT URL) =  5 THEN '05'
          					WHEN COUNT(DISTINCT URL) =  6 THEN '06'
          					WHEN COUNT(DISTINCT URL) =  7 THEN '07'
          					WHEN COUNT(DISTINCT URL) =  8 THEN '08'
          					WHEN COUNT(DISTINCT URL) =  9 THEN '09'
          					WHEN COUNT(DISTINCT URL) = 10 THEN '10'
          					WHEN COUNT(DISTINCT URL) BETWEEN 11 AND 15 THEN '11'
          					WHEN COUNT(DISTINCT URL) BETWEEN 16 AND 20 THEN '12'
          					WHEN COUNT(DISTINCT URL) BETWEEN 21 AND 25 THEN '13'
          					WHEN COUNT(DISTINCT URL) BETWEEN 26 AND 30 THEN '14'
          					WHEN COUNT(DISTINCT URL) >= 31             THEN '15'
          					ELSE '99'
          					END AS PAGE_CD,
          					SUM(PAGE_VIEW) AS PAGE_VIEW,
          					SUM(PAGE_CNT) AS PAGE_CNT,
          					SUM(DUR_TIME) AS DUR_TIME
          					FROM TB_ACCESS_SESSION2
          					GROUP BY GVHOST, SESSION_ID
  					       )
  		     GROUP BY GVHOST, PAGE_CD
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.PAGE_CD = TC.PAGE_CD
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  GVHOST                                                      AS GVHOST
                , PAGE_CD                                                     AS PAGE_CD
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)         AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, PAGE_CD
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.PAGE_CD = TD.PAGE_CD
           LEFT OUTER JOIN
           (
           SELECT
                  GVHOST                           AS GVHOST
                , PAGE_CD                          AS PAGE_CD
                , COUNT(DISTINCT V_ID)             AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = STATIS_DATE THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != STATIS_DATE THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM    (
               SELECT TA.GVHOST             AS GVHOST
                    , TA.V_ID               AS V_ID
                    , STATIS_DATE
                    , NVL(TB.PAGE_CD, '99') AS PAGE_CD
               FROM   TB_REFERER_SESSION3 TA
               LEFT OUTER JOIN VID_PAGE_CNT TB
               ON TA.GVHOST = TB.GVHOST AND  TA.V_ID = TB.V_ID
               ) TA
           GROUP BY GVHOST, PAGE_CD
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.PAGE_CD = TE.PAGE_CD
    GROUP BY TA.GVHOST, TA.PAGE_CD
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    """
           SELECT GVHOST                                                                        AS GVHOST
                , PAGE_CD                                                                       AS PAGE_CD
                , SUM(VISIT_CNT)                                                                AS VISIT_CNT
                , COUNT(DISTINCT V_ID)                                                          AS TOT_VISITOR_CNT
                , COUNT(DISTINCT CASE WHEN '20'||SUBSTR(V_ID, 2, 6) = '20181219' OR V_ID LIKE '.'
                       THEN V_ID
                       ELSE NULL
                  END)                                                                          AS NEW_VISITOR_CNT
                , COUNT(DISTINCT CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '20181219' AND V_ID NOT LIKE '.'
                       THEN V_ID
                       ELSE NULL
                  END)                                                                          AS RE_VISITOR_CNT
           FROM   TB_ACCESS_SESSION
           GROUP BY GVHOST, PAGE_CD
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
