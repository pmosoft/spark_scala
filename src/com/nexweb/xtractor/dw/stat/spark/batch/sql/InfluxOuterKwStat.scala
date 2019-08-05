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
 * 설    명 :
 * 입    력 :

TB_WL_REFERER
TB_WL_URL_ACCESS
TB_MEMBER_CLASS

 * 출    력 : TB_INFLUX_OUTER_KW_STAT
 * 수정내역 :
 * 2019-02-07 | 피승현 | 최초작성
 */
object InfluxOuterKwStat {

  var spark: SparkSession = null
  var objNm  = "TB_INFLUX_OUTER_KW_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_INFLUX_OUTER_KW_STAT";var statisDate = "20190313"; var statisType = "D"
  //var prevYyyymmDt = "201902";var statisDate = "201812"; var statisType = "M"

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

          INSERT INTO TB_INFLUX_OUTER_KW_STAT(STATIS_DATE, VHOST, KEYWORD, SEARCH_TYPE, ATTR_ID, VISIT_CNT, TOT_VISITOR_CNT, NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH SEARCH_TYPE AS
          (
          SELECT SESSION_ID, NVL(KEYWORD, 'NONE') KEYWORD, DOMAIN
          FROM    TB_WL_REFERER_SEGMENT
          --WHERE KEYWORD IS NOT NULL
          WHERE CATEGORY = 'SK '
          )
          SELECT  ? AS STATIS_DATE,
            NVL(TA.VHOST, 'TOTAL') VHOST,
            NVL(TA.DOMAIN, 'TOTAL') AS DOMAIN,
            'DM' AS SEARCH_TYPE,
            NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
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
            FROM
            (
            -- 방문수, 방문자수, 신규 방문자수, 재방문자수
            SELECT  NVL(VHOST, 'TOTAL') AS VHOST,NVL(DOMAIN, 'TOTAL') AS DOMAIN, 'ALL' AS ATTR_ID,
                COUNT(V_ID) AS VISIT_CNT,
                COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM    TB_WL_REFERER_SEGMENT
            WHERE KEYWORD IS NOT NULL
            GROUP   BY  ROLLUP (VHOST, DOMAIN)
            ) TA,
            (
            -- 로그인 수, 로그인 회원수
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(DOMAIN, 'TOTAL') AS DOMAIN, 'ALL' AS ATTR_ID,
              SUM(LOGIN_CNT) AS LOGIN_CNT, COUNT(UNIQUE L_ID) AS LOGIN_VISITOR_CNT
              FROM
              (
              SELECT VHOST, DOMAIN, L_ID, LOGIN_CNT
                  FROM TB_MEMBER_CLASS_SEG_STAT TA, SEARCH_TYPE TB
                  WHERE TA.SESSION_ID = TB.SESSION_ID
              )
              GROUP BY ROLLUP (VHOST, DOMAIN)
            ) TB,
            (
            -- 페이지뷰, 체류시간
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(DOMAIN, 'TOTAL') AS DOMAIN, 'ALL' AS ATTR_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(DUR_TIME) AS DUR_TIME
              FROM
              (
              SELECT  VHOST, DOMAIN,
                  PAGE_VIEW, DUR_TIME
              FROM    TB_SEGMENT_SESSION_LOG TA, SEARCH_TYPE TB
              WHERE TA.SESSION_ID = TB.SESSION_ID
              )
              GROUP BY ROLLUP (VHOST, DOMAIN)
            ) TC,
            (
            -- 페이지수, 이탈수
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(DOMAIN, 'TOTAL') AS DOMAIN, 'ALL' AS ATTR_ID,
            SUM(PAGE_CNT) AS PAGE_CNT,
            COUNT(DECODE(BOUNCE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT
            FROM
            (
            SELECT  TA.VHOST, DOMAIN, TA.SESSION_ID,
              COUNT(1) BOUNCE_VIEW,
              COUNT(DISTINCT TA.URL) PAGE_CNT
            FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, SEARCH_TYPE TC
            WHERE  TA.URL = TB.URL
            AND    TA.VHOST = TB.VHOST
            AND    TB.SUB_TYPE ='Y'
            AND    TA.SESSION_ID = TC.SESSION_ID
            GROUP BY V_ID, TA.SESSION_ID, TA.VHOST, DOMAIN
            )
            GROUP BY ROLLUP (VHOST, DOMAIN)
            ) TD
            WHERE TA.VHOST = TB.VHOST(+) AND TA.DOMAIN = TB.DOMAIN(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
            AND   TA.VHOST = TC.VHOST(+) AND TA.DOMAIN = TC.DOMAIN(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
            AND   TA.VHOST = TD.VHOST(+) AND TA.DOMAIN = TD.DOMAIN(+) AND TA.ATTR_ID = TD.ATTR_ID(+)
          UNION
          SELECT  ? AS STATIS_DATE,
            NVL(TA.VHOST, 'TOTAL') VHOST,
            NVL(TA.KEYWORD, 'TOTAL') AS KEYWORD,
            'KW' AS SEARCH_TYPE,
            NVL(TA.ATTR_ID, 'ALL') ATTR_ID,
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
            FROM
            (
            -- 방문수, 방문자수, 신규 방문자수, 재방문자수
            SELECT  NVL(VHOST, 'TOTAL') AS VHOST,NVL(KEYWORD, 'TOTAL') AS KEYWORD, 'ALL' AS ATTR_ID,
                COUNT(V_ID) AS VISIT_CNT,
                COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM    TB_WL_REFERER_SEGMENT
            WHERE KEYWORD IS NOT NULL
            GROUP   BY  ROLLUP (VHOST, KEYWORD)
            ) TA,
            (
            -- 로그인 수, 로그인 회원수
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(KEYWORD, 'TOTAL') AS KEYWORD, 'ALL' AS ATTR_ID,
              SUM(LOGIN_CNT) AS LOGIN_CNT, COUNT(UNIQUE L_ID) AS LOGIN_VISITOR_CNT
              FROM
              (
              SELECT VHOST, KEYWORD, L_ID, LOGIN_CNT
                  FROM TB_MEMBER_CLASS_SEG_STAT TA, SEARCH_TYPE TB
                  WHERE TA.SESSION_ID = TB.SESSION_ID
              )
              GROUP BY ROLLUP (VHOST, KEYWORD)
            ) TB,
            (
            -- 페이지뷰, 체류시간
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(KEYWORD, 'TOTAL') AS KEYWORD, 'ALL' AS ATTR_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(DUR_TIME) AS DUR_TIME
              FROM
              (
              SELECT  VHOST, KEYWORD,
                  PAGE_VIEW, DUR_TIME
              FROM    TB_SEGMENT_SESSION_LOG TA, SEARCH_TYPE TB
              WHERE TA.SESSION_ID = TB.SESSION_ID
              )
              GROUP BY ROLLUP (VHOST, KEYWORD)
            ) TC,
            (
            -- 페이지수, 이탈수
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(KEYWORD, 'TOTAL') AS KEYWORD, 'ALL' AS ATTR_ID,
            SUM(PAGE_CNT) AS PAGE_CNT,
            COUNT(DECODE(BOUNCE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT
            FROM
            (
            SELECT  TA.VHOST, TC.KEYWORD, TA.SESSION_ID,
              COUNT(1) BOUNCE_VIEW,
              COUNT(DISTINCT TA.URL) PAGE_CNT
            FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, SEARCH_TYPE TC
            WHERE  TA.URL = TB.URL
            AND    TA.VHOST = TB.VHOST
            AND    TB.SUB_TYPE ='Y'
            AND    TA.SESSION_ID = TC.SESSION_ID
            GROUP BY V_ID, TA.SESSION_ID, TA.VHOST, TC.KEYWORD
            )
            GROUP BY ROLLUP (VHOST, KEYWORD)
            ) TD
            WHERE TA.VHOST = TB.VHOST(+) AND TA.KEYWORD = TB.KEYWORD(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
            AND   TA.VHOST = TC.VHOST(+) AND TA.KEYWORD = TC.KEYWORD(+) AND TA.ATTR_ID = TC.ATTR_ID(+)
            AND   TA.VHOST = TD.VHOST(+) AND TA.KEYWORD = TD.KEYWORD(+) AND TA.ATTR_ID = TD.ATTR_ID(+)

 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST, 
           SESSION_ID
         , MIN(DOMAIN) AS DOMAIN
         , MIN(KEYWORD) AS KEYWORD
    FROM   TB_REFERER_SESSION
    WHERE KEYWORD IS NOT NULL
    AND    KEYWORD != 'N'
    GROUP BY GVHOST, SESSION_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("REFERER_DOMAIN_KEYWORD"); sqlDf2.count()

    qry =
    s"""
      SELECT DISTINCT GVHOST, SEARCH_TYPE, SEARCH
      FROM
          (
          SELECT GVHOST
               , (CASE WHEN KEYWORD IS NULL OR KEYWORD = '' THEN 'N' ELSE KEYWORD END) AS SEARCH
               , 'KW' AS SEARCH_TYPE
          FROM   TB_REFERER_SESSION
          WHERE KEYWORD IS NOT NULL
          AND    KEYWORD != 'N'
          UNION
          SELECT GVHOST
               , (CASE WHEN KEYWORD IS NULL OR KEYWORD = '' THEN 'N' ELSE KEYWORD END) AS SEARCH
               , 'KW' AS SEARCH_TYPE
          FROM   TB_REFERER_SESSION3
          WHERE KEYWORD IS NOT NULL
          AND    KEYWORD != 'N'
          UNION
          SELECT GVHOST
               , (CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END) AS SEARCH
               , 'DM' AS SEARCH_TYPE
          FROM   TB_REFERER_SESSION
          WHERE KEYWORD IS NOT NULL
          AND    KEYWORD != 'N'
          UNION
          SELECT GVHOST
               , (CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END) AS SEARCH
               , 'DM' AS SEARCH_TYPE
          FROM   TB_REFERER_SESSION3
          WHERE KEYWORD IS NOT NULL
          AND    KEYWORD != 'N'
          )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("REFERER_KEYWORD_META"); sqlDf3.count()

    qry =
    s"""
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.GVHOST                                             AS GVHOST
         , TA.DOMAIN                                             AS KEYWORD
         , 'DM'                                                                                                   AS SEARCH_TYPE
         , 'ALL'                                                                                                  AS ATTR_ID
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
           SELECT GVHOST, SEARCH AS DOMAIN
           FROM REFERER_KEYWORD_META
           WHERE SEARCH_TYPE = 'DM'
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT
                  TA.GVHOST                                                        AS GVHOST
                ,(CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END) AS DOMAIN
                , SUM(TA.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TA.T_ID)                                          AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , T_ID                                                     AS T_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TA
                  LEFT OUTER JOIN
                  (
                  SELECT DISTINCT
                         GVHOST
                       , DOMAIN
                       , SESSION_ID
                  FROM   TB_REFERER_SESSION
                  WHERE KEYWORD IS NOT NULL
                  AND   KEYWORD != 'N'
                  ) TB
           ON  TA.GVHOST     = TB.GVHOST AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END)
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.DOMAIN = TB.DOMAIN
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간, 이탈수
           SELECT
                  TA.GVHOST
                ,(CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END) AS DOMAIN
                , SUM(TA.PAGE_VIEW)                                           AS PAGE_VIEW
                , SUM(TA.PAGE_CNT)                                            AS PAGE_CNT
                , SUM(TA.DUR_TIME)                                            AS DUR_TIME
           FROM   TB_ACCESS_SESSION2 TA
                  LEFT OUTER JOIN REFERER_DOMAIN_KEYWORD  TB
                  ON TA.GVHOST = TB.GVHOST AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END)
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.DOMAIN = TC.DOMAIN
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  TA.GVHOST
                ,(CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END) AS DOMAIN
                , COUNT(CASE WHEN TA.PAGE_VIEW = 1 THEN 1 ELSE NULL END)      AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION TA
                  LEFT OUTER JOIN REFERER_DOMAIN_KEYWORD  TB
                  ON TA.GVHOST = TB.GVHOST AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.DOMAIN IS NULL OR TB.DOMAIN = '' THEN 'N' ELSE TB.DOMAIN END)
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.DOMAIN = TD.DOMAIN
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END) AS DOMAIN
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           WHERE KEYWORD IS NOT NULL
           AND   KEYWORD != 'N'
           GROUP BY GVHOST, (CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END)
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.DOMAIN = TE.DOMAIN
           LEFT OUTER JOIN
           (
           -- 방문수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END) AS DOMAIN
                , COUNT(*)             AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           WHERE KEYWORD IS NOT NULL
           AND   KEYWORD != 'N'
           GROUP BY GVHOST, (CASE WHEN DOMAIN IS NULL OR DOMAIN = '' THEN 'N' ELSE DOMAIN END)
           ) TF
           ON  TA.GVHOST = TF.GVHOST AND TA.DOMAIN = TF.DOMAIN
    GROUP BY TA.GVHOST, TA.DOMAIN
    UNION
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.GVHOST                                             AS GVHOST
         , TA.KEYWORD                                           AS KEYWORD
         , 'KW'                                                                                                   AS SEARCH_TYPE
         , 'ALL'                                                                                                  AS ATTR_ID
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
           SELECT GVHOST, SEARCH AS KEYWORD
           FROM REFERER_KEYWORD_META
           WHERE SEARCH_TYPE = 'KW'
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT
                  TA.GVHOST                                                        AS GVHOST
                , TB.KEYWORD                                                       AS KEYWORD
                , SUM(TA.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TA.T_ID)                                          AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , T_ID                                                     AS T_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TA
                  LEFT OUTER JOIN
                  (
                  SELECT DISTINCT
                         GVHOST
                       , KEYWORD
                       , SESSION_ID
                  FROM   TB_REFERER_SESSION
                  WHERE KEYWORD IS NOT NULL
                  AND   KEYWORD != 'N'
                  ) TB
           ON  TA.GVHOST     = TB.GVHOST AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, TB.KEYWORD
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.KEYWORD = TB.KEYWORD
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
           SELECT
                  TA.GVHOST             AS GVHOST
                , TB.KEYWORD            AS KEYWORD
                , SUM(TA.PAGE_VIEW)                                           AS PAGE_VIEW
                , SUM(TA.PAGE_CNT)                                            AS PAGE_CNT
                , SUM(TA.DUR_TIME)                                            AS DUR_TIME
           FROM   TB_ACCESS_SESSION2 TA
                  LEFT OUTER JOIN REFERER_DOMAIN_KEYWORD  TB
                  ON TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, TB.KEYWORD
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.KEYWORD = TC.KEYWORD
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  TA.GVHOST      AS GVHOST
                , TB.KEYWORD     AS KEYWORD
                , COUNT(CASE WHEN TA.PAGE_VIEW = 1 THEN 1 ELSE NULL END)      AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION TA
                  LEFT OUTER JOIN REFERER_DOMAIN_KEYWORD  TB
                  ON TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, TB.KEYWORD
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.KEYWORD = TD.KEYWORD
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                ,KEYWORD               AS KEYWORD
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           WHERE KEYWORD IS NOT NULL
           AND   KEYWORD != 'N'
           GROUP BY GVHOST, KEYWORD
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.KEYWORD = TE.KEYWORD
           LEFT OUTER JOIN
           (
           -- 방문수
           SELECT
                  GVHOST               AS GVHOST
                , KEYWORD              AS KEYWORD
                , COUNT(*)             AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           WHERE KEYWORD IS NOT NULL
           
           GROUP BY GVHOST, KEYWORD
           ) TF
           ON  TA.GVHOST = TF.GVHOST AND TA.KEYWORD = TF.KEYWORD
    GROUP BY TA.GVHOST, TA.KEYWORD
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
    s"""
                  SELECT GVHOST                                                   AS GVHOST
                       , CATEGORY                                                  AS OS
                       , SUM(PAGE_VIEW)                                           AS PAGE_VIEW
                       , SUM(PAGE_CNT)                                            AS PAGE_CNT
                       , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END)      AS BOUNCE_CNT
                  FROM  (
                         SELECT GVHOST                 AS GVHOST
                              , SESSION_ID             AS SESSION_ID
                              , CATEGORY                AS OS
                              , COUNT(URL)             AS PAGE_VIEW
                              , COUNT(DISTINCT URL)    AS PAGE_CNT
                         FROM   TB_WL_URL_ACCESS
                         GROUP  BY GVHOST, SESSION_ID, OS
                         )
                  GROUP BY GVHOST, OS
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


/*


[20181219,Linux,TMAP,24,6,6,0,0,0,505,1,56,0]
[20181219,iOS,TMAP,2636428,184948,45697,139251,0,0,1439,128,15775711,0]
[20181219,TOTAL,TOTAL,6729448,701174,188593,512581,0,0,7510,294,53150244,0]
[20181219,Windows,TMOW,52,12,1,11,0,0,1375,2,252,0]
[20181219,Macintosh,TMAP,384,89,81,8,0,0,1357,3,1043,0]
[20181219,Android,TMAP,4091864,515986,142685,373301,0,0,1439,129,37369661,0]
[20181219,TOTAL,TMOW,52,12,1,11,0,0,1375,2,252,0]
[20181219,Windows,TMAP,696,133,123,10,0,0,1395,31,3521,0]
[20181219,TOTAL,TMAP,6729396,701162,188592,512570,0,0,6135,292,53149992,0]

 */