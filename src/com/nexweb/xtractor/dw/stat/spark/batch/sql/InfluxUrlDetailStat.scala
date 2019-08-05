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

 * 출    력 : TB_INFLUX_URL_DETAIL_STAT
 * 수정내역 :
 * 2019-02-07 | 피승현 | 최초작성
 */
object InfluxUrlDetailStat {

  var spark: SparkSession = null
  var objNm  = "TB_INFLUX_URL_DETAIL_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_INFLUX_URL_DETAIL_STAT";var statisDate = "20190312"; var statisType = "D"
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

          INSERT INTO TB_INFLUX_URL_DETAIL_STAT(STATIS_DATE, VHOST, VISIT_PATH, INFLUX_DIR_CGI, INFLUX_URL, ATTR_ID, VISIT_CNT, TOT_VISITOR_CNT, NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT)
          WITH REFERER_VISIT_PATH AS
          (
          SELECT SESSION_ID, TRIM(CATEGORY) AS VISIT_PATH, HOST AS INFLUX_DIR_CGI, DIR_CGI AS CHILD_URL
          FROM    TB_WL_REFERER_SEGMENT
          )
          SELECT  ? AS STATIS_DATE,
            NVL(TA.VHOST, 'TOTAL') VHOST,
            NVL(TA.VISIT_PATH, 'N') AS VISIT_PATH,
            NVL(TA.INFLUX_DIR_CGI, 'N') AS INFLUX_DIR_CGI,
            NVL(TA.CHILD_URL, 'N') AS CHILD_URL,
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
            SELECT  NVL(VHOST, 'TOTAL') AS VHOST, NVL(TRIM(CATEGORY), 'TOTAL') AS VISIT_PATH, NVL(HOST, 'TOTAL') AS INFLUX_DIR_CGI, NVL(DIR_CGI, 'TOTAL') AS CHILD_URL, 'ALL' AS ATTR_ID,
                COUNT(V_ID) AS VISIT_CNT,
                COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
            FROM    TB_WL_REFERER_SEGMENT
            GROUP   BY  ROLLUP ((VHOST, TRIM(CATEGORY), HOST, DIR_CGI))
            ) TA,
            (
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(VISIT_PATH, 'TOTAL') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'TOTAL') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'TOTAL') AS CHILD_URL, 'ALL' AS ATTR_ID,
              SUM(LOGIN_CNT) AS LOGIN_CNT, COUNT(UNIQUE L_ID) AS LOGIN_VISITOR_CNT
              FROM
              (
              SELECT VHOST, NVL(VISIT_PATH, 'N') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'N') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'N') AS CHILD_URL, L_ID, LOGIN_CNT
                  FROM TB_MEMBER_CLASS_SEG_STAT TA, REFERER_VISIT_PATH TB
                  WHERE TA.SESSION_ID = TB.SESSION_ID(+)
              )
              GROUP BY ROLLUP ((VHOST, VISIT_PATH, INFLUX_DIR_CGI, CHILD_URL))
            ) TB,
            (
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(VISIT_PATH, 'TOTAL') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'TOTAL') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'TOTAL') AS CHILD_URL, 'ALL' AS ATTR_ID,
              SUM(PAGE_VIEW) AS PAGE_VIEW,
              SUM(DUR_TIME) AS DUR_TIME
              FROM
              (
              SELECT  VHOST, NVL(VISIT_PATH, 'N') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'N') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'N') AS CHILD_URL,
                  PAGE_VIEW, DUR_TIME
              FROM    TB_SEGMENT_SESSION_LOG TA, REFERER_VISIT_PATH TB
              WHERE TA.SESSION_ID = TB.SESSION_ID(+)
              )
              GROUP BY ROLLUP ((VHOST, VISIT_PATH, INFLUX_DIR_CGI, CHILD_URL))
            ) TC,
            (
            SELECT NVL(VHOST, 'TOTAL') AS VHOST, NVL(VISIT_PATH, 'TOTAL') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'TOTAL') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'TOTAL') AS CHILD_URL, 'ALL' AS ATTR_ID,
            SUM(PAGE_CNT) AS PAGE_CNT,
            COUNT(DECODE(BOUNCE_VIEW,1,SESSION_ID, NULL)) BOUNCE_CNT
            FROM
            (
            SELECT  TA.VHOST, NVL(VISIT_PATH, 'N') AS VISIT_PATH, NVL(INFLUX_DIR_CGI, 'N') AS INFLUX_DIR_CGI, NVL(CHILD_URL, 'N') AS CHILD_URL, TA.SESSION_ID,
              COUNT(1) BOUNCE_VIEW,
              COUNT(DISTINCT TA.URL) PAGE_CNT
            FROM    TB_WL_URL_ACCESS_SEGMENT TA, TB_URL_COMMENT TB, REFERER_VISIT_PATH TC
            WHERE  TA.URL = TB.URL
            AND    TA.VHOST = TB.VHOST
            AND    TB.SUB_TYPE ='Y'
            AND    TA.SESSION_ID = TC.SESSION_ID(+)
            GROUP BY V_ID, TA.SESSION_ID, TA.VHOST, VISIT_PATH, NVL(INFLUX_DIR_CGI, 'N'), NVL(CHILD_URL, 'N')
            )
            GROUP BY ROLLUP ((VHOST, VISIT_PATH, INFLUX_DIR_CGI, CHILD_URL))
            ) TD
            WHERE TA.VHOST = TB.VHOST(+) AND TA.VISIT_PATH = TB.VISIT_PATH(+) AND TA.INFLUX_DIR_CGI = TB.INFLUX_DIR_CGI(+) AND TA.CHILD_URL = TB.CHILD_URL(+) AND TA.ATTR_ID = TB.ATTR_ID(+)
            AND   TA.VHOST = TC.VHOST(+) AND TA.VISIT_PATH = TC.VISIT_PATH(+) AND TA.INFLUX_DIR_CGI = TC.INFLUX_DIR_CGI(+) AND TA.CHILD_URL = TC.CHILD_URL(+)  AND TA.ATTR_ID = TC.ATTR_ID(+)
            AND   TA.VHOST = TD.VHOST(+) AND TA.VISIT_PATH = TD.VISIT_PATH(+) AND TA.INFLUX_DIR_CGI = TD.INFLUX_DIR_CGI(+) AND TA.CHILD_URL = TD.CHILD_URL(+)  AND TA.ATTR_ID = TD.ATTR_ID(+)

 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST
         , SESSION_ID
         , MIN(CATEGORY) AS CATEGORY
         , MIN(HOST) AS HOST
         , MIN(DIR_CGI) AS DIR_CGI
    FROM   TB_REFERER_SESSION
    GROUP BY GVHOST, SESSION_ID
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("REFERER_CATEGORY_HOST_DIR_CGI"); sqlDf2.count()

    qry =
    s"""
      SELECT DISTINCT GVHOST, CATEGORY, HOST, DIR_CGI
      FROM
          (
          SELECT GVHOST
               , (CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END) AS CATEGORY
               , (CASE WHEN HOST IS NULL OR HOST = '' THEN 'N' ELSE HOST END)    AS HOST
               , (CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)    AS DIR_CGI
          FROM   TB_REFERER_SESSION
          UNION
          SELECT GVHOST
               , (CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END) AS CATEGORY
               , (CASE WHEN HOST IS NULL OR HOST = '' THEN 'N' ELSE HOST END)    AS HOST
               , (CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)    AS DIR_CGI
          FROM   TB_REFERER_SESSION3
          )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
    println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("REFERER_HOST_DIR_CGI_META"); sqlDf3.count()

    qry =
    s"""
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.GVHOST                                             AS GVHOST
         , TA.CATEGORY                                         AS VISIT_PATH
         , TA.HOST                                                 AS INFLUX_DOMAIN
         , TA.DIR_CGI                                           AS INFLUX_URL
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
           SELECT GVHOST, CATEGORY, HOST, DIR_CGI
           FROM REFERER_HOST_DIR_CGI_META
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT
                  TA.GVHOST                                                        AS GVHOST
                ,(CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END) AS CATEGORY
                ,(CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END)    AS HOST
                ,(CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)    AS DIR_CGI
                , SUM(TA.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TA.T_ID)                                          AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , T_ID                                                     AS T_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, T_ID
                  ) TA,
                  (
                  SELECT DISTINCT
                         GVHOST
                       , CATEGORY
                       , HOST
                       , DIR_CGI
                       , SESSION_ID
                  FROM   TB_REFERER_SESSION
                  ) TB
           WHERE  TA.GVHOST     = TB.GVHOST
           AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END), 
           (CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END), 
           (CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.CATEGORY = TB.CATEGORY AND TA.HOST = TB.HOST AND TA.DIR_CGI = TB.DIR_CGI
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
           SELECT
                  TA.GVHOST
                ,(CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END) AS CATEGORY
                ,(CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END)    AS HOST
                ,(CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)    AS DIR_CGI
                , SUM(TA.PAGE_VIEW)                                           AS PAGE_VIEW
                , SUM(TA.PAGE_CNT)                                            AS PAGE_CNT
                , SUM(TA.DUR_TIME)                                            AS DUR_TIME
           FROM   TB_ACCESS_SESSION2 TA
                  LEFT OUTER JOIN  REFERER_CATEGORY_HOST_DIR_CGI  TB
                  ON TA.GVHOST = TB.GVHOST AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END), 
           (CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END), 
           (CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.CATEGORY = TC.CATEGORY AND TA.HOST = TC.HOST AND TA.DIR_CGI = TC.DIR_CGI
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  TA.GVHOST
                ,(CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END) AS CATEGORY
                ,(CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END)    AS HOST
                ,(CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)    AS DIR_CGI
                , COUNT(CASE WHEN TA.PAGE_VIEW = 1 THEN 1 ELSE NULL END)      AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION TA
                  LEFT OUTER JOIN  REFERER_CATEGORY_HOST_DIR_CGI  TB
                  ON TA.GVHOST = TB.GVHOST AND TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, (CASE WHEN TB.CATEGORY IS NULL OR TB.CATEGORY = '' THEN 'N' ELSE TB.CATEGORY END), 
           (CASE WHEN TB.HOST IS NULL   OR TB.HOST = ''   THEN 'N' ELSE TB.HOST   END), 
           (CASE WHEN TB.DIR_CGI IS NULL OR TB.DIR_CGI = '' THEN 'N' ELSE TB.DIR_CGI END)
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.CATEGORY = TD.CATEGORY AND TA.HOST = TD.HOST AND TA.DIR_CGI = TD.DIR_CGI
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END) AS CATEGORY
                ,(CASE WHEN HOST IS NULL   OR HOST = ''   THEN 'N' ELSE HOST   END)       AS HOST
                ,(CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)       AS DIR_CGI
                , COUNT(DISTINCT V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6)  = '${statisDate}' THEN V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(V_ID, 2, 6) != '${statisDate}' THEN V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   TB_REFERER_SESSION3
           GROUP BY GVHOST, (CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END), 
           (CASE WHEN HOST IS NULL   OR HOST = ''   THEN 'N' ELSE HOST   END), 
           (CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.CATEGORY = TE.CATEGORY AND TA.HOST = TE.HOST AND TA.DIR_CGI = TE.DIR_CGI
           LEFT OUTER JOIN
           (
           -- 방문수
           SELECT
                  GVHOST               AS GVHOST
                ,(CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END) AS CATEGORY
                ,(CASE WHEN HOST IS NULL   OR HOST = ''   THEN 'N' ELSE HOST   END)       AS HOST
                ,(CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)       AS DIR_CGI
                , COUNT(*)             AS VISIT_CNT
           FROM   TB_REFERER_SESSION
           GROUP BY GVHOST, (CASE WHEN CATEGORY IS NULL OR CATEGORY = '' THEN 'N' ELSE CATEGORY END), 
           (CASE WHEN HOST IS NULL   OR HOST = ''   THEN 'N' ELSE HOST   END), 
           (CASE WHEN DIR_CGI IS NULL OR DIR_CGI = '' THEN 'N' ELSE DIR_CGI END)
           ) TF
           ON  TA.GVHOST = TF.GVHOST AND TA.CATEGORY = TF.CATEGORY AND TA.HOST = TF.HOST AND TA.DIR_CGI = TF.DIR_CGI
    GROUP BY TA.GVHOST, TA.CATEGORY, TA.HOST, TA.DIR_CGI
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