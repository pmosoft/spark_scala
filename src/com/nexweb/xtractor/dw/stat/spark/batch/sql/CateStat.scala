package com.nexweb.xtractor.dw.stat.spark.batch.sql

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

 * 출    력 : TB_CATE_STAT
 * 수정내역 :
 * 2019-01-31 | 피승현 | 최초작성
 */
object CateStat {

  var spark : SparkSession = null
  var objNm  = "TB_CATE_STAT"

  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""
  //var objNm  = "TB_CATE_STAT";var statisDate = "2019031"; var statisType = "D"
  //var objNm  = "TB_CATE_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D"

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
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
  }

/*
 *
    INSERT INTO POC_XTRACTOR.TB_CATE_LOG NOLOGGING (
     VISIT_DATE, CATE_ID, URL, V_ID, U_ID, VISIT_CNT, VHOST
    )
       SELECT /*+ LEADING(TA TB TC) USE_HASH(TA TB TC) FULL(TB) PARALLEL(TB 4) FULL(TC) PARALLEL(TC 4)
           PQ_DISTRIBUTE(TB BROADCAST NONE) PQ_DISTRIBUTE(TC HASH HASH) */
           ? AS VISIT_DATE, TA.CATE_ID, TA.URL, TB.V_ID, TB.U_ID, COUNT(TC.V_ID) AS VISIT_CNT, TA.VHOST
       FROM POC_XTRACTOR.TB_CATE_URL_MAP TA
       INNER JOIN (
                   SELECT  V_ID, U_ID, URL, C_TIME, VHOST FROM TB_WL_URL_ACCESS_SESSION
                   UNION   ALL
                   SELECT  /*+ INDEX(TA)*/ V_ID, U_ID, PAGE_ID URL, NULL C_TIME, 'www.sktmembership.co.kr' VHOST
                   FROM  TB_APP_HISTORY TA
                   WHERE   C_TIME BETWEEN TO_DATE(?,'YYYYMMDD') AND TO_DATE(?,'YYYYMMDD') + 0.99999
     AND  PAGE_ID NOT IN ('sktvip','sktelecom','sktpromo')
               ) TB
       ON TA.URL = TB.URL
       AND TA.VHOST = TB.VHOST
       LEFT OUTER JOIN POC_XTRACTOR.TB_WL_REFERER_SESSION TC
       ON TB.V_ID = TC.V_ID
       AND TB.C_TIME = TC.V_DATE
       GROUP BY TA.CATE_ID, TA.URL, TB.V_ID, TB.U_ID, TA.VHOST


        INSERT INTO TB_CATE_STAT
        (STATIS_TYPE, STATIS_DATE, CATE_ID, VHOST, PVIEW, VISIT_CNT, VISITOR_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, BOUNCE_CNT, LAST_CNT, REG_DATE)
        WITH CATE_URL_MAP AS (
                    SELECT ANC_CATE_ID, CATE_ID, URL, VHOST
                    FROM (
                        SELECT SUBSTR(TA.CATE_PATH, 1, DECODE(INSTR(TA.CATE_PATH, '>', 2), 0, LENGTH(TA.CATE_PATH) + 1, INSTR(TA.CATE_PATH, '>', 2)) - 1) AS ANC_CATE_ID,
                            TA.CATE_ID, TB.URL, VHOST
                        FROM (
                            SELECT TA.CATE_ID, LEVEL AS DEPTH_LEV, SUBSTR(SYS_CONNECT_BY_PATH(TA.CATE_ID, '>'), 2) AS CATE_PATH
                            FROM TB_CATE_MAP TA
                            WHERE TA.USE_YN = 'Y'
                            START WITH TA.CATE_ID IN (
                                SELECT CATE_ID
                                FROM TB_CATE_MAP
                            )
                            CONNECT BY PRIOR TA.CATE_ID = TA.PRNT_CATE_ID
                            ORDER SIBLINGS BY SEQ
                        ) TA
                        INNER JOIN TB_CATE_URL_MAP TB
                        ON TA.CATE_ID = TB.CATE_ID
                    ) TA
                )
                SELECT ? STATIS_TYPE, ? STATIS_DATE, CATE_ID, 'tmembership.tworld.co.kr' VHOST,
                    NVL(SUM(PVIEW), 0) AS PVIEW,
                    NVL(SUM(VISIT_CNT), 0) AS VISIT_CNT,
                    NVL(SUM(VISITOR_CNT), 0) AS VISITOR_CNT,
                    NVL(SUM(LOGIN_VISITOR_CNT), 0) AS LOGIN_VISITOR_CNT,
                    NVL(SUM(DUR_TIME), 0) AS DUR_TIME,
                    NVL(SUM(PAGE_CNT), 0) AS PAGE_CNT,
                    NVL(SUM(BOUNCE_CNT), 0) AS BOUNCE_CNT,
                    NVL(SUM(LAST_CNT), 0) AS LAST_CNT,
                    SYSDATE
                FROM (
                    --페이지뷰
                    SELECT /*+ LEADING(TA TB) USE_NL(TA, TB) */
                        ANC_CATE_ID AS CATE_ID, TA.VHOST,
                        SUM(TB.PAGE_VIEW) AS PVIEW,
                        0 AS VISIT_CNT,
                        0 AS VISITOR_CNT,
                        0 AS LOGIN_VISITOR_CNT,
                        AVG(TB.DUR_TIME) AS DUR_TIME,
                        SUM(TB.PAGE_CNT) AS PAGE_CNT,
                        SUM(TB.BOUNCE_CNT)  AS BOUNCE_CNT,
                        SUM(TB.LAST_CNT)  AS LAST_CNT
                    FROM CATE_URL_MAP TA
                    INNER JOIN TB_URL_STAT TB
                    ON TA.URL = TB.URL
                    AND TA.VHOST = TB.VHOST
                    WHERE TB.STATIS_DATE >= ?
                    AND TB.STATIS_DATE <= ?
                    GROUP BY ANC_CATE_ID, TA.VHOST
                    UNION ALL
                    --진입수
                    SELECT /*+ LEADING(TA TB) USE_HASH(TA TB) FULL(TB) PARALLEL(TB 4) PQ_DISTRIBUTE(TB BROADCAST NONE) */
                        ANC_CATE_ID AS CATE_ID, TA.VHOST,
                        0 AS PVIEW,
                        SUM(VISIT_CNT) AS VISIT_CNT,
                        COUNT(DISTINCT V_ID) AS VISITOR_CNT,
                        COUNT(DISTINCT U_ID) AS LOGIN_VISITOR_CNT,
                        0 AS DUR_TIME,
                        0 AS PAGE_CNT,
                        0 AS BOUNCE_CNT,
                        0 AS LAST_CNT
                    FROM CATE_URL_MAP TA
                    INNER JOIN TB_CATE_LOG TB
                    ON TA.CATE_ID = TB.CATE_ID
                    AND TA.URL = TB.URL
                    AND TA.VHOST = TB.VHOST
                    WHERE VISIT_DATE >= ?
                    AND VISIT_DATE <= ?
                    GROUP BY ANC_CATE_ID, TA.VHOST
                ) TA
                GROUP BY CATE_ID



 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisType}'                                                      AS STATIS_TYPE
         , '${statisDate}'                                                      AS STATIS_DATE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END          AS GVHOST
         , CASE WHEN TA.CATE_ID IS NULL THEN 'TOTAL' ELSE TA.CATE_ID END        AS CATE_ID
         , NVL(SUM(TC.PAGE_VIEW),0)                                             AS PAGE_VIEW
         , NVL(SUM(TC.PAGE_CNT),0)                                              AS PAGE_CNT
         , NVL(SUM(TA.VISITOR_CNT),0)                                           AS VISITOR_CNT
         , NVL(SUM(LOGIN_VISITOR_CNT),0)                                        AS LOGIN_VISITOR_CNT
         , NVL(SUM(TE.FIRST_CNT),0)                                             AS FIRST_CNT
         , NVL(SUM(TF.LAST_CNT),0)                                              AS LAST_CNT
         , NVL(SUM(TC.DUR_TIME),0)                                              AS DUR_TIME
         , NVL(SUM(TD.BOUNCE_CNT),0)                                            AS BOUNCE_CNT
    FROM
           (
           -- 방문수, 방문자수,신규방문자수,재방문자수
           SELECT
                  TA.GVHOST                    AS GVHOST
                , TA.ANC_CATE_ID               AS CATE_ID
                , COUNT(DISTINCT SESSION_ID)  AS VISIT_CNT
                , COUNT(DISTINCT V_ID)      AS VISITOR_CNT
           FROM   CATE_URL_MAP_FRONT TA
                  LEFT OUTER JOIN TB_ACCESS_SESSION2 TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.URL    = TB.URL
           GROUP BY TA.GVHOST, TA.ANC_CATE_ID
           ) TA
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간, 로그인방문자수
           SELECT
                  TA.GVHOST                  AS GVHOST
                , TA.ANC_CATE_ID             AS CATE_ID
                , SUM(TB.PAGE_VIEW)          AS PAGE_VIEW
                , SUM(TB.PAGE_CNT)           AS PAGE_CNT
                , SUM(TB.DUR_TIME)           AS DUR_TIME
                , COUNT(DISTINCT TB.T_ID)    AS LOGIN_VISITOR_CNT
           FROM   CATE_URL_MAP_FRONT TA
                  LEFT OUTER JOIN TB_ACCESS_SESSION2 TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.URL    = TB.URL
           GROUP BY TA.GVHOST, TA.ANC_CATE_ID
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.CATE_ID = TC.CATE_ID
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  TA.GVHOST               AS GVHOST
                , TA.ANC_CATE_ID          AS CATE_ID
                , COUNT(CASE WHEN TB.PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   CATE_URL_MAP_FRONT TA
                  LEFT OUTER JOIN TB_ACCESS_SESSION TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.URL    = TB.URL
           GROUP BY TA.GVHOST, TA.ANC_CATE_ID
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.CATE_ID = TD.CATE_ID
           LEFT OUTER JOIN
           (
           -- 진입수
           SELECT
                  TA.GVHOST               AS GVHOST
                , TA.ANC_CATE_ID          AS CATE_ID
                , COUNT(1)                AS FIRST_CNT
           FROM   CATE_URL_MAP_FRONT TA
                  LEFT OUTER JOIN
                  (
                  SELECT GVHOST
                       , URL
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME ASC) RNK
                  FROM   TB_ACCESS_SESSION2
                  ) TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.URL    = TB.URL
           WHERE  TB.RNK=1
           GROUP BY TA.GVHOST, TA.ANC_CATE_ID
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.CATE_ID = TE.CATE_ID
           LEFT OUTER JOIN
           (
           -- 접속종료수
           SELECT
                  TA.GVHOST                       AS GVHOST
                , TA.ANC_CATE_ID                  AS CATE_ID
                , COUNT(1)                        AS LAST_CNT
           FROM   CATE_URL_MAP_FRONT TA
                  LEFT OUTER JOIN
                  (
                  SELECT GVHOST
                       , URL
                       , ROW_NUMBER() OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY START_TIME DESC) RNK
                  FROM   TB_ACCESS_SESSION2
                  ) TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.URL    = TB.URL
           WHERE  TB.RNK=1
           GROUP BY TA.GVHOST, TA.ANC_CATE_ID
           ) TF
           ON  TA.GVHOST = TF.GVHOST AND TA.CATE_ID = TF.CATE_ID
    GROUP BY ROLLUP(TA.GVHOST, TA.CATE_ID)
    """

    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
           SELECT
                  TB.GVHOST                       AS GVHOST
                , TB.CATE_ID                      AS CATE_ID
                , SUM(PAGE_VIEW)  AS PAGE_VIEW
                , SUM(PAGE_CNT)   AS PAGE_CNT
                , SUM(DUR_TIME)   AS DUR_TIME
                , COUNT(CASE WHEN PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
                , COUNT(DISTINCT TA.T_ID)         AS LOGIN_VISITOR_CNT
           FROM   TB_ACCESS_SESSION TA
                , CATE_URL_MAP_FRONT TB
           WHERE  TA.GVHOST = TB.GVHOST
           AND    TA.URL    = TB.URL
           GROUP BY TB.GVHOST, TB.CATE_ID
    """

    spark.sql(qry).take(100).foreach(println);

    spark.sql("SELECT DISTINCT URL FROM TB_ACCESS_SESSION WHERE URL LIKE '%recommendMain%'").take(100).foreach(println);
    spark.sql("SELECT * FROM CATE_URL_MAP_FRONT WHERE URL LIKE '%recom%'").take(10000).foreach(println);
    spark.sql("DROP TABLE TB_ACCESS_SESSION").take(10000).foreach(println);
    spark.sql("SELECT * FROM CATE_URL_MAP_FRONT WHERE URL LIKE '%recom%'").take(10000).foreach(println);

    qry =
    s"""
    SELECT *
    FROM   parquet.`/user/xtractor/parquet/entity/CATE_URL_MAP_FRONT/CATE_URL_MAP_FRONT_20190214`
    WHERE  CATE_ID = 'TMAP_A00'
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    s"""
    SELECT *
    FROM   parquet.`/user/xtractor/parquet/entity/TB_CATE_STAT/TB_CATE_STAT_20190214`
    """
    spark.sql(qry).take(100).foreach(println);

     */

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
