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
 * 설    명 : 국가별 방문자 추이 통계
 * 입    력 :

TB_WL_REFERER
TB_WL_URL_ACCESS
TB_MEMBER_CLASS

 * 출    력 : TB_VISIT_COUNTRY_STAT
 * 수정내역 :
 * 2019-01-23 | 피승현 | 최초작성
 */
object VisitCountryStat {

  var spark: SparkSession = null
  var objNm  = "TB_VISIT_COUNTRY_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_COUNTRY_STAT";var statisDate = "20190312"; var statisType = "D"
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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION3"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_SESSION" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_IP_INFO"              ,statisDate,statisType,"",true)
  }

/*
          INSERT INTO TB_IP_LIST_DAY(IP, IP_CD)
          SELECT UNIQUE IP,
                 REGEXP_REPLACE(REPLACE('.'||IP, '.', '.00'), '([^.]{3}(\.|$))|.', '\1') AS VIP
                 , REGEXP_REPLACE(REGEXP_REPLACE(CONCAT('.',IP), '[.]', '.00'), '([^.]{3}(\\.|$))|.', '$1')
            FROM TB_WL_URL_ACCESS_DAY TA
            WHERE NOT EXISTS
                (
                SELECT IP
                  FROM TB_URL_ACCESS_IP_INFO
                  WHERE TA.IP = IP
                )

          MERGE INTO TB_URL_ACCESS_IP_INFO TA
          USING
          (
          SELECT IP, COUNTRY, COUNTRY_NM, CITY, ISP_NM
            FROM TB_IP_LIST_DAY TA, TB_COUNTRY_IP_SETTING TB
            WHERE TA.IP_CD >= START_IP_NUM
            AND TA.IP_CD <= END_IP_NUM
          ) TB
          ON
          (
            TA.IP = TB.IP
          )
          WHEN MATCHED THEN
            UPDATE  SET TA.COUNTRY = TB.COUNTRY, TA.COUNTRY_NM = TB.COUNTRY_NM, TA.CITY=TB.CITY, TA.ISP_NM=TB.ISP_NM
          WHEN NOT MATCHED THEN
            INSERT( TA.IP, TA.COUNTRY, TA.COUNTRY_NM, TA.CITY, TA.ISP_NM)
            VALUES( TB.IP, TB.COUNTRY, TB.COUNTRY_NM, TB.CITY, TB.ISP_NM)

CREATE OR REPLACE FUNCTION              FN_COUNTRY_SEARCH_B (VIP VARCHAR2) RETURN VARCHAR2
IS
      v_country  TB_URL_ACCESS_IP_INFO.COUNTRY%TYPE;
BEGIN
        SELECT COUNTRY INTO v_country
        FROM
        (
        SELECT COUNTRY
        FROM TB_URL_ACCESS_IP_INFO
        WHERE IP = VIP
        )
        WHERE ROWNUM = 1;
   RETURN v_country;
END;

          INSERT  INTO TB_VISIT_COUNTRY_STAT
                    ( STATIS_DATE, VHOST, COUNTRY_NM, VISIT_CNT, TOT_VISITOR_CNT, NEW_VISITOR_CNT, RE_VISITOR_CNT, LOGIN_CNT, LOGIN_VISITOR_CNT, DUR_TIME, PAGE_CNT, PAGE_VIEW, BOUNCE_CNT )
                     SELECT ? STATIS_DATE,
                                 NVL(TA.VHOST, 'TOTAL') VHOST,
                                 NVL(TA.COUNTRY_NM, 'TOTAL') COUNTRY_NM,
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
                  SELECT NVL(COUNTRY_NM, 'TOTAL') AS COUNTRY_NM,
                         NVL(VHOST, 'TOTAL') AS VHOST,
                         SUM(VISIT_CNT) AS VISIT_CNT,
                         SUM(TOT_VISITOR_CNT) AS TOT_VISITOR_CNT,
                         SUM(NEW_VISITOR_CNT) AS NEW_VISITOR_CNT,
                         SUM(RE_VISITOR_CNT) AS RE_VISITOR_CNT
                         FROM
                         (
                  SELECT  NVL(FN_COUNTRY_SEARCH_B(V_IP), '알수없음') AS COUNTRY_NM,
                    NVL(V_HOST, 'TOTAL') AS  VHOST,
                         COUNT(1) VISIT_CNT,
                         COUNT(DISTINCT V_ID) TOT_VISITOR_CNT,
                         COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) = ? OR V_ID LIKE '.' THEN V_ID ELSE '' END)) AS NEW_VISITOR_CNT,
                         COUNT(UNIQUE (CASE  WHEN '20'||SUBSTR(V_ID, 2, 6) != ? AND V_ID NOT LIKE '.' THEN V_ID ELSE '' END)) AS RE_VISITOR_CNT
                    FROM
                      (
                      SELECT  V_HOST, MAX(V_IP) OVER(PARTITION BY V_ID) AS V_IP, V_ID
                      FROM    TB_WL_REFERER_SESSION
                      )
                    GROUP   BY V_HOST, FN_COUNTRY_SEARCH_B(V_IP)
                    )
                  GROUP BY ROLLUP ( VHOST, COUNTRY_NM )
                                  ) TA,
                                  (
                                   --유입경로별 로그인수
                                  SELECT   NVL(COUNTRY_NM, 'TOTAL') AS COUNTRY_NM,
                                               NVL(TA.VHOST, 'TOTAL') AS VHOST,
                                               SUM(LOGIN_CNT) LOGIN_CNT,
                                               COUNT(DISTINCT L_ID) LOGIN_VISITOR_CNT
                                  FROM    (
                                              SELECT  U_ID, SESSION_ID,
                                                       NVL(FN_COUNTRY_SEARCH_B(V_IP), '알수없음') AS COUNTRY_NM, V_HOST VHOST
                                              FROM    TB_WL_REFERER_SESSION
                                              ) TA,  TB_MEMBER_CLASS_DAY_STAT TB
                                  WHERE   TA.SESSION_ID = TB.SESSION_ID
                                  AND        TA.VHOST=  TB.VHOST
                                  GROUP   BY ROLLUP(TA.VHOST, COUNTRY_NM)
                                  ) TB,
                                  (
                                  --유입경로별 체류시간, 페이지뷰
                  SELECT NVL(COUNTRY_NM, 'TOTAL') AS COUNTRY_NM,
                     NVL(VHOST, 'TOTAL') AS VHOST,
                     SUM(DUR_TIME) DUR_TIME,
                     SUM(PAGE_VIEW) PAGE_VIEW
                  FROM
                  (
                  SELECT TA.VHOST, TA.SESSION_ID, NVL(COUNTRY_NM, '알수없음') AS COUNTRY_NM, PAGE_VIEW, DUR_TIME
                  FROM
                  (
                  SELECT VHOST, SESSION_ID, PAGE_VIEW, DUR_TIME
                  FROM TB_SESSION_LOG
                  ) TA,
                  (
                  SELECT   NVL(FN_COUNTRY_SEARCH_B(V_IP), '알수없음') AS COUNTRY_NM,
                     SESSION_ID, V_HOST VHOST
                  FROM    TB_WL_REFERER_SESSION
                  ) TB
                  WHERE TA.VHOST = TB.VHOST(+)
                  AND   TA.SESSION_ID = TB.SESSION_ID(+)
                  )
                  GROUP BY ROLLUP (VHOST, COUNTRY_NM)
                                  ) TC,
                                  (
                                  --유입경로별 이탈수
                                  SELECT  NVL(COUNTRY_NM, 'TOTAL') AS COUNTRY_NM,
                                               NVL(TA.VHOST, 'TOTAL') AS VHOST,
                                               SUM(PAGE_CNT) AS PAGE_CNT,
                                               COUNT(DECODE(PAGE_VIEW,1,TA.SESSION_ID, NULL)) BOUNCE_CNT
                                      FROM    (
                                              SELECT  SESSION_ID, V_HOST VHOST,
                                                       NVL(FN_COUNTRY_SEARCH_B(V_IP), '알수없음') AS COUNTRY_NM
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
                                  GROUP   BY ROLLUP(TA.VHOST, COUNTRY_NM)
                                  ) TD
                      WHERE   TA.COUNTRY_NM = TB.COUNTRY_NM(+) AND TA.VHOST = TB.VHOST(+)
                      AND       TA.COUNTRY_NM = TC.COUNTRY_NM(+) AND TA.VHOST = TC.VHOST(+)
                      AND       TA.COUNTRY_NM = TD.COUNTRY_NM(+) AND TA.VHOST = TD.VHOST(+)


 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'                                                                                        AS STATIS_DATE
         , TA.GVHOST                                            AS GVHOST
         , TA.COUNTRY                                          AS COUNTRY_NM
         , SUM(CASE WHEN TA.VISIT_CNT         IS NULL THEN 0 ELSE TA.VISIT_CNT         END)                       AS VISIT_CNT
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
           -- 방문수
           SELECT TA.GVHOST
                , CASE WHEN TB.COUNTRY IS NULL THEN 'ETC' ELSE TB.COUNTRY END  AS COUNTRY
                , SUM(VISIT_CNT)             AS VISIT_CNT
                , COUNT(DISTINCT TA.V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 6)  = '${statisDate}' THEN TA.V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 6) != '${statisDate}' THEN TA.V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   (
                   SELECT GVHOST, V_ID, MIN(V_IP) AS V_IP, COUNT(*) AS VISIT_CNT
                   FROM TB_REFERER_SESSION
                   GROUP BY GVHOST, V_ID
                  ) TA
           LEFT OUTER JOIN
                  (
                   SELECT IP, MIN(COUNTRY) AS COUNTRY
                    FROM TB_IP_INFO
                    GROUP BY IP
                  ) TB
           ON  TA.V_IP = TB.IP
           GROUP  BY TA.GVHOST, TB.COUNTRY
           ) TA
           LEFT OUTER JOIN
           (
           -- 로그인방문자수, 로그인수
           SELECT
                  TA.GVHOST                                                        AS GVHOST
                , TA.COUNTRY                                                       AS COUNTRY
                , SUM(TB.LOGIN_CNT)                                                AS LOGIN_CNT
                , COUNT(DISTINCT TB.LOGIN_ID)                                      AS LOGIN_VISITOR_CNT
           FROM   (
                  SELECT DISTINCT
                         TA.GVHOST
                       , CASE WHEN TB.COUNTRY IS NULL THEN 'ETC' ELSE TB.COUNTRY END  AS COUNTRY
                       , TA.SESSION_ID
                  FROM   TB_REFERER_SESSION TA
                  LEFT OUTER JOIN
                         (
                         SELECT IP, MIN(COUNTRY) AS COUNTRY
                          FROM TB_IP_INFO
                          GROUP BY IP
                         )         TB
                  ON  TA.V_IP = TB.IP
                  ) TA,
                  (
                  SELECT GVHOST                                                   AS GVHOST
                       , SESSION_ID                                               AS SESSION_ID
                       , LOGIN_ID                                                 AS LOGIN_ID
                       , COUNT(*)                                                 AS LOGIN_CNT
                  FROM   TB_MEMBER_CLASS_SESSION
                  GROUP BY GVHOST, SESSION_ID, LOGIN_ID
                  ) TB
           WHERE  TA.GVHOST     = TB.GVHOST
           AND    TA.SESSION_ID = TB.SESSION_ID
           GROUP BY TA.GVHOST, TA.COUNTRY
           ) TB
           ON  TA.GVHOST = TB.GVHOST AND TA.COUNTRY = TB.COUNTRY
           LEFT OUTER JOIN
           (
           -- 페이지뷰, 페이지수, 체류시간
           SELECT
                  TA.GVHOST          AS GVHOST
                , CASE WHEN TB.COUNTRY IS NULL THEN 'ETC' ELSE TB.COUNTRY END  AS COUNTRY
                , SUM(TA.PAGE_VIEW)  AS PAGE_VIEW
                , SUM(TA.PAGE_CNT)   AS PAGE_CNT
                , SUM(TA.DUR_TIME)   AS DUR_TIME
           FROM   TB_ACCESS_SESSION2  TA
           LEFT OUTER JOIN
                  (
                   SELECT IP, MIN(COUNTRY) AS COUNTRY
                    FROM TB_IP_INFO
                    GROUP BY IP
                  )         TB
           ON  TA.IP = TB.IP
           GROUP BY TA.GVHOST, TB.COUNTRY
           ) TC
           ON  TA.GVHOST = TC.GVHOST AND TA.COUNTRY = TC.COUNTRY
           LEFT OUTER JOIN
           (
           -- 이탈수
           SELECT
                  TA.GVHOST          AS GVHOST
                , CASE WHEN TB.COUNTRY IS NULL THEN 'ETC' ELSE TB.COUNTRY END  AS COUNTRY
                , COUNT(CASE WHEN TA.PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION  TA
           LEFT OUTER JOIN
                  (
                   SELECT IP, MIN(COUNTRY) AS COUNTRY
                    FROM TB_IP_INFO
                    GROUP BY IP
                  )         TB
           ON  TA.IP = TB.IP
           GROUP BY TA.GVHOST, TB.COUNTRY
           ) TD
           ON  TA.GVHOST = TD.GVHOST AND TA.COUNTRY = TD.COUNTRY
           LEFT OUTER JOIN
           (
           -- 방문자수,신규방문자수,재방문자수
           SELECT TA.GVHOST
                , CASE WHEN TB.COUNTRY IS NULL THEN 'ETC' ELSE TB.COUNTRY END  AS COUNTRY
                , COUNT(DISTINCT TA.V_ID) AS TOT_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 6)  = '${statisDate}' THEN TA.V_ID ELSE NULL END)) AS NEW_VISITOR_CNT
                , COUNT(DISTINCT (CASE WHEN '20'||SUBSTR(TA.V_ID, 2, 6) != '${statisDate}' THEN TA.V_ID ELSE NULL END)) AS RE_VISITOR_CNT
           FROM   (
                   SELECT GVHOST, V_ID, MIN(V_IP) AS V_IP, COUNT(*) AS VISIT_CNT
                   FROM TB_REFERER_SESSION3
                   GROUP BY GVHOST, V_ID
                  ) TA
           LEFT OUTER JOIN
                  (
                   SELECT IP, MIN(COUNTRY) AS COUNTRY
                    FROM TB_IP_INFO
                    GROUP BY IP
                  ) TB
           ON  TA.V_IP = TB.IP
           GROUP  BY TA.GVHOST, TB.COUNTRY
           ) TE
           ON  TA.GVHOST = TE.GVHOST AND TA.COUNTRY = TE.COUNTRY
    GROUP BY TA.GVHOST, TA.COUNTRY
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
           SELECT
                  TA.GVHOST          AS GVHOST
                , TB.COUNTRY         AS COUNTRY
                , SUM(TA.PAGE_VIEW)  AS PAGE_VIEW
                , SUM(TA.PAGE_CNT)   AS PAGE_CNT
                , SUM(TA.DUR_TIME)   AS DUR_TIME
                , COUNT(CASE WHEN TA.PAGE_VIEW = 1 THEN 1 ELSE NULL END) AS BOUNCE_CNT
           FROM   TB_ACCESS_SESSION  TA
                , TB_IP_INFO         TB
           WHERE  TA.IP = TB.IP
           GROUP BY TA.GVHOST, TB.COUNTRY
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
