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
 * 설    명 : 일/월별 방문자 추이 통계
 * 입    력 :

 * 출    력 : TB_ACT_FLOW_STAT
 * 수정내역 :
 * 2019-02-01 | 피승현 | 최초작성
 */
object ActFlowStat {

  var spark: SparkSession = null
  var objNm = "TB_ACT_FLOW_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm = "TB_ACT_FLOW_STAT"; var statisDate = "20190311"; var statisType = "D"
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
    //LoadTable.lodAccessTable(spark,statisDate,statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION3",statisDate,statisType,"",true)
  }

/*

          INSERT INTO TB_ACT_FLOW_STAT
          SELECT TO_CHAR(CREATE_TIME, 'YYYYMMDD') AS STATIS_DATE, DEPTH, DOMAIN, REF_HOST AS URL, URL AS NEXT_URL, COUNT(SESSION_ID) AS PAGE_VIEW
            FROM TB_SESSION_ACT_LOG
            WHERE DEPTH = 1
            GROUP BY TO_CHAR(CREATE_TIME, 'YYYYMMDD'), DOMAIN, DEPTH, REF_HOST, URL
          UNION
          SELECT TO_CHAR(CREATE_TIME, 'YYYYMMDD') AS STATIS_DATE, DEPTH, DOMAIN, URL, NEXT_URL, COUNT(SESSION_ID) AS PAGE_VIEW
            FROM TB_SESSION_ACT_LOG
            WHERE DEPTH > 1 AND DEPTH < 21
            AND NEXT_URL IS NOT NULL
            GROUP BY TO_CHAR(CREATE_TIME, 'YYYYMMDD'), DOMAIN, DEPTH, URL, NEXT_URL


          INSERT INTO TB_SESSION_ACT_LOG
          WITH CATE_INFO AS
          (
          SELECT TA.CATE_ID, LEVEL AS DEPTH_LEV, SUBSTR(SYS_CONNECT_BY_PATH(TA.CATE_NM, '>'), 2) AS CATE_PATH
          FROM TB_CATE_MAP TA
          WHERE TA.USE_YN = 'Y'
          START WITH TA.CATE_ID IN (
          SELECT CATE_ID
          FROM TB_CATE_MAP
          )
          CONNECT BY PRIOR TA.CATE_ID = TA.PRNT_CATE_ID
          ORDER SIBLINGS BY SEQ
          ),
          URL_MAP AS
          (
          SELECT CATE_ID, URL, CATE_PATH
          FROM
          (
          SELECT  TA.CATE_ID, URL, CATE_PATH, DEPTH_LEV,
              ROW_NUMBER() OVER(PARTITION BY URL ORDER BY DEPTH_LEV DESC) AS RNUM
          FROM TB_CATE_URL_MAP TA, CATE_INFO TB
          WHERE TA.CATE_ID = TB.CATE_ID
          )
          WHERE RNUM = 1
          )
          SELECT SESSION_ID, RNUM AS DEPTH, VHOST AS DOMAIN, URL, NEXT_URL, DECODE(RNUM, 1, F_REF_HOST, NULL) AS REF_HOST, C_TIME AS CREATE_TIME
          FROM
          (
          SELECT SESSION_ID, VHOST,
                 DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) AS URL,
                 NVL(F_REF_HOST, 'N') AS F_REF_HOST, C_TIME,
                 LEAD( DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) ) OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS NEXT_URL,
                 ROW_NUMBER() OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS RNUM
              FROM TB_WL_URL_ACCESS_SEGMENT TA, URL_MAP TB
              WHERE TA.URL = TB.URL
              AND TA.URL IN
              (
              SELECT URL
                  FROM TB_URL_COMMENT
                  WHERE SUB_TYPE = 'Y'
              )
              AND USER_AGENT != 'first'
          )

    SELECT SESSION_ID, VHOST,
           DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) AS URL,
           NVL(F_REF_HOST, 'N') AS F_REF_HOST, C_TIME,
           LEAD( DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) ) OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS NEXT_URL,
           ROW_NUMBER() OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS RNUM
        FROM TB_WL_URL_ACCESS_SEGMENT TA, URL_MAP TB

          SELECT SESSION_ID, RNUM AS DEPTH, VHOST AS DOMAIN, URL, NEXT_URL, DECODE(RNUM, 1, F_REF_HOST, NULL) AS REF_HOST, C_TIME AS CREATE_TIME
          FROM
          (
          SELECT SESSION_ID, VHOST,
                 DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) AS URL,
                 NVL(F_REF_HOST, 'N') AS F_REF_HOST, C_TIME,
                 LEAD( DECODE(TB.CATE_ID, NULL, '알수없음', CATE_PATH) ) OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS NEXT_URL,
                 ROW_NUMBER() OVER(PARTITION BY SESSION_ID ORDER BY C_TIME ASC) AS RNUM
              FROM TB_WL_URL_ACCESS_SEGMENT TA, URL_MAP TB
              WHERE TA.URL = TB.URL
              AND TA.URL IN
              (
              SELECT URL
                  FROM TB_URL_COMMENT
                  WHERE SUB_TYPE = 'Y'
              )
              AND USER_AGENT != 'first'
          )

 * */

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST
         , VHOST
         , SESSION_ID
         , URL                                                    AS URL
         , (CASE WHEN RNUM = 1 THEN F_REF_HOST ELSE NULL END )    AS REF_HOST
         , RNUM                                                   AS DEPTH
         , NEXT_URL                                               AS NEXT_URL
    FROM   (
           SELECT GVHOST
                , VHOST
                , SESSION_ID
                , URL
                ,(CASE WHEN PREV_DOMAIN IS NULL THEN 'N' ELSE PREV_DOMAIN END)        AS F_REF_HOST
                , ROW_NUMBER() OVER (PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME ASC) AS RNUM
                , LEAD(URL)    OVER (PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME ASC) AS NEXT_URL
           FROM   TB_ACCESS_SESSION3
           )
    """
    //spark.sql(qry).take(100).foreach(println);

    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("TB_SESSION_ACT_LOG"); sqlDf2.count()

    qry =
    """
    SELECT
          '"""+statisDate+"""' AS STATIS_DATE
         , GVHOST
         , VHOST
         , DEPTH
         ,(CASE WHEN URL IS NULL OR URL = ''           THEN 'N' ELSE URL      END) AS URL
         ,(CASE WHEN NEXT_URL IS NULL OR NEXT_URL = '' THEN 'N' ELSE NEXT_URL END) AS NEXT_URL
         , PAGE_VIEW
    FROM   (
           SELECT
                  GVHOST            AS GVHOST
                , VHOST             AS VHOST
                , DEPTH             AS DEPTH
                , REF_HOST          AS URL
                , URL               AS NEXT_URL
                , COUNT(SESSION_ID) AS PAGE_VIEW
           FROM   TB_SESSION_ACT_LOG
           WHERE  DEPTH = 1
           GROUP BY GVHOST, VHOST, DEPTH, REF_HOST, URL
           UNION
           SELECT
                  GVHOST            AS GVHOST
                , VHOST             AS VHOST
                , DEPTH             AS DEPTH
                , URL               AS URL
                , NEXT_URL          AS NEXT_URL
                , COUNT(SESSION_ID) AS PAGE_VIEW
           FROM   TB_SESSION_ACT_LOG
           WHERE  DEPTH > 1 AND DEPTH < 21
           AND    NEXT_URL IS NOT NULL
           GROUP BY GVHOST, VHOST, DEPTH, URL, NEXT_URL
           )
    """
    //spark.sql(qry).take(100).foreach(println);

    /*
    qry =
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
    OJDBC.deleteTable(spark, "DELETE FROM " + objNm + " WHERE STATIS_DATE='" + statisDate + "'")
    OJDBC.insertTable(spark, objNm)
  }

}
