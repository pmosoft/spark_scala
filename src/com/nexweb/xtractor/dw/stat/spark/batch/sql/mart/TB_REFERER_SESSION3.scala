package com.nexweb.xtractor.dw.stat.spark.batch.sql.mart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch

/*
 * 설    명 : [일별] 세션아이디별 접속 정보
 * 입    력 :

TB_WL_URL_ACCESS

 * 출    력 : TB_REFERER_LOG
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_REFERER_SESSION3 {

  var spark : SparkSession = null
  var objNm  = "TB_REFERER_SESSION3"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_REFERER_SESSION3";var statisDate = "20190312"; var statisType = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut()
  }

  def loadTables() = {
    LoadTable.lodRefererTable(spark, statisDate, statisType)
    LoadTable.lodAccessTable(spark, statisDate, statisType)
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT 
          GVHOST
        , V_ID
        , VISIT_CNT
        , STATIS_DATE
        , STATIS_TIME
        , DUR_TIME
        , CASE WHEN DUR_TIME <= 10                       THEN '01'
            WHEN DUR_TIME >10 AND DUR_TIME <=60       THEN '02'
            WHEN DUR_TIME >60 AND DUR_TIME <=60*2     THEN '03'
            WHEN DUR_TIME >60*2 AND DUR_TIME <=60*3   THEN '04'
            WHEN DUR_TIME >60*3 AND DUR_TIME <=60*4   THEN '05'
            WHEN DUR_TIME >60*4 AND DUR_TIME <=60*5   THEN '06'
            WHEN DUR_TIME >60*5 AND DUR_TIME <=60*7   THEN '07'
            WHEN DUR_TIME >60*7 AND DUR_TIME <=60*10  THEN '08'
            WHEN DUR_TIME >60*10 AND DUR_TIME <=60*20 THEN '09'
            WHEN DUR_TIME >60*20 AND DUR_TIME <=60*30 THEN '10'
            ELSE '99'
          END AS DUR_TIME_CD
        , PAGE_CNT
        , CASE WHEN PAGE_CNT =  1 THEN '01'
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
        , CASE WHEN '20'||SUBSTR(V_ID, 2, 6) = '${statisDate}'             THEN 'NV' ELSE 'RV' END AS REVISIT_DAY
        , CASE WHEN '20'||SUBSTR(V_ID, 2, 4) = SUBSTR('${statisDate}',1,6) THEN 'NV' ELSE 'RV' END AS REVISIT_MONTH
        , URL
        , HOST
        , DIR_CGI
        , DOMAIN
        , CATEGORY
        , V_IP
        , KEYWORD
        , NVL(MOBILE_YN, 'ETC') AS MOBILE_YN
        , NVL(OS, 'ETC') AS OS
        , NVL(BROWSER, 'ETC') AS BROWSER
        , NVL(OS_VER, 'ETC') AS OS_VER
        , NVL(BROWSER_VER, 'ETC') AS BROWSER_VER
        , NVL(XLOC, 'ETC') AS XLOC
        , NVL(LANG, 'ETC') AS LANG
        , NVL(DEVICE_ID, 'ETC') AS DEVICE_ID
        FROM
        (
       SELECT GVHOST
            , V_ID
            , VISIT_CNT
            , DATE_FORMAT(START_TIME,'yyyyMMdd') AS STATIS_DATE
            , DATE_FORMAT(START_TIME,'yyyyMMddHH') AS STATIS_TIME
            , CAST(((UNIX_TIMESTAMP(END_TIME,'yyyy-MM-dd HH:mm:ss.SSS') - UNIX_TIMESTAMP(START_TIME,'yyyy-MM-dd HH:mm:ss.SSS')) ) AS INTEGER) AS DUR_TIME
            , PAGE_CNT
            , URL
            , HOST
            , DIR_CGI
            , DOMAIN
            , CATEGORY
            , V_IP
            , KEYWORD
            , MOBILE_YN
            , OS
            , BROWSER
            , OS_VER
            , BROWSER_VER
            , XLOC
            , LANG
            , DEVICE_ID
       FROM   (
              SELECT GVHOST                   AS GVHOST
                   , V_ID                     AS V_ID
                   , COUNT(DISTINCT SESSION_ID) AS VISIT_CNT
                   , COUNT(DISTINCT URL)      AS PAGE_CNT
                   , MIN(C_TIME)              AS START_TIME
                   , MAX(C_TIME)              AS END_TIME
                   , MIN(URL        )         AS URL
                   , MIN(HOST       )         AS HOST
                   , MIN(DIR_CGI    )         AS DIR_CGI
                   , MIN(DOMAIN     )         AS DOMAIN
                   , MIN(CATEGORY   )         AS CATEGORY
                   , MIN(V_IP       )         AS V_IP
                   , MIN(KEYWORD    )         AS KEYWORD
                   , MIN(MOBILE_YN  )         AS MOBILE_YN
                   , MIN(OS         )         AS OS
                   , MIN(BROWSER    )         AS BROWSER
                   , MIN(OS_VER     )         AS OS_VER
                   , MIN(BROWSER_VER)         AS BROWSER_VER
                   , MIN(XLOC       )         AS XLOC
                   , MIN(LANG       )         AS LANG
                   , MIN(DEVICE_ID  )         AS DEVICE_ID
              FROM   (
                     SELECT GVHOST
                           , V_ID
                           , NVL(SESSION_ID, V_ID) AS SESSION_ID
                           , URL
                           , C_TIME
                           , HOST
                           , DIR_CGI
                           , DOMAIN
                           , IF(LENGTH(CATEGORY) > 10, SUBSTR(CATEGORY,1,2), CATEGORY) AS CATEGORY
                           , V_IP
                           , TRIM(REPLACE(REPLACE(KEYWORD,'?', ''),'？', '')) AS KEYWORD
                           , MOBILE_YN
                           , OS
                           , BROWSER
                           , OS_VER
                           , BROWSER_VER
                           , XLOC
                           , LANG
                           , SUBSTR(DEVICE_ID,1,50) AS DEVICE_ID
                    FROM   TB_WL_REFERER
                    WHERE LENGTH(GVHOST) > 0
                    AND   LENGTH(URL) > 0
                    UNION
                    SELECT GVHOST, V_ID, SESSION_ID, URL, C_TIME, HOST, '' AS DIR_CGI, 
              		       DOMAIN, CATEGORY, V_IP, TRIM(REPLACE(REPLACE(KEYWORD,'?', ''),'？', '')) AS KEYWORD, MOBILE_YN, OS, BROWSER, OS_VER, BROWSER_VER, XLOC, LANG, DEVICE_ID
          					FROM
          					(
          					SELECT TA.GVHOST, NVL(TA.SESSION_ID, V_ID) AS SESSION_ID, URL, V_ID, C_TIME, F_REF_HOST AS HOST, '' AS DIR_CGI, 
          					'' AS DOMAIN, '' AS CATEGORY, IP AS V_IP, KEYWORD, 
          					MOBILE_YN, OS, BROWSER, OS_VER, BROWSER_VER, XLOC, LANG, 
          					SUBSTR(DEVICE_ID,1,50) AS DEVICE_ID,
          					ROW_NUMBER() OVER(PARTITION BY TA.GVHOST, TA.SESSION_ID ORDER BY C_TIME  ASC) AS RNUM
          					FROM TB_WL_URL_ACCESS TA,
          					(
          					SELECT DISTINCT GVHOST, SESSION_ID
          					FROM TB_WL_URL_ACCESS
          					WHERE LENGTH(SESSION_ID) > 0
          					MINUS
          					SELECT DISTINCT GVHOST, SESSION_ID
          					FROM TB_WL_REFERER
          					WHERE LENGTH(SESSION_ID) > 0
          					) TB
          					WHERE TA.GVHOST = TB.GVHOST
          					AND   TA.SESSION_ID = TB.SESSION_ID
          					AND LENGTH(TA.GVHOST) > 0
          					)
          					WHERE RNUM = 1
          					AND LENGTH(GVHOST) > 0
                    AND LENGTH(URL) > 0
                    )
              GROUP  BY GVHOST, V_ID
              )
         )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
    //sqlDf.cache.createOrReplaceTempView("TB_REFERER_SESSION");sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }
  
}
