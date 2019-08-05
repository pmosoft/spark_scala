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

 * 출    력 : TB_ACCESS_SESSION
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_ACCESS_SESSION2 {

  var spark : SparkSession = null
  var objNm  = "TB_ACCESS_SESSION2"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_ACCESS_SESSION2";var statisDate = "20190312"; var statisType = "D"
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
    LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodRefererTable(spark, statisDate, statisType)
    //spark.sql("DROP TABLE TB_WL_URL_ACCESS")
    //spark.sql("SELECT COUNT(*) FROM TB_WL_URL_ACCESS").take(100).foreach(println);
    //spark.sql("SELECT * FROM TB_WL_URL_ACCESS").take(100).foreach(println);
 }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
      SELECT GVHOST
         , SESSION_ID
         , V_ID
         , T_ID
         , VHOST
         , URL
         , NEXT_URL
         , PREV_DOMAIN
         , PAGE_VIEW
         , START_TIME
         , END_TIME
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
         , IP
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
                 , SESSION_ID
                 , V_ID
                 , T_ID
                 , VHOST
                 , URL
                 , NEXT_URL
                 , PREV_DOMAIN
                 , PAGE_VIEW
                 , START_TIME
                 , END_TIME
                 , CAST(((UNIX_TIMESTAMP(END_TIME,'yyyy-MM-dd HH:mm:ss.SSS') - UNIX_TIMESTAMP(START_TIME,'yyyy-MM-dd HH:mm:ss.SSS'))) AS INTEGER) AS DUR_TIME
                 , PAGE_CNT
                 , IP
                 , MOBILE_YN
                 , OS
                 , BROWSER
                 , OS_VER
                 , BROWSER_VER
                 , XLOC
                 , LANG
                 , DEVICE_ID
            FROM
                   (
                   SELECT GVHOST
                        , SESSION_ID
                        , VHOST
                        , URL
                        , PREV_DOMAIN
                        , V_ID
                        , T_ID
                        , LEAD(URL) OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME) NEXT_URL
                        , PAGE_VIEW
                        , C_TIME AS START_TIME
                        , NVL(LEAD(C_TIME) OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME), C_TIME) END_TIME
                        , PAGE_CNT
                        , IP
                        , MOBILE_YN
                        , OS
                        , BROWSER
                        , OS_VER
                        , BROWSER_VER
                        , XLOC
                        , LANG
                        , DEVICE_ID
                   FROM   (
                          SELECT GVHOST                        AS GVHOST
                               , SESSION_ID                    AS SESSION_ID
                               , VHOST                         AS VHOST
                               , URL                           AS URL
                               , PREV_DOMAIN                   AS PREV_DOMAIN
                               , MIN(V_ID)                     AS V_ID
                               , MIN(T_ID)                     AS T_ID
                               , COUNT(*)                      AS PAGE_VIEW
                               , COUNT(DISTINCT SESSION_ID)    AS PAGE_CNT
                               , MIN(C_TIME)                   AS C_TIME
                               , MIN(IP         )              AS IP
                               , MIN(MOBILE_YN  )              AS MOBILE_YN
                               , MIN(OS         )              AS OS
                               , MIN(BROWSER    )              AS BROWSER
                               , MIN(OS_VER     )              AS OS_VER
                               , MIN(BROWSER_VER)              AS BROWSER_VER
                               , MIN(XLOC       )              AS XLOC
                               , MIN(LANG       )              AS LANG
                               , MIN(DEVICE_ID  )              AS DEVICE_ID
                          FROM   (
                          				SELECT GVHOST
                          				   , VHOST
                          				   , NVL(SESSION_ID, V_ID) AS SESSION_ID
                          				   , V_ID
                          				   , T_ID
                          				   , URL
                          				   , PREV_DOMAIN
                          				   , C_TIME
                          				   , IP
                          				   , MOBILE_YN
                          				   , OS
                          				   , BROWSER
                          				   , OS_VER
                          				   , BROWSER_VER
                          				   , XLOC
                          				   , LANG
                          				   , SUBSTR(DEVICE_ID,1,50) AS DEVICE_ID
                          				FROM   TB_WL_URL_ACCESS
                          				WHERE LENGTH(GVHOST) > 0
                          				AND LENGTH(URL) > 0
                          				UNION
                          				SELECT TA.GVHOST
                          				   , TA.VHOST
                          				   , NVL(TA.SESSION_ID, V_ID) AS SESSION_ID
                          				   , V_ID
                          				   , T_ID
                          				   , URL
                          				   , DOMAIN AS PREV_DOMAIN
                                  	 , C_TIME
                                  	 , V_IP AS IP
                          				   , MOBILE_YN
                          				   , OS
                          				   , BROWSER
                          				   , OS_VER
                          				   , BROWSER_VER
                          				   , XLOC
                          				   , LANG
                          				   , SUBSTR(DEVICE_ID,1,50) AS DEVICE_ID
                          				FROM TB_WL_REFERER TA,
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
                        					AND LENGTH(TA.URL) > 0
                                )
                          GROUP  BY GVHOST, VHOST, SESSION_ID, URL, PREV_DOMAIN
                          )
                   )
             )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------

    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
    //spark.sql("DROP TABLE TB_ACCESS_SESSION")
    //sqlDf.cache.createOrReplaceTempView("TB_ACCESS_SESSION");sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
