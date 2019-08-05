package com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.del

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

/*
 * 설    명 : [일별] 방문 통계정보 생성
 * 입    력 :

TB_WL_URL_ACCESS


 * 출    력 : TB_WL_VISITOR_STAT
 * 수정내역 :
 * 2018-11-20 | 피승현 | 최초작성
 */
object TB_WL_URL_ACCESS_SESSION {

  var spark : SparkSession = null
  var objNm  = "TB_WL_VISITOR_STAT"

  var statisDate = ""
  var statisType = ""
  //var statisDate = "20181219"; var statisType = "D"

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
    LoadTable.lodMemberTable(spark, statisDate, statisType)
 }

  def excuteSql() = {

    var qry =
    """


    INSERT /*+ APPEND */ INTO TB_WL_URL_ACCESS_SESSION
    (VHOST, URL, V_ID, U_ID, C_TIME, IP, P_ID, KEYWORD, F_REF_HOST, F_REF_DOMAIN, F_REF_CATE,
    OPT, OPT1, OPT2, AREA_CODE, SESSION_ID,USERAGENT, MOBILE_YN, OS, OS_VER, BROWSER, BROWSER_VER, IP_STR, XLOC, LANG, DEVICE_ID, EBM_URL, APPLY_STAT, PREV_URL, PREV_DOMAIN, TIMELINE)

        WITH SESSION_GAP AS
            (
            SELECT 30 GAP FROM DUAL
            ),
    ALL_LOG AS
        (
    SELECT
          VHOST, URL, V_ID, U_ID, C_TIME, IP, P_ID, KEYWORD, F_REF_HOST, F_REF_DOMAIN, F_REF_CATE,
          OPT, OPT1, OPT2, AREA_CODE, SESSION_ID,USERAGENT,
            NVL(MOBILE_YN, 'ETC')   AS MOBILE_YN,
            NVL(OS, 'ETC')          AS OS
          , NVL(OS_VER, 'ETC')      AS OS_VER,
            NVL(BROWSER, 'ETC')     AS BROWSER
          , NVL(BROWSER_VER, 'ETC') AS BROWSER_VER,
            NVL(IP_STR, 'ETC')      AS IP_STR
          , NVL(XLOC, 'ETC')        AS XLOC,
            NVL(LANG, 'ETC')        AS LANG
          , DEVICE_ID,
            EBM_URL
          , APPLY_STAT, PREV_URL, PREV_DOMAIN, TIMELINE,
            NVL(TRUNC((C_TIME - PREV_TIME)*1440), TB.GAP) PREV_GAP
    FROM  (
          SELECT
                VHOST, URL, V_ID, U_ID, C_TIME, IP, P_ID, KEYWORD, F_REF_HOST, F_REF_DOMAIN, F_REF_CATE, OPT, OPT1, OPT2, AREA_CODE, SESSION_ID,USERAGENT, MOBILE_YN, OS, OS_VER, BROWSER, BROWSER_VER, IP_STR, XLOC, LANG, DEVICE_ID, EBM_URL, APPLY_STAT, PREV_URL, PREV_DOMAIN, TIMELINE,
                LAG(C_TIME) OVER(PARTITION BY V_ID ORDER BY C_TIME, URL) PREV_TIME
          FROM  TB_WL_URL_ACCESS TA
          ), SESSION_GAP TB
        ),
    SESSION_LOG AS
          (
          SELECT    V_ID, URL, C_TIME START_TIME,
                    NVL(LEAD(C_TIME) OVER(PARTITION BY V_ID ORDER BY C_TIME, URL), TO_DATE('99991231 235959','YYYYMMDD HH24MISS')) NEXT_TIME,
                    V_ID||TO_CHAR(C_TIME, 'HH24MISS') SESSION_ID
            FROM    ALL_LOG, SESSION_GAP TB
            WHERE   PREV_GAP >= TB.GAP
          )
    SELECT     VHOST, TA.URL, TA.V_ID, U_ID, C_TIME, IP, P_ID, KEYWORD, F_REF_HOST, F_REF_DOMAIN, F_REF_CATE,
               OPT, OPT1, OPT2, AREA_CODE, TB.SESSION_ID,
             NVL(MAX(USERAGENT) OVER(PARTITION BY TA.V_ID), 'ETC') AS USERAGENT,
             NVL(MAX(MOBILE_YN) OVER(PARTITION BY TA.V_ID), 'ETC') AS MOBILE_YN,
             NVL(MAX(OS) OVER(PARTITION BY TA.V_ID), 'ETC') AS OS,
             NVL(MAX(OS_VER) OVER(PARTITION BY TA.V_ID), 'ETC') AS OS_VER,
             NVL(MAX(BROWSER) OVER(PARTITION BY TA.V_ID), 'ETC') AS BROWSER,
             NVL(MAX(BROWSER_VER) OVER(PARTITION BY TA.V_ID), 'ETC') AS BROWSER_VER,
             IP_STR, XLOC, LANG, DEVICE_ID,
               EBM_URL, APPLY_STAT, PREV_URL, PREV_DOMAIN, TIMELINE
      FROM    ALL_LOG TA, SESSION_LOG TB
      WHERE   TA.V_ID = TB.V_ID
      AND     TA.C_TIME >= TB.START_TIME
      AND     TA.C_TIME < TB.NEXT_TIME


      SELECT CASE WHEN (C_TIME - PREV_TIME)*1440 IS NULL THEN 30 ELSE (C_TIME - PREV_TIME)*1440 END
      FROM
         (SELECT TA.*
               , LAG(C_TIME) OVER(PARTITION BY V_ID ORDER BY C_TIME, URL) AS PREV_TIME
          FROM   parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219` TA
         ) TA

    """



    qry =
    """
      SELECT C_TIME, PREV_TIME, PREV_GAP
      FROM
         (SELECT TA.*
               ,                LAG(C_TIME) OVER(PARTITION BY V_ID ORDER BY C_TIME, URL)                            AS PREV_TIME
               ,(UNIX_TIMESTAMP(C_TIME,'yyyy-MM-dd HH:mm:ss.SSS')
                 -
                 UNIX_TIMESTAMP(LAG(C_TIME) OVER(PARTITION BY V_ID ORDER BY C_TIME, URL),'yyyy-MM-dd HH:mm:ss.SSS')
                 )                                                                                                  AS PREV_GAP
          FROM   parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219` TA
         ) TA

    """
    spark.sql(qry).take(10).foreach(println);

    //--------------------------------------
    //    println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
