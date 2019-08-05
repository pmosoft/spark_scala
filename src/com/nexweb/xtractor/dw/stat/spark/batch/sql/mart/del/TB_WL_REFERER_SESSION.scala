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

TB_WL_URL_ACCESS_SESSION


 * 출    력 : TB_WL_VISITOR_STAT
 * 수정내역 :
 * 2018-11-20 | 피승현 | 최초작성
 */
object TB_WL_REFERER_SESSION {

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

    val qry =
    """
    SELECT
           GVHOST                                               AS GVHOST
         , F_REF_HOST                                           AS F_REF_HOST
         , PREV_URL                                             AS PREV_URL
         , KEYWORD                                              AS KEYWORD
         , V_ID                                                 AS V_ID
         , U_ID                                                 AS U_ID
         , C_TIME                                               AS C_TIME
         , F_REF_DOMAIN                                         AS F_REF_DOMAIN
         , F_REF_CATE                                           AS F_REF_CATE
         , MIN(IP) OVER(PARTITION BY V_ID)                      AS IP
         , AREA_CODE                                            AS AREA_CODE
         , PREV_DOMAIN                                          AS PREV_DOMAIN
         , URL                                                  AS URL
         , MIN(USERAGENT) OVER(PARTITION BY V_ID)               AS USERAGENT
         , SESSION_ID                                           AS SESSION_ID
         , MIN(MOBILE_YN) OVER(PARTITION BY V_ID)               AS MOBILE_YN
         , MIN(OS) OVER(PARTITION BY V_ID)                      AS OS
         , MIN(OS_VER) OVER(PARTITION BY V_ID)                  AS OS_VER
         , MIN(BROWSER) OVER(PARTITION BY V_ID)                 AS BROWSER
         , MIN(BROWSER_VER) OVER(PARTITION BY V_ID)             AS BROWSER_VER
         , IP_STR, 'ETC')                                       AS IP_STR
         , XLOC, 'ETC')                                         AS XLOC
         , LANG, 'ETC')                                         AS LANG
         , DEVICE_ID                                            AS DEVICE_ID
    FROM   (
           SELECT GVHOST
                , F_REF_HOST
                , PREV_URL
                , KEYWORD
                , V_ID
                , U_ID
                , C_TIME
                , F_REF_DOMAIN
                , F_REF_CATE
                , IP
                , AREA_CODE
                , PREV_DOMAIN
                , URL
                , USERAGENT
                , SESSION_ID
                , MOBILE_YN
                , OS
                , OS_VER
                , BROWSER
                , BROWSER_VER
                , IP_STR
                , XLOC
                , LANG
                , DEVICE_ID
           FROM   TB_WL_URL_ACCESS_SESSION
           WHERE  APPLY_STAT = 'R'
           )
    WHERE  RNK = 1
    """
    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }


}
