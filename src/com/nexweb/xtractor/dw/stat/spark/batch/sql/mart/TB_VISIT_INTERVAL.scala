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
import com.nexweb.xtractor.dw.stat.spark.common.Batch

/*
 * 설    명 : [일별] 방문전일자 정보 생성
 * 입    력 :

TB_WL_URL_ACCESS

 * 출    력 : TB_ACCESS_SESSION
 * 수정내역 :
 * 2018-11-20 | 피승현 | 최초작성
 */
object TB_VISIT_INTERVAL {

  var spark : SparkSession = null
  var objNm  = "TB_VISIT_INTERVAL"

  var statisDate = ""
  var preStatisDate = ""
  var statisType = ""
  //var objNm  = "TB_VISIT_INTERVAL";var statisDate = "20190214"; var preStatisDate = "20190213"; var statisType = "D"
  //var objNm  = "TB_VISIT_INTERVAL";var statisDate = "20190213"; var preStatisDate = "20190212"; var statisType = "D"
  //var objNm  = "TB_VISIT_INTERVAL";var statisDate = "20190212"; var preStatisDate = "20190211"; var statisType = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    preStatisDate = Batch.getSubDt(spark,statisDate,1)
    statisType = "D"
    loadTables();excuteSql();saveToParqeut()
  }

  def loadTables() = {
    //spark.sql("DROP TABLE TB_WL_REFERER")
    LoadTable.lodRefererTable(spark, statisDate, statisType)
    //spark.sql("DROP TABLE TB_VISIT_INTERVAL")
    LoadTable.lodAllColTable(spark,"TB_VISIT_INTERVAL" ,preStatisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT GVHOST, V_ID, STATIS_DATE
    FROM   TB_WL_REFERER
    GROUP BY GVHOST, V_ID, STATIS_DATE
    """
    val sqlDf1 = spark.sql(qry)
    sqlDf1.cache.createOrReplaceTempView("REFERER");sqlDf1.count()

    qry =
    s"""
    SELECT GVHOST
         , V_ID
         , STATIS_DATE
         , PRE_STATIS_DATE
         , CASE WHEN REVISIT_INTERVAL <=  9             THEN '0'||REVISIT_INTERVAL
                WHEN REVISIT_INTERVAL  = 10             THEN '10'
                WHEN REVISIT_INTERVAL BETWEEN 11 AND 15 THEN '11'
                WHEN REVISIT_INTERVAL BETWEEN 16 AND 20 THEN '12'
                WHEN REVISIT_INTERVAL BETWEEN 21 AND 30 THEN '13'
                WHEN REVISIT_INTERVAL BETWEEN 31 AND 60 THEN '14'
                WHEN REVISIT_INTERVAL BETWEEN 61 AND 90 THEN '15'
                ELSE '99'
           END AS REVISIT_INTERVAL_CD
    FROM   (
           SELECT TA.GVHOST
                , TA.V_ID
                , TB.STATIS_DATE
                , TA.PRE_STATIS_DATE
                , CAST((UNIX_TIMESTAMP(TB.STATIS_DATE,'yyyyMMdd') - UNIX_TIMESTAMP(TA.PRE_STATIS_DATE,'yyyyMMdd')) / (60*60*24) AS INTEGER) AS REVISIT_INTERVAL
           FROM   TB_VISIT_INTERVAL TA,
                  REFERER TB
           WHERE  TA.GVHOST = TB.GVHOST
           AND    TA.V_ID   = TB.V_ID
           UNION ALL
           SELECT TA.GVHOST
                , TA.V_ID
                , TA.STATIS_DATE
                , TA.STATIS_DATE AS PRE_STATIS_DATE
                , 0 AS REVISIT_INTERVAL
           FROM   REFERER TA
                  LEFT OUTER JOIN TB_VISIT_INTERVAL TB
                  ON  TA.GVHOST = TB.GVHOST
                  AND TA.V_ID   = TB.V_ID
           WHERE  TB.V_ID IS NULL
           )
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""
    SELECT *
    FROM   parquet.`/user/xtractor/parquet/entity/TB_VISIT_INTERVAL/TB_VISIT_INTERVAL_201902*`
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    s"""
    SELECT STATIS_DATE
         , MAX(STATIS_DATE)
         , MIN(PRE_STATIS_DATE)
    FROM   parquet.`/user/xtractor/parquet/entity/TB_VISIT_INTERVAL/TB_VISIT_INTERVAL_201902*`
    GROUP BY STATIS_DATE
    ORDER BY STATIS_DATE
    """
    spark.sql(qry).take(100).foreach(println);


    qry =
    s"""
           SELECT MIN(TB.STATIS_DATE)
                , MIN(TA.PRE_STATIS_DATE)
                , MAX(CAST((UNIX_TIMESTAMP(TB.STATIS_DATE,'yyyyMMdd') - UNIX_TIMESTAMP(TA.PRE_STATIS_DATE,'yyyyMMdd')) / (60*60*24) AS INTEGER))
           FROM   TB_VISIT_INTERVAL TA,
                  REFERER TB
           WHERE  TA.GVHOST = TB.GVHOST
           AND    TA.V_ID   = TB.V_ID

    """
    spark.sql(qry).take(100).foreach(println);

    */


    //--------------------------------------
        println(qry);
    //--------------------------------------

    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()

  }

  def excuteInit() = {
    var qry = ""

    qry =
    s"""
    SELECT GVHOST, V_ID, STATIS_DATE, STATIS_DATE AS PRE_STATIS_DATE
    FROM   TB_WL_URL_ACCESS
    GROUP BY GVHOST, V_ID, STATIS_DATE
    """
    //spark.sql(qry).take(100).foreach(println);
    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView("TB_VISIT_INTERVAL");sqlDf.count()

    MakeParquet.dfToParquet("TB_VISIT_INTERVAL",true,"20190205")
    //MakeParquet.dfToParquet("TB_VISIT_INTERVAL",true,"20181219")
  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
