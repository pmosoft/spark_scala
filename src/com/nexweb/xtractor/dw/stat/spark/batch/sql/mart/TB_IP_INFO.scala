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
 * 설    명 :
 * 입    력 :

TB_WL_URL_ACCESS

 * 출    력 : TB_IP_INFO
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_IP_INFO {

  var spark : SparkSession = null
  var objNm  = "TB_IP_INFO"

  var statisDate = ""
  var preStatisDate = ""
  var statisType = ""
  //var objNm  = "TB_IP_INFO";var statisDate = "20190313"; var preStatisDate = "20190313"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION2"       ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_IP_INFO"              ,preStatisDate,statisType,"",true)
    LoadTable.lodAllColAllDataTable(spark,"TB_COUNTRY_IP_SETTING" ,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    """
    SELECT TA.*
    FROM
           (
           SELECT IP
                , CAST(
                  REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',1)*16777216
                 +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',2)*65536
                 +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',3)*256
                 +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',4) AS LONG) AS IP_NUM
           FROM   (
                  SELECT DISTINCT IP
                  FROM
                      (
                      SELECT DISTINCT
                             V_IP AS IP
                      FROM   TB_REFERER_SESSION
                      UNION ALL
                      SELECT DISTINCT
                             IP
                      FROM   TB_ACCESS_SESSION2
                      )
                  )
           ) TA
           LEFT OUTER JOIN TB_IP_INFO TB
           ON TA.IP = TB.IP
    WHERE  TB.IP IS NULL
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("TB_IP_LIST_DAY");sqlDf2.count()

    qry =
    """
    SELECT
           CAST(
           REGEXP_EXTRACT(START_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',1)*16777216
          +REGEXP_EXTRACT(START_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',2)*65536
          +REGEXP_EXTRACT(START_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',3)*256
          +REGEXP_EXTRACT(START_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',4) AS LONG) AS START_IP_NUM
         , CAST(
           REGEXP_EXTRACT(END_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',1)*16777216
          +REGEXP_EXTRACT(END_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',2)*65536
          +REGEXP_EXTRACT(END_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',3)*256
          +REGEXP_EXTRACT(END_IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',4) AS LONG) AS END_IP_NUM
         , COUNTRY
         , COUNTRY_NM
         , CITY
         , ISP_NM
    FROM   TB_COUNTRY_IP_SETTING
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("TB_COUNTRY_IP_SETTING2");sqlDf3.count()

    qry =
    """
    SELECT TA.IP
         , TB.COUNTRY
         , TB.COUNTRY_NM
         , TB.CITY
         , TB.ISP_NM
    FROM   TB_IP_LIST_DAY TA,
           TB_COUNTRY_IP_SETTING2 TB
    WHERE  TA.IP_NUM BETWEEN TB.START_IP_NUM AND TB.END_IP_NUM
    UNION
    SELECT * FROM TB_IP_INFO
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
    //sqlDf.cache.createOrReplaceTempView("TB_URL_ACCESS_IP_INFO");sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
