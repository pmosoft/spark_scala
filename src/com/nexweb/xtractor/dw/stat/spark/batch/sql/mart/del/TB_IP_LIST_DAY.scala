package com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.del

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

 * 출    력 : TB_IP_LIST_DAY
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_IP_LIST_DAY {

  var spark : SparkSession = null
  var objNm  = "TB_IP_LIST_DAY"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_IP_LIST_DAY";var statisDate = "20181219"; var statisType = "D"

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
    LoadTable.lodAllColTable(spark,"TB_REFERER_SESSION"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_SESSION"       ,statisDate,statisType,"",true)
  }

  def excuteSql() = {
    var qry = ""
    qry =
    """
    SELECT IP
         , CAST(
           REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',1)*16777216
          +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',2)*65536
          +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',3)*256
          +REGEXP_EXTRACT(IP,'([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3}).([0-9]{1,3})',4) AS LONG) AS IP_NUM
    FROM   (
           SELECT DISTINCT
                  V_IP AS IP
           FROM   TB_REFERER_SESSION
           UNION ALL
           SELECT DISTINCT
                  IP
           FROM   TB_ACCESS_SESSION
           )
    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()
    //sqlDf.cache.createOrReplaceTempView("TB_IP_LIST_DAY");sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
