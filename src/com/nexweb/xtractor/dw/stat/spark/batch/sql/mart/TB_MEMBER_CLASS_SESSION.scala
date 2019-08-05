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

 * 출    력 : TB_MEMBER_CLASS_SESSION
 * 수정내역 :
 * 2019-01-24 | 피승현 | 최초작성
 */
object TB_MEMBER_CLASS_SESSION {

  var spark : SparkSession = null
  var objNm  = "TB_MEMBER_CLASS_SESSION"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_MEMBER_CLASS_SESSION"; var statisDate = "20190312"; var statisType = "D"

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
    LoadTable.lodMemberTable(spark, statisDate, statisType)
  }

/*

    SELECT * FROM TB_MEMBER_CLASS_SESSION

    //spark.sql("SELECT * FROM TB_MEMBER_CLASS_SESSION").take(100).foreach(println);
    //spark.sql("SELECT * FROM TB_MEMBER_CLASS").take(100).foreach(println);
    spark.sql("SELECT DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE, GVHOST       , VHOST        , C_TIME       , V_ID         , U_ID         , T_ID         , LOGIN_ID     , SESSION_ID   , TYPE         , GENDER       , AGE           FROM parquet.`/user/xtractor/parquet/entity/TB_MEMBER_CLASS/TB_MEMBER_CLASS_20190128`").take(100).foreach(println);

    qry =
    s"""
    SELECT *
    FROM
    (SELECT 'TW' GVHOST, 'lifehelp' T_ID, '2018-12-20 23:59' C_TIME, 'a001' SESSION_ID FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifehelp' T_ID, '2018-12-20 23:58' C_TIME, 'a002' SESSION_ID FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifehelp' T_ID, '2018-12-20 23:57' C_TIME, 'a002' SESSION_ID FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifedomy' T_ID, '2018-12-20 22:59' C_TIME, 'a001' SESSION_ID FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifedomy' T_ID, '2018-12-20 22:58' C_TIME, 'a001' SESSION_ID FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifedomy' T_ID, '2018-12-20 22:57' C_TIME, 'a002' SESSION_ID FROM DUAL
    )
    """
    //spark.sql(qry).take(100).foreach(println);
    val sqlDf22 = spark.sql(qry)
    sqlDf22.cache.createOrReplaceTempView("ACCESS");sqlDf22.count()

    qry =
    s"""
    SELECT *
    FROM
    (SELECT 'TW' GVHOST, 'lifehelp' T_ID, '2018-12-20 23:58' C_TIME FROM DUAL UNION ALL
     SELECT 'TW' GVHOST, 'lifedomy' T_ID, '2018-12-20 22:57' C_TIME FROM DUAL
    )
    """
    //spark.sql(qry).take(100).foreach(println);
    val sqlDf21 = spark.sql(qry)
    sqlDf21.cache.createOrReplaceTempView("MEMBER");sqlDf21.count()

    qry =
    s"""
    SELECT TA.GVHOST, TA.T_ID,TA.SESSION_ID
         , MIN(TA.C_TIME) AS MIN_C_TIME_ACCESS
         , MAX(TA.C_TIME) AS MAX_C_TIME_ACCESS
    FROM   ACCESS TA
    GROUP BY TA.GVHOST,TA.T_ID,TA.SESSION_ID
    """
    spark.sql(qry).take(100).foreach(println);

    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("TB_MEMBER_CLASS_SESSION_T01");sqlDf2.count()

    qry =
    s"""
    SELECT TB.GVHOST
         , TB.T_ID
         , TB.C_TIME
         , MIN(TA.SESSION_ID) SESSION_ID
    FROM   TB_MEMBER_CLASS_SESSION_T01 TA,
           MEMBER TB
    WHERE  TA.GVHOST = TB.GVHOST
    AND    TA.T_ID   = TB.T_ID
    AND    TB.C_TIME BETWEEN TA.MIN_C_TIME_ACCESS AND TA.MAX_C_TIME_ACCESS
    GROUP BY TB.GVHOST, TB.T_ID, TB.C_TIME
    """
    spark.sql(qry).take(100).foreach(println);
 *
 * */

  def excuteSql() = {
/*
    qry =
    s"""
    SELECT TA.GVHOST      AS GVHOST
         , TA.T_ID        AS T_ID
         , TA.SESSION_ID  AS SESSION_ID
         , MIN(TA.C_TIME) AS MIN_C_TIME_ACCESS
         , MAX(TA.C_TIME) AS MAX_C_TIME_ACCESS
    FROM   TB_WL_URL_ACCESS TA
    GROUP BY TA.GVHOST,TA.T_ID,TA.SESSION_ID
    """
    spark.sql(qry).take(100).foreach(println);

    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("TB_MEMBER_CLASS_SESSION_T01");sqlDf2.count()

    qry =
    s"""
    SELECT TB.GVHOST
         , TB.T_ID
         , TB.C_TIME
         , MIN(TA.SESSION_ID) SESSION_ID
    FROM   TB_MEMBER_CLASS_SESSION_T01 TA,
           TB_MEMBER_CLASS TB
    WHERE  TA.GVHOST = TB.GVHOST
    AND    TA.T_ID   = TB.T_ID
    AND    TB.C_TIME BETWEEN TA.MIN_C_TIME_ACCESS AND TA.MAX_C_TIME_ACCESS
    GROUP BY TB.GVHOST, TB.T_ID, TB.C_TIME
    """
    spark.sql(qry).take(100).foreach(println);
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("TB_MEMBER_CLASS_SESSION_T02");sqlDf3.count()

    qry =
    s"""
    SELECT DISTINCT
           TA.STATIS_DATE
         , TA.GVHOST
         , TA.VHOST
         , TA.V_ID
         , TA.T_ID
         , TA.LOGIN_ID
         , TB.SESSION_ID
         , TA.C_TIME
         , TA.TYPE
         , TA.GENDER
         , TA.AGE
    FROM   TB_MEMBER_CLASS TA,
           TB_MEMBER_CLASS_SESSION_T02 TB
    WHERE  TA.GVHOST = TB.GVHOST
    AND    TA.T_ID   = TB.T_ID
    AND    TA.C_TIME = TB.C_TIME
    """
    //spark.sql(qry).take(100).foreach(println);
*/
    var qry = ""
        qry =
          s"""
          SELECT DISTINCT
                 STATIS_DATE
               , GVHOST
               , VHOST
               , V_ID
               , T_ID
               , LOGIN_ID
               , NVL(SESSION_ID, V_ID) AS SESSION_ID
               , C_TIME
               , TYPE
               , GENDER
               , AGE
          FROM   TB_MEMBER_CLASS
          WHERE LENGTH(SESSION_ID) > 0
          AND LENGTH(GVHOST) > 0 
          UNION
        	SELECT DISTINCT
               STATIS_DATE
             , TA.GVHOST
             , TA.VHOST
             , TA.V_ID
             , TA.T_ID
             , TA.LOGIN_ID
             , NVL(TB.SESSION_ID, V_ID) AS SESSION_ID
             , TA.C_TIME
             , TA.TYPE
             , TA.GENDER
             , TA.AGE
          FROM TB_MEMBER_CLASS TA
          LEFT OUTER JOIN
          (
          SELECT GVHOST, T_ID, SESSION_ID
          FROM
          (			
          SELECT GVHOST, T_ID, SESSION_ID,
          ROW_NUMBER() OVER(PARTITION BY GVHOST, T_ID ORDER BY C_TIME ASC) AS RNUM
          FROM TB_WL_URL_ACCESS
          WHERE GVHOST = 'TMAP'
          )
          WHERE RNUM =1
          ) TB
          ON TA.GVHOST = TB.GVHOST AND TA.T_ID = TB.T_ID
          WHERE LENGTH(TA.SESSION_ID) < 1
          AND  LENGTH(TA.GVHOST) > 0
          """
          
          //spark.sql(qry).take(100).foreach(println);
    
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

}
