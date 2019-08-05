package com.nexweb.xtractor.dw.stat.spark.batch.sql.tw

import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.common.OJDBC
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
import org.apache.spark.sql.SparkSession


/*
 * 설    명 : 소스상 정확한 의도 파악이 어려우며, 미디버깅 상태이며 SQL 점검후 배치수행요(2019.02.15 피승현)
 * 입    력 :

 * 출    력 : TB_SIMPLE_LOGIN_STAT
 * 수정내역 :
 * 2019-02-15 | 피승현 | 최초작성
 */
object SimpleLoginStat {

  var spark : SparkSession = null
  var objNm  = "TB_SIMPLE_LOGIN_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_SIMPLE_LOGIN_STAT";var statisDate = "20190324"; var statisType = "D"
  //var prevYyyymmDt = "201812";var statisDate = "201812"; var statisType = "M"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {

    //sub query get
    LoadTable.lodAllColTable(spark,"CATE_URL_MAP_FRONT"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_NCATE_URL_MAP_FRONT"  ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_WL_PV_STAT_FRONT"     ,statisDate,statisType,"",true)
    
    LoadTable.lodAllColTable(spark,"TB_NCATE_STAT_FRONT"     ,statisDate,statisType,"",true)
    
    LoadTable.lodAllColTable(spark,"TB_MEMBER_CLASS_DAY"     ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_REFERER_DAY"      ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY"       ,statisDate,statisType,"",true)
  }


  def excuteSql() = {
    var qry = ""
    qry =
    s"""
    SELECT
           '${statisType}'                                             AS STATIS_TYPE
         , '${statisDate}'                                             AS STATIS_DATE
         , CASE WHEN TA.GVHOST IS NULL THEN 'TOTAL' ELSE TA.GVHOST END AS GVHOST
         , SUM(NVL(NON_MEMBER_PVIEW, 0))                               AS NON_MEMBER_PVIEW
         , SUM(NVL(NON_MEMBER_UVIEW, 0))                               AS NON_MEMBER_UVIEW
         , SUM(NVL(NON_MEMBER_PVIEW+MEMBER_PVIEW, 0))                  AS TOT_MEMBER_PVIEW
         , SUM(NVL(MEMBER_UVIEW + NON_MEMBER_UVIEW, 0))                AS TOT_MEMBER_UVIEW
         , SUM(NVL(MEMBER_PVIEW, 0))                                   AS MEMBER_PVIEW
         , SUM(NVL(MEMBER_UVIEW, 0))                                   AS MEMBER_UVIEW
         , SUM(NVL(MBL_DUP_UVIEW, 0))                                  AS DUPL_MEMBER_UVIEW
         , SUM(NVL(ISITE_TWORLD_UVIEW,0))                              AS ALL_MEMBER_VIEW
    FROM
           (
           SELECT
                  SUM (NVL(SIMPLE_PVIEW, 0)) AS NON_MEMBER_PVIEW
                , 0                  AS TOT_MEMBER_PVIEW
                , SUM (NVL(MEMBER_PVIEW, 0)) AS MEMBER_PVIEW
                , 0                  AS NON_MEMBER_UVIEW
                , 0                  AS TOT_MEMBER_UVIEW
                , 0                  AS MEMBER_UVIEW
                , 0                  AS ISITE_TWORLD_UVIEW
                , 0                  AS MBL_DUP_UVIEW
           FROM
                  (
                  SELECT CATE_ID                        AS CATE_ID
                       , SUM(NVL(NCATE_MEMBER_PVIEW,0)) AS MEMBER_PVIEW
                       , SUM(SIMPLE_PVIEW)              AS SIMPLE_PVIEW
                  FROM   CATE_URL_MAP_FRONT TA
                       , TB_WL_PV_STAT_FRONT TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.URL    = TB.URL
                  GROUP BY CATE_ID
                  )
           WHERE  CATE_ID IN ('MA', 'MW')
           UNION ALL
           SELECT
                  0                  AS NON_MEMBER_PVIEW
                , 0                  AS TOT_MEMBER_PVIEW
                , 0                  AS MEMBER_PVIEW
                , 0                  AS NON_MEMBER_UVIEW
                , 0                  AS TOT_MEMBER_UVIEW
                , 0                  AS MEMBER_UVIEW
                , ISITE_TWORLD_UVIEW AS ISITE_TWORLD_UVIEW
                , 0                  AS MBL_DUP_UVIEW
           FROM
                  (
                  SELECT COUNT(DISTINCT L_ID) AS ISITE_TWORLD_UVIEW
                  FROM
                         (
                         SELECT L_ID
                         FROM   TB_MEMBER_CLASS
                         WHERE  TYPE = 'ZZ'
                         UNION ALL
                         SELECT L_ID
                         FROM   TB_MEMBER_CLASS
                         WHERE  TYPE = 'FM'
                         UNION ALL
                         SELECT L_ID
                         FROM   TB_MEMBER_CLASS
                         WHERE OPT4 >= '4'
                         )
                  )
           UNION ALL
           SELECT
                  0                                                   AS NON_MEMBER_PVIEW
                , 0                                                   AS TOT_MEMBER_PVIEW
                , 0                                                   AS MEMBER_PVIEW
                , COUNT(DISTINCT DECODE( LOGIN_TYPE,'Z', L_ID, NULL)) AS NON_MEMBER_UVIEW
                , 0                                                   AS TOT_MEMBER_UVIEW
                , COUNT(DISTINCT DECODE(LOGIN_TYPE,'A',L_ID))         AS MEMBER_UVIEW
                , 0                                                   AS ISITE_TWORLD_UVIEW
                , 0                                                   AS MBL_DUP_UVIEW
           FROM
                  (
                  SELECT TO_CHAR(C_TIME,'YYYYMMDD') AS C_TIME,L_ID,LOGIN_TYPE,TYPE
                  FROM   TB_MEMBER_CLASS
                  AND OPT4 >= '4'
                  ) GROUP BY C_TIME
           UNION ALL
           SELECT
                  0                                                   AS NON_MEMBER_PVIEW
                , 0                                                   AS TOT_MEMBER_PVIEW
                , 0                                                   AS MEMBER_PVIEW
                , 0                                                   AS NON_MEMBER_UVIEW
                , 0                                                   AS TOT_MEMBER_UVIEW
                , 0                                                   AS MEMBER_UVIEW
                , 0                                                   AS ISITE_TWORLD_UVIEW
                , COUNT (TA.L_ID)                                     AS MBL_DUP_UVIEW
           FROM
                  (
                  SELECT DISTINCT L_ID
                  FROM   TB_MEMBER_CLASS
                  WHERE  TYPE = 'FM'
                  ) TA
                  ,
                  (
                  SELECT DISTINCT L_ID
                  FROM   TB_MEMBER_CLASS
                  WHERE  TYPE = 'ZZ'
                  ) TB
           WHERE  TA.L_ID = TB.L_ID
           )
    GROUP BY ROLLUP(TA.GVHOST, TA.CATE_ID)
    """

    //spark.sql(qry).take(100).foreach(println);

    /*

    qry =
    s"""

           SELECT
                  SUM (SIMPLE_PVIEW) AS NON_MEMBER_PVIEW
                , 0                  AS TOT_MEMBER_PVIEW
                , SUM (MEMBER_PVIEW) AS MEMBER_PVIEW
                , 0                  AS NON_MEMBER_UVIEW
                , 0                  AS TOT_MEMBER_UVIEW
                , 0                  AS MEMBER_UVIEW
                , 0                  AS ISITE_TWORLD_UVIEW
                , 0                  AS MBL_DUP_UVIEW
           FROM
                  (
                  SELECT CATE_ID                        AS CATE_ID
                       , SUM(NVL(NCATE_MEMBER_PVIEW,0)) AS MEMBER_PVIEW
                       , SUM(SIMPLE_PVIEW)              AS SIMPLE_PVIEW
                  FROM   CATE_URL_MAP_FRONT TA
                       , TB_WL_PV_STAT_FRONT TB
                  WHERE  TA.GVHOST = TB.GVHOST
                  AND    TA.URL    = TB.URL
                  GROUP BY CATE_ID
                  )
           WHERE  CATE_ID = 'MW'
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    s"""
           SELECT * FROM CATE_URL_MAP_FRONT
    """
    spark.sql(qry).take(100).foreach(println);

    qry =
    s"""
           SELECT * FROM TB_WL_PV_STAT_FRONT
    """
    spark.sql(qry).take(100).foreach(println);



     */

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf = spark.sql(qry)
    sqlDf.cache.createOrReplaceTempView(objNm);sqlDf.count()

  }

  def saveToParqeut() {
    MakeParquet.dfToParquet(objNm,true,statisDate)
  }

  def ettToOracle() {
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
