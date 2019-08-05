package com.nexweb.xtractor.dw.stat.spark.batch.sql.tw

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
import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch

/*
 * 설    명 : 일별
 * 입    력 :
  - TB_URL_COMMENT_FRONT
  - TB_WL_URL_ACCESS
 * 출    력 : TB_OPT_PARAM_STAT
 * 수정내역 :
 * 2018-12-10 | 피승현 | 최초작성
 */
object OptParamStat {

  var spark : SparkSession = null
  var objNm  = "TB_OPT_PARAM_STAT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_OPT_PARAM_STAT";var statisDate = "20190401"; var statisType = "D"

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    //LoadTable.lodAccessTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY" ,statisDate,statisType,"",true)
    LoadTable.lodAllColTable(spark,"TB_URL_COMMENT_FRONT",statisDate,statisType,"",true)
  }
  
  def excuteSql() = {

    /*
    qry =
    s"""
        SELECT
              TB.GVHOST
            , TB.VHOST
            , LAG(TB.FRONT_URL) OVER(PARTITION BY TB.V_ID ORDER BY TB.C_TIME ) AS PARAM
            , CASE WHEN TB.FRONT_URL IS NULL OR TB.FRONT_URL = '' THEN 'NA' ELSE TB.FRONT_URL END FRONT_URL
            , TB.V_ID
            , TB.U_ID
            , TB.C_TIME
            , TB.LOGIN_TYPE
        FROM  TB_URL_COMMENT_FRONT TA
            , TB_ACCESS_DAY TB
        WHERE TA.GVHOST     = TB.GVHOST
        AND   TA.VHOST      = TB.VHOST
        AND   TA.URL        = TB.FRONT_URL
        AND   LENGTH(TB.OPT_PARAM) < 1
        AND   LENGTH(TB.FRONT_URL) > 0
    """
    */
    var qry = ""
    qry =
    s"""
        SELECT
              TB.GVHOST
            , TB.VHOST
            , LAG(TB.FRONT_URL) OVER(PARTITION BY TB.V_ID ORDER BY TB.C_TIME ) AS PARAM
            , CASE WHEN TB.FRONT_URL IS NULL OR TB.FRONT_URL = '' THEN 'NA' ELSE TB.FRONT_URL END FRONT_URL
            , TB.V_ID
            , TB.U_ID
            , TB.C_TIME
            , TB.LOGIN_TYPE
        FROM  TB_URL_COMMENT_FRONT TA
            , TB_ACCESS_DAY TB
        WHERE TA.GVHOST     = TB.GVHOST
        AND   TA.VHOST      = TB.VHOST
        AND   TA.URL        = TB.FRONT_URL
        AND   LENGTH(TB.FRONT_URL) > 0
    """

    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf2 = spark.sql(qry)
    sqlDf2.cache.createOrReplaceTempView("PREV_LOG");sqlDf2.count()
 
    qry =
    s"""
        SELECT
              GVHOST                    AS GVHOST
            , VHOST                     AS VHOST
            , PARAM                     AS PARAM
            , FRONT_URL                 AS URL
            , COUNT(DISTINCT V_ID)      AS VISITOR_CNT
            , COUNT(DISTINCT U_ID)      AS TOTAL_LOGIN_VISITOR_CNT
            , COUNT(DISTINCT U_ID)      AS LOGIN_VISITOR_CNT
            , 0                         AS DUPL_LOGIN_VISITOR_CNT
            , COUNT(DISTINCT CASE WHEN LOGIN_TYPE = 'A' OR LOGIN_TYPE IS NULL THEN U_ID ELSE NULL END)    AS MEMBER_LOGIN_VISITOR_CNT
            , COUNT(DISTINCT CASE WHEN LOGIN_TYPE = 'Z' THEN U_ID ELSE NULL END)                          AS SIMPLE_LOGIN_VISITOR_CNT
            , COUNT(1)                  AS PVIEW
            , SUM(CASE WHEN U_ID IS NOT NULL THEN 1 ELSE 0 END) - SUM(CASE WHEN LOGIN_TYPE = 'Z' THEN 1 ELSE 0 END)      AS MEMBER_PVIEW
            , SUM(CASE WHEN LOGIN_TYPE = 'Z' THEN 1 ELSE 0 END)                                                          AS SIMPLE_PVIEW
        FROM PREV_LOG
        WHERE LENGTH(PARAM) > 0
        GROUP BY GVHOST, VHOST, PARAM, FRONT_URL
    		UNION ALL
    		SELECT 
    		      GVHOST
    		    , VHOST
    		    , PARAM
    		    , FRONT_URL AS URL
    		    , 0 AS VISITOR_CNT
    		    , 0 AS TOTAL_LOGIN_VISITOR_CNT
    		    , 0 AS LOGIN_VISITOR_CNT
    				, COUNT(CASE WHEN DUPL_LOGIN_VISITOR_CNT > 1 THEN U_ID ELSE NULL END) AS DUPL_LOGIN_VISITOR_CNT
    				, 0 AS MEMBER_LOGIN_VISITOR_CNT
    				, 0 AS SIMPLE_LOGIN_VISITOR_CNT
    				, 0 AS PVIEW
    				, 0 AS MEMBER_PVIEW
    				, 0 AS SIMPLE_PVIEW
    		 FROM 
    		 (
    				SELECT GVHOST, VHOST, PARAM, FRONT_URL, U_ID,
    						COUNT(DISTINCT LOGIN_TYPE) AS DUPL_LOGIN_VISITOR_CNT
    				FROM PREV_LOG
    				WHERE LENGTH(PARAM) > 0
    				GROUP BY GVHOST, VHOST, PARAM, FRONT_URL, U_ID
    		 )
    		GROUP BY GVHOST, VHOST, PARAM, FRONT_URL
    """
    //spark.sql(qry).take(100).foreach(println);
    
    //--------------------------------------
        println(qry);
    //--------------------------------------
    val sqlDf3 = spark.sql(qry)
    sqlDf3.cache.createOrReplaceTempView("OPT_TEMP01");sqlDf3.count()

    
//    qry =
//    s"""
//        SELECT
//              GVHOST
//            , VHOST
//            , OPT_PARAM
//            , CASE WHEN LENGTH(FRONT_URL) < 1 OR FRONT_URL = '' or UPPER(FRONT_URL) = 'NULL' THEN 'NA' ELSE FRONT_URL END URL
//            , V_ID
//            , U_ID
//            , C_TIME
//            , LOGIN_TYPE
//        FROM  TB_ACCESS_DAY
//        WHERE LENGTH(OPT_PARAM) > 0
//    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
//    val sqlDf4 = spark.sql(qry)
//    sqlDf4.cache.createOrReplaceTempView("OPT_PARAM_LOG");sqlDf4.count()
//       
//    qry =
//    s"""
//        SELECT
//              GVHOST                                AS GVHOST
//            , VHOST                                 AS VHOST
//            , OPT_PARAM                             AS PARAM
//            , URL                                   AS URL
//            , COUNT(DISTINCT V_ID)                  AS VISITOR_CNT
//            , COUNT(DISTINCT U_ID)                  AS TOTAL_LOGIN_VISITOR_CNT
//            , COUNT(DISTINCT U_ID)                  AS LOGIN_VISITOR_CNT
//            , 0                                     AS DUPL_LOGIN_VISITOR_CNT
//            , COUNT(DISTINCT CASE WHEN LOGIN_TYPE = 'A' OR LOGIN_TYPE IS NULL THEN U_ID ELSE NULL END)    AS MEMBER_LOGIN_VISITOR_CNT
//            , COUNT(DISTINCT CASE WHEN LOGIN_TYPE = 'Z' THEN U_ID ELSE NULL END)                          AS SIMPLE_LOGIN_VISITOR_CNT
//            , COUNT(1)                  AS PVIEW
//            , SUM(CASE WHEN U_ID IS NOT NULL THEN 1 ELSE 0 END) - SUM(CASE WHEN LOGIN_TYPE = 'Z' THEN 1 ELSE 0 END)      AS MEMBER_PVIEW
//            , SUM(CASE WHEN LOGIN_TYPE = 'Z' THEN 1 ELSE 0 END)                                                          AS SIMPLE_PVIEW
//        FROM  OPT_PARAM_LOG
//        GROUP BY GVHOST, VHOST, OPT_PARAM, URL
//        UNION ALL
//        SELECT
//              GVHOST                                AS GVHOST
//            , VHOST                                 AS VHOST
//            , PARAM                                 AS PARAM
//            , URL                                   AS URL
//            , 0                                     AS VISITOR_CNT
//            , 0                                     AS TOTAL_LOGIN_VISITOR_CNT
//            , 0                                     AS LOGIN_VISITOR_CNT
//            , COUNT(CASE WHEN DUPL_LOGIN_VISITOR_CNT > 1 THEN U_ID ELSE NULL END) AS DUPL_LOGIN_VISITOR_CNT
//            , 0                                     AS MEMBER_LOGIN_VISITOR_CNT
//            , 0                                     AS SIMPLE_LOGIN_VISITOR_CNT
//            , 0                                     AS PVIEW
//            , 0                                     AS MEMBER_PVIEW
//            , 0                                     AS SIMPLE_PVIEW
//        FROM
//    		 (
//    				SELECT GVHOST, VHOST, OPT_PARAM AS PARAM, URL, U_ID,
//    						COUNT(DISTINCT LOGIN_TYPE) AS DUPL_LOGIN_VISITOR_CNT
//    				FROM OPT_PARAM_LOG
//    				GROUP BY GVHOST, VHOST, OPT_PARAM, URL, U_ID
//    		 )
//        GROUP BY GVHOST, VHOST, PARAM, URL
//    """
    //spark.sql(qry).take(100).foreach(println);

    //--------------------------------------
        println(qry);
    //--------------------------------------
//    val sqlDf5 = spark.sql(qry)
//    sqlDf5.cache.createOrReplaceTempView("OPT_TEMP02");sqlDf5.count()

//    qry =
//    s"""
//    SELECT
//         '"""+statisDate+"""'                       AS STATIS_DATE
//        , GVHOST                                    AS GVHOST
//        , VHOST                                     AS VHOST
//        , PARAM                                     AS OPT_PARAM
//        , 'PREV_URL'                                AS OPT_TYPE
//        , URL                                       AS URL
//        , SUM(VISITOR_CNT)                          AS VISITOR_CNT
//        , SUM(TOTAL_LOGIN_VISITOR_CNT)              AS TOTAL_LOGIN_VISITOR_CNT
//        , SUM(LOGIN_VISITOR_CNT)                    AS LOGIN_VISITOR_CNT
//        , SUM(DUPL_LOGIN_VISITOR_CNT)               AS DUPL_LOGIN_VISITOR_CNT
//        , SUM(MEMBER_LOGIN_VISITOR_CNT)             AS MEMBER_LOGIN_VISITOR_CNT
//        , SUM(SIMPLE_LOGIN_VISITOR_CNT)             AS SIMPLE_LOGIN_VISITOR_CNT
//        , SUM(PVIEW)                                AS PVIEW
//        , SUM(MEMBER_PVIEW)                         AS MEMBER_PVIEW
//        , SUM(SIMPLE_PVIEW)                         AS SIMPLE_PVIEW
//    FROM OPT_TEMP01
//    GROUP BY STATIS_DATE, GVHOST, VHOST, PARAM, URL
//    UNION ALL
//    SELECT
//          '"""+statisDate+"""'                       AS STATIS_DATE
//        , GVHOST                                    AS GVHOST
//        , VHOST                                     AS VHOST
//        , PARAM                                     AS OPT_PARAM
//        , 'PARAMETER'                               AS OPT_TYPE
//        , URL                                       AS URL
//        , SUM(VISITOR_CNT)                          AS VISITOR_CNT
//        , SUM(TOTAL_LOGIN_VISITOR_CNT)              AS TOTAL_LOGIN_VISITOR_CNT
//        , SUM(LOGIN_VISITOR_CNT)                    AS LOGIN_VISITOR_CNT
//        , SUM(DUPL_LOGIN_VISITOR_CNT)               AS DUPL_LOGIN_VISITOR_CNT
//        , SUM(MEMBER_LOGIN_VISITOR_CNT)             AS MEMBER_LOGIN_VISITOR_CNT
//        , SUM(SIMPLE_LOGIN_VISITOR_CNT)             AS SIMPLE_LOGIN_VISITOR_CNT
//        , SUM(PVIEW)                                AS PVIEW
//        , SUM(MEMBER_PVIEW)                         AS MEMBER_PVIEW
//        , SUM(SIMPLE_PVIEW)                         AS SIMPLE_PVIEW
//    FROM  OPT_TEMP02
//    GROUP BY STATIS_DATE, GVHOST, VHOST, PARAM, URL
//    """
    qry =
    s"""
    SELECT
         '"""+statisDate+"""'                       AS STATIS_DATE
        , GVHOST                                    AS GVHOST
        , VHOST                                     AS VHOST
        , PARAM                                     AS OPT_PARAM
        , 'PREV_URL'                                AS OPT_TYPE
        , URL                                       AS URL
        , SUM(VISITOR_CNT)                          AS VISITOR_CNT
        , SUM(TOTAL_LOGIN_VISITOR_CNT)              AS TOTAL_LOGIN_VISITOR_CNT
        , SUM(LOGIN_VISITOR_CNT)                    AS LOGIN_VISITOR_CNT
        , SUM(DUPL_LOGIN_VISITOR_CNT)               AS DUPL_LOGIN_VISITOR_CNT
        , SUM(MEMBER_LOGIN_VISITOR_CNT)             AS MEMBER_LOGIN_VISITOR_CNT
        , SUM(SIMPLE_LOGIN_VISITOR_CNT)             AS SIMPLE_LOGIN_VISITOR_CNT
        , SUM(PVIEW)                                AS PVIEW
        , SUM(MEMBER_PVIEW)                         AS MEMBER_PVIEW
        , SUM(SIMPLE_PVIEW)                         AS SIMPLE_PVIEW
    FROM OPT_TEMP01
    GROUP BY STATIS_DATE, GVHOST, VHOST, PARAM, URL
    """
    //spark.sql(qry).take(100).foreach(println);

    /*

    var qry = ""
    qry =
    s"""
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"'")
    OJDBC.insertTable(spark, objNm)
  }

}
