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
  - TB_WL_URL_ACCESS
 * 출    력 : TB_WL_PV_STAT_FRONT
 * 수정내역 :
 * 2018-12-03 | 피승현 | 최초작성
 */
object PvStatFront {

  var spark : SparkSession = null
  var objNm  = "TB_WL_PV_STAT_FRONT"

  var statisDate = ""
  var statisType = ""
  //var objNm  = "TB_WL_PV_STAT_FRONT";var statisDate = "20190303"; var statisType = "D"

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
    //LoadTable.lodAccessTable(spark,statisDate,statisType)
    LoadTable.lodAllColTable(spark,"TB_ACCESS_DAY" ,statisDate,statisType,"",true)
  }

  def excuteSql() = {

/*
    var qry = "" 
    qry =
    s"""
    SELECT
         '${statisDate}'        AS STATIS_DATE
        , GVHOST                AS GVHOST
        , VHOST                 AS VHOST
        , FRONT_URL             AS URL
        , COUNT(1)              AS PVIEW
        , COUNT(DISTINCT V_ID)  AS VISITOR_CNT
        , 0                     AS IS_FIRST_CNT
        , 0                     AS IS_LAST_CNT
        , 0                     AS IS_RELOAD_CNT
        , AVG(DUR_TIME)         AS DUR_TIME
        , 0                     AS MEMBER_VISITOR_CNT
        , 0                     AS MEMBER_PVIEW
        , COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))                       AS SIMPLE_VISITOR_CNT
        , SUM(IF(LOGIN_TYPE=='Z', 1, 0))                                        AS SIMPLE_PVIEW
        , SUM(IF(LOGIN_TYPE=='A', 1, 0))                                        AS NCATE_MEMBER_PVIEW
        , 0                     AS SIMPLE_VISITOR_CNT
        , 0                     AS SIMPLE_PVIEW
        , 0                     AS NCATE_MEMBER_PVIEW

    FROM  TB_ACCESS_DAY
    WHERE FRONT_URL > ' '
    GROUP BY GVHOST, VHOST, URL
    """
*/
/*    
    qry =
    s"""
    SELECT
         '${statisDate}'                                    AS STATIS_DATE
        , GVHOST                                            AS GVHOST
        , VHOST                                             AS VHOST
        , URL                                      		      AS URL
        , COUNT(1)                                          AS PVIEW
        , COUNT(DISTINCT V_ID)                              AS VISITOR_CNT
        , 0                                                 AS IS_FIRST_CNT
        , 0                                                 AS IS_LAST_CNT
        , 0                                                 AS IS_RELOAD_CNT
        , ROUND(AVG(DUR_TIME))                              AS DUR_TIME
        , 0                                                 AS MEMBER_VISITOR_CNT
        , 0                                                 AS MEMBER_PVIEW
        , COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))   AS SIMPLE_VISITOR_CNT
        , SUM(IF(LOGIN_TYPE=='Z', 1, 0))                    AS SIMPLE_PVIEW
        , SUM(IF(LOGIN_TYPE=='A', 1, 0))                    AS NCATE_MEMBER_PVIEW
    FROM  (
		   SELECT
		     GVHOST                                                          AS GVHOST
		   , VHOST                                                           AS VHOST
		   , FRONT_URL                                                       AS URL
		   , V_ID                                                            AS V_ID
		   , U_ID                                                            AS U_ID
		   , CAST(((
					 UNIX_TIMESTAMP(LEAD(C_TIME) OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME),'yyyy-MM-dd HH:mm:ss.SSS')
					 -
					 UNIX_TIMESTAMP(C_TIME,'yyyy-MM-dd HH:mm:ss.SSS')
					 )) AS INTEGER) AS DUR_TIME
		   , LOGIN_TYPE                                                      AS LOGIN_TYPE
		   FROM TB_ACCESS_DAY
	)
    WHERE URL > ' '
    GROUP BY GVHOST, VHOST, URL
    """
*/
    var qry = "" 
    qry =
    s"""
    SELECT
         '${statisDate}'                                                        AS STATIS_DATE
        , GVHOST                                                                AS GVHOST
        , VHOST                                                                 AS VHOST
        , URL                                                                   AS URL
        , COUNT(*)                                                              AS PVIEW
        , COUNT(DISTINCT V_ID)                                                  AS VISITOR_CNT
        , COUNT(CASE WHEN RNUM = 1 THEN V_ID END)                               AS IS_FIRST_CNT
        , COUNT(CASE WHEN RNUM = MAX_RNUM THEN V_ID END)                        AS IS_LAST_CNT
        , COUNT(CASE WHEN URL = BF_URL THEN V_ID END)                           AS IS_RELOAD_CNT
        , ROUND(AVG(DUR_TIME))                                                  AS DUR_TIME
        , COUNT(DISTINCT U_ID)- COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL)) AS MEMBER_VISITOR_CNT
        , SUM(IF(U_ID is NULL,0,1))- SUM(IF(LOGIN_TYPE=='Z',1,0))               AS MEMBER_PVIEW
        , COUNT(DISTINCT IF(LOGIN_TYPE=='Z', U_ID, NULL))                       AS SIMPLE_VISITOR_CNT
        , SUM(IF(LOGIN_TYPE=='Z', 1, 0))                                        AS SIMPLE_PVIEW
        , SUM(IF(LOGIN_TYPE=='A', 1, 0))                                        AS NCATE_MEMBER_PVIEW
    FROM
        (
        SELECT
              GVHOST
            , VHOST
            , URL
            , BF_URL
            , V_ID
            , U_ID
            , DUR_TIME
            , LOGIN_TYPE
            , RNUM
            , MAX(RNUM) OVER(PARTITION BY V_ID) AS MAX_RNUM
        FROM
             (
             SELECT
                   ROW_NUMBER() OVER (ORDER BY C_TIME)                             AS ROWNUM
                 , C_TIME                                                          AS C_TIME
                 , GVHOST                                                          AS GVHOST
                 , VHOST                                                           AS VHOST
                 , URL                                                             AS URL
                 , V_ID                                                            AS V_ID
                 , U_ID                                                            AS U_ID
                 , DUR_TIME                                                        AS DUR_TIME
                 , LOGIN_TYPE                                                      AS LOGIN_TYPE
                 , LAG(URL, 1, 'ZZZ') OVER(PARTITION BY V_ID ORDER BY C_TIME ASC)  AS BF_URL
                 , ROW_NUMBER() OVER(PARTITION BY V_ID ORDER BY C_TIME ASC)        AS RNUM
             FROM  (
			           SELECT
                       C_TIME                                                            AS C_TIME
                       , GVHOST                                                          AS GVHOST
                       , VHOST                                                           AS VHOST
                       , FRONT_URL                                                       AS URL
                       , V_ID                                                            AS V_ID
                       , U_ID                                                            AS U_ID
                       , CAST(((
              					 UNIX_TIMESTAMP(LEAD(C_TIME) OVER(PARTITION BY GVHOST, SESSION_ID ORDER BY C_TIME),'yyyy-MM-dd HH:mm:ss.SSS')
              					 -
              					 UNIX_TIMESTAMP(C_TIME,'yyyy-MM-dd HH:mm:ss.SSS')
              					 )) AS INTEGER) AS DUR_TIME
                       , LOGIN_TYPE                                                      AS LOGIN_TYPE
        			   FROM TB_ACCESS_DAY
        			 ) A
           ) A
           WHERE URL IS NOT NULL
        ) A
    WHERE LENGTH(URL) > 0
    GROUP BY STATIS_DATE, GVHOST, VHOST, URL
    """
    
    //spark.sql(qry).take(100).foreach(println);
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
