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
 * 설    명 : 일/월별
 * 입    력 :
  - TB_CS_CODE_PROCESS
  - TB_CS_CODE_INFO
  - TB_CS_LAST_VER_FRONT
 * 출    력 : - TB_CS_CODE_STAT
 * 수정내역 :
 * 2018-12-10 | 피승현 | 최초작성
 */
object CsCodeStat {

  var spark : SparkSession = null
  var objNm  = "TB_CS_CODE_STAT"
  var obj2Nm = "CS_LOG"

  var statisDate = ""
  var statisType = ""
  var statisDate2 = ""
  var statisType2 = ""
  //var objNm  = "TB_CS_CODE_STAT";var obj2Nm = "CS_LOG";var statisDate = "20190509"; var statisType = "M"; 
  //var objNm  = "TB_CS_CODE_STAT"; var prevYyyymmDt = "201904";var statisDate = "201904"; var statisType = "M"; var statisDate2 = "20190430"; var statisType2 = "D" 

  def executeDaily() = {
    //------------------------------------------------------
        println(objNm+".executeDaily() 일배치 시작");
    //------------------------------------------------------
    spark  = StatDailyBatch.spark
    statisDate = StatDailyBatch.statisDate
    statisType = "D"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def executeMonthly() = {
    //------------------------------------------------------
        println(objNm+".executeMonthly() 일배치 시작");
    //------------------------------------------------------
    spark  = StatMonthlyBatch.spark
    statisDate = StatMonthlyBatch.prevYyyymmDt
    statisType = "M"
    statisDate2 = StatDailyBatch.statisDate
    statisType2 = "D"
    loadTables();excuteSql();saveToParqeut();ettToOracle()
  }

  def loadTables() = {
    LoadTable.lodProcessTable(spark, statisDate, statisType)
    LoadTable.lodAllColTable(spark,"TB_CS_CODE_INFO",statisDate2,statisType2,"",true)
  }

  def excuteSql() = {

    var qry = ""
    qry =
    s"""
    SELECT
           '${statisDate}'             AS VERSION_ID
        , TA.GVHOST                    AS GVHOST
        , TA.U_ID                      AS L_ID
        , TA.V_ID                      AS V_ID
        , TA.E_ID                      AS E_ID
        , TA.T_ID                      AS LOGIN_ID
        , TA.SVC_UPDATE_ID             AS SVC_UPDATE_ID
        , NVL(TA.ACTION, TB.ACTION)    AS ACTION
        , TA.OS                        AS OS_CODE
        , TB.SS_CODE                   AS SS_CODE
        , TB.MENU_CODE                 AS MENU_CODE
        , TB.TARGET_CODE               AS TARGET_CODE
    FROM
        TB_CS_CODE_PROCESS TA,
        TB_CS_CODE_INFO TB
    WHERE TA.E_ID = TB.E_ID
    AND TA.GVHOST IS NOT NULL
    """
    
    //spark.sql(qry).take(100).foreach(println);
    
    //--------------------------------------
        println(qry);
    //--------------------------------------
    val obj2Df = spark.sql(qry)
    obj2Df.cache.createOrReplaceTempView(obj2Nm);obj2Df.count()

    qry =
    s"""
    SELECT
         '${statisType}'            AS STATIS_TYPE
        ,'${statisDate}'            AS STATIS_DATE
        , GVHOST                    AS GVHOST
        ,'${statisDate2}'           AS VERSION_ID
        , E_ID                      AS E_ID
        , OS_CODE                   AS OS_CODE
        , SS_CODE                   AS SS_CODE
        , MENU_CODE                 AS MENU_CODE
        , TARGET_CODE               AS TARGET_CODE
        , VID_BV_UV                 AS VID_BV_UV
        , VID_BC_UV                 AS VID_BC_UV
        , LOGINID_BV_UV             AS LOGINID_BV_UV
        , LOGINID_BC_UV             AS LOGINID_BC_UV
        , LID_BV_UV                 AS LID_BV_UV
        , LID_BC_UV                 AS LID_BC_UV
        , UID_BV_UV                 AS UID_BV_UV
        , UID_BC_UV                 AS UID_BC_UV
        , BV_PV                     AS BV_PV
        , BC_PV                     AS BC_PV
    FROM
        (
        /* 전체 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , 'TOTAL'                                               AS OS_CODE
            , 'TOTAL'                                               AS SS_CODE
            , 'TOTAL'                                               AS MENU_CODE
            , 'TOTAL'                                               AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG
        GROUP BY GVHOST
        UNION  ALL
        /* 운영체제 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , OS_CODE                                               AS OS_CODE
            , 'TOTAL'                                               AS SS_CODE
            , 'TOTAL'                                               AS MENU_CODE
            , 'TOTAL'                                               AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG TA
        GROUP BY GVHOST, OS_CODE
        UNION  ALL
        /* 메뉴 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , OS_CODE                                               AS OS_CODE
            , 'NONE'                                                AS SS_CODE
            , MENU_CODE                                             AS MENU_CODE
            , 'TOTAL'                                               AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG
        GROUP BY GVHOST, OS_CODE, MENU_CODE
        UNION  ALL
        /* 소재 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , OS_CODE                                               AS OS_CODE
            , SS_CODE                                               AS SS_CODE
            , 'NONE'                                                AS MENU_CODE
            , 'TOTAL'                                               AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG
        GROUP BY GVHOST, OS_CODE, SS_CODE
        UNION  ALL
        /* 메뉴-타겟 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , OS_CODE                                               AS OS_CODE
            , 'NONE'                                                AS SS_CODE
            , MENU_CODE                                             AS MENU_CODE
            , TARGET_CODE                                           AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG TA
        GROUP BY GVHOST, OS_CODE, MENU_CODE, TARGET_CODE
        UNION  ALL
        /* 소재-타겟 */
        SELECT
              GVHOST                                                AS GVHOST
            , 'TOTAL'                                               AS E_ID
            , OS_CODE                                               AS OS_CODE
            , SS_CODE                                               AS SS_CODE
            , 'NONE'                                                AS MENU_CODE
            , TARGET_CODE                                           AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG TA
        GROUP BY GVHOST, OS_CODE, SS_CODE, TARGET_CODE
        UNION  ALL
        /* 메뉴-오퍼코드 */
        SELECT
              GVHOST                                                AS GVHOST
            , E_ID                                                  AS E_ID
            , OS_CODE                                               AS OS_CODE
            , 'NONE'                                                AS SS_CODE
            , MENU_CODE                                             AS MENU_CODE
            , TARGET_CODE                                           AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG TA
        GROUP BY GVHOST, E_ID, OS_CODE, MENU_CODE, TARGET_CODE
        UNION  ALL
        /* 소재-오퍼코드 */
        SELECT
              GVHOST                                                AS GVHOST
            , E_ID                                                  AS E_ID
            , OS_CODE                                               AS OS_CODE
            , SS_CODE                                               AS SS_CODE
            , 'NONE'                                                AS MENU_CODE
            , TARGET_CODE                                           AS TARGET_CODE
            , COUNT(DISTINCT IF(ACTION='bv', V_ID, NULL))           AS VID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', V_ID, NULL))           AS VID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', LOGIN_ID, NULL))       AS LOGINID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', LOGIN_ID, NULL))       AS LOGINID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', L_ID, NULL))           AS LID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', L_ID, NULL))           AS LID_BC_UV
            , COUNT(DISTINCT IF(ACTION='bv', SVC_UPDATE_ID, NULL))  AS UID_BV_UV
            , COUNT(DISTINCT IF(ACTION='bc', SVC_UPDATE_ID, NULL))  AS UID_BC_UV
            , SUM(IF(ACTION='bv', 1,0))                             AS BV_PV
            , SUM(IF(ACTION='bc', 1,0))                             AS BC_PV
        FROM  CS_LOG TA
        GROUP BY GVHOST, E_ID, OS_CODE, SS_CODE, TARGET_CODE
        ) TA
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
    OJDBC.deleteTable(spark, "DELETE FROM "+ objNm + " WHERE STATIS_DATE='"+statisDate+"' AND STATIS_TYPE='"+statisType+"'")
    OJDBC.insertTable(spark, objNm)
    //OJDBC.selectCountTable(spark, objNm, statisDate);
  }

}
