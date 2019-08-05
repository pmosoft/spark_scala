package com.nexweb.xtractor.dw.stat.spark.batch

import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_MEMBER_CLASS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_VISIT_INTERVAL
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_IP_INFO
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_SNS_HISTORY_DAY
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mbrs.SnsStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mbrs.OsStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mbrs.PvUvStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mbrs.XtosStat
import org.apache.spark.sql.SparkSession
import com.nexweb.xtractor.dw.stat.spark.common.Batch



/*
 * 유저 접속 정보 통계 배치 수행
 * 사용방법
   StatDailyBatch            : 일자 파라미터가 없는 경우 전일자로 배치 수행
   StatDailyBatch statisDate : 디폴트 전일자 세팅. 그외 해당일자로 배치 수행
 *
 * */
object StatDailyBatch6 {

  //val logger = LoggerFactory.getLogger("StatDailyBatch")

  //--------------------------------------
  println("SparkSession 생성");
  //--------------------------------------
  val spark = SparkSession.builder().appName("StatDailyBatch").getOrCreate() //.config("spark.master","local")

  //var statisDate = "20190308"
  var statisDate = Batch.getStatDt
  var preStatisDate = Batch.getPreStatDt

  //var statisDate = "20181219"
  //val spark = SparkSession.builder().getOrCreate()

  def main(args: Array[String]): Unit = {

    //--------------------------------------
    println("입력파라미터 처리");
    //--------------------------------------
    //val arg1 = if (args.length == 1) args(0)
    //val statisDate = Batch.getStatDt else arg1
    val statisDate = if (args.length < 1) Batch.getStatDt else args(0)
    StatDailyBatch.setStatDt(statisDate);

    //---------------------------------------------------------------------------------------------
    println("배치 시작 : " + this.statisDate);
    //---------------------------------------------------------------------------------------------
    //val statisDate = Batch.getStatisDate
    //statisDate = "20181028";this.statisDate = "20181028"
    setStatDt(statisDate)
    
    //-------------------------------------------------------------------------
    println("6. MEMBERSHIP");
    //-------------------------------------------------------------------------
    executeTbSnsDay()
    
    executeDailySnsStat()            // 일별                                   POC_SNS_STAT
    executeDailyOsStat()             // 일별                                   POC_OS_STAT
    executeDailyPvUvStat()           // 일별                                   POC_PVUV_STAT
    executeDailyXtosStat()           // 일별                                   POC_XTOS_STAT
    
    //---------------------------------------------------------------------------------------------
    println("배치 종료");
    //---------------------------------------------------------------------------------------------
    spark.stop()
  }

  def setStatDt(statisDate: String) = {
    this.statisDate = statisDate;
    //this.preStatisDate = Batch.getSubDt(spark,statisDate,1)
  }

  def loadTablesDaily() = {
    //LoadTable.lodBasicLogTables(spark,statisDate,"D")
    //LoadTable.lodOracleAllTables(spark)
    //LoadTable.lodOracleDailyTables(spark,statisDate)
  }
  
  def executeTbSnsDay()                 = { TB_SNS_HISTORY_DAY.executeDaily()  } //완료

  def executeDailySnsStat()             = { SnsStat.executeDaily()             } //완료
  def executeDailyOsStat()              = { OsStat.executeDaily()              } //완료
  def executeDailyPvUvStat()            = { PvUvStat.executeDaily()            } //완료
  def executeDailyXtosStat()            = { XtosStat.executeDaily()            } //완료
     
}