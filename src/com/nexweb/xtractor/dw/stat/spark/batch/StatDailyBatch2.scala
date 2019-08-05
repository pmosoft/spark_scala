package com.nexweb.xtractor.dw.stat.spark.batch

import org.apache.spark.sql.SparkSession
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCountryStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCityStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitIspStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_IP_INFO
import com.nexweb.xtractor.dw.stat.spark.common.Batch
import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch

/*
 * 유저 접속 정보 통계 배치 수행
 * 사용방법
   StatDailyBatch            : 일자 파라미터가 없는 경우 전일자로 배치 수행
   StatDailyBatch statisDate : 디폴트 전일자 세팅. 그외 해당일자로 배치 수행

import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
StatDailyBatch.setStatDt("20190210")
//StatDailyBatch.setStatDt("20181219")

StatDailyBatch.executeDailyTbIpInfo()
StatDailyBatch.executeDailyVisitCountryStat()
StatDailyBatch.executeDailyVisitCityStat()
StatDailyBatch.executeDailyVisitIspStat()

 *
 * */
object StatDailyBatch2 {

  //val logger = LoggerFactory.getLogger("StatDailyBatch")

  //--------------------------------------
  println("SparkSession 생성");
  //--------------------------------------
  val spark = SparkSession.builder().appName("StatDailyBatch").getOrCreate() //.config("spark.master","local")

  var statisDate = Batch.getStatDt
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
    println("1. 방문(공통)");
    //-------------------------------------------------------------------------
    executeDailyTbIpInfo()              // 지역별                  마트        TB_IP_INFO
    executeDailyVisitCountryStat()      // 지역별                  방문현황    TB_VISIT_COUNTRY_STAT
    executeDailyVisitCityStat()         // 지역별                  방문현황    TB_VISIT_CITY_STAT
    executeDailyVisitIspStat()          // 네트워크별              방문현황    TB_VISIT_ISP_STAT

    //---------------------------------------------------------------------------------------------
    println("배치 종료");
    //---------------------------------------------------------------------------------------------
    spark.stop()
  }

  def setStatDt(statisDate: String) = {
    this.statisDate = statisDate;
  }

  def loadTablesDaily() = {
    //LoadTable.lodBasicLogTables(spark,statisDate,"D")
    //LoadTable.lodOracleAllTables(spark)
    //LoadTable.lodOracleDailyTables(spark,statisDate)
    //

  }

  def executeDailyTbIpInfo()            = { TB_IP_INFO.executeDaily()         } //완료 [성은 이슈 - 메모리 부족]
  def executeDailyVisitCountryStat()    = { VisitCountryStat.executeDaily()   } //완료
  def executeDailyVisitCityStat()       = { VisitCityStat.executeDaily()      } //완료
  def executeDailyVisitIspStat()        = { VisitIspStat.executeDaily()       } //완료

}
