package com.nexweb.xtractor.dw.stat.spark.batch

import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.RevisitLoginStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitPageViewStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCountryStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.RevisitIntervalStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateLogUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitDurTimeStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.PvStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.SegStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxUrlDetailStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCityStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.CateUrlStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitTwStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.ActFlowStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitBrowserStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.UrlPrdStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxOuterKwStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitVgaStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.CateStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.OptParamStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxPathRefStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitOsStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitLangStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateLogFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitOsVerStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_MEMBER_CLASS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsCodeStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.PvStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitIspStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxDomainDetailStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxPathStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitBrowserVerStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.PvStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitorStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.CatePathStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxUrlUrlStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.PageStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.ProdStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_VISIT_INTERVAL
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitorStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.RefererKeywordStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitorStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitTimeStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.UrlPathStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitDeviceStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.UrlRefStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.RevisitDurStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_IP_INFO
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitDayStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxPageStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCntStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.ProdLog
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InnerKwStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitEquipStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.RefererStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.EngLoginUserStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxDomainUrlStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.RevisitStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.InfluxKwPageStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.AreaStat
import org.apache.spark.sql.SparkSession
import com.nexweb.xtractor.dw.stat.spark.common.Batch
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.SimpleLoginStat


/*
 * 유저 접속 정보 통계 배치 수행
 * 사용방법
   StatDailyBatch            : 일자 파라미터가 없는 경우 전일자로 배치 수행
   StatDailyBatch statisDate : 디폴트 전일자 세팅. 그외 해당일자로 배치 수행

import com.nexweb.xtractor.dw.stat.spark.batch.StatDailyBatch
StatDailyBatch.setStatDt("20190214")

StatDailyBatch.executeTbRefererSession()
StatDailyBatch.executeTbAccessSession()
StatDailyBatch.executeTbMemberSession()

// 방문
StatDailyBatch.executeDailyVisitStat()
StatDailyBatch.executeDailyVisitTimeStat()
//StatDailyBatch.executeDailyVisitDayStat()
StatDailyBatch.executeDailyVisitCntStat()
StatDailyBatch.executeDailyVisitDurTimeStat()
StatDailyBatch.executeDailyVisitPageViewStat()
StatDailyBatch.executeDailyRevisitStat()
StatDailyBatch.executeDailyRevisitLoginStat()
StatDailyBatch.executeTbVisitInternal()
StatDailyBatch.executeDailyRevisitIntervalStat()
StatDailyBatch.executeDailyVisitOsStat()
StatDailyBatch.executeDailyVisitOsVerStat()
StatDailyBatch.executeDailyVisitBrowserStat()
StatDailyBatch.executeDailyVisitBrowserVerStat()
StatDailyBatch.executeDailyVisitEquipStat()
StatDailyBatch.executeDailyVisitDeviceStat()
StatDailyBatch.executeDailyVisitLangStat()
StatDailyBatch.executeDailyVisitVgaStat()

StatDailyBatch.executeDailyTbIpInfo()
StatDailyBatch.executeDailyVisitCountryStat()
StatDailyBatch.executeDailyVisitCityStat()
StatDailyBatch.executeDailyVisitIspStat()

// 트래픽
StatDailyBatch.executeDailyUrlPrdStat()
StatDailyBatch.executeDailyPageStat()
StatDailyBatch.executeDailyCateStat()
StatDailyBatch.executeDailyCateUrlStat()
StatDailyBatch.executeDailyActFlowStat()
StatDailyBatch.executeDailyInnerKwStat()
StatDailyBatch.executeDailyUrlRefStat()
StatDailyBatch.executeDailyAreaStat()
StatDailyBatch.executeDailyUrlPathStat()
StatDailyBatch.executeDailyCatePathStat()

// 유입
StatDailyBatch.executeDailyInfluxPathStat()
StatDailyBatch.executeDailyInfluxPathRefStat()
StatDailyBatch.executeDailyInfluxPageStat()
StatDailyBatch.executeDailyInfluxDomainDetailStat()
StatDailyBatch.executeDailyInfluxDomainUrlStat()
StatDailyBatch.executeDailyInfluxUrlDomainStat()
StatDailyBatch.executeDailyInfluxUrlUrlStat()
StatDailyBatch.executeDailyInfluxOuterKwStat()

// 티월드
StatDailyBatch.executeDailyPvStat()
StatDailyBatch.executeDailyVisitorStat()
StatDailyBatch.executeDailyRevisitDurStat()
StatDailyBatch.executeDailyRefererStat()
StatDailyBatch.executeDailyRefererKeywordStat()
StatDailyBatch.executeDailyEngLoginUserStat()
StatDailyBatch.executeDailySegStat()
StatDailyBatch.executeDailyProdLog()
StatDailyBatch.executeDailyProdStat()
StatDailyBatch.executeDailyNCateLogFront()
StatDailyBatch.executeDailyPvStatFront()
StatDailyBatch.executeDailyVisitorStatFront()
StatDailyBatch.executeDailyNCateURLStatFront()
StatDailyBatch.executeDailyNCateStatFront()
StatDailyBatch.executeDailyVisitStatFront()
StatDailyBatch.executeDailyNCateLogUser()
StatDailyBatch.executeDailyPvStatUser()
StatDailyBatch.executeDailyVisitorStatUser()
StatDailyBatch.executeDailyNCateURLStatUser()
StatDailyBatch.executeDailyNCateStatUser()
StatDailyBatch.executeDailyOptParamStat()
StatDailyBatch.executeDailyCsCodeStat()

선행마트 생성

TB_WL_URL_ACCESS_SESSION
TB_WL_REFERER_SESSION

spark.sql("SELECT COUNT(*) FROM TB_WL_URL_ACCESS").take(100).foreach(println);
spark.sql("SELECT * FROM TB_WL_URL_ACCESS").take(100).foreach(println);
spark.sql("SELECT COUNT(*) FROM TB_WL_REFERER").take(100).foreach(println);

spark.sql("SELECT GVHOST,COUNT(*) FROM TB_WL_REFERER GROUP BY GVHOST").take(100).foreach(println);
spark.sql("SELECT GVHOST,COUNT(*) FROM TB_WL_URL_ACCESS GROUP BY GVHOST").take(100).foreach(println);
spark.sql("SELECT * FROM TB_WL_REFERER").take(100).foreach(println);

spark.sql("SELECT * FROM TB_WL_REFERER WHERE GVHOST = 'TMAP'").take(100).foreach(println);
spark.sql("SELECT DISTINCT CATEGORY FROM TB_WL_REFERER WHERE GVHOST = 'TMAP'").take(100).foreach(println);
spark.sql("SELECT GVHOST,CATEGORY,COUNT(*) FROM TB_WL_REFERER GROUP BY GVHOST,CATEGORY").take(100).foreach(println);

spark.sql("""
SELECT A.*
FROM   TB_WL_REFERER A
     , TB_WL_URL_ACCESS B
WHERE  A.GVHOST = B.GVHOST
AND    A.V_ID   = B.V_ID
""").take(100).foreach(println);

var statisDate = "20190130";var statisType = "D"

spark.sql("DROP TABLE TB_WL_URL_ACCESS")
spark.sql("DROP TABLE TB_WL_REFERER")
spark.sql("DROP TABLE TB_MEMBER_CLASS")

LoadTable.lodRefererTable(spark,statisDate,statisType)
LoadTable.lodAccessTable(spark,statisDate,statisType)
LoadTable.lodMemberTable(spark,statisDate,statisType)

 *
 * */
object StatDailyBatch4 {

  //val logger = LoggerFactory.getLogger("StatDailyBatch")

  //--------------------------------------
  println("SparkSession 생성");
  //--------------------------------------
  val spark = SparkSession.builder().appName("StatDailyBatch").getOrCreate() //.config("spark.master","local")

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
    StatDailyBatch.setStatDt(statisDate)


    //---------------------------------------------------------------------------------------------
    println("배치 시작 : " + this.statisDate);
    //---------------------------------------------------------------------------------------------
    //val statisDate = Batch.getStatisDate
    //statisDate = "20181028";this.statisDate = "20181028"
    setStatDt(statisDate)
    

    //-------------------------------------------------------------------------
    println("3. 유입(공통)");
    //-------------------------------------------------------------------------
    executeDailyInfluxPathStat()        // 유입경로                            TB_INFLUX_PATH_STAT
    executeDailyInfluxPathRefStat()     // 유입경로 상세                       TB_INFLUX_PATH_REF_STAT
    executeDailyInfluxPageStat()        // 유입경로 팝업                       TB_INFLUX_PAGE_STAT
    executeDailyInfluxDomainDetailStat()// 유입경로 도메인                     TB_INFLUX_DOMAIN_DETAIL_STAT
    executeDailyInfluxDomainUrlStat()   // 유입경로 도메인 상세                TB_INFLUX_DOMAIN_URL_STAT
    executeDailyInfluxUrlDetailStat()   // 유입경로 URL                        TB_INFLUX_URL_DETAIL_STAT
    executeDailyInfluxUrlUrlStat()      // 유입경로 URL 상세                   TB_INFLUX_URL_URL_STAT
    executeDailyInfluxOuterKwStat()     // 자연검색어                          TB_INFLUX_OUTER_KW_STAT
    executeDailyInfluxKwPageStat()      // 자연검색어 상세                     TB_INFLUX_KW_PAGE_STAT
                                        // 사용자정의 유입경로                                              [화면존재]
                                        // 캠페인효과 분석                                                  [화면존재]
                                        // 검색어/검색엔진별 방문경로                                       [화면존재]

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

  def executeDailyInfluxPathStat()         = { InfluxPathStat.executeDaily()         } //완료
  def executeDailyInfluxPathRefStat()      = { InfluxPathRefStat.executeDaily()      } //완료 
  def executeDailyInfluxPageStat()         = { InfluxPageStat.executeDaily()         } //완료
  def executeDailyInfluxDomainDetailStat() = { InfluxDomainDetailStat.executeDaily() } //완료 
  def executeDailyInfluxDomainUrlStat()    = { InfluxDomainUrlStat.executeDaily()    } //완료
  def executeDailyInfluxUrlDetailStat()    = { InfluxUrlDetailStat.executeDaily()    } //완료 
  def executeDailyInfluxUrlUrlStat()       = { InfluxUrlUrlStat.executeDaily()       } //완료 
  def executeDailyInfluxOuterKwStat()      = { InfluxOuterKwStat.executeDaily()      } //완료 [키워드 통계 안맞음]
  def executeDailyInfluxKwPageStat()       = { InfluxKwPageStat.executeDaily()       } //완료 [키워드 통계 안맞음]

}