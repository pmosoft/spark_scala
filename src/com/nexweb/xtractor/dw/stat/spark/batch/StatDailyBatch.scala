package com.nexweb.xtractor.dw.stat.spark.batch

import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_SESSION2
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_SESSION3
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_SESSION2
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_SESSION3
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_MEMBER_CLASS_SESSION
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_REFERER_DAY
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_ACCESS_DAY
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_MEMBER_CLASS_DAY
import com.nexweb.xtractor.dw.stat.spark.batch.sql.mart.TB_SNS_HISTORY_DAY

import com.nexweb.xtractor.dw.stat.spark.batch.sql.RevisitLoginStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitPageViewStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitCountryStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.RevisitIntervalStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateLogUser
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
object StatDailyBatch {

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

    //---------------------------------------------------------------------------------------------
    println("배치 시작 : " + this.statisDate);
    //---------------------------------------------------------------------------------------------
    //val statisDate = Batch.getStatisDate
    //statisDate = "20181028";this.statisDate = "20181028"
    setStatDt(statisDate)

    //-------------------------------------------------------------------------
    println("0. 당일 로그 및 오라클 테이블 로딩");
    //-------------------------------------------------------------------------
    //loadTablesDaily()

    //-------------------------------------------------------------------------
    println("0. 세션마트");
    //-------------------------------------------------------------------------
    executeTbRefererSession()
    executeTbRefererSession2()
    executeTbRefererSession3()
    executeTbAccessSession()
    executeTbAccessSession2()
    executeTbAccessSession3()
    executeTbMemberSession()
    executeTbRefererDay()
    executeTbAccessDay()
    executeTbMemberDay()
    

    //-------------------------------------------------------------------------
    println("1. 방문(공통)");
    //-------------------------------------------------------------------------
    executeDailyVisitStat()             // 기간별                  방문현황    TB_VISIT_STAT
    executeDailyVisitTimeStat()         // 시간대별                방문현황    TB_VISIT_TIME_STAT
                                        // 요일별                  방문현황    TB_VISIT_WEEK_STAT           
    //executeDailyVisitDayStat()        // 방문일수별              방문현황    TB_VISIT_DAY_STAT           [월통계]
    executeDailyVisitCntStat()          // 방문횟수별              방문현황    TB_VISIT_CNT_STAT
    executeDailyVisitDurTimeStat()      // 체류시간별              방문현황    TB_VISIT_DUR_TIME_STAT
    executeDailyVisitPageViewStat()     // 순페이지별              방문현황    TB_VISIT_PAGEVIEW_STAT
    executeDailyRevisitStat()           // 신규/재                 방문현황    TB_REVISIT_STAT
    executeDailyRevisitLoginStat()      // 신규/재로그인           방문현황    TB_REVISIT_LOGIN_STAT
    executeTbVisitInternal()            // 재방문간격              마트        TB_VISIT_INTERNAL
    executeDailyRevisitIntervalStat()   // 재방문간격별            방문현황    TB_REVISIT_INTERVAL_STAT
                                        // 재방문일수              방문현황    TB_REVISIT_MONTH_STAT        [개발대상][Tworld]
    executeDailyVisitOsStat()           // 운영체제별              방문현황    TB_VISIT_OS_STAT
    executeDailyVisitOsVerStat()        // 운영체제버젼별          방문현황    TB_VISIT_OS_VER_STAT
    executeDailyVisitBrowserStat()      // 브라우저별              방문현황    TB_VISIT_BROWSER_STAT
    executeDailyVisitBrowserVerStat()   // 브라우저버젼별          방문현황    TB_VISIT_BROWSER_VER_STAT
    executeDailyVisitVgaStat()          // 화면해상도별            방문현황    TB_VISIT_VGA_STAT
    executeDailyVisitEquipStat()        // 기기유형별              방문현황    TB_VISIT_EQUIP_STAT
    executeDailyVisitDeviceStat()       // 휴대기기별              방문현황    TB_VISIT_DEVICE_STAT
    executeDailyVisitLangStat()         // 언어별                  방문현황    TB_VISIT_LANG_STAT

    //executeDailyTbIpInfo()            // 지역별                  마트        TB_IP_INFO
    //executeDailyVisitCountryStat()    // 지역별                  방문현황    TB_VISIT_COUNTRY_STAT
    //executeDailyVisitCityStat()       // 지역별                  방문현황    TB_VISIT_CITY_STAT
    //executeDailyVisitIspStat()        // 네트워크별              방문현황    TB_VISIT_ISP_STAT
    //                                  // 방문현황분석                        TB_VISIT_STAT

    //-------------------------------------------------------------------------
    println("2. 트래픽(공통)");
    //-------------------------------------------------------------------------
    //executeDailyUrlPrdStat()            // 기간별                  트래픽현황  TB_URL_PRD_STAT
//    executeDailyPageStat()              // 페이지별                트래픽현황  TB_PAGE_STAT
//                                        // 시간대별                트랙픽현황  TB_URL_TIME_STAT             [개발대상][티월드]
//                                        // 요일별                  트랙픽현황  TB_URL_WEEK_STAT             [삭제대상]
//    executeDailyCateStat()              // 사용자정의 메뉴별       트랙픽현황  TB_CATE_STAT
//    executeDailyCateUrlStat()           // 사용자정의 메뉴별(팝업) 트랙픽현황  TB_CATE_URL_STAT
//                                        // 방문페이지별            트랙픽현황  TB_PAGE_STAT
//                                        // 종료페이지별            트랙픽현황  TB_PAGE_STAT
//                                        // 재요청페이지            트랙픽현황  TB_PAGE_STAT
//    executeDailyActFlowStat()           // 행동흐름                            TB_ACT_FLOW_STAT             [화면미맵핑]
//    executeDailyInnerKwStat()           // 내부검색어              트랙픽현황  TB_INNER_KW_STAT
//    executeDailyUrlRefStat()            // 페이지별 현황(상세보기)             TB_URL_REF_STAT
//    executeDailyAreaStat()              // 영역 클릭 분석                      TB_AREA_STAT
//    executeDailyUrlPathStat()           // 페이지별 이동경로                   TB_URL_PATH_STAT
//    executeDailyCatePathStat()          // 메뉴별 이동경로                     TB_CATE_PATH_STAT
//
//                                        // 카테고리별 페이지       트랙픽현황                               [화면존재]
//                                        // 프로세스 분석 (단계별 인입 통계)    ST_PROC_STEP_IN_STAT
//                                        // 프로세스 분석 (단계별 인입 리스트)  ST_PROC_STEP_IN_LIST
//                                        // 프로세스 분석 (단계별 이탈 통계)    ST_PROC_STEP_OUT_STAT
//                                        // 프로세스 분석 (단계별 이탈 리스트)  ST_PROC_STEP_OUT_LIST
//                                        // 프로세스 분석 (최초 인입 통계)      ST_PROC_STEP_REF_STAT
//                                        // 프로세스 분석 (최초 인입 리스트)    ST_PROC_STEP_REF_LIST

    //-------------------------------------------------------------------------
    println("3. 유입(공통)");
    //-------------------------------------------------------------------------
//    executeDailyInfluxPathStat()        // 유입경로                            TB_INFLUX_PATH_STAT
//    executeDailyInfluxPathRefStat()     // 유입경로 상세                       TB_INFLUX_PATH_REF_STAT
//    executeDailyInfluxPageStat()        // 유입경로 팝업                       TB_INFLUX_PAGE_STAT
//    executeDailyInfluxDomainDetailStat()// 유입경로 도메인                     TB_INFLUX_DOMAIN_DETAIL_STAT
//    executeDailyInfluxDomainUrlStat()   // 유입경로 도메인 상세                TB_INFLUX_DOMAIN_URL_STAT
//    executeDailyInfluxUrlDetailStat()   // 유입경로 URL                        TB_INFLUX_URL_DETAIL_STAT
//    executeDailyInfluxUrlUrlStat()      // 유입경로 URL 상세                   TB_INFLUX_URL_URL_STAT
//    executeDailyInfluxOuterKwStat()     // 자연검색어                          TB_INFLUX_OUTER_KW_STAT
//    executeDailyInfluxKwPageStat()      // 자연검색어 상세                     TB_INFLUX_KW_PAGE_STAT
//                                        // 사용자정의 유입경로                                              [화면존재]
//                                        // 캠페인효과 분석                                                  [화면존재]
//                                        // 검색어/검색엔진별 방문경로                                       [화면존재]

    //-------------------------------------------------------------------------
    println("4. TWORLD");
    //-------------------------------------------------------------------------
//    executeDailyPvStat()              // 일별    페이지뷰 통계               TB_WL_PV_STAT
//    executeDailyVisitorStat()         // 일별    방문자 통계                 TB_WL_VISITOR_STAT
//    executeDailyRevisitDurStat()      // 일별    재방문자 통계               TB_WL_REVISIT_DUR_STAT
//    executeDailyRefererStat()         // 일별    유입 통계                   TB_WL_REFERER_STAT
//    executeDailyRefererKeywordStat()  // 일별    유입 키워드 통계            TB_WL_REFERER_KEYWORD_STAT
//    executeDailyEngLoginUserStat()    // 일/월별 영문 로그인 통계            TB_ENG_LOGIN_USER_STAT
//    executeDailySegStat()             // 일/월별 메뉴별 방문자 통계          TB_SEG_STAT
//    executeDailyProdLog()             // 일별                                TB_PROD_LOG
//    executeDailyProdStat()            // 일/월별                             TB_PROD_STAT
//    executeDailyNCateLogFront()       // 일별                                TB_NCATE_LOG_FRONT
//    executeDailyPvStatFront()         // 일별                                TB_WL_PV_STAT_FRONT
//    executeDailyVisitorStatFront()    // 일별                                TB_WL_VISITOR_STAT_FRONT
//    executeDailyNCateURLStatFront()   // 일/월별                             TB_NCATE_URL_STAT_FRONT
//    executeDailyNCateStatFront()      // 일/월별                             TB_NCATE_STAT_FRONT
//    executeDailyVisitStatFront()      // 일/월별                             TB_VISIT_STAT_FRONT
    //executeDailyNCateLogUser()        // 일별                                TB_NCATE_LOG_USER (2019.02.13 오라클 USER MAP 정보 미존재)
//    executeDailyPvStatUser()          // 일별                                TB_WL_PV_STAT_USER
//    executeDailyVisitorStatUser()     // 일별                                TB_WL_VISITOR_STAT_USER
    //executeDailyNCateURLStatUser()    // 일/월별                             TB_NCATE_URL_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)
    //executeDailyNCateStatUser()       // 일/월별                             TB_NCATE_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)
//    executeDailyOptParamStat()        // 일별                                TB_OPT_PARAM_STAT
    //executeDailyCsCodeStat()          // 일/월별                             TB_CS_CODE_STAT (2019.02.13 오라클 CODE_INFO 정보 미존재)

//    executeDailyVisitTwStat()         //                                     TB_VISIT_TW_STAT
                                      //                                     TB_MEMBER_NOT_VID_CNT(소스 코드 미존재)
    //executeDailySimpleLoginStat()                                          TB_SIMPLE_LOGIN_STAT (2019.02.15 소스코드 정비후 실행요)
                                      //자동로그인 UV                         TB_MEMBER_ETC_VIEW(소스 미발견)
                                      //Tworld 통합 방문자수                  TB_TWS_VISIT_STAT(소스 미발견)
                                      //Tworld 통합 방문자수                  TB_TWS_VISIT_STAT(소스 미발견)


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

  def executeTbRefererSession()            = { TB_REFERER_SESSION.executeDaily()     } //완료
  def executeTbRefererSession2()           = { TB_REFERER_SESSION2.executeDaily()    } //완료
  def executeTbRefererSession3()           = { TB_REFERER_SESSION3.executeDaily()    } //완료
  def executeTbAccessSession()             = { TB_ACCESS_SESSION.executeDaily()      } //완료
  def executeTbAccessSession2()            = { TB_ACCESS_SESSION2.executeDaily()     } //완료
  def executeTbAccessSession3()            = { TB_ACCESS_SESSION3.executeDaily()     } //완료
  def executeTbMemberSession()             = { TB_MEMBER_CLASS_SESSION.executeDaily()} //완료
  def executeTbRefererDay()                = { TB_REFERER_DAY.executeDaily()         } //완료
  def executeTbAccessDay()                 = { TB_ACCESS_DAY.executeDaily()          } //완료
  def executeTbMemberDay()                 = { TB_MEMBER_CLASS_DAY.executeDaily()    } //완료  

  def executeDailyVisitStat()              = { VisitStat.executeDaily()              } //완료
  def executeDailyVisitTimeStat()          = { VisitTimeStat.executeDaily()          } //완료
  def executeDailyVisitDayStat()           = { VisitDayStat.executeDaily()           } //[월통계] -4/1 확인
  def executeDailyVisitCntStat()           = { VisitCntStat.executeDaily()           } //완료
  def executeDailyVisitDurTimeStat()       = { VisitDurTimeStat.executeDaily()       } //완료
  def executeDailyVisitPageViewStat()      = { VisitPageViewStat.executeDaily()      } //완료 
  def executeDailyRevisitStat()            = { RevisitStat.executeDaily()            } //보류!!!!!!!!!!!!!!!!!
  def executeDailyRevisitLoginStat()       = { RevisitLoginStat.executeDaily()       } //보류!!!!!!!!!!!!!!!!!
  def executeTbVisitInternal()             = { TB_VISIT_INTERVAL.executeDaily()      } //보류!!!!!!!!!!!!!!!!!
  def executeDailyRevisitIntervalStat()    = { RevisitIntervalStat.executeDaily()    } //보류!!!!!!!!!!!!!!!!!
  def executeDailyVisitBrowserStat()       = { VisitBrowserStat.executeDaily()       } //완료
  def executeDailyVisitBrowserVerStat()    = { VisitBrowserVerStat.executeDaily()    } //완료
  def executeDailyVisitOsStat()            = { VisitOsStat.executeDaily()            } //완료
  def executeDailyVisitOsVerStat()         = { VisitOsVerStat.executeDaily()         } //완료
  def executeDailyVisitVgaStat()           = { VisitVgaStat.executeDaily()           } //완료
  def executeDailyVisitEquipStat()         = { VisitEquipStat.executeDaily()         } //완료
  def executeDailyVisitDeviceStat()        = { VisitDeviceStat.executeDaily()        } //완료
  def executeDailyVisitLangStat()          = { VisitLangStat.executeDaily()          } //완료

  def executeDailyTbIpInfo()               = { TB_IP_INFO.executeDaily()             }
  def executeDailyVisitCountryStat()       = { VisitCountryStat.executeDaily()       }
  def executeDailyVisitCityStat()          = { VisitCityStat.executeDaily()          }
  def executeDailyVisitIspStat()           = { VisitIspStat.executeDaily()           }

  def executeDailyUrlPrdStat()             = { UrlPrdStat.executeDaily()             }
  def executeDailyPageStat()               = { PageStat.executeDaily()               }
  def executeDailyCateStat()               = { CateStat.executeDaily()               }
  def executeDailyCateUrlStat()            = { CateUrlStat.executeDaily()            }
  def executeDailyActFlowStat()            = { ActFlowStat.executeDaily()            }
  def executeDailyInnerKwStat()            = { InnerKwStat.executeDaily()            }
  def executeDailyUrlRefStat()             = { UrlRefStat.executeDaily()             }
  def executeDailyAreaStat()               = { AreaStat.executeDaily()               }
  def executeDailyUrlPathStat()            = { UrlPathStat.executeDaily()            }
  def executeDailyCatePathStat()           = { CatePathStat.executeDaily()           }

  def executeDailyInfluxPathStat()         = { InfluxPathStat.executeDaily()         }
  def executeDailyInfluxPathRefStat()      = { InfluxPathRefStat.executeDaily()      }
  def executeDailyInfluxPageStat()         = { InfluxPageStat.executeDaily()         }
  def executeDailyInfluxDomainDetailStat() = { InfluxDomainDetailStat.executeDaily() }
  def executeDailyInfluxDomainUrlStat()    = { InfluxDomainUrlStat.executeDaily()    }
  def executeDailyInfluxUrlDetailStat()    = { InfluxUrlDetailStat.executeDaily()    }
  def executeDailyInfluxUrlUrlStat()       = { InfluxUrlUrlStat.executeDaily()       }
  def executeDailyInfluxOuterKwStat()      = { InfluxOuterKwStat.executeDaily()      }
  def executeDailyInfluxKwPageStat()       = { InfluxKwPageStat.executeDaily()       }

  def executeDailyPvStat()                 = { PvStat.executeDaily()                 }
  def executeDailyVisitorStat()            = { VisitorStat.executeDaily()            }
  def executeDailyRevisitDurStat()         = { RevisitDurStat.executeDaily()         }
  def executeDailyRefererStat()            = { RefererStat.executeDaily()            }
  def executeDailyRefererKeywordStat()     = { RefererKeywordStat.executeDaily()     }
  def executeDailyEngLoginUserStat()       = { EngLoginUserStat.executeDaily()       }
  def executeDailySegStat()                = { SegStat.executeDaily()                }
  def executeDailyProdLog()                = { ProdLog.executeDaily()                }
  def executeDailyProdStat()               = { ProdStat.executeDaily()               }
  def executeDailyNCateLogFront()          = { NCateLogFront.executeDaily()          }
  def executeDailyPvStatFront()            = { PvStatFront.executeDaily()            }
  def executeDailyVisitorStatFront()       = { VisitorStatFront.executeDaily()       }
  def executeDailyNCateURLStatFront()      = { NCateURLStatFront.executeDaily()      }
  def executeDailyNCateStatFront()         = { NCateStatFront.executeDaily()         }
  def executeDailyVisitStatFront()         = { VisitStatFront.executeDaily()         }
  def executeDailyNCateLogUser()           = { NCateLogUser.executeDaily()           }
  def executeDailyPvStatUser()             = { PvStatUser.executeDaily()             }
  def executeDailyVisitorStatUser()        = { VisitorStatUser.executeDaily()        }
  def executeDailyNCateURLStatUser()       = { NCateURLStatUser.executeDaily()       }
  def executeDailyNCateStatUser()          = { NCateStatUser.executeDaily()          }
  def executeDailyOptParamStat()           = { OptParamStat.executeDaily()           }
  def executeDailyCsCodeStat()             = { CsCodeStat.executeDaily()             }

  def executeDailyVisitTwStat()            = { VisitTwStat.executeDaily()            }
  def executeDailySimpleLoginStat()        = { SimpleLoginStat.executeDaily()        }

}