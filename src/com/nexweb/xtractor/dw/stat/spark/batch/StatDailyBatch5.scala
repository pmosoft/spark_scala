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
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.ChnlUserStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.MemberNotVID
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.AutoLoginStat
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
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatFront2
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsCodeStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsEventCode
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsCode
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
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.AppVersionStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.TotalVisitStat



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
object StatDailyBatch5 {

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
    println("4. TWORLD");
    //-------------------------------------------------------------------------
    executeDailyCsCodeStat()            // 일/월별                                   TB_CS_CODE_STAT
    executeDailyCsStat()                // 일/월별                                   TB_CS_STAT

    executeDailyPvStat()                // 일별    페이지뷰 통계                 TB_WL_PV_STAT
    executeDailyVisitorStat()           // 일별    방문자 통계                    TB_WL_VISITOR_STAT
    executeDailyRevisitDurStat()        // 일별    재방문자 통계                 TB_WL_REVISIT_DUR_STAT
    executeDailyRefererStat()           // 일별    유입 통계                       TB_WL_REFERER_STAT
    executeDailyRefererKeywordStat()    // 일별    유입 키워드 통계             TB_WL_REFERER_KEYWORD_STAT
    executeDailyEngLoginUserStat()      // 일/월별 영문 로그인 통계           TB_ENG_LOGIN_USER_STAT
    executeDailySegStat()               // 일/월별 메뉴별 방문자 통계        TB_SEG_STAT
    executeDailyProdLog()               // 일별                                        TB_PROD_LOG
    executeDailyProdStat()              // 일/월별                                   TB_PROD_STAT
    
                                      //Tworld 통합 방문자수                  TB_TWS_VISIT_STAT(소스 미발견)
                                      //Tworld 통합 방문자수                  TB_TWS_VISIT_STAT(소스 미발견)
    //executeDailyGradeMemberStat()     // 일/얼별                                    TB_TOTAL_MEMBER_STAT (추가 소스)

    executeDailyNCateLogFront()         // 일별                                        TB_NCATE_LOG_FRONT
    executeDailyPvStatFront()           // 일별                                        TB_WL_PV_STAT_FRONT
    executeDailyVisitorStatFront()      // 일별                                        TB_WL_VISITOR_STAT_FRONT
    executeDailyNCateURLStatFront()     // 일/월별                                   TB_NCATE_URL_STAT_FRONT
    //executeDailyNCateURLStatFront2()  // 일/월별                                   TB_NCATE_URL_STAT_FRONT2    
    executeDailyNCateStatFront()        // 일/월별                                   TB_NCATE_STAT_FRONT
    executeDailyVisitStatFront()        // 일/월별                                   TB_VISIT_STAT_FRONT

    excuteDailyAppVersionStat()         // 일별                                         TB_APP_VERSION_STAT
    
    //executeDailyNCateLogUser()        // 일별                                        TB_NCATE_LOG_USER (2019.02.13 오라클 USER MAP 정보 미존재)
    //executeDailyPvStatUser()          // 일별                                        TB_WL_PV_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)
    //executeDailyVisitorStatUser()     // 일별                                        TB_WL_VISITOR_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)
    //executeDailyNCateURLStatUser()    // 일/월별                                   TB_NCATE_URL_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)
    //executeDailyNCateStatUser()       // 일/월별                                   TB_NCATE_STAT_USER (2019.02.13 오라클 USER MAP 정보 미존재)

    //executeDailySimpleLoginStat()                            TB_SIMPLE_LOGIN_STAT (2019.02.15 소스코드 정비후 실행요)
    executeDailyVisitTwStat()           // 일별                                         TB_VISIT_TW_STAT
    excuteDailyChnlUserStat()           // 일별                                         TB_CHNL_USER_STAT
    excuteDailyMemberNotVID()           // 일별                                         TB_MEMBER_NOT_VID_CNT
    excuteDailyAutoLoginStat()          // 일별                                         TB_MEMBER_ETC_VIEW
    
    executeDailyOptParamStat()          // 일별                                        TB_OPT_PARAM_STAT

    excuteDailyTotalVisitStat()         // 일별                                        TB_TOTAL_VISIT_STAT
    executeDailyCsEventCode()           // 일별                                         TB_CS_EVENT_CODE
    executeDailyCsCode()                // 일별                                         TB_CS_CODE

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

  def executeDailyCsCodeStat()             = { CsCodeStat.executeDaily()             } //완료
  def executeDailyCsStat()                 = { CsStat.executeDaily()                 } //완료

  def executeDailyPvStat()                 = { PvStat.executeDaily()                 } //완료
  def executeDailyVisitorStat()            = { VisitorStat.executeDaily()            } //완료
  def executeDailyRevisitDurStat()         = { RevisitDurStat.executeDaily()         } //완료
  def executeDailyRefererStat()            = { RefererStat.executeDaily()            } //완료
  def executeDailyRefererKeywordStat()     = { RefererKeywordStat.executeDaily()     } //완료
  def executeDailyEngLoginUserStat()       = { EngLoginUserStat.executeDaily()       } //완료
  def executeDailySegStat()                = { SegStat.executeDaily()                } //완료 
  def executeDailyProdLog()                = { ProdLog.executeDaily()                } //완료
  def executeDailyProdStat()               = { ProdStat.executeDaily()               } //완료
 
  def executeDailyNCateLogFront()          = { NCateLogFront.executeDaily()          } //완료
  def executeDailyPvStatFront()            = { PvStatFront.executeDaily()            } //완료
  def executeDailyVisitorStatFront()       = { VisitorStatFront.executeDaily()       } //완료
  def executeDailyNCateURLStatFront()      = { NCateURLStatFront.executeDaily()      } //완료
  def executeDailyNCateURLStatFront2()     = { NCateURLStatFront2.executeDaily()     } //완료
  def executeDailyNCateStatFront()         = { NCateStatFront.executeDaily()         } //완료
  def executeDailyVisitStatFront()         = { VisitStatFront.executeDaily()         } //완료
  
  def executeDailyVisitTwStat()            = { VisitTwStat.executeDaily()            } //완료
  def executeDailySimpleLoginStat()        = { SimpleLoginStat.executeDaily()        }
  def excuteDailyChnlUserStat()            = { ChnlUserStat.executeDaily()           } //완료
  def excuteDailyMemberNotVID()            = { MemberNotVID.executeDaily()           } //완료
  def excuteDailyAutoLoginStat()           = { AutoLoginStat.executeDaily()          } //완료
  
  def excuteDailyAppVersionStat()          = { AppVersionStat.executeDaily()         } //완료
  def executeDailyOptParamStat()           = { OptParamStat.executeDaily()           } //완료
  
  def executeDailyNCateLogUser()           = { NCateLogUser.executeDaily()           }
  def executeDailyPvStatUser()             = { PvStatUser.executeDaily()             }
  def executeDailyVisitorStatUser()        = { VisitorStatUser.executeDaily()        }
  def executeDailyNCateURLStatUser()       = { NCateURLStatUser.executeDaily()       }
  def executeDailyNCateStatUser()          = { NCateStatUser.executeDaily()          }

  def excuteDailyTotalVisitStat()          = { TotalVisitStat.executeDaily()         } //완료
  
  def executeDailyCsEventCode()            = { CsEventCode.executeDaily()            } //완료
  def executeDailyCsCode()                 = { CsCode.executeDaily()                 } //완료
 
}