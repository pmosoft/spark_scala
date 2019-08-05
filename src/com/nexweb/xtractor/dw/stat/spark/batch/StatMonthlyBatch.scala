package com.nexweb.xtractor.dw.stat.spark.batch

import org.apache.spark.sql.SparkSession

import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.common.Batch

import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.VisitDayStat

import com.nexweb.xtractor.dw.stat.spark.batch.sql.CateStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.CateUrlStat

import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsCodeStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.CsStat

import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatFront
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateStatFront

import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.VisitorStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateURLStatUser
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.NCateStatUser

import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.EngLoginUserStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.SegStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.ProdStat

import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.ChnlUserStat
import com.nexweb.xtractor.dw.stat.spark.batch.sql.tw.TotalVisitStat
/*
 * 유저 접속 정보 통계 배치 수행
 * 사용방법
   StatMonthlyBatch        : 일자 파라미터가 없는 경우 현재일자 기준 전월로 배치 수행
   StatMonthlyBatch yyyyMM : 디폴트 현재일자 기준 전월 세팅

import com.nexweb.xtractor.dw.stat.spark.batch.StatMonthlyBatch
StatMonthlyBatch.setPrevYyyymmDt("201812")
StatMonthlyBatch.executeMonthlySegStat()
StatMonthlyBatch.executeMonthlyCsCodeStat()

StatMonthlyBatch.executeMonthlyEngLoginUserStat()
StatMonthlyBatch.executeMonthlyProdStat()
StatMonthlyBatch.executeMonthlyNCateURLStatFront()
StatMonthlyBatch.executeMonthlyVisitStatFront()
StatMonthlyBatch.executeMonthlySegStat()
StatMonthlyBatch.executeMonthlyNCateURLStatUser()
StatMonthlyBatch.executeMonthlyNCateStatUser()



 *
 * */

object StatMonthlyBatch {

  //val logger = LoggerFactory.getLogger("StatMonthlyBatch")

  //--------------------------------------
      println("SparkSession 생성");
  //--------------------------------------
  val spark = SparkSession.builder().appName("StatMonthlyBatch").getOrCreate() //.config("spark.master","local")
  var statisDate = Batch.getStatDt
  var prevYyyymmDt = Batch.getPrevMonthDt
  //var prevYyyymmDt = "20181219"
  //val spark = SparkSession.builder().getOrCreate()

  def main(args: Array[String]): Unit = {

    //--------------------------------------
        println("입력파라미터 처리");
    //--------------------------------------
    //val arg1 = if (args.length == 1) args(0)
    //val prevYyyymmDt = Batch.getprevYyyymmDt else arg1
    val prevYyyymmDt = if (args.length < 1) Batch.getPrevMonthDt else args(0)
    val statisDate = if (args.length < 2) Batch.getStatDt else args(1)

    //---------------------------------------------------------------------------------------------
                                             println("배치 시작 : " + this.prevYyyymmDt);
    //---------------------------------------------------------------------------------------------
    //val prevYyyymmDt = "201902";this.prevYyyymmDt = "201902"
    setPrevYyyymmDt(prevYyyymmDt)
    setstatisDate(statisDate)
    //------------------------------------------------
        println("당일 로그 및 오라클 테이블 로딩");
    //------------------------------------------------
    loadTablesMonthly()

    //--------------------------------------------------------
        println("통계 배치 수행");
    //--------------------------------------------------------
    
    executeMonthlyVisitStat()             // 월별 방문자수 
    executeMonthlyVisitDayStat()          // 월별 방문일수별 방문자수
    
    executeMonthlyCsCodeStat()            // 월별                                   TB_CS_CODE_STAT
    //executeMonthlyCsStat()              // 월별                                   TB_CS_STAT

    executeMonthlyVisitStatFront()        // 월별                                   TB_VISIT_STAT_FRONT
    executeMonthlyNCateURLStatFront()     // 월별                                   TB_NCATE_URL_STAT_FRONT
    executeMonthlyNCateStatFront()        // 월별                                   TB_NCATE_STAT_FRONT

    executeMonthlyProdStat()              // 월별                                   TB_PROD_STAT
    executeMonthlySegStat()               //
    
    executeMonthlyChnlUserStat()          // 월별                                   TB_CHNL_USER_STAT
    executeMonthlyTotalVisitStat()        // 월별                                   TB_TOTAL_VISIT_STAT
    //executeMonthlyVisitStatUser()       // 월별                                   TB_VISIT_STAT_FRONT
    //executeMonthlyNCateURLStatUser()    //
    //executeMonthlyNCateStatUser()       //
    //executeMonthlyEngLoginUserStat()    // 일/월별 영문 로그인 통계           TB_ENG_LOGIN_USER_STAT
    //executeMonthlyCateStat()              // 월별                                    TB_CATE_STAT
    //executeMonthlyCateUrlStat()           // 월별                                    TB_CATE_URL_STAT

    //---------------------------------------------------------------------------------------------
                                             println("배치 종료");
    //---------------------------------------------------------------------------------------------
    spark.stop()
  }

  def setPrevYyyymmDt(prevYyyymmDt:String) = {
    this.prevYyyymmDt = prevYyyymmDt;
  }
  
  def setstatisDate(statisDate:String) = {
    this.statisDate = statisDate;
  }

  def loadTablesMonthly() = {
//    LoadTable.lodBasicLogTables(spark,prevYyyymmDt,"M")
//    LoadTable.lodOracleAllTables(spark)
//    LoadTable.lodOracleMonthlyTables(spark,prevYyyymmDt)
  }
  
  
  def executeMonthlyVisitStat()          = { VisitStat.executeMonthly()          } //완료
  def executeMonthlyVisitDayStat()       = { VisitDayStat.executeMonthly()       } //완료

  def executeMonthlyCsCodeStat()         = { CsCodeStat.executeMonthly()         } //완료
  def executeMonthlyCsStat()             = { CsStat.executeMonthly()             } //삭제

  def executeMonthlyVisitStatFront()     = { VisitStatFront.executeMonthly()     } //완료
  def executeMonthlyNCateURLStatFront()  = { NCateURLStatFront.executeMonthly()  } //완료
  def executeMonthlyNCateStatFront()     = { NCateStatFront.executeMonthly()     } //완료

  def executeMonthlyEngLoginUserStat()   = { EngLoginUserStat.executeMonthly()   } //완료
  def executeMonthlySegStat()            = { SegStat.executeMonthly()            } //완료
  def executeMonthlyProdStat()           = { ProdStat.executeMonthly()           } //완료
  
  def executeMonthlyVisitorStatUser()    = { VisitorStatUser.executeMonthly()    }
  def executeMonthlyNCateURLStatUser()   = { NCateURLStatUser.executeMonthly()   }
  def executeMonthlyNCateStatUser()      = { NCateStatUser.executeMonthly()      }
  
  def executeMonthlyChnlUserStat()       = { ChnlUserStat.executeMonthly()       } //완료
  def executeMonthlyTotalVisitStat()     = { TotalVisitStat.executeMonthly()     } //완료
  
  def executeMonthlyCateStat()           = { CateStat.executeMonthly()           }
  def executeMonthlyCateUrlStat()        = { CateUrlStat.executeMonthly()        }
  
}