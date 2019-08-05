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
   StatWeeklyBatch          : 일자 파라미터가 없는 경우 현재일자 기준 전주로 배치 수행
   StatWeeklyBatch yyyymmdd : 디폴트 현재일자 기준 전주 세팅

 *
 * */

object StatWeeklyBatch {

  //val logger = LoggerFactory.getLogger("StatMonthlyBatch")

  //--------------------------------------
      println("SparkSession 생성");
  //--------------------------------------
  val spark = SparkSession.builder().appName("StatWeeklyBatch").getOrCreate() //.config("spark.master","local")
  var mon = Batch.getStatMon
  var tue = Batch.getStatTue
  var wed = Batch.getStatWed
  var thu = Batch.getStatThu
  var fri = Batch.getStatFri
  var sat = Batch.getStatSat
  var sun = Batch.getStatSun
  var yyyyww = Batch.getStatWeek
  var statisType = "W"
  
  def main(args: Array[String]): Unit = {

    //--------------------------------------
        println("입력파라미터 처리");
    //--------------------------------------
    //val arg1 = if (args.length == 1) args(0)
    //val prevYyyymmDt = Batch.getprevYyyymmDt else arg1
    val prevYyyymmDt = if (args.length < 1) Batch.getStatDt else args(0)

    //---------------------------------------------------------------------------------------------
    println("배치 시작 : " + this.yyyyww);
    //---------------------------------------------------------------------------------------------

    //------------------------------------------------
        println("당일 로그 및 오라클 테이블 로딩");
    //------------------------------------------------
    loadTablesWeekly()

    //--------------------------------------------------------
        println("통계 배치 수행");
    //--------------------------------------------------------
    

    //---------------------------------------------------------------------------------------------
    println("배치 종료");
    //---------------------------------------------------------------------------------------------
    spark.stop()
  }


  def loadTablesWeekly() = {
    
    
    
  }
  
  
  
}