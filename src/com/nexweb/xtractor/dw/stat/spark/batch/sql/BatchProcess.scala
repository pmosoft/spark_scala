package com.nexweb.xtractor.dw.stat.spark.batch.sql

import org.apache.spark.sql.SparkSession

trait BatchProcess {

  /*
   * 전체 프로세스 수행
   * */
  def executeDaily()
  
  /*
   * 0단계 : 배치수행에 필요한 테이블 로딩
   * */
  def loadTablesDaily()()
  
  /*
   * 1단계 : SQL 수행 및 DF 캐쉬 테이블 생성
   * */
  def excuteDailySql()

  /*
   * 2단계 : Parquet로 저장
   * */
  def saveToParqeutDaily()
  
  /*
   * 3단계 : 오라클 테이블에 저장
   * */
  def ettToOracle()
  
}