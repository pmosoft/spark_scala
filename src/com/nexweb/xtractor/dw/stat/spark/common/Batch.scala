package com.nexweb.xtractor.dw.stat.spark.common

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object Batch {

  /**********************************************************
   * 전역 변수 세팅
   **********************************************************/
  //val localBasicPath       = "file:////data/xtractor/entity"
  val localBasicPath       = "file:////home/xtractor/entity"

  val hdfsLogBasicPath     = "/user/xtractor/data/entity"
  val hdfsParquetBasicPath = "/user/xtractor/parquet/entity"

  val localPcBasicPath     = "file:////c:/Users/P136391/Documents/data"

  val spark = SparkSession.builder().appName("Batch").getOrCreate() //.config("spark.master","local")

  /**********************************************************
   * 전일자 가져오기
   **********************************************************/
  val getStatDt:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -1)
    dataFormat.format(cal.getTime())
  }

  /**********************************************************
   * 전전일자 가져오기
   **********************************************************/
  val getPreStatDt:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -2)
    dataFormat.format(cal.getTime())
  }

   /**********************************************************
   * 전주 월요일 가져오기
   **********************************************************/
  val getStatMon:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -7)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 화요일 가져오기
   **********************************************************/
  val getStatTue:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -6)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 수요일 가져오기
   **********************************************************/
  val getStatWed:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -5)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 목요일 가져오기
   **********************************************************/
  val getStatThu:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -4)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 금요일 가져오기
   **********************************************************/
  val getStatFri:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -3)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 토요일 가져오기
   **********************************************************/
  val getStatSat:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -2)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 일요일 가져오기
   **********************************************************/
  val getStatSun:String = {
    val dataFormat = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -1)
    dataFormat.format(cal.getTime())
  }
  
   /**********************************************************
   * 전주 가져오기
   **********************************************************/
  val getStatWeek:String = {
    val dataFormat = new SimpleDateFormat("yyyyww")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -1)
    dataFormat.format(cal.getTime())
  }

  
  
  
  /**********************************************************
   * 전월 가져오기
   **********************************************************/
  val getPrevMonthDt:String = {
    val dataFormat = new SimpleDateFormat("yyyyMM")
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, -28)
    dataFormat.format(cal.getTime())
  }


  /**********************************************************
   * DUAL
   **********************************************************/
  def createDualTable(spark: SparkSession) = {
    val schema : StructType= StructType(Array(StructField("COL",StringType)))
    spark.read.format("csv").option("delimiter","|").schema(schema).load("/user/xtractor/data/entity/DUAL/DUAL.dat").createOrReplaceTempView("dual")
  }




  /**********************************************************
   * SUB 일자
   **********************************************************/
  def getSubDt(spark: SparkSession,srcDt:String, subCnt:Int) = {
    createDualTable(spark)
    var qry = " SELECT DATE_FORMAT(DATE_SUB( CONCAT(SUBSTR('"+srcDt+"',1,4),'-',SUBSTR('"+srcDt+"',5,2),'-',SUBSTR('"+srcDt+"',7,2)) , "+subCnt+"),'yyyyMMdd') FROM DUAL"
    println(qry)
    //val tarDt = spark.sql(qry).select("TAR_DT").take(1)
    val tarDt = spark.sql(qry).take(1)
    tarDt(0)(0).toString()
    //println(tarDt.seq(0).)
  //getSubDt(spark,"20181219",1)
  //var qry = " SELECT DATE_FORMAT(DATE_SUB( CONCAT(SUBSTR('20181219',1,4),'-',SUBSTR('20181219',5,2),'-',SUBSTR('20181219',7,2)) , 2),'yyyyMMdd') FROM DUAL "
  //spark.sql("SELECT DATE_ADD('2018-12-19', 1) FROM dual")
  }

  /**********************************************************
   * ADD 일자
   **********************************************************/
  def getAddDt(spark: SparkSession,srcDt:String, subCnt:Int) = {
    createDualTable(spark)
    var qry = " SELECT DATE_FORMAT(DATE_ADD( CONCAT(SUBSTR('"+srcDt+"',1,4),'-',SUBSTR('"+srcDt+"',5,2),'-',SUBSTR('"+srcDt+"',7,2)) , "+subCnt+"),'yyyyMMdd') FROM DUAL"
    println(qry)
    val tarDt = spark.sql(qry).take(1)
    tarDt(0)(0).toString()
  }

}