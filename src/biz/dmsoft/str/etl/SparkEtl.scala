package biz.dmsoft.str.etl

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import biz.dmsoft.str.schema.load.RuntimeLoader
import biz.dmsoft.str.comm.App

/*
 * SparkEtl pathNm tabNm
 * */
object SparkEtl {

  def main(args: Array[String])   {

    val tabNm   = args(0);
    val batchDt = args(1);
    //var src = App.src;

    // $example on:init_session$
    val spark = SparkSession.builder().appName("SparkEtl").getOrCreate()

    localToHdfs(spark,tabNm)
    localCsvToHdfsParquet(spark,tabNm)
    //localJsonToHdfsParquet(spark,tabNm)
    localCsvToHdfsParquetDay(spark,tabNm,batchDt)
    //localJsonToHdfsParquetDay(spark,tabNm,batchDt)

    //hdfsCsvToLocalCsv(spark)
    //hdfsParquetToLocalCsv(spark)
    //hdfsJsonToLocalJson(spark)
    //hdfsParquetToLocalJson(spark)

    spark.stop()
  }

  private def localToHdfs(spark: SparkSession, tabNm: String): Unit = {
    //hadoop fs -put /data/tos/1901 /tos/hdfs/xtractor/test/weather
    //hadoop fs -put /data/tos/1902 hdfs://master/tos/hdfs/xtractor/test/weather
  }


  private def localSamToHdfsParquet(spark: SparkSession, tabNm: String, batchDt: String, delimiter: String): Unit = {

    val schema: StructType = RuntimeLoader.execute(tabNm)
    var partNm =  tabNm +"_"+ batchDt; if(batchDt.equals("all")) partNm =  tabNm
    var src = App.localPath + partNm + ".dat"
    var tar = App.parquetPath +"/"+tabNm+"/"+partNm

    spark.read.format("csv").option("delimiter",delimiter).schema(schema).load(src).write.parquet(tar)
  }

  private def localCsvToHdfsParquet(spark: SparkSession, tabNm: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, "all", ",")
  }

  private def localBarToHdfsParquet(spark: SparkSession, tabNm: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, "all", "|")
  }

  private def localCsvToHdfsParquetDay(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, batchDt, ",")
  }

  private def localBarToHdfsParquetDay(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, batchDt, "|")
  }

  private def localJsonToHdfsParquet(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    var partNm =  tabNm +"_"+ batchDt; if(batchDt.equals("all")) partNm =  tabNm
    var src = App.localPath + partNm + ".dat"
    var tar = App.parquetPath + partNm

    val df = spark.read.json("examples/src/main/resources/people.json")
  }

}
