package biz.dmsoft.str.comm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import biz.dmsoft.str.schema.load.RuntimeLoader
import biz.dmsoft.str.comm.App

/*
 * SparkEtl pathNm tabNm
import biz.dmsoft.str.comm.SparkEtl
import biz.dmsoft.str.comm.App

var tabNm   = "TSTRTRN001";
var batchDt = "20190505";
var delimiter = "|";

SparkEtl.localSamToHdfsParquet(spark, tabNm, batchDt, "|")
localSamToHdfsParquet(spark, tabNm, batchDt, "|")
SparkEtl.test01(spark)
SparkEtl.test02()
test02(spark)


 * */
object SparkEtl {

  def main(args: Array[String])   {

    val tabNm   = args(0);
    val batchDt = args(1);
    //var src = App.src;

    // $example on:init_session$
    val spark = SparkSession.builder().appName("SparkEtl").getOrCreate()

    localSamToHdfsParquet(spark, tabNm, batchDt, "|")

    //localToHdfs(spark,tabNm)
    //localCsvToHdfsParquet(spark,tabNm)
    //localJsonToHdfsParquet(spark,tabNm)
    //localCsvToHdfsParquetDay(spark,tabNm,batchDt)
    //localJsonToHdfsParquetDay(spark,tabNm,batchDt)

    //hdfsCsvToLocalCsv(spark)
    //hdfsParquetToLocalCsv(spark)
    //hdfsJsonToLocalJson(spark)
    //hdfsParquetToLocalJson(spark)

    spark.stop()
  }

  def test01(spark: SparkSession): Unit = {
    var aa = "aaaa";
  }
  def test02(): Unit = {
    var aa = "aaaa";
    println(aa);
  }
  def test03(): Unit = {
    var aa = "aaaa";
  }


  def localToHdfs(spark: SparkSession, tabNm: String): Unit = {
    //hadoop fs -put /data/tos/1901 /tos/hdfs/xtractor/test/weather
    //hadoop fs -put /data/tos/1902 hdfs://master/tos/hdfs/xtractor/test/weather
  }


  def localSamToHdfsParquet(spark: SparkSession, tabNm: String, batchDt: String, delimiter: String): Unit = {

    val schema: StructType = RuntimeLoader.execute(tabNm)
    var partNm =  tabNm +"_"+  batchDt; if(batchDt.equals("all")) partNm =  tabNm
    var src = App.localPath + partNm + ".dat"
    var tar = App.parquetPath +tabNm+"/"+partNm

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.delete(new Path(tar))
    //fs.mkdirs(new Path(tar))

    //spark.read.format("csv").option("delimiter","|").schema(schema).load("file:///"+src).write.parquet(tar)
    spark.read.format("csv").option("delimiter",delimiter).schema(schema).load(src).write.parquet(tar)
  }

  def localCsvToHdfsParquet(spark: SparkSession, tabNm: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, "all", ",")
  }

  def localBarToHdfsParquet(spark: SparkSession, tabNm: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, "all", "|")
  }

  def localCsvToHdfsParquetDay(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, batchDt, ",")
  }

  def localBarToHdfsParquetDay(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    localSamToHdfsParquet(spark, tabNm, batchDt, "|")
  }

  def localJsonToHdfsParquet(spark: SparkSession, tabNm: String, batchDt: String): Unit = {
    var partNm =  tabNm +"_"+ batchDt; if(batchDt.equals("all")) partNm =  tabNm
    var src = App.localPath + partNm + ".dat"
    var tar = App.parquetPath + partNm
    val df = spark.read.json("examples/src/main/resources/people.json")
  }

}
