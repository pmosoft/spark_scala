package biz.dmsoft.str.comm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import biz.dmsoft.str.schema.load.RuntimeLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/*
 * SparkEtl pathNm tabNm
import biz.dmsoft.str.etl.SparkEtl

val tabNm   = "TSTRTRN001";
val batchDt = "20190504";
SparkEtl.localSamToHdfsParquet(spark, tabNm, batchDt, "|")
localSamToHdfsParquet(spark, tabNm, batchDt, "|")
SparkEtl.test01(spark)
SparkEtl.test02()
test02(spark)

 * */
object LoadTable {

  def main(args: Array[String])   {

    val tabNm  = args(0);
    val baseDt = args(1);
    val spark  = SparkSession.builder().appName("LoadTable").getOrCreate()
    spark.stop()
  }

  def parquetDayPartition(spark: SparkSession, tabNm: String, baseDt: String): Unit = {
    val parquetDF = spark.sql("SELECT * FROM parquet.`"+App.parquetPath+tabNm+"/"+tabNm+"_"+baseDt+"`")
    parquetDF.cache().createOrReplaceTempView(tabNm);parquetDF.count()
  }
}
