package biz.dmsoft.str.comm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import biz.dmsoft.str.schema.load.RuntimeLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/*

import biz.dmsoft.str.comm.LoadTable

val tabNm   = "TSTRTRN001";
val baseDt = "20190504";
LoadTable.parquetDayPartition(spark, tabNm, baseDt)

spark.sql("SELECT * FROM TSTRTRN001").show()
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
