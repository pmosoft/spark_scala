package biz.dmsoft.str.batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import biz.dmsoft.str.schema.load.RuntimeLoader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import biz.dmsoft.str.comm.App
import biz.dmsoft.str.comm.LoadTable

/*

import biz.dmsoft.str.batch.TranTest
val spark  = SparkSession.builder().appName("TranTest").getOrCreate()

TranTest.dailyTables()

spark.sql("SELECT * FROM TSTRTRN001").show()
 * */
object TranTest {

  var spark: SparkSession = null

  def main(args: Array[String])   {
    val spark  = SparkSession.builder().appName("TranTest").getOrCreate()
    spark.stop()
  }

  def execute(spark: SparkSession): Unit = {
    this.spark = spark
    dailyTables()
    executeQuery()
  }

  def dailyTables(): Unit = {
    LoadTable.parquetDayPartition(spark,"TSTRTRN001","20190504")
  }

  def executeQuery(): Unit = {
    var qry = """
    SELECT * FROM TSTRTRN001
    """
    spark.sql(qry).show();
  }

}
