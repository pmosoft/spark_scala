import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
 * SparkEtl pathNm fileNm
 * */
object SparkEtl {

  def main(args: Array[String])   {

    val pathNm = args(0);
    val fileNm = args(1);

    // $example on:init_session$
    val spark = SparkSession.builder().appName("SparkEtl").getOrCreate()

    localToHdfs(spark,pathNm,fileNm)
    localCsvToHdfsParquet(spark,pathNm,fileNm)
    localJsonToHdfsParquet(spark,pathNm,fileNm)

    //hdfsCsvToLocalCsv(spark)
    //hdfsParquetToLocalCsv(spark)
    //hdfsJsonToLocalJson(spark)
    //hdfsParquetToLocalJson(spark)

    spark.stop()
  }

  private def localToHdfs(spark: SparkSession, pathNm: String, fileNm: String): Unit = {
    //hadoop fs -put /data/tos/1901 /tos/hdfs/xtractor/test/weather
    //hadoop fs -put /data/tos/1902 hdfs://master/tos/hdfs/xtractor/test/weather
  }

  private def localCsvToHdfsParquet(spark: SparkSession, pathNm: String, fileNm: String): Unit = {
    //spark.read.format("csv").option("delimiter","|").schema(schema).load(source).write.parquet(target)
  }

  private def localJsonToHdfsParquet(spark: SparkSession, pathNm: String, fileNm: String): Unit = {
    // $example on:create_df$
    val df = spark.read.json("examples/src/main/resources/people.json")
  }
}
