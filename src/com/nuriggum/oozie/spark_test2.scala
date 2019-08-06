import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example2")
  .master("local[*]")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

import spark.implicits._

val df = spark.read.json("/user/shjeong/people.json")
df.show()
