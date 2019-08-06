import org.apache.spark.sql.SparkSession

val spark = SparkSession
   .builder()
   .appName("Spark SQL basic example")
   .master("local[*]")
   .config("spark.some.config.option", "some-value")
   .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
import spark.implicits._

val df = spark.read.csv("/user/shjeong/people.csv")

df.show()
