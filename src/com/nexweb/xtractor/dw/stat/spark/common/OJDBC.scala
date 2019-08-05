package com.nexweb.xtractor.dw.stat.spark.common

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.dmg.pmml.True

/*
 * 오라클 JDBC
 * 사용방법 :
 * deleteTable(spark,"DELETE FROM TB_NCATE_LAST_VER")
 * insertTable(spark,"TB_NCATE_LAST_VER")
 * selectTable(spark,"TB_NCATE_LAST_VER")
 *
 */
object OJDBC {

  //val url      = "jdbc:oracle:thin:@//150.204.15.124:1521/TWCRMDB"
  //val user     = "ISITE_XTRACTOR"
  //val password = "ISITE_sprtmxm12!"

  val url      = "jdbc:oracle:thin:@//150.206.15.231:1532/TOSP"
  val user     = "TXTR"
  val password = "dhvjfld!23"
  val driver   = "oracle.jdbc.driver.OracleDriver"

  def deleteTable(spark:SparkSession,qry:String) = {
    //--------------------------------------
        println("(ORACLE) "+qry);
    //--------------------------------------
    Class.forName(driver)
    var con:Connection = DriverManager.getConnection(url,user,password)
    var stat=con.createStatement()
    stat.execute(qry)
    con.close()
  }

  def insertTable(spark:SparkSession,objNm:String) = {

    //--------------------------------------
        println("(ORACLE) INSERT "+objNm);
    //--------------------------------------
    spark.table(objNm).write
    .format("jdbc").option("url",url).option("user",user).option("password",password).option("driver",driver)
    .option("dbtable", objNm)
    .mode("append")
    .save()

  }

  def selectTable(spark:SparkSession,objNm:String) = {

    // spark.read.parquet("parquet/entity/TB_NCATE_LAST_VER").createOrReplaceTempView("TB_NCATE_LAST_VER")

    //--------------------------------------
        println("(ORACLE) SELECT "+objNm);
    //--------------------------------------
    spark.read
    .format("jdbc").option("url",url).option("user",user).option("password",password).option("driver",driver)
    .option("dbtable", objNm)
    .load().createOrReplaceTempView(objNm)

    spark.sql("SELECT * FROM "+objNm).collect().foreach(println)

  }

/*
 * 일자별 배치 결과 db 인서트 - 테이블 필수
 * 테이블 컬럼 : 일자, 테이블명, GVHOST, 인서트 건수
    def selectCountTable(spark:SparkSession,objNm:String,statisDate:String) = {

    // spark.read.parquet("parquet/entity/TB_NCATE_LAST_VER").createOrReplaceTempView("TB_NCATE_LAST_VER")

    //--------------------------------------
        println("(ORACLE) SELECT "+objNm);
    //--------------------------------------
    spark.read
    .format("jdbc").option("url",url).option("user",user).option("password",password).option("driver",driver)
    .option("dbtable", objNm)
    .load().createOrReplaceTempView(objNm)

    spark.sql("SELECT "+statisDate+", "+objNm+", GVHOST, COUNT(*) FROM "+objNm+ "WHERE STATIS_DATE ='"+statisDate+"' GROUP BY GVHOST").collect().foreach(println)

  }
*/
  
}

/*

//spark-shell --jars /home/xtractor/common/ojdbc6.jar
//val pushdown_query = "(select * from ISITE_XTRACTOR.TB_WL_REFERER where rownum < 100) VB_WL_REFERER
val tDF = spark.read.parquet("parquet/entity/TB_NCATE_LAST_VER")
tDF.count()
tDF.createOrReplaceTempView("TB_NCATE_LAST_VER")
spark.sql("select * from TB_NCATE_LAST_VER").collect().foreach(println)

val connectionProperties = new Properties()
connectionProperties.put("user", "ISITE_XTRACTOR")
connectionProperties.put("password", "ISITE_sprtmxm12!")
connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")
val jdbcDF2 = spark.read
.jdbc("jdbc:oracle:thin:@//150.204.15.124:1521/TWCRMDB", "TB_NCATE_LAST_VER", connectionProperties)

//val pushdown_query = "(SELECT * FROM TB_NCATE_LAST_VER WERE ROWNUM < 3) TB_NCATE_LAST_VER"
val pushdown_query = "TB_NCATE_LAST_VER"

jdbcDF33.show()

spark.table("TB_NCATE_LAST_VER").write
.format("jdbc")
.option("url", "jdbc:oracle:thin:@//150.204.15.124:1521/TWCRMDB")
.option("dbtable", "ISITE_XTRACTOR.TB_NCATE_LAST_VER")
.option("user", "ISITE_XTRACTOR")
.option("password", "ISITE_sprtmxm12!")
.option("driver", "oracle.jdbc.driver.OracleDriver")
//.option("mode", "SaveMode.Append")
//.option("truncate", "true")
.mode("append")
.save()

spark.sql("select * from TB_NCATE_LAST_VER").collect().foreach(println)
res172.createOrReplaceTempView("TB_NCATE_LAST_VER")

*/