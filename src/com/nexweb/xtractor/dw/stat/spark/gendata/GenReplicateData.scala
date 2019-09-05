package com.nexweb.xtractor.dw.stat.spark.gendata

import org.apache.spark.sql.SparkSession
import com.nexweb.xtractor.dw.stat.spark.common.Batch
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet
/*


import org.apache.spark.sql.SparkSession
import com.nexweb.xtractor.dw.stat.spark.common.Batch
import com.nexweb.xtractor.dw.stat.spark.batch.load.LoadTable
import com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet

  val spark = SparkSession.builder().appName("GenReplicateData").config("spark.debug.maxToStringFields","100").getOrCreate()











 * */
object GenReplicateData {

  val spark = SparkSession.builder().appName("GenReplicateData").config("spark.debug.maxToStringFields","100").getOrCreate()

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: MakeParquet local objNm statisDate")
      System.exit(1)
    }

    val arg1 = args(0) 
    val arg2 = args(1)
    val arg3 = args(2)
    val arg4 = args(3)
    val arg5 = args(4)

//    var objNm  = arg1
//    var srcDt  = arg2
//    var tarDt  = arg3
//    var genCd  = arg4
//    var repCnt = arg5.toInt

    var srcObjNm  = "TB_WL_URL_ACCESS"
    var srcDt     = "20181219"
    var tarObjNm  = "TB_WL_URL_ACCESS"
    var tarDt     = "20181218"
    var repCnt    = 7
    var whereCond = ""
    var subCnt    = 1

    genAccessAsTimes(srcObjNm,srcDt,whereCond,repCnt,subCnt)
  }

  def genCsCodeProcessAsTimes(srcObjNm:String,srcDt:String,whereCond:String,repCnt:Int,subCnt:Int) = {
    // loop 복제횟수 시작
    //hadoop fs -appendToFile /home/xtractor/entity/TB_CS_CODE_PROCESS/TB_CS_CODE_PROCESS_20181219.dat /user/xtractor/data/entity/TB_CS_CODE_PROCESS/TB_CS_CODE_PROCESS_20181219.dat
    // loop 복제횟수 끝
    //spark-submit --driver-memory 5g --executor-memory 20g --class com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet /home/xtractor/spark/bin/xtractorSpark.jar hdfs TB_CS_CODE_PROCESS 20181219

  }

  def genCsCodeProcessAddByDate(srcObjNm:String,srcDt:String,subCnt:Int) = {
/*
 * */
    //var srcObjNm  = "TB_WL_URL_ACCESS"
    //var srcDt     = "20181028"
    var colInfo   = """
CONCAT(DATE_ADD(C_TIME,"""+subCnt+"""),SUBSTR(C_TIME,11,30)) AS C_TIME
,L_ID
,V_ID
,E_ID
,CS_ID
,P_ID
,LOGIN_ID
,ACTION
,SVC_UPDATE_ID
"""
    var whereCond = ""

    var tarDt     = Batch.getSubDt(spark,srcDt,subCnt)
    //var tarDt     = getSubDt(spark,"20181028",2)
    LoadTable.lodPartColTable(spark,srcObjNm, srcDt,"D", colInfo, whereCond, false)
    //spark.sql("SELECT * FROM TB_WL_URL_ACCESS").take(10).foreach(println);
    MakeParquet.dfToParquet(srcObjNm,true,tarDt)
  }

  def genAccessAsTimes(srcObjNm:String,srcDt:String,whereCond:String,repCnt:Int,subCnt:Int) = {
    // loop 복제횟수 시작
    //hadoop fs -appendToFile /home/xtractor/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219.dat /user/xtractor/data/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219.dat
    // loop 복제횟수 끝
    //spark-submit --driver-memory 5g --executor-memory 20g --class com.nexweb.xtractor.dw.stat.spark.parquet.MakeParquet /home/xtractor/spark/bin/xtractorSpark.jar hdfs TB_WL_URL_ACCESS 20181219
  }
/*


    for(i <- 1 until 1+1) {
      print(i)
    }




 * */


  def genAccessByDate(srcObjNm:String,srcDt:String,addSub:String,cnt:Int) = {
    //var srcObjNm  = "TB_WL_URL_ACCESS"
    //var srcDt     = "20181219"
    var colInfo   = """
             GVHOST,VHOST,URL,V_ID,U_ID,T_ID
            ,CONCAT("""+addSub+"""(C_TIME,"""+cnt+"""),SUBSTR(C_TIME,11,30)) AS C_TIME
            ,IP,PREV_URL,DUR_TIME,APPLY_STAT,P_ID,KEYWORD,AREA_CODE,PREV_DOMAIN,SESSION_ID
            ,F_REF_HOST,F_REF_DOMAIN,F_REF_CATE,USERAGENT,MOBILE_YN,OS,BROWSER,OS_VER,BROWSER_VER
            ,XLOC,LANG,DEVICE_ID,URL_PARAM,LOGIN_TYPE,FRONT_URL,USER_URL,OPT_PARAM,USER_PARAM
            ,SVC_UPDATE_ID,SVC_ID,OPT1,OPT2,OPT3,OPT4,OPT5"""
    var whereCond = ""

    var tarDt     = if(addSub=="DATE_SUB") Batch.getSubDt(spark,srcDt,cnt) else Batch.getAddDt(spark,srcDt,cnt)
    //var tarDt     = getSubDt(spark,"20181219",2)
    LoadTable.lodPartColTable(spark,srcObjNm, srcDt,"D", colInfo, whereCond, false)
    //spark.sql("SELECT * FROM TB_WL_URL_ACCESS").take(10).foreach(println);
    MakeParquet.dfToParquet(srcObjNm,true,tarDt)
  }

  def genAccessLoop(srcObjNm:String,srcDt:String,addSub:String,cnt:Int) = {
    for(i <- 1 until cnt+1) {
      genAccessByDate(srcObjNm,srcDt,addSub,i)
    }
    //genAccessLoop("TB_WL_URL_ACCESS","20181220","DATE_ADD",11)
    //genAccessLoop("TB_WL_URL_ACCESS","20181213","DATE_SUB",13)
    //spark.sql("SELECT COUNT(*) FROM parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181213`").take(5).foreach(println);
    //spark.sql("SELECT * FROM parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181218`").take(5).foreach(println);
    //spark.sql("SELECT DISTINCT DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE FROM parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219`").take(5).foreach(println);
  }

  def genRefererAsTimes(srcObjNm:String,srcDt:String,whereCond:String,repCnt:Int) = {
    var srcObjNm  = "TB_WL_REFERER"
    var srcDt     = "20181219"
    var whereCond = ""
    //var whereCond = "  "
    var repCnt    = 4
    LoadTable.lodAllColTable(spark,srcObjNm,srcDt,"D",whereCond,false)
    genReplicate(srcObjNm,srcDt,repCnt)
  }


  def genRefererByDate(srcObjNm:String,srcDt:String,subCnt:Int) = {
    //var srcObjNm  = "TB_WL_REFERER"
    //var srcDt     = "20181219"
    var colInfo   = """
             GVHOST,VHOST,URL,HOST,DIR_CGI,KEYWORD,V_ID,U_ID,T_ID
            ,CONCAT(DATE_SUB(C_TIME,"""+subCnt+"""),SUBSTR(C_TIME,11,30)) AS C_TIME
            ,DOMAIN,CATEGORY,V_IP,SESSION_ID,REF_URL,REF_PARAM,USER_AGENT,MOBILE_YN
            ,OS,BROWSER,OS_VER,BROWSER_VER,XLOC,LANG,DEVICE_ID,URL_PARAM,AREA_CODE
            ,CAMPAIGN_ID,LOGIN_TYPE,OPT1,OPT2,OPT3,OPT4,OPT5"""
    var whereCond = ""

    var tarDt     = Batch.getSubDt(spark,srcDt,subCnt)
    //var tarDt     = getSubDt(spark,"20181219",2)
    LoadTable.lodPartColTable(spark,srcObjNm, srcDt,"D", colInfo, whereCond, false)
    //spark.sql("SELECT * FROM TB_WL_REFERER").take(10).foreach(println);
    MakeParquet.dfToParquet(srcObjNm,true,tarDt)
  }

  def genRefererLoop(srcObjNm:String,srcDt:String,subCnt:Int) = {
    for(i <- 0 until subCnt) {
      genRefererByDate(srcObjNm,srcDt,i)
    }
    //genRefererLoop("TB_WL_REFERER","20181219",52)
    //spark.sql("SELECT * FROM parquet.`/user/xtractor/parquet/entity/TB_WL_REFERER/TB_WL_REFERER_20181217`").take(5).foreach(println);
    //spark.sql("SELECT COUNT(*) FROM parquet.`/user/xtractor/parquet/entity/TB_WL_REFERER/TB_WL_REFERER_20181217`").take(5).foreach(println);
  }

  def genMemberAsTimes(srcObjNm:String,srcDt:String,whereCond:String,repCnt:Int) = {
    var srcObjNm  = "TB_MEMBER_CLASS"
    var srcDt     = "20181219"
    var colInfo   = ""
    var whereCond = ""
    var repCnt    = 9
    //LoadTable.lodPartColDailyTable(spark,srcObjNm, srcDt, colInfo, whereCond, false)
    LoadTable.lodAllColTable(spark,srcObjNm, srcDt,"D", whereCond, false)
    genReplicate(srcObjNm,srcDt,repCnt)

     val tDF = spark.sql("SELECT GVHOST,VHOST,C_TIME,V_ID,U_ID,T_ID,LOGIN_ID,SESSION_ID,TYPE,GENDER,AGE,OS,OS_VER,MODEL,APP_VER,LOGIN_TYPE,AUTO_LOGIN_YN,SITE_TYPE,OPT1,OPT2,OPT3,OPT4,OPT5 FROM parquet.`/user/xtractor/parquet/entity/TB_MEMBER_CLASS/TB_MEMBER_CLASS_20181028`")
     tDF.cache.createOrReplaceTempView("TB_MEMBER_CLASS");tDF.count()
  }

  def genMemberByDate(srcObjNm:String,srcDt:String,subCnt:Int) = {
    //var srcObjNm  = "TB_MEMBER_CLASS"
    //var srcDt     = "20181219"
    var colInfo   = """
           GVHOST,VHOST
          ,CONCAT(DATE_SUB(C_TIME,"""+subCnt+"""),SUBSTR(C_TIME,11,30)) AS C_TIME
          ,V_ID,U_ID,T_ID,LOGIN_ID,SESSION_ID,TYPE,GENDER,AGE,OS,OS_VER,MODEL,APP_VER,LOGIN_TYPE,AUTO_LOGIN_YN,SITE_TYPE,OPT1,OPT2,OPT3,OPT4,OPT5"""
    var whereCond = ""

    var tarDt     = Batch.getSubDt(spark,srcDt,subCnt)
    //var tarDt     = getSubDt(spark,"20181219",2)
    LoadTable.lodPartColTable(spark,srcObjNm, srcDt,"D", colInfo, whereCond, false)
    //spark.sql("SELECT * FROM TB_MEMBER_CLASS").take(10).foreach(println);
    MakeParquet.dfToParquet(srcObjNm,true,tarDt)
  }

  def genMemberLoop(srcObjNm:String,srcDt:String,subCnt:Int) = {
    for(i <- 0 until subCnt) {
      genMemberByDate(srcObjNm,srcDt,i)
    }
    //genMemberLoop("TB_MEMBER_CLASS","20181219",52)
    //spark.sql("SELECT * FROM parquet.`/user/xtractor/parquet/entity/TB_MEMBER_CLASS/TB_MEMBER_CLASS_20181217`").take(5).foreach(println);
    //spark.sql("SELECT COUNT(*) FROM parquet.`/user/xtractor/parquet/entity/TB_MEMBER_CLASS/TB_MEMBER_CLASS_20181217`").take(5).foreach(println);
  }

  def genReplicate(srcObjNm:String,srcDt:String,repCnt:Int) = {
    var repQry = ""
    for(i <- 0 until repCnt) {
      if(i<repCnt-1) {
        repQry += "SELECT * FROM " + srcObjNm + " UNION ALL ";
      } else {
        repQry += "SELECT * FROM " + srcObjNm;
      }
    }
    println("repQry : "+repQry);
    MakeParquet.dfToParquet(srcObjNm,true,srcDt,repQry)
  }

  def genAccessByDate() = {
    var srcObjNm  = "TB_WL_URL_ACCESS"
    var srcDt     = "20181019"
    var whereCond = " WHERE GVHOST IN ('TMAP','TMOW') "
    var repCnt    = 5
    LoadTable.lodAllColTable(spark,srcObjNm,srcDt,"D",whereCond,true)
  }

  def genCopy(srcObjNm:String, srcDt:String) = {

    val colArr = spark.table(srcObjNm).schema.fieldNames

    var selectStat = "SELECT "
    var colsStat = ""
    for(i <- 0 until colArr.length) {
      if(colArr(i) == "C_TIME") {
        if(i>0) colsStat += ",'"+ srcDt + "' AS "+ colArr(i)
        else    colsStat += " '"+ srcDt + "  AS "+ colArr(i)
      } else {
        if(i>0) colsStat += "," + colArr(i)
        else    colsStat += colArr(i)
      }
    }
  }


}

/*


var srcObjNm  = "TB_WL_URL_ACCESS"
var srcDt     = "20181219"
//var whereCond = " WHERE GVHOST IN ('TMAP','TMOW') "
var whereCond = "  "

LoadTable.lodAllColTable(spark,srcObjNm,srcDt,whereCond,false)


spark.sql("SELECT DISTINCT GVHOST FROM TB_WL_URL_ACCESS WHERE GVHOST = 'www.sktmembership.co.kr:90'").take(100).foreach(println);

spark.sql("SELECT LENGTH(GVHOST) FROM TB_WL_URL_ACCESS GROUP BY LENGTH(GVHOST) ORDER BY LENGTH(GVHOST)").take(100).foreach(println);

spark.sql("SELECT GVHOST, count(*) cnt FROM TB_WL_URL_ACCESS GROUP BY GVHOST ORDER BY GVHOST").take(10000).foreach(println);

spark.sql("""
SELECT GVHOST, COUNT(*) cnt
FROM   TB_WL_URL_ACCESS
GROUP BY GVHOST
HAVING  LENGTH(GVHOST) < 10
ORDER BY GVHOST
""").take(100).foreach(println);

spark.sql(""" SELECT COUNT(*) FROM TB_WL_URL_ACCESS """).take(1).foreach(println);


spark.sql("""
SELECT * FROM TB_WL_URL_ACCESS
""").take(1).foreach(printl


  def executeDaily() = {
//    if     (objNm=="TB_WL_REFERER"   ) genReferer()
//    else if(objNm=="TB_WL_URL_ACCESS") genAccess()
//    else if(objNm=="TB_MEMBER_CLASS" ) genMember()
  }



  def genReferer() = {
  }

  def genMember() = {
  }


  def genCommon(objNm:String,whereCond:String,srcDt:String,tarDt:String,genCd:String,repCnt:Int) = {


    //val parDF = spark.sql("SELECT * FROM parquet.`/user/xtractor/parquet/entity/TB_WL_URL_ACCESS/TB_WL_URL_ACCESS_20181219`");
    val parDF = spark.sql("SELECT * FROM parquet.`/user/xtractor/parquet/entity/"+objNm+"/"+objNm+"_"+srcDt+" "+whereCond+"`");
    parDF.cache.createOrReplaceTempView(objNm+"_PAR");

    if     (genCd=="replication") genReplicate(repCnt)
    else if(genCd=="copy")        genCopy()

//    tDF.cache.createOrReplaceTempView("objNm");
//    spark.sql("SELECT DISTINCT GVHOST FROM TB_WL_URL_ACCESS WHERE GVHOST = 'www.sktmembership.co.kr:90'").take(100).foreach(println);
//
//    spark.sql("SELECT LENGTH(GVHOST) FROM TB_WL_URL_ACCESS GROUP BY LENGTH(GVHOST) ORDER BY LENGTH(GVHOST)").take(100).foreach(println);
//
//    spark.sql("SELECT GVHOST, count(*) cnt FROM TB_WL_URL_ACCESS GROUP BY GVHOST ORDER BY GVHOST").take(10000).foreach(println);
//
//    spark.sql("""
//    SELECT GVHOST, count(*) cnt
//    FROM   TB_WL_URL_ACCESS
//    GROUP BY GVHOST
//    HAVING  LENGTH(GVHOST) < 15
//    ORDER BY GVHOST
//    """).take(1).foreach(println);
//

  }

  def genReplicate(srcObjNm:String,repCnt:Int) = {
    var repQry = ""
    for(i <- 0 until repCnt) {
      if(i<repCnt-1) {
        repQry += "SELECT * FROM " + srcObjNm + " UNION ALL ";
      } else {
        repQry += "SELECT * FROM " + srcObjNm;
      }
    }
    MakeParquet.dfToParquet(srcObjNm,true,srcDt,repQry)
  }

  def genCopy() = {
//    val colArr = parDF.schema.fieldNames
//
//    var selectStat = "SELECT "
//    var colsStat = ""
//    for(i <- 0 until colArr.length) {
//      if(colArr(i) == "C_TIME") {
//        if(i>0) colsStat += ",'"+ srcDt + "' AS "+ colArr(i)
//        else    colsStat += " '"+ srcDt + "  AS "+ colArr(i)
//      } else {
//        if(i>0) colsStat += "," + colArr(i)
//        else    colsStat += colArr(i)
//      }
//    }

  }

  def setMemberVar(srcObjNm:String,srcDt:String,tarDt:String,tarObjNm:String,genCd:String,repCnt:Int) = {
    this.srcObjNm = srcObjNm
    this.srcDt    = srcDt
    this.tarObjNm = tarObjNm
    this.tarDt    = tarDt
    this.genCd    = genCd
    this.repCnt   = repCnt
  }














 */
