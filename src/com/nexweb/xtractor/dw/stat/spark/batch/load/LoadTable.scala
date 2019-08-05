package com.nexweb.xtractor.dw.stat.spark.batch.load

import org.apache.spark.sql.SparkSession
import scala.sys.process

object LoadTable {

  def main(args: Array[String]): Unit = {

    //var statisDate = "20190519"; var statisType = "D"
    val spark = SparkSession.builder().appName("LoadTable").getOrCreate() //.config("spark.master","local")

  }

  def lodTable(
                spark      : SparkSession
              , objNm      : String
              , statisDate : String //D:yyyymmdd M:yyyymm W:yyyyww
              , statisType : String //ALL:전체 D:일별 M:월별 W:주별
              , colInfo    : String
              , whereCond  : String
              , reuseTf    : Boolean
              ) =  {

      val parquetPath = "parquet.`/user/xtractor/parquet/entity/"
      var qry = ""

      if(statisType=="ALL") {
          qry = "SELECT "+colInfo+" FROM "+parquetPath+objNm+"` "+whereCond
      } else if(statisType=="D") {
          qry = "SELECT "+colInfo+" FROM "+parquetPath+objNm+"/"+objNm+ "_" + statisDate+"` "+whereCond
      } else if(statisType=="M") {
          qry = "SELECT "+colInfo+" FROM "+parquetPath+objNm+"/"+objNm+ "_" + statisDate.substring(0,6)+"*` "+whereCond
      } else if(statisType=="W") {
          qry = "SELECT "+colInfo+" FROM "+parquetPath+objNm+"/"+objNm+ "_*` "+whereCond
      }

      println("-------------------------------------------------------");
      println(qry);
      println("-------------------------------------------------------");

      if(reuseTf) {
          if(spark.catalog.tableExists(objNm)) {
              println("reuse is true && table["+objNm+"] is already exist")
          } else {
              println("reuse is true && table["+objNm+"] is not exist && create table")
              val tDF = spark.sql(qry);tDF.cache.createOrReplaceTempView(objNm);tDF.count()
          }
      } else {
          println("reuse is false && && recreate table ["+objNm+"]")
          spark.sql("DROP TABLE IF EXISTS "+objNm)
          val tDF = spark.sql(qry);tDF.cache.createOrReplaceTempView(objNm);tDF.count()
      }
  }

  def lodPartColTable(spark:SparkSession,objNm:String, statisDate:String, statisType:String, colInfo:String, whereCond : String, reuseTf:Boolean) = {
      lodTable(spark,objNm, statisDate, statisType, colInfo, whereCond, reuseTf)
  }

  def lodAllColTable(spark:SparkSession,objNm:String, statisDate:String, statisType:String, whereCond : String, reuseTf:Boolean) = {
      var colInfo = "*"
      lodTable(spark,objNm, statisDate, statisType, colInfo, whereCond, reuseTf)
  }

  def lodAllColAllDataTable(spark:SparkSession,objNm:String, whereCond : String, reuseTf:Boolean) = {
      var statisType = "ALL"
      var statisDate = ""
      var colInfo = "*"
      lodTable(spark,objNm, statisDate, statisType, colInfo, whereCond, reuseTf)
  }


  def lodBasicLogTables(spark:SparkSession, statisDate:String, statisType:String) = {

    //------------------------------------------------
        println("lodBasicLogTables 시작 : "+statisDate);
    //------------------------------------------------
    lodRefererTable(spark,statisDate,statisType)
    lodAccessTable (spark,statisDate,statisType)
    lodMemberTable (spark,statisDate,statisType)
    lodProcessTable (spark,statisDate,statisType)
    lodSnsTable (spark,statisDate,statisType)
    //objNm = "TB_CS_PROCESS"

  }

  def lodRefererTable(spark:SparkSession, statisDate:String, statisType:String) = {
      var objNm     = "TB_WL_REFERER"
      var colInfo   = ""
      colInfo += "DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE"
      colInfo += ", GVHOST        "
      colInfo += ", VHOST         "
      colInfo += ",(CASE WHEN SUBSTR(URL,1,150)         IS NULL THEN '' ELSE URL         END) URL         "
      colInfo += ",(CASE WHEN HOST        IS NULL THEN '' ELSE HOST        END) HOST        "
      colInfo += ",(CASE WHEN DIR_CGI     IS NULL THEN '' ELSE DIR_CGI     END) DIR_CGI     "
      colInfo += ",(CASE WHEN KEYWORD     IS NULL THEN '' ELSE KEYWORD     END) KEYWORD     "
      colInfo += ", V_ID          "
      colInfo += ", U_ID          "
      colInfo += ", T_ID          "
      colInfo += ", C_TIME        "
      colInfo += ",(CASE WHEN DOMAIN      IS NULL THEN '' ELSE DOMAIN      END) DOMAIN    "
      colInfo += ",(CASE WHEN CATEGORY    IS NULL THEN '' ELSE CATEGORY    END) CATEGORY    "
      colInfo += ",(CASE WHEN V_IP IS NULL THEN '' ELSE V_IP END) V_IP "
      colInfo += ",(CASE WHEN SESSION_ID  IS NULL THEN '' ELSE SESSION_ID  END) SESSION_ID  "
      //colInfo += ", REF_URL       "
      //colInfo += ", REF_PARAM     "
      //colInfo += ", USER_AGENT    "
      colInfo += ",(CASE WHEN MOBILE_YN   IS NULL THEN '' ELSE MOBILE_YN   END) MOBILE_YN   "
      colInfo += ",(CASE WHEN OS          IS NULL THEN '' ELSE OS          END) OS          "
      colInfo += ",(CASE WHEN BROWSER     IS NULL THEN '' ELSE BROWSER     END) BROWSER     "
      colInfo += ",(CASE WHEN OS_VER      IS NULL THEN '' ELSE OS_VER      END) OS_VER      "
      colInfo += ",(CASE WHEN BROWSER_VER IS NULL THEN '' ELSE BROWSER_VER END) BROWSER_VER "
      colInfo += ",(CASE WHEN XLOC        IS NULL THEN '' ELSE XLOC        END) XLOC        "
      colInfo += ",(CASE WHEN LANG        IS NULL THEN '' ELSE LANG        END) LANG        "
      colInfo += ",(CASE WHEN DEVICE_ID   IS NULL THEN '' ELSE DEVICE_ID   END) DEVICE_ID   "
      //colInfo += ", URL_PARAM     "
      //colInfo += ", AREA_CODE     "
      //colInfo += ", CAMPAIGN_ID   "
      colInfo += ", LOGIN_TYPE    "
      colInfo += ", OPT2    "
      var whereCond = if(statisType=="D") "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') = '"+statisDate+"'" else /*M*/ ""
      lodPartColTable(spark,objNm,statisDate,statisType,colInfo,whereCond,true)
      //수동 배치 작업시 아래 명령어 수행
      //lodPartColTable(spark,"TB_WL_REFERER",statisDate,statisType,colInfo,whereCond,false)
  }

  def lodAccessTable(spark:SparkSession, statisDate:String, statisType:String) = {
      var objNm     = "TB_WL_URL_ACCESS"
      var colInfo   = ""
      colInfo += "DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE"
      colInfo += ", GVHOST    "
      colInfo += ", VHOST     "
      colInfo += ", SUBSTR(URL,1,150) AS URL       "
      colInfo += ", V_ID      "
      colInfo += ", U_ID      "
      colInfo += ", T_ID      "
      colInfo += ", C_TIME    "
      colInfo += ",(CASE WHEN IP IS NULL THEN '' ELSE IP END) IP "
      colInfo += ",(CASE WHEN PREV_URL IS NULL THEN '' ELSE PREV_URL END) PREV_URL "
      colInfo += ", DUR_TIME "
      //colInfo += ", APPLY_STAT "
      colInfo += ", P_ID      "
      colInfo += ",(CASE WHEN KEYWORD IS NULL THEN '' ELSE KEYWORD END) KEYWORD "
      colInfo += ",(CASE WHEN AREA_CODE IS NULL THEN '' ELSE AREA_CODE END) AREA_CODE "
      colInfo += ", PREV_DOMAIN "
      colInfo += ",(CASE WHEN SESSION_ID  IS NULL THEN '' ELSE SESSION_ID  END) SESSION_ID "
      colInfo += ",(CASE WHEN F_REF_HOST   IS NULL THEN '' ELSE F_REF_HOST   END) F_REF_HOST  "
      //colInfo += ", F_REF_DOMAIN "
      //colInfo += ", F_REF_CATE   "
      colInfo += ", USERAGENT    "
      colInfo += ",(CASE WHEN MOBILE_YN   IS NULL THEN '' ELSE MOBILE_YN   END) MOBILE_YN  "
      colInfo += ",(CASE WHEN OS          IS NULL THEN '' ELSE OS          END) OS         "
      colInfo += ",(CASE WHEN BROWSER     IS NULL THEN '' ELSE BROWSER     END) BROWSER    "
      colInfo += ",(CASE WHEN OS_VER      IS NULL THEN '' ELSE OS_VER      END) OS_VER     "
      colInfo += ",(CASE WHEN BROWSER_VER IS NULL THEN '' ELSE BROWSER_VER END) BROWSER_VER"
      colInfo += ",(CASE WHEN XLOC        IS NULL THEN '' ELSE XLOC        END) XLOC       "
      colInfo += ",(CASE WHEN LANG        IS NULL THEN '' ELSE LANG        END) LANG       "
      colInfo += ",(CASE WHEN DEVICE_ID   IS NULL THEN '' ELSE DEVICE_ID   END) DEVICE_ID  "
      //colInfo += ", URL_PARAM      "
      colInfo += ",(CASE WHEN LOGIN_TYPE  IS NULL THEN '' ELSE LOGIN_TYPE  END) LOGIN_TYPE "
      colInfo += ",(CASE WHEN FRONT_URL IS NULL THEN '' ELSE FRONT_URL END) FRONT_URL "
      colInfo += ", USER_URL       "
      colInfo += ", OPT_PARAM      "
      //colInfo += ", USER_PARAM     "
      //colInfo += ", SVC_UPDATE_ID  "
      colInfo += ", SVC_ID    "
      colInfo += ", OPT2    "
      colInfo += ", OPT3    "
      var whereCond = if(statisType=="D") "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') = '"+statisDate+"'" else /*M*/ ""
      lodPartColTable(spark,objNm,statisDate,statisType,colInfo,whereCond,true)
      //lodPartColTable(spark,"TB_WL_URL_ACCESS",statisDate,statisType,colInfo,whereCond,false)
  }

  def lodMemberTable(spark:SparkSession, statisDate:String, statisType:String) = {
      var objNm     = "TB_MEMBER_CLASS"
      var colInfo   = ""
      colInfo += "DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE"
      colInfo += ", GVHOST       "
      colInfo += ", VHOST        "
      colInfo += ", C_TIME       "
      colInfo += ", V_ID         "
      colInfo += ", U_ID         "
      colInfo += ", T_ID         "
      colInfo += ", LOGIN_ID     "
      colInfo += ", (CASE WHEN SESSION_ID  	 IS NULL THEN '' ELSE SESSION_ID  	END) SESSION_ID    "
      colInfo += ", (CASE WHEN TYPE        	 IS NULL THEN '' ELSE TYPE        	END) TYPE          "
      colInfo += ", (CASE WHEN GENDER      	 IS NULL THEN '' ELSE GENDER      	END) GENDER        "
      colInfo += ", (CASE WHEN AGE         	 IS NULL THEN '' ELSE AGE         	END) AGE           "
      //colInfo += ", (CASE WHEN GRADE       	 IS NULL THEN '' ELSE GRADE       	END) GRADE     		 "
      colInfo += ", '' AS GRADE   "
      colInfo += ", (CASE WHEN OS            IS NULL THEN 'ETC' ELSE OS            END) OS          "
      //colInfo += ", (CASE WHEN OS_VER        IS NULL THEN '' ELSE OS_VER        END) OS_VER      "
      colInfo += ", (CASE WHEN MODEL         IS NULL THEN '' ELSE MODEL         END) MODEL         "
      colInfo += ", (CASE WHEN APP_VER       IS NULL THEN '' ELSE APP_VER       END) APP_VER       "
      colInfo += ", (CASE WHEN LOGIN_TYPE    IS NULL THEN '' ELSE LOGIN_TYPE    END) LOGIN_TYPE    "
      colInfo += ", (CASE WHEN AUTO_LOGIN_YN IS NULL THEN '' ELSE AUTO_LOGIN_YN END) AUTO_LOGIN_YN "
      //colInfo += ", (CASE WHEN SITE_TYPE	 	 IS NULL THEN '' ELSE SITE_TYPE 	  END) SITE_TYPE     "
      //colInfo += ", (CASE WHEN OPT3          IS NULL THEN '' ELSE OPT3          END) OPT3    			 "
      //colInfo += ", (CASE WHEN OPT4          IS NULL THEN '' ELSE OPT4          END) OPT4    			 "
      colInfo += ", '' AS OPT3 "
      colInfo += ", '' AS OPT4 "
      var whereCond = if(statisType=="D") "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') = '"+statisDate+"'" else /*M*/ ""
      lodPartColTable(spark,objNm,statisDate,statisType,colInfo,whereCond,true)
      //lodPartColTable(spark,"TB_MEMBER_CLASS",statisDate,statisType,colInfo,whereCond,false)
  }

  def lodProcessTable(spark:SparkSession, statisDate:String, statisType:String) = {
      var objNm     = "TB_CS_CODE_PROCESS"
      var colInfo   = ""
      colInfo += "DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE"
      colInfo += ", GVHOST       "
      colInfo += ", VHOST        "
      colInfo += ", C_TIME       "
      colInfo += ", V_ID         "
      colInfo += ", U_ID         "
      colInfo += ", T_ID         "
      colInfo += ", E_ID         "
      colInfo += ", CS_ID         "
      colInfo += ", LOWER(ACTION)    AS ACTION    "
      colInfo += ", OS           "
      colInfo += ", BROWSER      "
      colInfo += ", OS_VER       "
      colInfo += ", BROWSER_VER  "
      colInfo += ", XLOC         "
      colInfo += ", LANG         "
      colInfo += ", DEVICE_ID    "
      colInfo += ", SVC_UPDATE_ID "
      var whereCond = if(statisType=="D") "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') = '"+statisDate+"'" else /*M*/ ""
      lodPartColTable(spark,objNm,statisDate,statisType,colInfo,whereCond,true)
  }

  def lodSnsTable(spark:SparkSession, statisDate:String, statisType:String) = {
      var objNm     = "TB_SNS_HISTORY"
      var colInfo   = ""
      colInfo += "DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') AS STATIS_DATE"
      colInfo += ", GVHOST       "
      colInfo += ", VHOST        "
      colInfo += ", C_TIME       "
      colInfo += ", V_ID         "
      colInfo += ", U_ID         "
      colInfo += ", T_ID         "
      colInfo += ", PROD_ID      "
      colInfo += ", SNS_ID       "
      colInfo += ", OS           "
      colInfo += ", BROWSER      "
      colInfo += ", OS_VER       "
      colInfo += ", BROWSER_VER  "
      colInfo += ", XLOC         "
      colInfo += ", LANG         "
      colInfo += ", DEVICE_ID    "
      var whereCond = if(statisType=="D") "WHERE DATE_FORMAT(TO_DATE(C_TIME),'yyyyMMdd') = '"+statisDate+"'" else /*M*/ ""
      lodPartColTable(spark,objNm,statisDate,statisType,colInfo,whereCond,true)
  }


}