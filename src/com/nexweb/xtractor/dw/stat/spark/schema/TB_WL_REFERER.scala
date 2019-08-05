package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_WL_REFERER {
final val schema : StructType= StructType( Array(
StructField("GVHOST"       ,StringType, nullable = false),
StructField("VHOST"        ,StringType, nullable = false),
StructField("URL"          ,StringType, nullable = false),
StructField("HOST"         ,StringType, nullable = false),
StructField("DIR_CGI"      ,StringType, nullable = false),
StructField("KEYWORD"      ,StringType, nullable = false),
StructField("V_ID"         ,StringType, nullable = false),
StructField("U_ID"         ,StringType, nullable = false),
StructField("T_ID"         ,StringType, nullable = false),
StructField("C_TIME"       ,StringType, nullable = false),
StructField("DOMAIN"       ,StringType, nullable = false),
StructField("CATEGORY"     ,StringType, nullable = false),
StructField("V_IP"         ,StringType, nullable = false),
StructField("SESSION_ID"   ,StringType, nullable = false),
StructField("REF_URL"      ,StringType, nullable = false),
StructField("REF_PARAM"    ,StringType, nullable = false),
StructField("USER_AGENT"   ,StringType, nullable = false),
StructField("MOBILE_YN"    ,StringType, nullable = false),
StructField("OS"           ,StringType, nullable = false),
StructField("BROWSER"      ,StringType, nullable = false),
StructField("OS_VER"       ,StringType, nullable = false),
StructField("BROWSER_VER"  ,StringType, nullable = false),
StructField("XLOC"         ,StringType, nullable = false),
StructField("LANG"         ,StringType, nullable = false),
StructField("DEVICE_ID"    ,StringType, nullable = false),
StructField("URL_PARAM"    ,StringType, nullable = false),
StructField("AREA_CODE"    ,StringType, nullable = false),
StructField("CAMPAIGN_ID"  ,StringType, nullable = false),
StructField("LOGIN_TYPE"   ,StringType, nullable = false),
StructField("OPT1"         ,StringType, nullable = false),
StructField("OPT2"         ,StringType, nullable = false),
StructField("OPT3"         ,StringType, nullable = false),
StructField("OPT4"         ,StringType, nullable = false),
StructField("OPT5"         ,StringType, nullable = false),
StructField("XTSUB_ID"     ,StringType, nullable = false),
StructField("STATIS_DATE"  ,StringType, nullable = false)
))
}