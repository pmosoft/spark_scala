package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_WL_URL_ACCESS {
final val schema = StructType( Array(
StructField("GVHOST"       ,StringType),
StructField("VHOST"        ,StringType),
StructField("URL"          ,StringType),
StructField("V_ID"         ,StringType),
StructField("U_ID"         ,StringType),
StructField("T_ID"         ,StringType),
StructField("C_TIME"       ,StringType),
StructField("IP"           ,StringType),
StructField("PREV_URL"     ,StringType),
StructField("DUR_TIME"     ,StringType),
StructField("APPLY_STAT"   ,StringType),
StructField("P_ID"         ,StringType),
StructField("KEYWORD"      ,StringType),
StructField("AREA_CODE"    ,StringType),
StructField("PREV_DOMAIN"  ,StringType),
StructField("SESSION_ID"   ,StringType),
StructField("F_REF_HOST"   ,StringType),
StructField("F_REF_DOMAIN" ,StringType),
StructField("F_REF_CATE"   ,StringType),
StructField("USERAGENT"    ,StringType),
StructField("MOBILE_YN"    ,StringType),
StructField("OS"           ,StringType),
StructField("BROWSER"      ,StringType),
StructField("OS_VER"       ,StringType),
StructField("BROWSER_VER"  ,StringType),
StructField("XLOC"         ,StringType),
StructField("LANG"         ,StringType),
StructField("DEVICE_ID"    ,StringType),
StructField("URL_PARAM"    ,StringType),
StructField("LOGIN_TYPE"   ,StringType),
StructField("FRONT_URL"    ,StringType),
StructField("USER_URL"     ,StringType),
StructField("OPT_PARAM"    ,StringType),
StructField("USER_PARAM"   ,StringType),
StructField("SVC_UPDATE_ID",StringType),
StructField("SVC_ID"       ,StringType),
StructField("OPT1"         ,StringType),
StructField("OPT2"         ,StringType),
StructField("OPT3"         ,StringType),
StructField("OPT4"         ,StringType),
StructField("OPT5"         ,StringType),
StructField("XTSUB_ID"     ,StringType),
StructField("STATIS_DATE"  ,StringType)
))
}
