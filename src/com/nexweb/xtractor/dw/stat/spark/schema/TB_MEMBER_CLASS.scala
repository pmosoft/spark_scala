package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_MEMBER_CLASS {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("GVHOST"       ,StringType),
StructField("VHOST"        ,StringType),
StructField("C_TIME"       ,StringType),
StructField("V_ID"         ,StringType),
StructField("U_ID"         ,StringType),
StructField("T_ID"         ,StringType),
StructField("LOGIN_ID"     ,StringType),
StructField("SESSION_ID"   ,StringType),
StructField("TYPE"         ,StringType),
StructField("GRADE"         ,StringType),
StructField("GENDER"       ,StringType),
StructField("AGE"          ,StringType),
StructField("OS"           ,StringType),
StructField("OS_VER"       ,StringType),
StructField("MODEL"        ,StringType),
StructField("APP_VER"      ,StringType),
StructField("LOGIN_TYPE"   ,StringType),
StructField("AUTO_LOGIN_YN",StringType),
StructField("SITE_TYPE"    ,StringType),
StructField("OPT1"         ,StringType),
StructField("OPT2"         ,StringType),
StructField("OPT3"         ,StringType),
StructField("OPT4"         ,StringType),
StructField("OPT5"         ,StringType),
StructField("XTSUB_ID"     ,StringType),
StructField("STATIS_DATE"  ,StringType)
))
}

