package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_CS_CODE_INFO {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("VERSION_ID"  ,StringType),
StructField("GVHOST"      ,StringType),
StructField("VHOST"       ,StringType),
StructField("E_ID"        ,StringType),
StructField("OS_CODE"     ,StringType),
StructField("SS_CODE"     ,StringType),
StructField("SS_NAME"     ,StringType),
StructField("MENU_CODE"   ,StringType),
StructField("MENU_NAME"   ,StringType),
StructField("TARGET_CODE" ,StringType),
StructField("ACTION"      ,StringType),
StructField("MENU_DEPTH1" ,StringType),
StructField("MENU_DEPTH2" ,StringType),
StructField("MENU_DEPTH3" ,StringType),
StructField("MENU_DEPTH4" ,StringType),
StructField("MENU_DEPTH5" ,StringType),
StructField("MENU_DEPTH6" ,StringType)
))
}

















