package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_SEG_MENU {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("GVHOST"     ,StringType),
StructField("MENU_ID"    ,StringType),
StructField("COMMENTS"   ,StringType),
StructField("USE_FLAG"   ,StringType),
StructField("DAY"        ,StringType),
StructField("WEEK"       ,StringType),
StructField("MONTH"      ,StringType),
StructField("CTIME"      ,StringType),
StructField("REQ_USER_ID",StringType)
))
}

