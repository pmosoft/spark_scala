package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_SEG_URL_MAPPING {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("GVHOST"      ,StringType),
StructField("URL"         ,StringType),
StructField("MENU_ID"     ,StringType),
StructField("URL_COMMENTS",StringType),
StructField("CTIME"       ,StringType)
))
}

