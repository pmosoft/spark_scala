package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_INNER_KW {
final val schema : StructType= StructType( Array(
StructField("GVHOST"       ,StringType),
StructField("VHOST"        ,StringType),
StructField("C_TIME"       ,StringType),
StructField("V_ID"         ,StringType),
StructField("U_ID"         ,StringType),
StructField("S_ID"         ,StringType),
StructField("URL"          ,StringType),
StructField("KEYWORD"      ,StringType),
StructField("TYPE"         ,StringType),
StructField("XTSUB_ID"     ,StringType)
))
}
