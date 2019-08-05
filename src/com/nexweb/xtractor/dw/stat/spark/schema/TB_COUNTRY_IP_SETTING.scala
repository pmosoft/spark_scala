package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_COUNTRY_IP_SETTING {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("START_IP"          ,StringType),
StructField("END_IP"            ,StringType),
StructField("COUNTRY"           ,StringType),
StructField("COUNTRY_NM"        ,StringType),
StructField("CITY"              ,StringType),
StructField("ISP_NM"            ,StringType),
StructField("START_IP_NUM"      ,StringType),
StructField("END_IP_NUM"        ,StringType)
))
}
