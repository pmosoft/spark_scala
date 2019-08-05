package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_URL_ACCESS_IP_INFO {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("IP"             ,StringType),
StructField("COUNTRY"        ,StringType),
StructField("COUNTRY_NM"     ,StringType),
StructField("CITY_CD"        ,StringType),
StructField("CITY"           ,StringType),
StructField("ISP_NM"         ,StringType)
))
}
