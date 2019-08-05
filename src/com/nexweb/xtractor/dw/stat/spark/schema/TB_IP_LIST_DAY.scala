package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_IP_LIST_DAY {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("IP"       ,StringType),
StructField("IP_CD"    ,StringType)
))
}

