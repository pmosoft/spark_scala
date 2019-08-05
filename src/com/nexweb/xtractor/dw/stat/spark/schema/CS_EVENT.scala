package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CS_EVENT {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("E_ID",StringType)
))
}

