package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CS_CODE {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("CS_ID",StringType)
))
}

