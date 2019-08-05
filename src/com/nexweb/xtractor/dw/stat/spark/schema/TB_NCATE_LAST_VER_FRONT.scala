package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType


object TB_NCATE_LAST_VER_FRONT {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("VERSION_ID",StringType),
StructField("GVHOST"    ,StringType),
StructField("VHOST"     ,StringType),
StructField("REG_DATE"  ,StringType),
StructField("REG_USER"  ,StringType),
StructField("MOD_DATE"  ,StringType),
StructField("MOD_USER"  ,StringType)
))
}

