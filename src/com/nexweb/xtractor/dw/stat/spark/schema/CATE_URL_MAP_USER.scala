package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType


object CATE_URL_MAP_USER {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("GVHOST"     ,StringType),
StructField("VHOST"      ,StringType),
StructField("ANC_CATE_ID",StringType),
StructField("CATE_ID"    ,StringType),
StructField("URL"        ,StringType)
))
}

