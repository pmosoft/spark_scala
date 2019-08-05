package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType


object TB_URL_COMMENT_FRONT {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
StructField("VERSION_ID" ,StringType),
StructField("GVHOST"     ,StringType),
StructField("VHOST"      ,StringType),
StructField("URL"        ,StringType),
StructField("COMMENTS"   ,StringType),
StructField("REG_DATE"   ,StringType)
))
}

