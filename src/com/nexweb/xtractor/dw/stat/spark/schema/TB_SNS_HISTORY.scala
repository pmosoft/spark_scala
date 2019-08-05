package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_SNS_HISTORY {
final val schema = StructType( Array(
StructField("GVHOST"       ,StringType),
StructField("VHOST"        ,StringType),
StructField("C_TIME"       ,StringType),
StructField("V_ID"         ,StringType),
StructField("U_ID"         ,StringType),
StructField("T_ID"         ,StringType),
StructField("PROD_ID"      ,StringType),
StructField("SNS_ID"       ,StringType),
StructField("OS"           ,StringType),
StructField("BROWSER"      ,StringType),
StructField("OS_VER"       ,StringType),
StructField("BROWSER_VER"  ,StringType),
StructField("XLOC"         ,StringType),
StructField("LANG"         ,StringType),
StructField("DEVICE_ID"    ,StringType),
StructField("XTSUB_ID"     ,StringType),
StructField("STATIS_DATE"  ,StringType)
))
}
