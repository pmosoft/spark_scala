package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_NCATE_MAP_FRONT {
final val schema : StructType= StructType( Array(
StructField("VERSION_ID"  ,StringType),
StructField("GVHOST"      ,StringType),
StructField("VHOST"       ,StringType),
StructField("CATE_ID"     ,StringType),
StructField("PRNT_CATE_ID",StringType),
StructField("CATE_NM"     ,StringType),
StructField("CATE_COMMENT",StringType),
StructField("DEPTH_LEV"   ,StringType),
StructField("SEQ"         ,StringType),
StructField("LEAF_YN"     ,StringType),
StructField("USE_YN"      ,StringType),
StructField("HIDDEN_YN"   ,StringType),
StructField("REG_DATE"    ,StringType),
StructField("REG_USER"    ,StringType),
StructField("MOD_DATE"    ,StringType),
StructField("MOD_USER"    ,StringType)
))
}
