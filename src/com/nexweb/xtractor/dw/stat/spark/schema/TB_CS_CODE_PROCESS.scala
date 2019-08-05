package com.nexweb.xtractor.dw.stat.spark.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TB_CS_CODE_PROCESS {
//def schema(): StructType = StructType( Array(
final val schema : StructType= StructType( Array(
//StructField("C_TIME"        ,StringType),
//StructField("L_ID"          ,StringType),
//StructField("V_ID"          ,StringType),
//StructField("E_ID"          ,StringType),
//StructField("CS_ID"         ,StringType),
//StructField("P_ID"          ,StringType),
//StructField("LOGIN_ID"      ,StringType),
//StructField("ACTION"        ,StringType),
//StructField("SVC_UPDATE_ID" ,StringType),
//StructField("STATIS_DATE"  ,StringType)
    
StructField("GVHOST"       ,StringType),
StructField("VHOST"        ,StringType),
StructField("C_TIME"       ,StringType),
StructField("V_ID"         ,StringType),
StructField("U_ID"         ,StringType),
StructField("T_ID"         ,StringType),
StructField("E_ID"         ,StringType),
StructField("ACTION"       ,StringType),
StructField("OS"           ,StringType),
StructField("BROWSER"      ,StringType),
StructField("OS_VER"       ,StringType),
StructField("BROWSER_VER"  ,StringType),
StructField("XLOC"         ,StringType),
StructField("LANG"         ,StringType),
StructField("DEVICE_ID"    ,StringType),
StructField("CS_ID"        ,StringType),
StructField("SVC_UPDATE_ID" ,StringType)
))
}




