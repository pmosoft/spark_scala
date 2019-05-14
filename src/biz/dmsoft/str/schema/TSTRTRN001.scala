package biz.dmsoft.str.schema

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TSTRTRN001 {

  val schema = StructType(Array(
       StructField("TRAN_DT"    ,StringType) // 거래일자
      ,StructField("TRAN_NO"    ,StringType) // 거래번호
      ,StructField("TRAN_TM"    ,StringType) // 거래시각
      ,StructField("CUST_NO"    ,StringType) // 고객번호
      ,StructField("CUST_CD"    ,StringType) // 고객구분코드
      ,StructField("CUST_CD_NM" ,StringType) // 고객구분코드명
      ,StructField("CUST_NM"    ,StringType) // 고객명
      ,StructField("TRAN_CD"    ,StringType) // 거래코드
      ,StructField("TRAN_CD_NM" ,StringType) // 거래코드명
      ,StructField("INOUT_CD"   ,StringType) // 입지코드
      ,StructField("INOUT_CD_NM",StringType) // 입지코드명
      ,StructField("TRAN_AMT"   ,StringType) // 거래금액
      ,StructField("ACCT_NO"    ,StringType) // 계좌번호
      ))
}

