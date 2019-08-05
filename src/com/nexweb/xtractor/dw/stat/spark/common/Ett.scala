package com.nexweb.xtractor.dw.stat.spark.common

import org.apache.spark.sql.SparkSession

object Ett {

  def readLocalFile() = {

     //spark.table(objNm).coalesce(1).write.csv("file:////home/xtractor/TB_MEMBER_CLASS_201812194.dat")
    //spark.table(objNm).write.coalesce(1).csv("file:////home/xtractor/TB_MEMBER_CLASS_201812194.dat")
    
    //spark.table(objNm).write.repartitions(1).csv("file:////home/xtractor/TB_MEMBER_CLASS_201812195.dat")
    //spark.table(objNm).write.csv("/user/xtractor/TB_MEMBER_CLASS_201812194.dat")
    
  }
  
  def readHdfs() = {
  }

  
}