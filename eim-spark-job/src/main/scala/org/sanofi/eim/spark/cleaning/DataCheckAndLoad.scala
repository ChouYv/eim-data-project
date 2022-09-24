package org.sanofi.eim.spark.cleaning

import org.apache.spark.sql.SparkSession

object DataCheckAndLoad {
  def main(args: Array[String]): Unit = {



    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")





    spark.stop()
  }
}
