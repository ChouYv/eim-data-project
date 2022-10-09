package org.sanofi.eim.spark.example

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.math.random
import org.sanofi.eim.spark.domain.testDomain

/**
 * @project eim-data-project
 * @description ${description}
 * @author yahui
 * @date 2022/9/26 16:32:02
 * @version 1.0
 */
object test01 extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

//    val domain = JSON.parseObject(args(1), classOf[testDomain])
//
//    logError(domain.toString)
//    logError(args.mkString(","))
//    logError(args(1))
    val slices = if (args.length > 0) args(0).toInt else 200
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    logInfo(s"Pi is roughly ${4.0 * count / (n - 1)}")
    logWarning("~~~~~~1~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    logError("~~~~~~~~2~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    spark.stop()



  }
}
