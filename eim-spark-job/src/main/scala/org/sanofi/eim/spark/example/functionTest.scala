package org.sanofi.eim.spark.example

import com.alibaba.fastjson.JSON
import org.sanofi.eim.spark.domain.testDomain

object functionTest {

  def main(args: Array[String]): Unit = {
//      val jsonStr = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"arr\":[1,2,3]  }"
      val jsonStr = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"arr\":[1,2,3],\"RuleCol\": [{\"A\": 123},{\"B\": 456}]  }"
//      val json = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"RuleCol\": {\"A\": \"123\"}  }"


    println("Json String:--------")
    println(jsonStr)
    println(jsonStr.getClass)

    val domain = JSON.parseObject(jsonStr, classOf[testDomain])


    println("Parse Json:--------")
    val nObject = JSON.parseObject(jsonStr)
    println(nObject)
    println(nObject.getClass)


    println("获取map==============")
    val stringToObject = nObject.getObject("RuleCol", classOf[Array[Map[String, Object]]])
    println(stringToObject)
    println(domain)
  }
}
