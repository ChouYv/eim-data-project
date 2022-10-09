package org.sanofi.eim.spark.example

//import com.alibaba.fastjson.JSON
import org.sanofi.eim.spark.domain.{DqRuleDo, testDomain}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

object functionTest {

  def main(args: Array[String]): Unit = {
//    val ruleDo = new DqRuleDo()
    //      val jsonStr = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"arr\":[1,2,3]  }"
//    val jsonStr = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"arr\":[1,2,3],\"ruleCol\": [{\"A\":123},{\"B\":456}]  }"
//    val demoJson = "{\"arr\":[1,2,3],\"date\":\"2022-09-15\",\"ruleCol\":{\"colA\":{\"name\":\"张三\",\"age\":\"18\"},\"colB\":{\"name\":\"李四\",\"age\":\"26\"}},\"test\":\"abc\"}"

    //      val json = "{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\",\"RuleCol\": {\"A\": \"123\"}  }"

    //    val domain = JSON.parseObject(jsonStr, classOf[testDomain])
//        val domain = JSON.parseObject(demoJson, classOf[testDomain])
//    println(domain)
//    import collection.JavaConverters._
//    println(JSON.toJSONString(
//      new DqRuleDo(
//        "2022-09-15",
//        "ldgTable",
//        "stgTable",
//        "rejTable",
//        "ldgPkTable",
//        "stgPkTable",
//        "rejPkTable",
//        Array("k1","k2"),
//        Map("colA" -> Map("uniqueCheck" -> "Y", "enumCheck" -> "Y","enumRange"->"","nullCheck"->"Y","dateCheck"->"Y","dateFormatted"->"").asJava,
//          "colB" -> Map("uniqueCheck" -> "N", "enumCheck" -> "Y","enumRange"->"","nullCheck"->"Y","dateCheck"->"Y","dateFormatted"->"").asJava).asJava
//      )
//      , JSON.DEFAULT_GENERATE_FEATURE))
        val sss = "{\"jobDate\":\"2022-10-07\",\"ldgTableName\":\"ldg.cl_md_cl_content\",\"priKeyArr\":[\"contentid\"],\"rejTableName\":\"rej.ldg_cl_md_cl_content_rej\",\"ruleCol\":{\"fourmcode\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"is_deleted\":{\"dateCheck\":\"N\",\"intCheck\":\"Y\",\"enumRange\":\"0,1\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"Y\",\"nullCheck\":\"N\"},\"expiresdate\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"contentid\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"Y\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"Y\"},\"creatorname\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"creationtime\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"lastmodificationtime\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"title\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"lastmodifiername\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"contenttype\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"}},\"stgTableName\":\"stg.cl_md_cl_content\"}"
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val ruleDo = mapper.readValue(sss, classOf[DqRuleDo])
    println(ruleDo)




  }
}
