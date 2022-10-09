package org.sanofi.eim.spark.cleaning

import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.alibaba.fastjson.JSON
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.sanofi.eim.spark.domain.DqRuleDo
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}


object DataCheckAndLoad extends Logging with Serializable {
  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {
    //获取执行参数
    //    this.dqRuleDo = JSON.parseObject(args(0), classOf[DqRuleDo])
    //    val sss = "{\"jobDate\":\"2022-10-07\",\"ldgTableName\":\"ldg.cl_md_cl_content\",\"priKeyArr\":[\"contentid\"],\"rejTableName\":\"rej.ldg_cl_md_cl_content_rej\",\"ruleCol\":{\"fourmcode\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"is_deleted\":{\"dateCheck\":\"N\",\"intCheck\":\"Y\",\"enumRange\":\"0,1\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"Y\",\"nullCheck\":\"N\"},\"expiresdate\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"contentid\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"Y\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"Y\"},\"creatorname\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"creationtime\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"lastmodificationtime\":{\"dateCheck\":\"Y\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd HH:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"title\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"lastmodifiername\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"},\"contenttype\":{\"dateCheck\":\"N\",\"intCheck\":\"N\",\"uniqueCheck\":\"N\",\"dateFormatted\":\"yyyy-MM-dd hh:mm:ss\",\"enumCheck\":\"N\",\"nullCheck\":\"N\"}},\"stgTableName\":\"stg.cl_md_cl_content\"}"
    //    dqRuleDo = JSON.parseObject(sss, classOf[DqRuleDo])
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val dqRuleDo = mapper.readValue(args(0), classOf[DqRuleDo])
    //创建session
    val spark = SparkSession
      .builder()
      .appName("dq")
      .enableHiveSupport()
      //      .master("local[2]")
      //      .config(new SparkConf().setJars( Seq{"/home/hadoop/IdeaProjects/eim-data-project/eim-spark-job/target/eim-spark-job-1.0-SNAPSHOT.jar"}))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    this.spark = spark


    dq(dqRuleDo)

    spark.stop()
  }

  /*
   * @Author yav
   * @Description //TODO  获取DF 默认纯内存
   * @param tableName
   * @param jobDate
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @Date  2022-10-06 11:19:24
   **/
  def getLdgOrLdgPkDf(tableName: String, jobDate: String): DataFrame = {
    val getLdgDfSql = s"select * from $tableName where eim_dt='$jobDate'"
    logError(getLdgDfSql)

    val df1 = spark.sql(getLdgDfSql)

    val columns: Array[String] = df1.columns.filter(_ != "eim_dt")
    //TODO 改为直接从文件创建df
    df1.filter(x => {
      var target = 0
      for (elem <- columns) {
        if (null == x.getAs(elem) || x.getAs(elem).toString.toLowerCase() != elem) target = target + 1
      }
      if (target == 0) false else true
    }).persist(StorageLevel.MEMORY_ONLY)

  }

  def dq(dqRuleDo: DqRuleDo): Unit = {
    val spark = this.spark
    import spark.implicits._
    val ldgDf = getLdgOrLdgPkDf(dqRuleDo.ldgTableName, dqRuleDo.jobDate)
    logError(ldgDf.count().toString)
    //    是否进行唯一校验

    var duplicateFieldArr: Array[String] = Array()
    if (0 != dqRuleDo.priKeyArr.length) {
      logError(dqRuleDo.priKeyArr.length.toString)
      duplicateFieldArr = ldgDf.selectExpr("concat(" + dqRuleDo.priKeyArr.mkString(",") + ") as key").groupBy("key").count()
        .where("count > 1").map(x => if (x.get(0) != null) x.get(0).toString else "").collect()
    }

    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    val schema: StructType = ldgDf.schema
      .add("eim_flag", StringType)


    val columns: Array[String] = ldgDf.columns.filter(_ != "eim_dt")

    val checkDF = ldgDf.map(
      row => {
        val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
        var errLine: String = ""

        var flagArr: Array[String] = Array()
        var initInt = 0
        for (elem <- columns) {
          // TODO 判断是否错行(后续从CSV直接进行校验)
          if (null == row.getAs(elem)) {
            if (errLine == "") errLine = "错行数据"
          }

          val ruleMap = dqRuleDo.ruleCol.get(elem)

          //          空值校验    所有校验不校验空值
          if (null == row.getAs(elem) || "<None>" == row.getAs(elem) || row.getAs(elem).toString.isEmpty) {
            if ("Y" == ruleMap.get("nullCheck")) flagArr = flagArr :+ s"${elem}列未通过[非空检查]"
          } else {
            //      时间校验
            if ("Y" == ruleMap.get("dateCheck")) {
              if (!checkTimestamp(row.getAs(elem).toString, ruleMap.get("dateFormatted"))) {
                flagArr = flagArr :+ s"${elem}列未通过[数据格式校验-时间格式] errValue->${row.getAs(elem).toString}"
              } else {
                buffer.update(initInt, updateTimestamp(row.getAs(elem).toString, ruleMap.get("dateFormatted")))
              }
            }
            //    整型校验
            if ("Y" == ruleMap.get("intCheck")) {
              if (!checkInt(row.getAs(elem).toString)) {
                flagArr = flagArr :+ s"${elem}列未通过[数据格式校验-数值格式] errValue->${row.getAs(elem).toString}"
              }
            }
            //    枚举值校验
            if ("Y" == ruleMap.get("enumCheck")) {
              if (!checkEnum(row.getAs(elem).toString, ruleMap.get("enumRange"))) {
                flagArr = flagArr :+ s"${elem}列未通过[枚举值检查] errValue->${row.getAs(elem).toString},range->${ruleMap.get("enumRange")}"
              }
            }
          }
          initInt = initInt + 1
        }
        //添加唯一性校验结果
        if (0 != dqRuleDo.priKeyArr.length) {
          var key = ""
          for (i <- dqRuleDo.priKeyArr.indices) {
            key = key + row.get(i)
          }
          if (duplicateFieldArr.contains(key)) flagArr = flagArr :+ s"主键未通过[唯一性校验]"
        }

        if (errLine != "") flagArr = flagArr :+ errLine
        val flagStr: String = if (!flagArr.isEmpty) flagArr.mkString("||") else ""
        buffer.append(flagStr)
        val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
        newRow
      }
    )(RowEncoder(schema))


    //    插入操作
    val stgAndRejArr: Array[String] = checkDF.columns.filter(!Array("eim_dt", "eim_flag").contains(_))
    checkDF.createOrReplaceTempView("ldgToStgTable")

    val sumLong: Long = checkDF.count()
    logWarning(s"${dqRuleDo.ldgTableName}表总数据量:" + sumLong + "行")

    val insertStgSql = s"insert overwrite table ${dqRuleDo.stgTableName} partition(eim_dt='${dqRuleDo.jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")} from ldgToStgTable where eim_flag ='' "
    logWarning(insertStgSql)
    spark.sql(insertStgSql)
    val stgLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")} from ldgToStgTable where eim_flag ='' ").count()
    logWarning(s"插入到${dqRuleDo.stgTableName}表总数据量:" + stgLong + "行")

    val insertRejSql = s"insert overwrite table ${dqRuleDo.rejTableName} partition(eim_dt='${dqRuleDo.jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")},eim_flag from ldgToStgTable where (${stgAndRejArr.map(_ + " is not null ").mkString(" or ")}) and eim_flag <>'' "
    logWarning(insertRejSql)
    spark.sql(insertRejSql)
    val rejLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},eim_flag from ldgToStgTable where (${stgAndRejArr.map(_ + " is not null ").mkString(" or ")}) and eim_flag <>'' ").count()
    logWarning(s"插入到${dqRuleDo.rejTableName}表总数据量:" + rejLong + "行")

    val nullLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},eim_flag from ldgToStgTable where (${stgAndRejArr.map(_ + " is  null ").mkString(" and ")}) and eim_flag <>''").count()
    logWarning(s"${dqRuleDo.ldgTableName}表数据因错行产生空行量:" + nullLong + "行")
  }


  def checkInt(str: String): Boolean = {
    var flag: Boolean = true
    for (i <- 0 until str.length) {
      if ("0123456789".indexOf(str.charAt(i)) < 0) flag = false
    }
    flag
  }

  def checkTimestamp(str: String, dateFormat: String): Boolean = {
    var flag: Boolean = true
    if (str.length >= dateFormat.length) {
      val newStr: String = str.substring(0, dateFormat.length)
      val formatStan = new SimpleDateFormat(dateFormat)
      try {
        flag = newStr.equals(formatStan.format(formatStan.parse(newStr)))
      } catch {
        case _: Throwable => return false
      }
    } else flag = false
    flag

  }


  def updateTimestamp(str: String, dateFormatted: String): String = {
    val newStr: String = str.substring(0, dateFormatted.length)
    val formatStan = new SimpleDateFormat(dateFormatted)
    val yyyyMMddHHmmssFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    yyyyMMddHHmmssFormat.format(formatStan.parse(newStr))
    //    flag = newStr.equals(formatStan.format(formatStan.parse(newStr)))
    //
    //
    //
    //    var newStr = str
    //    if (str.contains("/")) {
    //      val format2 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    //      val format3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //
    //      val date2: Date = format2.parse(str)
    //      newStr = format3.format(date2)
    //    }

    //    if (str.length == "yyyy-MM-dd".length) newStr = str + " 00:00:00"
    //    newStr
  }


  def checkEnum(value: String, range: String): Boolean = {

    val enumRange: Array[String] = range.split(",")
    enumRange.contains(value)
  }

}
