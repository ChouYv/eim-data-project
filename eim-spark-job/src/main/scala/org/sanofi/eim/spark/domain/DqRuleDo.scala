package org.sanofi.eim.spark.domain

import scala.collection.mutable


case class DqRuleDo(
                     jobDate: String,
                     ldgTableName: String,
                     stgTableName: String,
                     rejTableName: String,
                     ldgPkTableName: String,
                     stgPkTableName: String,
                     rejPkTableName: String,
                     priKeyArr: Array[String],
//                     RuleCol: util.Map[String, util.Map[String, String]]
                     ruleCol: mutable.HashMap[String, mutable.HashMap[String, String]]
                   )
