package org.sanofi.eim.spark.domain

case class DqRuleTable(
                        ldgTableName: String,
                        stgTableName: String,
                        rejTableName: String,
                        ldgPkTableName: String,
                        stgPkTableName: String,
                        rejPkTableName: String,
                        RuleCol:Map[String,Map[String,String]]
                      )
