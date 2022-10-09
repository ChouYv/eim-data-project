package org.sanofi.eim.spark.domain

import scala.beans.BeanProperty
import java.util
case class testDomain(
                       @BeanProperty
                       var test: String,
                       @BeanProperty
                       var date: String,
                       @BeanProperty
                       var arr: Array[Integer],
                       @BeanProperty
                       var ruleCol: util.Map[String, util.Map[String,String]]
                     )
