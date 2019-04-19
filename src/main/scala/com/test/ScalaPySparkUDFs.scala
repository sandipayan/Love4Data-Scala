package com.test

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.mutable


object ScalaPySparkUDFs {

  def testFunction1(y: mutable.WrappedArray[Row]): Long = {

  val z = y.map(r => r.getAs[Long](0))

    print(z)
    z(0)
  }

  def getFun(): UserDefinedFunction = udf(testFunction1 _ )
}