package org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Aggregate

/**
  * 强类型用户自定义聚合函数
  *
  * @author ceek
  * @create 2020-12-04 16:46
  **/
object SparkSQL08_UDAF_Class {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL08_UDAF_Class");

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._


  }
}

case class UserBean(name: String, age: BigInt)

case class AvgBuffer(sum: BigInt, count: Int)

class MyAgeAvgClassFunction extends Aggregate[] {

}
