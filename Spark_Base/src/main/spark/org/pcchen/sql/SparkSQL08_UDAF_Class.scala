package org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

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

    //创建聚合函数对象
    val myAgeAvgClassFunction = new MyAgeAvgClassFunction();

    //将聚合函数转换为查询列
    val myDefFunc: TypedColumn[UserBean, Double] = myAgeAvgClassFunction.toColumn.name("avgAge");

    val df: DataFrame = spark.read.json("Spark_Base/in/jsonFile/user.json");
    val ds: Dataset[UserBean] = df.as[UserBean]

    //应用函数
    ds.select(myDefFunc).show();

    //释放资源
    spark.stop();
  }
}

case class UserBean(username: String, age: BigInt)

case class AvgBuffer(var sum: BigInt, var count: Int)

/**
  * 声明用户自定义聚合函数(强类型)
  * 1) 继承Aggregator，设定泛型
  * 2) 实现方法
  */
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0);
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age;
    b.count += 1;

    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum;
    b1.count = b1.count + b2.count;
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
