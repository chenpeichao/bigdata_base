package org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  *
  *
  * @author ceek
  * @create 2020-12-04 15:42
  **/
object SparkSQL06_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL06_UDAF");

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._

    var listRDD = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 23), (3, "wangwu", 18)));
    val df: DataFrame = listRDD.toDF("id", "name", "age");

    val avgUDAF = new MyAgeAvgFunction();

    spark.udf.register("avgAge", avgUDAF);

    df.createOrReplaceTempView("user");

    spark.sql("select avgAge(age) from user").show();

    spark.stop();
  }
}

/**
  * 生命用户自定义聚合函数
  * 1)、继承UserDefinedAggregateFunction
  * 2)、实现方法
  */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  //函数输入的数据类型
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  //函数是否稳定：函数用于对输入数据进行一致性检验，是一个布尔值，当为true时，表示对于同样的输入会得到同样的输出。因为对于同样的score输入，肯定要得到相同的score平均值，所以定义为true
  override def deterministic: Boolean = true

  //计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l;
    buffer(1) = 0l;
  }

  //根据查询结果更新缓冲区数据----input参数标识输出，由inputschema返回值知晓为sum
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //对sum值进行累加
    buffer(0) = buffer.getLong(0) + input.getLong(0);
    //对count值进行加1
    buffer(1) = buffer.getLong(1) + 1;
  }

  //对多个节点的缓冲区数据进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0);
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1);
  }

  //计算最后一步的结果值
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0) / buffer.getLong(1).toDouble
  }
}