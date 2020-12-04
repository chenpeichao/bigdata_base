package org.pcchen.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.{SparkSession, _}

/**
  *
  *
  * @author ceek
  * @create 2020-12-04 16:23
  **/
object SparkSQL07_UDAF_MAX {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local");
    sparkConf.setAppName("SparkSQL07_UDAF_MAX");

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import spark.implicits._

    var listRDD = spark.sparkContext.makeRDD(List((1, "zhangsan", 27), (1, "zhangsan", 30), (2, "lisi", 23), (3, "wangwu", 18)));
    val df: DataFrame = listRDD.toDF("id", "name", "age");

    //创建自定义聚合函数
    val maxAgeUDAF = new MyMaxAgeUDAF();

    //注册自定义聚合函数
    spark.udf.register("maxAge", maxAgeUDAF);

    //创建dataFrame的表，进行sparkSql查询
    df.createOrReplaceTempView("user");

    //查询sql
    spark.sql("select maxAge(age) from user").show();

    spark.stop();
  }
}

class MyMaxAgeUDAF extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //缓冲区数据类型
  override def bufferSchema: StructType = {
    new StructType().add("maxAge", LongType)
  }

  //返回值数据类型
  override def dataType: DataType = {
    LongType
  }

  //数据是否稳定
  override def deterministic: Boolean = true

  //初始化缓冲区数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l;
  }

  //输入数据到缓冲区，并更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (buffer.getLong(0) > input.getLong(0)) {
      buffer(0) = buffer.getLong(0)
    } else {
      buffer(0) = input.getLong(0)
    }
  }

  //合并不同分区的计算结果
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1.getLong(0) > buffer2.getLong(0)) {
      buffer1(0) = buffer1.getLong(0)
    } else {
      buffer1(0) = buffer2.getLong(0)
    }
  }

  //计算结果进行最后result的计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}