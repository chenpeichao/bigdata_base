package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用SparkStreaming完成WordCount
  *
  * @author ceek
  * @create 2020-12-09 16:48
  **/
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //此处的local必须大于1才能生效
    //Spark配置对象
    val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[2]");

    //实时数据分析环境对象
    //采集周期：以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(3));

    //从指定的socket端口采集数据
    val textStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    //将数据扁平化，改变结构，并进行value值的累加
    val result: DStream[(String, Int)] = textStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //记过打印
    result.print();

    //启动采集器
    streamingContext.start();
    //Driver等待采集器的执行
    streamingContext.awaitTermination();
  }
}
