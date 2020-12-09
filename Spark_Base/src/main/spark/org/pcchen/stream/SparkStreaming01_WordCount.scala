package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author ceek
  * @create 2020-12-09 16:48
  **/
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setAppName("SparkStreaming01_WordCount").setMaster("local");

    val streamContext = new StreamingContext(sparkConf, Seconds(5))

    val textStream: ReceiverInputDStream[String] = streamContext.socketTextStream("localhost", 9999)
    textStream.foreachRDD(x => {
      println("-------------");
      println(x.collect().foreach(println))
    })

    streamContext.start();
    streamContext.awaitTermination();
  }
}
