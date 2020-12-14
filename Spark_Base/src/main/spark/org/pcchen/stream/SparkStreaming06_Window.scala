package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author ceek
  * @create 2020-12-14 11:47
  **/
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming06_Window").setMaster("local[3]");

    val sparkStream = new StreamingContext(sparkConf, Seconds(3))


    //    val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
    //      sparkStream,
    //      "10.10.32.60:2181",
    //      "sparkstreaming_demo",
    //      Map("sparkstreaming_test" -> 3)
    //    )
    //    createStream.print();

    val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(sparkStream, "10.10.32.60:2181", "sparkstreaming_demo", Map("sparkstreaming_test" -> 3))

    //两个参数分别为滑动窗口大小和步长，大小均为采集周期的整数倍
    val windowDStream: DStream[(String, String)] = createStream.window(Seconds(6), Seconds(3))

    val result: DStream[(String, Int)] = windowDStream.flatMap(line => line._2.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print();

    sparkStream.start();
    sparkStream.awaitTermination();
  }
}
