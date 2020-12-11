package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * sparkstreaming整合kafka
  *
  * @author ceek
  * @create 2020-12-11 14:24
  **/
object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming04_Kafka");

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3));

    //启动后会反复消费kafka中的数据
    //从kafka中采集数据
    val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "10.10.32.60:2181",
      "sparkstreaming_demo", //分组id，唯一标识group_id
      Map("sparkstreaming_test" -> 3) //topic->partition
    )

    //直接答应数据
    createStream.print();

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}