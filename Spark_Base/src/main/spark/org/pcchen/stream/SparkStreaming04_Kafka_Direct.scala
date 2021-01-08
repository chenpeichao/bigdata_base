package org.pcchen.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author ceek
  * @create 2021-01-08 10:35
  **/
object SparkStreaming04_Kafka_Direct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming04_Kafka_Direct").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))

    val kafkaParams = Map(
      "bootstrap.servers" -> "10.10.32.60:9093",
      "group.id" -> "sparkstreaming_demo2",
      "auto.offset.reset" -> "smallest", //smallest   largest
      "auto.commit.enable" -> "true"
    )

    val topicSet = Set("sparkstreaming_test")

    val directStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topicSet)

    val result: DStream[(String, Int)] = directStream.flatMap(line => {
      line._2.split(" ").map((_, 1))
    }).reduceByKey(_ + _)
    /*val result: DStream[(String, Int)] = directStream.map {
      case x => x._2
    }.flatMap {
      line => {
        line.split(" ")
      }
    }.map((_, 1)).reduceByKey(_ + _)*/
    result.print()

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
