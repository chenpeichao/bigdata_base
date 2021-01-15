package org.pcchen.stream

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 0.8版本自动维护offset
  *
  * @author ceek
  * @create 2021-01-15 15:34
  **/
object SparkStreaming04_Kafka_Direct_Auto_offset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming04_Kafka_Direct_Auto_offset").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //    ssc.checkpoint("e:\\checkpoint")

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9093",
      ConsumerConfig.GROUP_ID_CONFIG -> "group_topic_spark_direct_auto_offset",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    val topicSet = Set("topic_spark_direct_auto_offset")

    val dataStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    dataStream.map {
      row => {
        (row._2, 1)
      }
    }.reduceByKey(_ + _).foreachRDD {
      row => {
        row.foreach(x => println(x._2 + "<-" + x._1))
      }
    }

    ssc.start();
    ssc.awaitTermination();
  }
}