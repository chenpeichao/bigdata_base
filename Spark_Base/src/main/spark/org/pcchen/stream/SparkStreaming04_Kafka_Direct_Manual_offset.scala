package org.pcchen.stream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 0.8版本手动维护offset
  *
  * @author ceek
  * @create 2021-01-18 10:34
  **/
object SparkStreaming04_Kafka_Direct_Manual_offset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming04_Kafka_Direct_Manual_offset").setMaster("local[*]")

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9093",
      ConsumerConfig.GROUP_ID_CONFIG -> "group_topic_spark_direct_auto_offset",
      //      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "(false:java.lang.Boolean)", //不生效
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //4.获取上一次启动最后保留的Offset=>getOffset(MySQL)
    val fromOffsets = Map(
      TopicAndPartition("topic_spark_direct_auto_offset", 0) -> 0l,
      TopicAndPartition("topic_spark_direct_auto_offset", 1) -> 0l,
      TopicAndPartition("topic_spark_direct_auto_offset", 2) -> 0l
    )

    //5.读取Kafka数据创建DStream
    val dataStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      fromOffsets,
      (m: MessageAndMetadata[String, String]) => m.message()
    )
    //    var offsetArray: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]();
    //    var offsetArray: ArrayBuffer[OffsetRange] = ArrayBuffer.empty[OffsetRange];

    //6.创建一个数组用于存放当前消费数据的offset信息
    var offsetArray = Array.empty[OffsetRange]

    //7.获取当前消费数据的offset信息
    val wordToCountDStream: DStream[(String, Int)] = dataStream.transform {
      row => {
        val offsetRanges: Array[OffsetRange] = row.asInstanceOf[HasOffsetRanges].offsetRanges
        //        for(offsetRange <- offsetRanges) {
        //          offsetArray += offsetRange
        //        }
        offsetArray = offsetRanges;
        row
      }
    }.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    var tmp = 3;

    //8.打印Offset信息
    wordToCountDStream.foreachRDD(rdd => {
      for (o <- offsetArray) {
        println(s"${o.topic}:${o.partition}:${o.fromOffset}:${o.untilOffset}")
      }
      tmp = 4;
      rdd.foreach(println)
    })

    //此方法只会执行一次，输出3
    println("--------" + tmp);

    //程序的最后，来更新offset的信息到外存中，手动维护offset
    ssc.start();
    ssc.awaitTermination();
  }
}
