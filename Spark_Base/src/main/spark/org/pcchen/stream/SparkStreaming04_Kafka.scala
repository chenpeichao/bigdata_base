package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkstreaming整合kafka
  * receive模式
  *
  * @author ceek
  * @create 2020-12-11 14:24
  **/
object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    /*val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming04_Kafka");

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3));

    /*val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreaming_demo",
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9092"
    )*/

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
    streamingContext.awaitTermination();*/

    val sparkConf = new SparkConf().setAppName("").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "10.10.32.60:2181,10.10.32.61:2181,10.10.32.62:2181", //zookeeper的地址中间不能添见空格
      "sparkstreaming_demo2",
      Map("sparkstreaming_test" -> 3)
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] // 定义一个集合，用来存放偏移量的范围
    createStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }.foreachRDD {
      rdd => {
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }
      }
    }

    val transform: DStream[(String, Int)] = createStream.transform {
      row => {
        row.flatMap {
          line => {
            line._2.split(" ").map((_, 1))
          }
        }
      }
    }
    val result = transform.reduceByKey(_ + _)

    /*val result: DStream[(String, Int)] = createStream.flatMap(line => {
      line._2.split(" ").map((_, 1))
    }).reduceByKey(_ + _)*/
    result.print()

    ssc.start();

    ssc.awaitTermination()
  }
}