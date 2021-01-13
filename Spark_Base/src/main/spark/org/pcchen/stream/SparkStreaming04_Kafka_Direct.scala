package org.pcchen.stream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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


    val groupId = "sparkstreaming_demo2";
    val topic = "spark_local_topic";
    val topics = Set[String](topic)
    var zookeeper = "10.10.32.60:2181,10.10.32.61:2181,10.10.32.62:2181";
    val kafkaParams = Map(
      "bootstrap.servers" -> "10.10.32.60:9093",
      "group.id" -> groupId,
      //      "auto.offset.reset" -> "smallest", //smallest   largest
      "auto.commit.enable" -> "false"
    )


    var offsetRanges = Array.empty[OffsetRange]
    var kafkaStream: InputDStream[(String, Array[Byte])] = null
    val zkClient = new ZkClient(zookeeper)
    val topicDirs = new ZKGroupTopicDirs(groupId, topic) //创建一个 ZKGroupTopicDirs 对象
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0) {
      //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }

      val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
      //      kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, StringDecoder,(String, Array[Byte])](streamingContext, kafkaParams, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](streamingContext, kafkaParams, topics)
    }


    val lines: DStream[Array[Byte]] = kafkaStream
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        print(offsetRanges.mkString("->"))

        rdd
      }.map(line => line._2)


    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
