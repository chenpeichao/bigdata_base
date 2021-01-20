package org.pcchen.stream

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder}
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将offset信息保存在zk中，并从其中获取
  *
  * @author ceek
  * @create 2021-01-08 10:35
  **/
object SparkStreaming04_Kafka_Direct_ZK_offset {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming04_Kafka_Direct").setMaster("local[*]")

    val streamingContext = new StreamingContext(sparkConf, Seconds(3))


    val groupId = "group_topic_spark_direct_zk_offset";
    val topic = "topic_spark_direct_auto_offset";
    val topics = Set[String](topic)
    var zookeeper = "10.10.32.60:2181,10.10.32.61:2181,10.10.32.62:2181";
    val kafkaParams = Map(
      "bootstrap.servers" -> "10.10.32.60:9093",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest", //smallest   largest
      "auto.commit.enable" -> "false" //不生效
    )

    var offsetRanges = Array.empty[OffsetRange]
    var kafkaStream: InputDStream[(String, String)] = null
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

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](streamingContext, kafkaParams, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    }

    //进行数据处理的操作(省略)
    kafkaStream.map(line => line._2).foreachRDD {
      rdd => {
        rdd.collect().foreach(x => println("输出字符串为：" + x))
      }
    }

    //进行offset的更新
    kafkaStream.foreachRDD {
      rdd => {
        //offsetRanges打印信息为：OffsetRange(topic: 'topic_spark_direct_auto_offset', partition: 0, range: [17 -> 17])->...
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          println(zkPath + "=>" + o.untilOffset);
          //TODO:pcchen 保存到zk中的offset乱码，不影响使用-呲牙
          ZkUtils.updatePersistentPath(zkClient, zkPath, new java.lang.String(o.untilOffset.toString.getBytes("utf-8"), "utf-8"))
        }
      }
    }

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}
