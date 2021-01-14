package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * streaming-kafka-0.8 receive模式(streaming-kafka-0.10参考commerce_basic/analysis/advertising/streaming_kafka)
  * 0.8中receive版本：0.10版本参考commerce_basic项目的advertising/streaming_kafka下
  * offset保存在zookeeper中，会自动提交
  * 使用高阶api，KafkaUtils.createStream;
  * zookeeper来控制offset，会自动提交offset给zookeeper
  * 服务重启后会从上次提交的offset处接着读取数据
  *
  * @author ceek
  * @create 2021-01-14 10:57
  **/
object SparkStreaming04_Kafka_receive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming04_Kafka_receive");

    val ssc = new StreamingContext(sparkConf, Seconds(5));

    val groupId = "group_sparkstreaming_test3";
    //当添加多个消费者组时，会根据offset消费
    val topicMap = Map("topic_spark_receive8" -> 3);
    //此可以指定消费几个kafka的分区，数值小于等于topic的总分区数
    val zookeeperInfo = "10.10.32.60:2181";

    val dataStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zookeeperInfo, groupId, topicMap)

    val key2SumStream: DStream[(String, Int)] = dataStream.map(_._2).map((_, 1)).reduceByKey(_ + _);

    val transform: DStream[(String, Int)] = key2SumStream.transform {
      row => {
        println("没消费");
        row.foreach(x => println(x._2 + "->" + x._1))
        row
      }
    }

    transform.foreachRDD {
      rdd => {
        rdd.collect()
      }
    }

    key2SumStream.foreachRDD {
      rdd => {
        println("-执行-");
        rdd.collect().foreach(println)
      }
    }

    //    key2SumStream.transform(x => {x.collect(); x})

    ssc.start()
    ssc.awaitTermination();
  }
}
