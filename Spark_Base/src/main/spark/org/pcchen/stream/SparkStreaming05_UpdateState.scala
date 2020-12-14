package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author ceek
  * @create 2020-12-11 16:10
  **/
object SparkStreaming05_UpdateState {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming04_Kafka");

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3));

    streamingContext.checkpoint("Spark_Base/cp");

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "10.10.32.60:2181",
      "sparkstreaming_groupid",
      Map("sparkstreaming_test" -> 3)
    )
    val kvDStream: DStream[(String, Int)] = kafkaDStream.flatMap(x => x._2.split(" ")).map((_, 1));

    val key: DStream[(String, Int)] = kvDStream.updateStateByKey {
      //a就是获取到的消息数据Seq集合，b标识缓存
      case (seq, buffer) => {
        if (null != seq && seq.size > 0) {
          var sum = seq.reduce(_ + _) // + b.getOrElse(5)
          sum = buffer.getOrElse(0) + sum;
          Option(sum)
        } else {
          buffer
        }
      }
    }
    key.print();

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}