import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark-kafka-0.10低阶apidirect方式消费kafka中数据
  *
  * @author ceek
  * @create 2021-01-18 16:48
  **/
object SparkStreaming_Kafka_Direct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming_Kafka_Direct").setMaster("local[*]");

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //当有状态计算的算子时，需要添加此行
    ssc.checkpoint("e:\\checkpoint")

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9093",
      ConsumerConfig.GROUP_ID_CONFIG -> "group_sparkstreaming_direct10_test",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
    val topicSet = Set("topic_spark_direct_10");

    val dataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    );

    //有状态计算时：注意设置checkpoint
    val key: DStream[(String, Int)] = dataStream.map(_.value()).map((_, 1)).updateStateByKey {
      case (seq, buffer) => {
        if (null != seq && seq.size > 0) {
          var sum = seq.reduce(_ + _);
          sum = buffer.getOrElse(0) + sum
          Option(sum)
        } else {
          buffer
        }
      }
    }

    key.foreachRDD {
      rdd => {
        rdd.foreach(row => println(row))
      }
    }

    //数据处理
    ssc.start();
    ssc.awaitTermination();
  }
}