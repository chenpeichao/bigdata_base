import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-kafka-0.10
  *
  * @author ceek
  * @create 2021-01-12 15:58
  **/
object SparkStreaming_Local {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming_Local").setMaster("local[*]")

    val checkpointPath = "e:\\checkpoint";

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(sparkContext, Seconds(3))
    //    ssc.checkpoint(checkpointPath)

    val bootstrapServers = "10.10.32.60:9093,10.10.32.61:9093,10.10.32.62:9093";
    val groupId = "spark_local_kafka-test-group";
    val topicName = "topic_spark_direct_auto_offset";
    val maxPoll = 500
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      //此方式的offset会默认提交
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val kafkaTopicDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topicName), kafkaParams)
    )
    println("获取到数据了======================");
    val transform1: DStream[ConsumerRecord[String, String]] = kafkaTopicDS.transform {
      row => {
        row.map {
          item => {
            //            println("------------" + item.value())
            //不打印？---因其不是在driver端执行，所以打印看不到，如果在map碗面就可以或者是foreach算子就可以
            println("------------" + item.value() + "------------")
          }
        }
        row.foreach(x => {
          println("------------foreach-----" + x.value() + "------------")
        })
        row
      }
    }
    val transform2: DStream[ConsumerRecord[String, String]] = transform1.transform {
      row => {
        println(row.map(_.offset()).collect().mkString(","))
        println(row.map(_.value()).collect().mkString(","))
        println(row.map(_.key()).collect().mkString("="))
        row
      }
    }

    transform2.foreachRDD {
      row => {
        row.foreach(x => println("============" + x.value()))
      }
    }
    //    transform1.print()

    /*transform1.transform{
      row => {
        val map: RDD[String] = row.map(_.value())
        map.foreach(x => println("日志打印"  + x))
        row
      }
    }*/
    /*kafkaTopicDS.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
      .print()*/

    /*val transform: DStream[(String, Long)] = kafkaTopicDS.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
    transform.foreachRDD{
      row => {
//        row.foreach(x => println(x._2 + "->" + x._1))
        row.foreach(x => println(x._1 + "->" + x._2.toInt))
      }
    }*/

    ssc.start();
    ssc.awaitTermination();
  }
}
