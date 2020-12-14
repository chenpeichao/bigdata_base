package org.pcchen.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * @author ceek
  * @create 2020-12-14 16:34
  **/
object SparkStreaming07_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkStreaming07_Transform").setMaster("local")

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    val socketTextStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    val mapStream: DStream[(String, Int)] = socketTextStream.map((_, 1))


    /*socketTextStream.transform{
      case rdd => {
          rdd.map{
            case x => {
              x
            }
          }
      }
    }*/

    streamingContext.start();
    streamingContext.awaitTermination()
  }
}
