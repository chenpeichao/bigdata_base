package org.pcchen.stream

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 虽然设置了检查点但是不生效，ssc的需通过getActiveOrCreate方法从checkpoint中获取
  *
  * @author ceek
  * @create 2021-01-08 10:35
  **/
object SparkStreaming04_Kafka_Direct_checkpoint {
  def main(args: Array[String]): Unit = {
    // 创建配置文件的对象
    val conf: SparkConf = new SparkConf().setAppName("Test05_DirectAPI_Auto01").setMaster("local[*]")

    // 创建Spark Streaming的上下文环境对象
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

    // 设置检查点目录
    ssc.checkpoint("E:\\checkPoint")

    // 准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "10.10.32.60:9093",
      "group.id" -> "group_topic_spark_direct_auto_offset",
      "auto.offset.reset" -> "smallest"
    )

    // 创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("topic_spark_direct_auto_offset")
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] // 定义一个集合，用来存放偏移量的范围
    kafkaDStream.transform {
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

    // 获取Kafka中的消息，我们只需要v的部分：消息体
    val lineDS: DStream[String] = kafkaDStream.map(_._2)

    // 扁平化
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    // 结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))

    // 聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)

    // 打印输出
    reduceDS.print()

    // 开启任务
    ssc.start()

    // 等待
    ssc.awaitTermination()
  }

  /*def main(args: Array[String]): Unit = {
    // 创建Streaming上下文的对象
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("E:\\checkPoint", () => getStreamingContext)

    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(): StreamingContext = {
    // 创建配置文件的对象
    val conf: SparkConf = new SparkConf().setAppName("Test05_DirectAPI_Auto02").setMaster("local[*]")

    // 创建Spark Streaming上下文环境对象
    val ssc = new StreamingContext(conf, Seconds(3))

    // 设置检查点目录
    ssc.checkpoint("E:\\checkPoint")

    // 准备Kafka参数
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> "10.10.32.60:9093",
      //      "group.id" -> "sparkstreaming_demo2",
      "group.id" -> "group_topic_spark_direct_auto_offset",
      "auto.offset.reset" -> "smallest"
    )

    // 创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("topic_spark_direct_auto_offset")
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange] // 定义一个集合，用来存放偏移量的范围
    kafkaDStream.transform {
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

    // 获取Kafka中的消息，我们只需要v的部分
    val lineDS: DStream[String] = kafkaDStream.map(_._2)

    //扁平化
    val flatMapDS: DStream[String] = lineDS.flatMap(_.split(" "))

    //结构转换  进行计数
    val mapDS: DStream[(String, Int)] = flatMapDS.map((_, 1))

    //聚合
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)

    //打印输出
    reduceDS.print

    ssc
  }*/
}
