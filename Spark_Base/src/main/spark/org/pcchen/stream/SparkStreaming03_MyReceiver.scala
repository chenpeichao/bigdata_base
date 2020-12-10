package org.pcchen.stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * 声明采集器
  * 继承Receiver
  *
  * @author ceek
  * @create 2020-12-10 16:39
  **/
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming03_MyReceiver")
    //实时数据分析环境对象
    //采集周期，以指定的时间为周期采集实时数据
    var streamingContext = new StreamingContext(sparkConf, Seconds(2));

    //从指定数据内容中采集数据---通过自定义receiver实现
    val receiverStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("localhost", 9999));
    receiverStream.print();
    println("=======================");

    streamingContext.start();
    streamingContext.awaitTermination();
  }
}


class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket: Socket = _;

  def receive(): Unit = {
    socket = new Socket(host, port)

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream));

    var line: String = null;
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && line.contains("END")) {
        this.store(line)
        return
      } else {
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start();
  }

  override def onStop(): Unit = {
    if (null != socket) {
      socket.close()
    }
  }
}