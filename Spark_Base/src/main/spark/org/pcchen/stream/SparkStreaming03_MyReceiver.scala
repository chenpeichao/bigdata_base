package org.pcchen.stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * 生命采集器
  * 继承Receiver
  *
  * @author ceek
  * @create 2020-12-10 16:39
  **/
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_MyReceiver")

    var streamingContext = new StreamingContext(sparkConf, Seconds(2));

    val receiverStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("localhost", 9999));
    receiverStream.print();

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
      if ("END".equals(line)) {
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