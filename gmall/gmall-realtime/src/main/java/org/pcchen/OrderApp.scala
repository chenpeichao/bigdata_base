package org.pcchen

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.pcchen.bean.OrderInfo
import org.pcchen.constants.GmallConstant
import org.pcchen.utils.{MyESUtils, MyKafkaUtil}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[3]").setAppName("OrderApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5));

    val dataDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    val orderInfoDataStream: DStream[OrderInfo] = dataDStream.map {
      rdd => {
        val jsonOrderInfo: String = rdd.value();
        val orderInfo: OrderInfo = JSON.parseObject(jsonOrderInfo, classOf[OrderInfo])

        val telSplit: (String, String) = orderInfo.consigneeTel.splitAt(4)
        orderInfo.consigneeTel = telSplit._1 + "*******"

        val datetimeArr: Array[String] = orderInfo.createTime.split(" ")
        orderInfo.createDate = datetimeArr(0)
        val timeArr: Array[String] = datetimeArr(1).split(":")
        orderInfo.createHour = timeArr(0)
        orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)
        orderInfo
      }
    }
    //数据中手机号进行脱敏，时间进行小时区分
    orderInfoDataStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          orderItr => {
            val orderInfoList: List[OrderInfo] = orderItr.toList
            MyESUtils.saveBulkData2ES(GmallConstant.ES_INDEX_ORDER, orderInfoList);
          }
        }
      }
    }

    ssc.start();
    ssc.awaitTermination();
  }
}
