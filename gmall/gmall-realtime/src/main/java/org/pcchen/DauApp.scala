package org.pcchen

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.pcchen.bean.StartUpLog
import org.pcchen.constants.GmallConstant
import org.pcchen.utils.{MyKafkaUtil, RedisUtil}
import redis.clients.jedis.Jedis

/**
  *
  *
  * @author ceek
  * @create 2021-03-02 15:16
  **/
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5));

    val dataDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //1、保存数据到redis去重
    val startUpLogStream: DStream[StartUpLog] = dataDStream.map {
      record => {
        val jsonString: String = record.value();
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, Class[StartUpLog])

        val ts: Long = startUpLog.ts;
        val date: Date = new Date(ts);
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr = dateStr.split(" ");
        startUpLog.logDate = dateArr(0)
        startUpLog.logHour = dateArr(1).split(":")(0)
        startUpLog.logHourMinute = dateArr(1)

        startUpLog
      }
    }

    //1.1、此处不能直接使用filter进行数据过滤，因其要使用redis进行key值去重，所以需要连接es，而filter是运行在executor中的，所以使用transform进行转换
    startUpLogStream.transform {
      row => {
        val jedis: Jedis = RedisUtil.getJedisClient
        row.filter {
          item => {
            val key = "dau_" + item.logDate;
            if (jedis.exists(key)) {
              false
            } else {
              //当key不存在redis中时，进行es的保存
              true
            }
          }
        }
      }
    }

    //保存数据到es
  }
}
