package org.pcchen

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
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

    //1、使用redis去重
    //1.1、数据格式变换
    val startUpLogStream: DStream[StartUpLog] = dataDStream.map {
      record => {
        val jsonString: String = record.value();
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])

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

    //1.2、数据经过redis去重---不能直接使用filter，因executor中需要使用大数据对象jedis中的集合数据进行判断去重
    val avaliableStartUpLogStream: DStream[StartUpLog] = startUpLogStream.transform {
      rdd => {
        val jedis: Jedis = RedisUtil.getJedisClient;
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        val itemSet: util.Set[String] = jedis.smembers("dap_" + dateStr)

        //大对象经过广播传递给executor
        val bdItemSet: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(itemSet)

        rdd.filter {
          startUpLog => {
            val setValue: util.Set[String] = bdItemSet.value;
            !setValue.contains(startUpLog.mid)
          }
        }
      }
    }

    //2、一个批次中有多个重复用户并且之前没有在redis中存在，进行当前批次中只取一个操作
    val distinctStartUpLogStream: DStream[StartUpLog] = avaliableStartUpLogStream.map(row => (row.mid, row)).groupByKey().flatMap {
      case (mid, iter: Iterable[StartUpLog]) => {
        iter.take(1)
      }
    }

    //3、去重后数据进行redis保存以及es保存
    distinctStartUpLogStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          part => {
            //TODO:pcchen 下面两行写在part上有什么区别
            val jedis: Jedis = RedisUtil.getJedisClient;
            val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            val itemStartUpLog: List[StartUpLog] = part.toList;

            for (item <- itemStartUpLog) {
              jedis.sadd("day_" + dateStr, item.mid)
            }

            jedis.close()
          }
        }
      }
    }

    ssc.start();
    ssc.awaitTermination();
  }
}
