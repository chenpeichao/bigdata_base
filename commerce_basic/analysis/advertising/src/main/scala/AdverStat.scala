import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{AdBlacklist, AdStat, AdUserClickCount}
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.{AdBlacklistDAO, AdStatDAO, AdUserClickCountDAO}

import scala.collection.mutable.ArrayBuffer

object AdverStat {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[3]")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sqlSession = SparkSession.builder().config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .enableHiveSupport()
      .getOrCreate();

    //StreamingContext.getActiveOrCreate(checkpoint, func)
    val streamingContext = new StreamingContext(sqlSession.sparkContext, Seconds(5))

    val topicSet = Set(ConfigurationManager.config.getString(Constants.KAFKA_TOPICS))
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9093",
      ConsumerConfig.GROUP_ID_CONFIG -> "AdverStat",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

      //AUTO_OFFSET_RESET_CONFIG取值范围为earlie st、latest、none
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    //获取kafka对接sparkstreaming的初始数据流
    val dataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamingContext,
      //LocationStrategy中三种模式：
      //PreferConsistent：一致性的方式分配分区所有 executor 上(常用)
      //PreferBrokers：当kafka的broker和Executor在相同机器上使用此
      //PreferFixed：固定的(当负载不均衡时使用，未添加进去的，使用默认的)
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams));

    var offsetRanges = Array.empty[OffsetRange];
    //遍历封装kafka的partition和topic信息后，将初始数据流返回dataStream
    val adRealTimeDStream = dataStream.transform {
      row => {
        offsetRanges = row.asInstanceOf[HasOffsetRanges].offsetRanges
        //          println(row + "-------")
        //          row.foreach(x => println(x.value()));
        row
      }
    }

    //adRealTimeDStream:DStream[RDD,RDD,RDD....]=>RDD[message] => message[(key, value)]
    // timestamp + " " + province + " " + city + " " + userid + " " + adid
    //1、首先获取到kafka消息中的message信息
    val adReadTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    //2、过滤掉黑名单中已经包含的访问日志
    val adRealTimeFilterDStream: DStream[String] = adReadTimeValueDStream.transform {
      rdd => {
        //查询黑名单中所有数据
        val allBlackList: Array[AdBlacklist] = AdBlacklistDAO.findAll();
        val userIdArrayBlackList: Array[Long] = allBlackList.map(adBlackList => adBlackList.userid)

        rdd.filter {
          row => {
            val splitMessage: Array[String] = row.split(" ")
            !userIdArrayBlackList.contains(splitMessage(3).toLong)
          }
        }
      }
    }

    //需求7：维护黑名单
    //    generateBlackList(adRealTimeFilterDStream)

    //需求8：各省各城市一天中的广告点击量（累积统计 采用updateStateByKey）
    //要是用updateStateByKey需要进行checkpoint
    streamingContext.checkpoint("e:\\advert_checkpoint")
    adRealTimeFilterDStream.checkpoint(Duration(10000))

    provinceCityClickStat(adRealTimeFilterDStream)


    /*dataStream.map(row => (row.topic(), row.value())).foreachRDD{
      row1 => row1.foreachPartition{
        row => {
          //用来打印当前数据的分区信息
          val offsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//          println("topic=>" + offsetRange.topic + "&" + offsetRange.partition)
          //TODO:pcchen foreachPartition中不能适用row.size， 因为iterator只能被执行一次
//          println("row.size===========" + row.size)
          row.foreach{
            line => {
              println(offsetRange.topic + "&" + offsetRange.partition + "value=" + line);
              println(line);
            }
          }
        }
      }
    }*/

    streamingContext.start();
    streamingContext.awaitTermination();
  }

  /**
    * 获取省市每天的累计点击量
    *
    * @param adRealTimeFilterDStream
    */
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    //adRealTimeFilterDStream为 timestamp + " " + province + " " + city + " " + userid + " " + adid
    val provinceCity2OneDStream = adRealTimeFilterDStream.map {
      row => {
        val logSplit: Array[String] = row.split(" ")

        val timestamp: Long = logSplit(0).toLong
        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adid: String = logSplit(4)
        //yyyyMM
        val timeStr = DateUtils.formatDateKey(new Date(timestamp));
        val key = province + "_" + city + "_" + adid + "_" + timeStr

        (key, 1l)
      }
    }
    val provinceAndCity2CountDStream: DStream[(String, Long)] = provinceCity2OneDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) => {
        var sum = values.sum;
        if (state.isDefined) {
          sum = sum + state.getOrElse(0l)
        }
        Option(sum)
      }
    }
    provinceAndCity2CountDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          val adStatArray = new ArrayBuffer[AdStat]()
          items => {
            //key:  province + "_" + city + "_" + adid + "_" + timeStr
            //value:  count
            for (item <- items) {
              val logSplit: Array[String] = item._1.split("_")
              val date = logSplit(3);
              val province = logSplit(0);
              val city = logSplit(1);
              val adid = logSplit(2).toLong;

              adStatArray += AdStat(date, province, city, adid, item._2);
            }
            //保存省市每天点击数据
            AdStatDAO.updateBatch(adStatArray.toArray)
          }
        }
      }
    }

  }

  /**
    * 实时维护黑名单信息
    *
    * @param adRealTimeFilterDStream
    */
  def generateBlackList(adRealTimeFilterDStream: DStream[String]): Unit = {
    //1、将访问日志进行(key，1)模式累加
    //timestamp + " " + province + " " + city + " " + userid + " " + adid
    val key2CountDStream: DStream[(String, Int)] = adRealTimeFilterDStream.map {
      lines => {
        val splitLine: Array[String] = lines.split(" ")
        val timeStamp = splitLine(0).toLong
        // yy-mm-dd
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = splitLine(3)
        val addId = splitLine(4)
        (userId + "_" + dateKey + "_" + addId, 1)
      }
    }.reduceByKey(_ + _)

    //2、更新访问日志到表中
    key2CountDStream.foreachRDD{
      row => {
        row.foreachPartition {
          part => {
            part.foreach {
              line => {
                val userId = line._1.split("_")(0).toLong;
                val date = line._1.split("_")(1);
                val addid = line._1.split("_")(2);
                val count = line._2.toLong;

                val adUserClickCount = Array(AdUserClickCount(date, userId, addid.toLong, count))

                AdUserClickCountDAO.updateBatch(adUserClickCount);
              }
            }
          }
        }
      }
    }
    //过滤得到点击数超标的记录，用于更新黑名单表
    val key2BlackListDStream: DStream[(String, Int)] = key2CountDStream.filter {
      row => {
        val userId: Long = row._1.split("_")(0).toLong;
        val date: String = row._1.split("_")(1);
        val addId: Long = row._1.split("_")(2).toLong;

        val count: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, addId);

        if (count > 20) {
          true
        } else {
          false
        }
      }
    }
    //需要去重
    val userIdDStream: DStream[String] = key2BlackListDStream.map {
      row => {
        row._1.split("_")(0)
      }
      //此为全局去重
    }.transform(r => r.distinct())

    //添加黑名单表数据
    userIdDStream.foreachRDD{
      rdd => {
        rdd.foreachPartition{
          part => {
            val blacklists = new ArrayBuffer[AdBlacklist]()
            part.foreach {
              userId => {
                blacklists += AdBlacklist(userId.toLong)
              }
            }
            println(blacklists.mkString(","))
            AdBlacklistDAO.insertBatch(blacklists.toArray)
          }
        }
      }
    }

    /*filterUserKey.transform{
      row => {
        row.distinct()
      }
    }.foreachRDD{
      rdd => {
        rdd.foreachPartition {
          var blackUserIdArray = Array.empty[AdBlacklist];
          part => {
            part.foreach {
              record => {
                if(record._2 > 100) {
                  val userId: Long = record._1.split("_")(0).toLong
                  blackUserIdArray :+ AdBlacklist(userId);
                }
              }
            }
          }
            println("===" + blackUserIdArray.mkString(","));
            AdBlacklistDAO.insertBatch(blackUserIdArray)
        }
      }
    }*/
  }
}