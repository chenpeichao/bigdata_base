import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model._
import commons.utils.DateUtils
import commons.utils.DateUtils.DATE_FORMAT
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime
import utils._

import scala.collection.mutable.ArrayBuffer

object AdverStat {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[3]")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sparkSession = SparkSession.builder().config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .enableHiveSupport()
      .getOrCreate();

    //StreamingContext.getActiveOrCreate(checkpoint, func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

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
    //    streamingContext.checkpoint("e:\\advert_checkpoint")
    //    adRealTimeFilterDStream.checkpoint(Duration(10000))

    //    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilterDStream)

    //需求9、统计各省Top3热门广告
    //    proveinceTope3Adver(sparkSession, key2ProvinceCityCountDStream)

    //需求10、最近一小时广告点击量
    getRecentHourClickCount(adRealTimeFilterDStream);

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
    * 最近一小时的广告点击量
    *
    * @param adRealTimeFilterDStream
    */
  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]): Unit = {
    //adRealTimeFilterDStream为 timestamp + " " + province + " " + city + " " + userid + " " + adid
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      line => {
        val logSplit: Array[String] = line.split(" ")

        val timestamp: Long = logSplit(0).toLong
        //        val province: String = logSplit(1)
        //        val city: String = logSplit(2)
        val adid: String = logSplit(4)
        //yyyyMM
        val timeStr = DateUtils.formatTimeMinute(new Date(timestamp));
        val key = adid + "_" + timeStr
        (key, 1l)
      }
    }

    //每分钟会执行一次，求出时间窗口长度的数据
    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((value1: Long, value2: Long) => {
      value1 + value2
    }, Minutes(60), Minutes(1))

    println(DateTime.now().toString(DateUtils.TIME_FORMAT))
    key2WindowDStream.print()
    key2WindowDStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          part => {
            val trendArray = new ArrayBuffer[AdClickTrend]()
            for (item <- part) {
              //adid + "_" + timeStr
              val keySplit: Array[String] = item._1.split("_")
              val adid = keySplit(0).toLong;
              val timeMinute = keySplit(1);

              val date = timeMinute.substring(0, 8)
              val hour = timeMinute.substring(8, 10)
              val minute = timeMinute.substring(10)

              trendArray += AdClickTrend(date, hour, minute, adid, item._2.toLong)
            }

            //数据保存
            AdClickTrendDAO.updateBatch(trendArray.toArray)
          }
        }
      }
    }
  }

  /**
    * 统计各省Top3热门广告---使用生成临时表并且进行开窗函数
    *
    * @param sparkSession
    * @param key2ProvinceCityCountDStream
    */
  def proveinceTope3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    //key:  province + "_" + city + "_" + adid + "_" + timeStr
    //value:  count
    val valCountLIst: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      row => {
        val line: String = row._1.toString

        val splitStr: Array[String] = line.split("_");
        val province = splitStr(0);
        val adid = splitStr(2);
        val timeStr = splitStr(3);

        (province + "_" + adid + "_" + timeStr, row._2)
      }
    }.reduceByKey(_ + _)
    //    valCountLIst.print()
    val transform: DStream[Row] = valCountLIst.transform {
      row => {
        val rdd: RDD[(String, String, Long, Long)] = row.map {
          line => {
            //province + "_" + adid + "_" + timeStr
            val splitStr = line._1.split("_");
            val province = splitStr(0);
            val adid = splitStr(1).toLong;
            val timeStr = splitStr(2);

            (timeStr, province, adid, line._2)
          }
        }

        import sparkSession.implicits._;
        val df: DataFrame = rdd.toDF("date", "province", "adid", "count")
        df.createOrReplaceTempView("tmp_basic_info")


        val sql = "select date, province, adid, count from " +
          "( select date, province, adid, count, " +
          "row_number() over(partition by date, province order by count desc) rank from tmp_basic_info " +
          ") t where rank <= 3";
        sparkSession.sql(sql).rdd
      }
    }

    transform.foreachRDD {
      row => {
        row.foreachPartition {
          items => {
            val top3Array = new ArrayBuffer[AdProvinceTop3]()

            for (item <- items) {
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }

            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
          }
        }
      }
    }
  }

  /**
    * 获取省市每天的累计点击量
    *
    * @param adRealTimeFilterDStream
    */
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]): DStream[(String, Long)] = {
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

    provinceAndCity2CountDStream
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