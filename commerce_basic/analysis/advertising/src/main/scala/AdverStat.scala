import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdverStat {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[*]")
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

      //AUTO_OFFSET_RESET_CONFIG取值范围为earliest、latest、none
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
    val adReadTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())



    /*dataStream.map(row => (row.topic(), row.value())).foreachRDD{
      row1 => row1.foreachPartition{
        row => {
          //用来打印当前数据的分区信息
          val offsetRange: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//          println("topic=>" + offsetRange.topic + "&" + offsetRange.partition)
          //foreachPartition中不能适用row.size， 因为iterator只能被执行一次
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
}