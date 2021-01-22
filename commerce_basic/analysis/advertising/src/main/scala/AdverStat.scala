import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdverStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[*]")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sqlSession = SparkSession.builder().config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .enableHiveSupport()
      .getOrCreate();

    //StreamingContext.getActiveOrCreate(checkpoint, func)
    val streamingContext = new StreamingContext(sqlSession.sparkContext, Seconds(5))

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "10.10.32.60:9093",
      ConsumerConfig.GROUP_ID_CONFIG -> "AdverStat",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    val topicSet = Set(ConfigurationManager.config.getString(Constants.KAFKA_TOPICS))

    //    val directStream: InputDStream[ConsumerRecord[Nothing, Nothing]] = KafkaUtils.createDirectStream(
    //      streamingContext,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe(Set(""), kafkaParams));
    //
    //
  }
}
