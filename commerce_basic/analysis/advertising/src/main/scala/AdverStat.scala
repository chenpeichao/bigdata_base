import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdverStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AdverStat").setMaster("local[*]")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    //StreamingContext.getActiveOrCreate(checkpoint, func)
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map(
      "" -> ""
    )

    //    val directStream: InputDStream[ConsumerRecord[Nothing, Nothing]] = KafkaUtils.createDirectStream(
    //      streamingContext,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe(Set(""), kafkaParams));
    //
    //
  }
}
