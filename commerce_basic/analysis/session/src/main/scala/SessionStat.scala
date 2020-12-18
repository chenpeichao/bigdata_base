import commons.model.UserVisitAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  *
  * @author ceek
  * @create 2020-12-18 15:12
  **/
object SessionStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SessionStat").setMaster("local[*]")
      .set("spark.ui.port", "8999") //用于设置spark的ui端口，避免easyvpn端口4040冲突

    val sparkSession = SparkSession.builder().config(sparkConf)
      .config("hive.metastore.uris", "thrift://10.10.32.60:9083")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://10.10.32.60:9000/user/hive/warehouse/commerce.db")
      .enableHiveSupport().getOrCreate();

    import sparkSession.implicits._

    val dataset = sparkSession.sql("select * from commerce.user_visit_action")

    dataset.take(10).foreach(println)
  }
}