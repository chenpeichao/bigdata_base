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

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    import sparkSession.implicits._

    val dataset: Dataset[UserVisitAction] = sparkSession.sql("select * from user_visit_action ").as[UserVisitAction]

    dataset.take(10).foreach(println)
  }
}
